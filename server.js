// server.js — ESM (в package.json: { "type": "module" })
import express from 'express';
import fs from 'fs';
import { promises as fsp } from 'fs';
import path from 'path';
import crypto from 'crypto';
import process from 'process';
import { fileURLToPath } from 'url';

// __dirname в ESM
const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

// ===== ENV / CONFIG =====
const PORT              = Number(process.env.PORT || 3000);
const TASK_KEY          = process.env.TASK_KEY || 'kK9f4JQ7uX2pL0aN';
const LOG_DIR           = process.env.LOG_DIR  || '/mnt/data/logs';
const TASK_DIR          = process.env.TASK_DIR || '/mnt/data/tasks';
const DEFAULT_REPLY     = process.env.DEFAULT_REPLY || 'Здравствуйте!';
const ONLY_FIRST_SYSTEM = String(process.env.ONLY_FIRST_SYSTEM || 'true').toLowerCase() === 'true';
const WEBHOOK_SECRET    = process.env.WEBHOOK_SECRET || '';
const LOG_TAIL_BYTES    = Number(process.env.LOG_TAIL_BYTES || 512 * 1024); // 512KB из хвоста

// ===== helpers =====
async function ensureDir(dir) { try { await fsp.mkdir(dir, { recursive: true }); } catch {} }
function nowIso() { return new Date().toISOString(); }
function genId() { return crypto.randomBytes(16).toString('hex'); }

function todayLogName() {
  const d = new Date();
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(d.getUTCDate()).padStart(2, '0');
  return `logs.${y}${m}${dd}.log`;
}

async function appendLog(text) {
  console.log(text);
  await ensureDir(LOG_DIR);
  const file = path.join(LOG_DIR, todayLogName());
  await fsp.appendFile(file, text + '\n', 'utf8');
  return file;
}

function ok(res, extra = {}) { return res.send({ ok: true, ...extra }); }
function bad(res, code, msg) { return res.status(code).send({ ok: false, error: msg }); }

// ===== FILE QUEUE =====
// структура задачи: { id, account, chat_id, reply_text, [author_id], [message_id], created_at }
async function createTask({ account, chat_id, reply_text, message_id, author_id }) {
  await ensureDir(TASK_DIR);
  const id  = genId();
  const acc = (account || 'hr-main').replace(/[^a-zA-Z0-9_-]/g, '_');

  const task = {
    id,
    account: acc,
    chat_id,
    reply_text: reply_text || DEFAULT_REPLY,
    message_id: message_id || null,
    author_id: author_id || null, // если знаешь ID своего отправителя — можно проставить
    created_at: nowIso(),
  };

  const file = path.join(TASK_DIR, `${acc}__${id}.json`);
  await fsp.writeFile(file, JSON.stringify(task, null, 2), 'utf8');
  return task;
}

// Claim: берём до 3 свежих по mtime (опц. фильтр по account)
async function claimTask(account) {
  await ensureDir(TASK_DIR);
  let files = (await fsp.readdir(TASK_DIR)).filter(f => f.endsWith('.json'));

  files.sort((a, b) => {
    const ta = fs.statSync(path.join(TASK_DIR, a)).mtimeMs;
    const tb = fs.statSync(path.join(TASK_DIR, b)).mtimeMs;
    return tb - ta;
  });

  if (account) {
    const pref = `${account}__`;
    files = files.filter(f => f.startsWith(pref));
  }

  files = files.slice(0, 3);

  for (const f of files) {
    const full   = path.join(TASK_DIR, f);
    const taking = full.replace(/\.json$/, '.json.taking');
    try {
      await fsp.rename(full, taking); // атомарная блокировка
      const raw = JSON.parse(await fsp.readFile(taking, 'utf8'));
      const lockId = path.basename(taking);
      return { task: raw, lockId };
    } catch {
      // могли перехватить параллельно
    }
  }
  return null;
}

async function doneTask(lockId) {
  const file = path.join(TASK_DIR, lockId);
  try { await fsp.unlink(file); } catch {}
  return true;
}

async function requeueTask(lockId) {
  const from = path.join(TASK_DIR, lockId);
  const to   = from.replace(/\.json\.taking$/, '.json');
  try { await fsp.rename(from, to); } catch {}
  return true;
}

// ===== LOG HELPERS =====
async function getTwoLatestLogFiles() {
  await ensureDir(LOG_DIR);
  const files = (await fsp.readdir(LOG_DIR))
    .filter(f => f.endsWith('.log'))
    .map(f => ({ f, t: fs.statSync(path.join(LOG_DIR, f)).mtimeMs }))
    .sort((a, b) => b.t - a.t)
    .slice(0, 2)
    .map(x => path.join(LOG_DIR, x.f));
  return files;
}

async function readLogTail(file, maxBytes = LOG_TAIL_BYTES) {
  let buf = await fsp.readFile(file, 'utf8');
  if (buf.length > maxBytes) buf = buf.slice(buf.length - maxBytes);
  return buf;
}

function escapeReg(s) { return String(s).replace(/[.*+?^${}()|[\]\\]/g, '\\$&'); }

// Построчная эвристика (на случай, если лог — одна JSON-строка)
function isLogMatch(line, chatId, authorId) {
  if (line && (line[0] === '{' || line[0] === '[')) {
    try {
      const obj = JSON.parse(line);
      if (obj && typeof obj === 'object') {
        const lChat = obj.chat_id ?? obj.chatId ?? obj.chat ?? null;
        const lAuth = obj.author_id ?? obj.authorId ?? obj.author ?? null;
        if (lChat != null && lAuth != null) {
          return String(lChat) === String(chatId) && String(lAuth) === String(authorId);
        }
      }
    } catch { /* не JSON */ }
  }
  if (line.includes(`chatId='${chatId}'`) && line.includes(`authorId='${authorId}'`)) return true;
  if (line.includes(`"chat_id": "${chatId}"`) && (line.includes(`"author_id": ${authorId}`) || line.includes(`"author_id": "${authorId}"`))) return true;

  const rx = new RegExp(`chat[_ ]?id\\s*[:=]\\s*'?${escapeReg(chatId)}'?.*author[_ ]?id\\s*[:=]\\s*'?${escapeReg(authorId)}'?`, 'i');
  return rx.test(line);
}

// ✅ ИЩЕМ ВО ВСЁМ ХВОСТЕ: chat_id + author_id могут быть в разных строках одного блока
function tailContainsPair(tail, chatId, authorId) {
  const chatNeedle = `"chat_id": "${chatId}"`;
  const authNeedle1 = `"author_id": ${authorId}`;      // author числом
  const authNeedle2 = `"author_id": "${authorId}"`;    // author строкой
  return tail.includes(chatNeedle) && (tail.includes(authNeedle1) || tail.includes(authNeedle2));
}

// ✅ Главная функция: смотрим ДВА последних лог-файла
async function hasConfirmationInTwoLogs(chatId, authorId) {
  const files = await getTwoLatestLogFiles();
  if (files.length === 0) return false;

  for (const f of files) {
    const tail = await readLogTail(f);

    // A) быстрый поиск по всему хвосту (многострочный JSON-блок)
    if (tailContainsPair(tail, chatId, authorId)) return true;

    // B) запасной вариант — построчная проверка (когда лог — «JSON-в-строку»)
    const lines = tail.split('\n');
    if (lines.some(line => isLogMatch(line, chatId, authorId))) return true;

    // C) узкое «окно» вокруг chat_id — если author_id рядом
    const idx = tail.indexOf(`"chat_id": "${chatId}"`);
    if (idx >= 0) {
      const win = tail.slice(Math.max(0, idx - 2000), Math.min(tail.length, idx + 2000));
      if (win.includes(`"author_id": ${authorId}`) || win.includes(`"author_id": "${authorId}"`)) return true;
    }
  }
  return false;
}

// ===== APP =====
const app = express();
app.use(express.json({ limit: '1mb' }));

// health
app.get('/', (req, res) => ok(res, { up: true }));

// debug: список файлов в TASK_DIR
app.get('/tasks/debug', async (req, res) => {
  try {
    await ensureDir(TASK_DIR);
    const files = (await fsp.readdir(TASK_DIR)).sort();
    res.send({ ok: true, files });
  } catch (e) {
    res.status(500).send({ ok: false, error: String(e) });
  }
});

// ручная постановка задачи (для тестов)
app.post('/tasks/enqueue', async (req, res) => {
  try {
    const { account, chat_id, reply_text, message_id, author_id } = req.body || {};
    if (!chat_id) return bad(res, 400, 'chat_id required');
    const t = await createTask({ account, chat_id, reply_text, message_id, author_id });
    res.send({ ok: true, task: t });
  } catch (e) {
    res.status(500).send({ ok: false, error: String(e) });
  }
});

// ===== ВЕБХУК АВИТО =====
const seenSystemToday = new Set(); // ключ: `${account}:${chatId}`

app.post('/webhook/:account', async (req, res) => {
  const account = req.params.account || 'hr-main';

  if (WEBHOOK_SECRET) {
    const headerSecret = req.headers['x-avito-secret'];
    const bodySecret   = req.body && req.body.secret;
    if (String(headerSecret || bodySecret || '') !== String(WEBHOOK_SECRET)) {
      return bad(res, 403, 'forbidden');
    }
  }

  const pretty = JSON.stringify(req.body || {}, null, 2);
  const header = `=== RAW AVITO WEBHOOK (${account}) @ ${nowIso()} ===\n`;
  const footer = `\n=========================\n`;
  await appendLog(header + pretty + footer);

  try {
    const payload = req.body?.payload || {};
    const val     = payload?.value || {};
    const isSystem = val?.type === 'system';
    const txt     = String(val?.content?.text || '');
    const chatId  = val?.chat_id;
    const msgId   = val?.id;

    const looksLikeCandidate = /кандидат|отклик/i.test(txt);

    if (isSystem && looksLikeCandidate && chatId) {
      let allowed = true;

      if (ONLY_FIRST_SYSTEM) {
        const key = `${account}:${chatId}`;
        if (seenSystemToday.has(key)) allowed = false;
        else seenSystemToday.add(key);
      }

      if (allowed) {
        await createTask({
          account,
          chat_id: chatId,
          reply_text: DEFAULT_REPLY,
          message_id: msgId
          // author_id можно проставить заранее, если знаешь ID отправителя
        });
      }
    }
  } catch { /* игнорируем, вебхуку отвечаем 200 */ }

  return ok(res);
});

// ===== /logs/has — ищем подтверждение в двух последних логах =====
app.get('/logs/has', async (req, res) => {
  const chat   = String(req.query.chat   || '').trim();
  const author = String(req.query.author || '').trim();
  if (!chat || !author) return bad(res, 400, 'chat & author required');
  try {
    const exists = await hasConfirmationInTwoLogs(chat, author);
    return ok(res, { exists });
  } catch (e) {
    return res.status(500).send({ ok: false, error: String(e) });
  }
});

// ===== Просмотр логов =====
app.get('/logs', async (req, res) => {
  try {
    await ensureDir(LOG_DIR);
    const files = (await fsp.readdir(LOG_DIR))
      .filter(f => f.endsWith('.log'))
      .map(f => ({ name: f, mtime: fs.statSync(path.join(LOG_DIR, f)).mtimeMs }))
      .sort((a, b) => b.mtime - a.mtime);
    res.send({ ok: true, files });
  } catch (e) {
    res.status(500).send({ ok: false, error: String(e) });
  }
});

app.get('/logs/read', async (req, res) => {
  try {
    const file = String(req.query.file || '').trim();
    if (!file || !/^[\w.\-]+$/.test(file)) return bad(res, 400, 'bad file');
    const full = path.join(LOG_DIR, file);
    if (!fs.existsSync(full)) return bad(res, 404, 'not found');

    const tail = Number(req.query.tail || LOG_TAIL_BYTES);
    let buf = await fsp.readFile(full, 'utf8');
    if (buf.length > tail) buf = buf.slice(buf.length - tail);
    res.type('text/plain').send(buf);
  } catch (e) {
    res.status(500).send({ ok: false, error: String(e) });
  }
});

// ===== AUTH helper =====
function checkKey(req, res) {
  const key = String(req.query.key || req.body?.key || '').trim();
  if (!TASK_KEY || key !== TASK_KEY) { bad(res, 403, 'bad key'); return false; }
  return true;
}

// ===== задачи: claim / done / requeue / doneSafe =====
app.all('/tasks/claim', async (req, res) => {
  if (!checkKey(req, res)) return;
  const account = String(req.query.account || req.body?.account || '').trim();
  const got = await claimTask(account);
  if (!got) return ok(res, { has: false });

  const { task, lockId } = got;
  return ok(res, {
    has: true,
    lockId,
    ChatId: task.chat_id,
    ReplyText: task.reply_text,
    MessageId: task.message_id || '',
    Account: task.account || ''
  });
});

app.post('/tasks/done', async (req, res) => {
  if (!checkKey(req, res)) return;
  const lock = String(req.query.lock || req.body?.lock || '').trim();
  if (!lock || !lock.endsWith('.json.taking')) return bad(res, 400, 'lock invalid');
  await doneTask(lock);
  return ok(res);
});

app.post('/tasks/requeue', async (req, res) => {
  if (!checkKey(req, res)) return;
  const lock = String(req.query.lock || req.body?.lock || '').trim();
  if (!lock || !lock.endsWith('.json.taking')) return bad(res, 400, 'lock invalid');
  await requeueTask(lock);
  return ok(res);
});

// ===== Новый: /tasks/doneSafe =====
// Принимает key, lock, а также (рекомендуется) chat и author.
// Сверяет chat из запроса с chat_id в taking. Ищет подтверждение по паре (chat, author) в двух последних логах.
// Если найдено — удаляет .json.taking и отвечает 204; если нет — 428 (ничего не удаляет).
app.post('/tasks/doneSafe', async (req, res) => {
  if (!checkKey(req, res)) return;

  const lock  = String(req.query.lock   || req.body?.lock   || '').trim();
  const chatQ = req.query.chat   != null ? String(req.query.chat).trim()   : null;
  const authQ = req.query.author != null ? String(req.query.author).trim() : null;

  if (!lock || !lock.endsWith('.json.taking')) return bad(res, 400, 'lock invalid');

  const takingPath = path.join(TASK_DIR, lock);
  if (!fs.existsSync(takingPath)) return bad(res, 404, 'taking not found');

  let chatFromTaking = null, authorFromTaking = null;
  try {
    const raw  = await fsp.readFile(takingPath, 'utf8');
    const task = JSON.parse(raw);
    chatFromTaking   = task?.chat_id ?? null;
    authorFromTaking = (task?.author_id ?? task?.account_id ?? null);
  } catch (e) {
    return res.status(422).send({ ok:false, error:'taking parse failed', details:String(e) });
  }

  // 1) chat обязателен: либо в taking, либо в query (и они должны совпасть)
  const chatId = chatFromTaking ?? chatQ;
  if (!chatId) return res.status(422).send({ ok:false, error:'chat_id required (taking or query)' });

  if (chatQ && chatQ !== chatId) {
    return res.status(409).send({ ok:false, error:'chat mismatch', taking_chat: chatId, query_chat: chatQ });
  }

  // 2) author обязателен: берём из taking, если нет — из query
  const authorId = (authorFromTaking != null ? String(authorFromTaking) : null) || authQ;
  if (!authorId) return res.status(422).send({ ok:false, error:'author_id required (taking or query)' });

  // 3) Ищем подтверждение chat+author в двух последних логах
  let confirmed = false;
  try {
    confirmed = await hasConfirmationInTwoLogs(String(chatId), String(authorId));
  } catch (e) {
    return res.status(500).send({ ok:false, error:'logs read failed', details:String(e) });
  }

  if (!confirmed) return res.status(428).send({ ok:false, confirmed:false });

  // 4) Удаляем .json.taking — ЗАКРЫТО
  try { await fsp.unlink(takingPath); }
  catch (e) { return res.status(500).send({ ok:false, error:'taking delete failed', details:String(e) }); }

  return res.status(204).end();
});

// ===== start =====
(async () => {
  await ensureDir(LOG_DIR);
  await ensureDir(TASK_DIR);
  console.log(`App root: ${path.resolve(__dirname)}`);
  console.log(`LOG_DIR=${path.resolve(LOG_DIR)}`);
  console.log(`TASK_DIR=${path.resolve(TASK_DIR)}`);
  console.log(`ONLY_FIRST_SYSTEM=${ONLY_FIRST_SYSTEM}`);
  console.log(`LOG_TAIL_BYTES=${LOG_TAIL_BYTES}`);
  app.listen(PORT, () => console.log(`Server on :${PORT}`));
})();
