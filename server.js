// ESM-версия: package.json должен содержать "type": "module"
import express from 'express';
import fs from 'fs';
import { promises as fsp } from 'fs';
import path from 'path';
import crypto from 'crypto';
import process from 'process';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ==== ENV ====
const PORT = Number(process.env.PORT || 3000);
const TASK_KEY = String(process.env.TASK_KEY || '').trim();
const LOG_DIR  = process.env.LOG_DIR  || '/mnt/data/logs';
const TASK_DIR = process.env.TASK_DIR || '/mnt/data/tasks';
const DEFAULT_REPLY = process.env.DEFAULT_REPLY || 'Здравствуйте!';
const ONLY_FIRST_SYSTEM = String(process.env.ONLY_FIRST_SYSTEM || 'true').toLowerCase() === 'true';
const WEBHOOK_SECRET = process.env.WEBHOOK_SECRET || '';

// ==== utils ====
async function ensureDir(dir) { try { await fsp.mkdir(dir, { recursive: true }); } catch {} }
function nowIso() { return new Date().toISOString(); }
function genId() { return crypto.randomBytes(16).toString('hex'); }

function todayLogName() {
  const d = new Date();
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth()+1).padStart(2,'0');
  const day = String(d.getUTCDate()).padStart(2,'0');
  return `logs.${y}${m}${day}.log`;
}
async function appendLog(text) {
  await ensureDir(LOG_DIR);
  const file = path.join(LOG_DIR, todayLogName());
  await fsp.appendFile(file, text, 'utf8');
  return file;
}
function ok(res, extra = {}) { return res.send({ ok: true, ...extra }); }
function bad(res, code, msg) { return res.status(code).send({ ok: false, error: msg }); }

// ==== FILE QUEUE ====
// Файл задачи: { id, account, chat_id, reply_text, message_id, created_at }
async function createTask({ account, chat_id, reply_text, message_id }) {
  await ensureDir(TASK_DIR);
  const id = genId();
  const acc = (account || 'hr-main').replace(/[^a-zA-Z0-9_-]/g, '_');
  const obj = {
    id,
    account: acc,
    chat_id,
    reply_text: reply_text || DEFAULT_REPLY,
    message_id: message_id || null,
    created_at: nowIso(),
  };
  const file = path.join(TASK_DIR, `${acc}__${id}.json`);
  await fsp.writeFile(file, JSON.stringify(obj, null, 2), 'utf8');
  return obj;
}

// Claim: смотреть только ПОСЛЕДНИЕ 3 json-файла (по mtime)
async function claimTask(account) {
  await ensureDir(TASK_DIR);
  let files = (await fsp.readdir(TASK_DIR)).filter(f => f.endsWith('.json'));

  // сортируем по времени изменения: новые впереди
  files.sort((a, b) => {
    const ta = fs.statSync(path.join(TASK_DIR, a)).mtimeMs;
    const tb = fs.statSync(path.join(TASK_DIR, b)).mtimeMs;
    return tb - ta;
  });

  // фильтруем по account префиксу (если задан)
  if (account) {
    const pref = `${account}__`;
    files = files.filter(f => f.startsWith(pref));
  }

  // берём только первые 3
  files = files.slice(0, 3);

  for (const f of files) {
    const full = path.join(TASK_DIR, f);
    const taking = full.replace(/\.json$/, '.json.taking');
    try {
      await fsp.rename(full, taking); // атомарная блокировка
      const raw = JSON.parse(await fsp.readFile(taking, 'utf8'));
      const lockId = path.basename(taking);
      return { task: raw, lockId };
    } catch {
      // файл могли увести параллельно — пробуем следующий
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
  const to = from.replace(/\.json\.taking$/, '.json');
  try { await fsp.rename(from, to); } catch {}
  return true;
}

// ==== APP ====
const app = express();
app.use(express.json({ limit: '1mb' }));

// health
app.get('/', (req, res) => ok(res, { up: true }));

// debug — посмотреть, что лежит в TASK_DIR
app.get('/tasks/debug', async (req, res) => {
  try {
    await ensureDir(TASK_DIR);
    const files = (await fsp.readdir(TASK_DIR)).sort();
    res.send({ ok: true, files });
  } catch (e) {
    res.status(500).send({ ok: false, error: String(e) });
  }
});

// ручная постановка в очередь (для теста)
app.post('/tasks/enqueue', async (req, res) => {
  try {
    const { account, chat_id, reply_text, message_id } = req.body || {};
    if (!chat_id) return bad(res, 400, 'chat_id required');
    const t = await createTask({ account, chat_id, reply_text, message_id });
    res.send({ ok: true, task: t });
  } catch (e) {
    res.status(500).send({ ok: false, error: String(e) });
  }
});

// ===== ВЕБХУК ОТ АВИТО =====
app.post('/webhook/:account', async (req, res) => {
  const account = req.params.account || 'hr-main';

  // (опц.) секрет
  if (WEBHOOK_SECRET) {
    const headerSecret = req.headers['x-avito-secret'];
    const bodySecret = req.body && req.body.secret;
    if (String(headerSecret || bodySecret || '') !== String(WEBHOOK_SECRET)) {
      return bad(res, 403, 'forbidden');
    }
  }

  // лог RAW
  const pretty = JSON.stringify(req.body || {}, null, 2);
  const header = `=== RAW AVITO WEBHOOK (${account}) @ ${nowIso()} ===\n`;
  const footer = `\n=========================\n\n`;
  await appendLog(header + pretty + footer);

  // простая логика: «Кандидат откликнулся» => ставим задачу
  try {
    const payload = req.body?.payload || {};
    const val = payload?.value || {};
    const isSystem = val?.type === 'system';
    const txt = String(val?.content?.text || '');
    const chatId = val?.chat_id;
    const msgId = val?.id;

    // фильтр: только первое системное по чату (если включен ONLY_FIRST_SYSTEM)
    let pass = /Кандидат/i.test(txt) || /отклик/i.test(txt);
    if (isSystem && pass && chatId) {
      if (ONLY_FIRST_SYSTEM) {
        // проверим в логах, было ли раньше системное для этого чата сегодня (очень грубо)
        // если нужно строго — можно вести small in-memory set
      }
      await createTask({
        account,
        chat_id: chatId,
        reply_text: DEFAULT_REPLY,
        message_id: msgId
      });
    }
  } catch {}

  return ok(res);
});

// ===== Проверка Done по логам =====
app.get('/logs/has', async (req, res) => {
  const chat = String(req.query.chat || '').trim();
  const author = String(req.query.author || '').trim();
  if (!chat || !author) return bad(res, 400, 'chat & author required');

  await ensureDir(LOG_DIR);
  let files = (await fsp.readdir(LOG_DIR))
    .filter(f => f.endsWith('.log'))
    .map(f => ({ f, t: fs.statSync(path.join(LOG_DIR, f)).mtimeMs }))
    .sort((a,b) => b.t - a.t);

  if (files.length === 0) return ok(res, { exists: false });

  // читаем хвост последнего (500 КБ)
  const latest = path.join(LOG_DIR, files[0].f);
  let buf = await fsp.readFile(latest, 'utf8');
  const MAX = 500 * 1024;
  if (buf.length > MAX) buf = buf.slice(buf.length - MAX);

  const has = buf.includes(`"chat_id": "${chat}"`) && buf.includes(`"author_id": ${author}`);
  return ok(res, { exists: has, file: files[0].f });
});

// ===== задачи: claim / done / requeue =====
function checkKey(req, res) {
  const key = String(req.query.key || req.body?.key || '').trim();
  if (!TASK_KEY || key !== TASK_KEY) { bad(res, 403, 'bad key'); return false; }
  return true;
}

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

// ==== start ====
(async () => {
  await ensureDir(LOG_DIR);
  await ensureDir(TASK_DIR);
  const appRoot = path.resolve(__dirname);
  console.log(`App root: ${appRoot}`);
  console.log(`LOG_DIR=${path.resolve(LOG_DIR)}`);
  console.log(`TASK_DIR=${path.resolve(TASK_DIR)}`);
  console.log(`ONLY_FIRST_SYSTEM=${ONLY_FIRST_SYSTEM}`);
  console.log(`Watching last 3 tasks in claim`);
  app.listen(PORT, () => console.log(`Server on :${PORT}`));
})();
