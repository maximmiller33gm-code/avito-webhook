// server.js — файловая очередь + лог-вебхуков + проверка Done по логам
const express = require('express');
const fs = require('fs');
const fsp = fs.promises;
const path = require('path');
const crypto = require('crypto');

const PORT = Number(process.env.PORT || 3000);
const TASK_KEY = (process.env.TASK_KEY || '').trim();
const LOG_DIR = process.env.LOG_DIR || './logs';
const TASK_DIR = process.env.TASK_DIR || './tasks';
const REPLY_DEFAULT = process.env.REPLY_DEFAULT || 'Здравствуйте!';
const WEBHOOK_SECRET = process.env.WEBHOOK_SECRET || ''; // если задан — проверяем

// ===== утилиты =====
async function ensureDir(dir) {
  try { await fsp.mkdir(dir, { recursive: true }); } catch {}
}
function nowIso() { return new Date().toISOString(); }
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
function ok(res, extra={}) { return res.send({ ok:true, ...extra }); }
function bad(res, code, msg) { return res.status(code).send({ ok:false, error: msg }); }
function genId() { return crypto.randomBytes(16).toString('hex'); }

// ===== файловая очередь задач =====
// структура файла задачи (.json):
// { id, account, chat_id, reply_text, message_id, created_at }
async function createTask({ account, chat_id, reply_text, message_id }) {
  await ensureDir(TASK_DIR);
  const id = genId();
  const obj = {
    id, account: account || 'hr-main',
    chat_id, reply_text: reply_text || REPLY_DEFAULT,
    message_id: message_id || null,
    created_at: nowIso(),
  };
  const file = path.join(TASK_DIR, `${id}.json`);
  await fsp.writeFile(file, JSON.stringify(obj, null, 2), 'utf8');
  return obj;
}

// claim: берем первый .json, лочим переименованием в .taking
async function claimTask(account) {
  await ensureDir(TASK_DIR);
  const files = (await fsp.readdir(TASK_DIR))
    .filter(f => f.endsWith('.json'))
    .sort(); // по имени (примерно FIFO)
  for (const f of files) {
    const full = path.join(TASK_DIR, f);
    const raw = JSON.parse(await fsp.readFile(full, 'utf8'));
    if (account && raw.account && String(raw.account) !== String(account)) continue;

    const taking = full.replace(/\.json$/, '.json.taking');
    try {
      await fsp.rename(full, taking); // атомарная блокировка
      const lockId = path.basename(taking);
      return { task: raw, lockId };
    } catch (e) {
      // файл могли забрать параллельно — пробуем следующий
    }
  }
  return null;
}

// done: удаляем .taking
async function doneTask(lockId) {
  const file = path.join(TASK_DIR, lockId);
  try { await fsp.unlink(file); } catch {}
  return true;
}

// requeue: возвращаем .taking -> .json
async function requeueTask(lockId) {
  const from = path.join(TASK_DIR, lockId);
  const to = from.replace(/\.json\.taking$/, '.json');
  try { await fsp.rename(from, to); } catch {}
  return true;
}

// ===== express =====
const app = express();
app.use(express.json({ limit: '1mb' }));

// health
app.get('/', (req, res) => ok(res, { up:true }));

// ===== вебхук от Авито =====
app.post('/webhook/:account', async (req, res) => {
  const account = req.params.account || 'hr-main';

  // (опц.) проверка секрета
  if (WEBHOOK_SECRET) {
    const headerSecret = req.headers['x-avito-secret'];
    const bodySecret = req.body && req.body.secret;
    if (String(headerSecret || bodySecret || '') !== String(WEBHOOK_SECRET)) {
      return bad(res, 403, 'forbidden');
    }
  }

  // логируем RAW вебхук (как просили)
  const pretty = JSON.stringify(req.body || {}, null, 2);
  const header = `=== RAW AVITO WEBHOOK (${account}) @ ${nowIso()} ===\n`;
  const footer = `\n=========================\n\n`;
  await appendLog(header + pretty + footer);

  // простая логика: если system-отклик — создаём задачу
  try {
    const payload = req.body?.payload || {};
    const val = payload?.value || {};
    const isSystem = val?.type === 'system';
    const txt = val?.content?.text || '';
    const chatId = val?.chat_id;
    const msgId = val?.id;

    // реагируем на "Кандидат откликнулся"
    const looksLikeCandidate = /Кандидат откликнулся/i.test(txt);

    if (isSystem && looksLikeCandidate && chatId) {
      await createTask({
        account,
        chat_id: chatId,
        reply_text: REPLY_DEFAULT,
        message_id: msgId
      });
    }
  } catch (e) {
    // не блокируем ответ вебхуку
  }

  return ok(res);
});

// ===== проверка по логам: был ли исходящий с author_id в этом чате =====
app.get('/logs/has', async (req, res) => {
  const chat = String(req.query.chat || '').trim();
  const author = String(req.query.author || '').trim();
  if (!chat || !author) return bad(res, 400, 'chat & author required');

  await ensureDir(LOG_DIR);
  const files = (await fsp.readdir(LOG_DIR))
    .filter(f => f.endsWith('.log'))
    .map(f => ({ f, t: fs.statSync(path.join(LOG_DIR, f)).mtimeMs }))
    .sort((a,b) => b.t - a.t);

  if (files.length === 0) return ok(res, { exists:false });

  // читаем «хвост» последнего лога (500 КБ)
  const latest = path.join(LOG_DIR, files[0].f);
  let buf = await fsp.readFile(latest, 'utf8');
  const MAX = 500 * 1024;
  if (buf.length > MAX) buf = buf.slice(buf.length - MAX);

  const has = buf.includes(`"chat_id": "${chat}"`) && buf.includes(`"author_id": ${author}`);
  return ok(res, { exists: has, file: files[0].f });
});

// ===== задачи: claim / done / requeue =====
function checkKey(req, res) {
  const key = (req.query.key || req.body?.key || '').trim();
  if (!TASK_KEY || key !== TASK_KEY) {
    bad(res, 403, 'bad key'); return false;
  }
  return true;
}

// Claim: Zenno спрашивает задачу
app.all('/tasks/claim', async (req, res) => {
  if (!checkKey(req, res)) return;
  const account = (req.query.account || req.body?.account || '').trim();

  const got = await claimTask(account);
  if (!got) return ok(res, { has:false });

  const { task, lockId } = got;
  // отдаём то, что ожидает Zenno
  return ok(res, {
    has: true,
    lockId,
    ChatId: task.chat_id,
    ReplyText: task.reply_text,
    MessageId: task.message_id || '',
    Account: task.account || ''
  });
});

// Done: Zenno (или сервер) подтверждает обработку
app.post('/tasks/done', async (req, res) => {
  if (!checkKey(req, res)) return;
  const lock = (req.query.lock || req.body?.lock || '').trim();
  if (!lock || !lock.endsWith('.json.taking')) return bad(res, 400, 'lock invalid');
  await doneTask(lock);
  return ok(res);
});

// Requeue: вернуть в очередь (если не получилось отправить)
app.post('/tasks/requeue', async (req, res) => {
  if (!checkKey(req, res)) return;
  const lock = (req.query.lock || req.body?.lock || '').trim();
  if (!lock || !lock.endsWith('.json.taking')) return bad(res, 400, 'lock invalid');
  await requeueTask(lock);
  return ok(res);
});

// ===== старт =====
(async () => {
  await ensureDir(LOG_DIR);
  await ensureDir(TASK_DIR);
  app.listen(PORT, () => {
    console.log(`Server on :${PORT}`);
    console.log(`LOG_DIR=${path.resolve(LOG_DIR)}  TASK_DIR=${path.resolve(TASK_DIR)}`);
  });
})();
