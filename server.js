import express from "express";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import dotenv from "dotenv";
dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// === Конфиг из env
const PORT = process.env.PORT || 8080;
const TASK_DIR = process.env.TASK_DIR || "/mnt/data/tasks";         // файловая очередь
const TASK_KEY = process.env.TASK_KEY || "super_secret_task_key";   // ключ для claim/done/requeue
const DEFAULT_REPLY =
  process.env.DEFAULT_REPLY ||
  "Здравствуйте! Спасибо за отклик 🙌 Сейчас пришлю детали и анкету.";
const ONLY_FIRST_SYSTEM =
  String(process.env.ONLY_FIRST_SYSTEM || "true").toLowerCase() === "true"; // если true — реагируем только на первое системное «Кандидат откликнулся»

fs.mkdirSync(TASK_DIR, { recursive: true });

const app = express();
app.use(express.json({ limit: "1mb" }));

// healthcheck
app.get("/", (_, res) => res.json({ ok: true, msg: "Avito webhook alive 🚀" }));

// ========================= WEBHOOK =========================
//
// Принимаем события от Авито: /webhook/:account
// Кладём задачу в файловую очередь (идемпотентно по messageId)
// Логируем сырой JSON в консоль для отладки
//
app.post("/webhook/:account", (req, res) => {
  const account = req.params.account || "default";

  // ЛОГ ПОЛНОГО ПЕЙЛОАДА
  console.log("=== RAW AVITO WEBHOOK ===");
  try {
    console.log(JSON.stringify(req.body, null, 2));
  } catch {
    console.log("[warn] cannot stringify req.body");
  }
  console.log("=========================");

  const body = req.body || {};
  const payload = body.payload || {};
  const v = payload.value || {};

  if (payload.type !== "message" || !v) {
    return res.json({ ok: true, skipped: "not a message event" });
  }

  const msgType = String(v.type || "").toLowerCase(); // "system" | "text" | ...
  const textIn = v.content?.text || "";
  const chatId = String(v.chat_id || "");
  const messageId = String(v.id || "");
  const authorId = Number(v.author_id || 0);

  if (!chatId || !messageId) {
    return res.status(400).json({ ok: false, error: "no chatId/messageId" });
  }

  // --- Логика триггера задачи ---
  // По умолчанию реагируем на два случая:
  // 1) Первое системное сообщение-уведомление об отклике: "[Системное сообщение] Кандидат откликнулся ..."
  // 2) Любое текстовое сообщение от человека (author_id > 0), если отключён режим ONLY_FIRST_SYSTEM
  let shouldCreateTask = false;

  const isSystemCandidate =
    msgType === "system" &&
    typeof textIn === "string" &&
    textIn.toLowerCase().includes("кандидат откликнулся");

  if (ONLY_FIRST_SYSTEM) {
    shouldCreateTask = isSystemCandidate;
  } else {
    const isHumanText = authorId > 0 && msgType !== "system";
    shouldCreateTask = isSystemCandidate || isHumanText;
  }

  if (!shouldCreateTask) {
    return res.json({ ok: true, skipped: "filter_no_task" });
  }

  const task = {
    account,
    source: "avito",
    receivedAt: new Date().toISOString(),

    // привязки
    messageId,
    chatId,
    chatType: String(v.chat_type || ""),
    itemId: Number(v.item_id || 0),
    userId: Number(v.user_id || 0),
    createdTs: Number(v.created || 0),
    publishedAt: String(v.published_at || ""),

    // содержимое
    textIn,
    replyText: DEFAULT_REPLY,
    chatUrl: null
  };

  // ИДЕМПОТЕНТНОЕ СОЗДАНИЕ: создаём *.json, если ещё не существует
  const filePath = path.join(TASK_DIR, `${messageId}.json`);
  try {
    const fd = fs.openSync(filePath, "wx"); // создаст новый файл, если нет
    fs.writeFileSync(fd, JSON.stringify(task, null, 2), "utf8");
    fs.closeSync(fd);
  } catch (e) {
    if (e?.code === "EEXIST") return res.json({ ok: true, dedup: true });
    console.error("write task error:", e);
    return res.status(500).json({ ok: false, error: "write failed" });
  }

  return res.json({ ok: true });
});

// ========================= HTTP-ОЧЕРЕДЬ ДЛЯ ZENNO =========================
//
// GET /tasks/claim?account=hr-main&key=TASK_KEY
//   → возвращает ближайшую задачу и ЛОК-файл (rename *.json → *.taking)
// POST /tasks/done?lock=<имя_лок_файла>&key=TASK_KEY
//   → подтверждает выполнение, удаляет ЛОК-файл
// POST /tasks/requeue?lock=<имя_лок_файла>&key=TASK_KEY
//   → возвращает задачу в очередь (rename *.taking → *.json)
//
app.get("/tasks/claim", (req, res) => {
  if ((req.query.key || "") !== TASK_KEY)
    return res.status(403).json({ ok: false, error: "forbidden" });

  const wantAccount = (req.query.account || "").trim(); // можно не указывать

  // Берём первый подходящий файл *.json
  let files;
  try {
    files = fs.readdirSync(TASK_DIR).filter((f) => f.endsWith(".json"));
  } catch (e) {
    console.error("claim list error:", e);
    return res.status(500).json({ ok: false, error: "list failed" });
  }

  for (const f of files) {
    const p = path.join(TASK_DIR, f);
    try {
      const raw = JSON.parse(fs.readFileSync(p, "utf8"));
      if (wantAccount && raw.account !== wantAccount) continue;

      // атомарно "залочим": переименуем в .taking
      const lockPath = p + ".taking";
      fs.renameSync(p, lockPath);

      // отдаём задачу и имя lock-файла
      return res.json({
        ok: true,
        task: raw,
        lock: path.basename(lockPath)
      });
    } catch (e) {
      console.error("claim parse/lock error:", e);
      // битый файл — попробуем удалить, чтобы не клинило очередь
      try { fs.unlinkSync(p); } catch {}
      continue;
    }
  }

  return res.json({ ok: true, task: null }); // задач нет
});

app.post("/tasks/done", (req, res) => {
  if ((req.query.key || "") !== TASK_KEY)
    return res.status(403).json({ ok: false, error: "forbidden" });

  const lock = (req.query.lock || "").trim();
  if (!lock) return res.status(400).json({ ok: false, error: "no lock" });

  const lockPath = path.join(TASK_DIR, lock);
  try {
    if (fs.existsSync(lockPath)) fs.unlinkSync(lockPath);
    return res.json({ ok: true });
  } catch (e) {
    console.error("done delete error:", e);
    return res.status(500).json({ ok: false, error: "delete failed" });
  }
});

app.post("/tasks/requeue", (req, res) => {
  if ((req.query.key || "") !== TASK_KEY)
    return res.status(403).json({ ok: false, error: "forbidden" });

  const lock = (req.query.lock || "").trim();
  if (!lock) return res.status(400).json({ ok: false, error: "no lock" });

  const lockPath = path.join(TASK_DIR, lock);
  try {
    if (!fs.existsSync(lockPath))
      return res.status(404).json({ ok: false, error: "not found" });
    const back = lockPath.replace(/\.taking$/, ".json");
    fs.renameSync(lockPath, back);
    return res.json({ ok: true });
  } catch (e) {
    console.error("requeue error:", e);
    return res.status(500).json({ ok: false, error: "requeue failed" });
  }
});

// ========================= DEBUG (опционально) =========================
//
// Список/чтение/очистка — удобно на старте, потом можно закрыть.
//
app.get("/tasks/list", (req, res) => {
  try {
    const files = fs
      .readdirSync(TASK_DIR)
      .filter((f) => f.endsWith(".json") || f.endsWith(".taking"))
      .map((f) => {
        const p = path.join(TASK_DIR, f);
        const st = fs.statSync(p);
        return {
          file: f,
          size: st.size,
          mtime: st.mtime.toISOString()
        };
      })
      .sort((a, b) => b.mtime.localeCompare(a.mtime));
    res.json({ ok: true, dir: TASK_DIR, count: files.length, files });
  } catch (e) {
    console.error("list error:", e);
    res.status(500).json({ ok: false, error: "list failed" });
  }
});

app.get("/tasks/read/:id", (req, res) => {
  const id = (req.params.id || "").trim();
  if (!id) return res.status(400).json({ ok: false, error: "no id" });

  // поддержим и .json, и .taking для чтения
  const pJson = path.join(TASK_DIR, `${id}.json`);
  const pTaking = path.join(TASK_DIR, `${id}.taking`);
  const filePath = fs.existsSync(pJson) ? pJson : pTaking;

  try {
    if (!filePath || !fs.existsSync(filePath))
      return res.status(404).json({ ok: false, error: "not found" });
    const data = fs.readFileSync(filePath, "utf8");
    res.setHeader("Content-Type", "application/json; charset=utf-8");
    res.send(data);
  } catch (e) {
    console.error("read error:", e);
    res.status(500).json({ ok: false, error: "read failed" });
  }
});

app.post("/tasks/purge", (req, res) => {
  try {
    const files = fs
      .readdirSync(TASK_DIR)
      .filter((f) => f.endsWith(".json") || f.endsWith(".taking"));
    let removed = 0;
    for (const f of files) {
      try {
        fs.unlinkSync(path.join(TASK_DIR, f));
        removed++;
      } catch {}
    }
    res.json({ ok: true, removed });
  } catch (e) {
    console.error("purge error:", e);
    res.status(500).json({ ok: false, error: "purge failed" });
  }
});

app.listen(PORT, () => console.log(`Webhook listening on :${PORT}`));
