import express from "express";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import dotenv from "dotenv";
dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PORT = process.env.PORT || 8080;
const TASK_DIR = process.env.TASK_DIR || "/mnt/data/tasks";
fs.mkdirSync(TASK_DIR, { recursive: true });

const app = express();
app.use(express.json({ limit: "1mb" }));

// healthcheck
app.get("/", (_, res) => res.json({ ok: true, msg: "Avito webhook alive 🚀" }));

// основной вебхук
app.post("/webhook/:account", (req, res) => {
  const account = req.params.account || "default";
  const body = req.body || {};

  const p = body.payload?.value;
  if (body.payload?.type !== "message" || !p) {
    return res.json({ ok: true, skipped: "not a message event" });
  }

  // системные сообщения (резюме и прочее) игнорируем
  const isSystem =
    String(p.type || "").toLowerCase() === "system" ||
    (p.content?.text || "").startsWith("[Системное сообщение]");
  if (isSystem) return res.json({ ok: true, skipped: "system message" });

  const authorId = Number(p.author_id || 0);
  if (!authorId) return res.json({ ok: true, skipped: "no author" });

  const messageId = String(p.id || "");
  const chatId = String(p.chat_id || "");
  if (!messageId || !chatId) {
    return res.status(400).json({ ok: false, error: "no messageId/chatId" });
  }

  const task = {
    account,
    source: "avito",
    receivedAt: new Date().toISOString(),
    messageId,
    chatId,
    chatType: String(p.chat_type || ""),
    itemId: Number(p.item_id || 0),
    userId: Number(p.user_id || 0),
    createdTs: Number(p.created || 0),
    publishedAt: String(p.published_at || ""),
    textIn: p.content?.text || "",
    replyText:
      process.env.DEFAULT_REPLY ||
      "Привет! Уже вижу ваш отклик 🙌 Сейчас пришлю детали и анкету.",
    chatUrl: null
  };

  const filePath = path.join(TASK_DIR, `${messageId}.json`);
  try {
    const fd = fs.openSync(filePath, "wx"); // создать новый файл, если нет
    fs.writeFileSync(fd, JSON.stringify(task, null, 2), "utf8");
    fs.closeSync(fd);
  } catch (e) {
    if (e?.code === "EEXIST") return res.json({ ok: true, dedup: true });
    console.error("write task error:", e);
    return res.status(500).json({ ok: false, error: "write failed" });
  }

  res.json({ ok: true });
});

// === Debug routes for tasks ===

// список задач
app.get("/tasks/list", (req, res) => {
  try {
    const files = fs.readdirSync(TASK_DIR)
      .filter(f => f.endsWith(".json"))
      .map(f => {
        const p = path.join(TASK_DIR, f);
        const st = fs.statSync(p);
        return {
          id: f.replace(/\.json$/, ""),
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

// чтение конкретной задачи
app.get("/tasks/read/:id", (req, res) => {
  const id = (req.params.id || "").trim();
  if (!id) return res.status(400).json({ ok: false, error: "no id" });
  const filePath = path.join(TASK_DIR, `${id}.json`);
  try {
    if (!fs.existsSync(filePath)) return res.status(404).json({ ok: false, error: "not found" });
    const data = fs.readFileSync(filePath, "utf8");
    res.setHeader("Content-Type", "application/json; charset=utf-8");
    res.send(data);
  } catch (e) {
    console.error("read error:", e);
    res.status(500).json({ ok: false, error: "read failed" });
  }
});

// подчистить все задачи
app.post("/tasks/purge", (req, res) => {
  try {
    const files = fs.readdirSync(TASK_DIR).filter(f => f.endsWith(".json"));
    let removed = 0;
    for (const f of files) {
      try { fs.unlinkSync(path.join(TASK_DIR, f)); removed++; } catch {}
    }
    res.json({ ok: true, removed });
  } catch (e) {
    console.error("purge error:", e);
    res.status(500).json({ ok: false, error: "purge failed" });
  }
});

app.listen(PORT, () => console.log(`Webhook listening on :${PORT}`));
