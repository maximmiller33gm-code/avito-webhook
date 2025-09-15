import express from "express";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import dotenv from "dotenv";
dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PORT = process.env.PORT || 8080;
// на Railway лучше так, чтобы переживало рестарты
const TASK_DIR = process.env.TASK_DIR || "/mnt/data/tasks";
fs.mkdirSync(TASK_DIR, { recursive: true });

const app = express();
app.use(express.json({ limit: "1mb" }));

app.get("/", (_, res) => res.json({ ok: true, msg: "Avito webhook alive" }));

// один проект для многих аккаунтов: /webhook/:account
app.post("/webhook/:account", (req, res) => {
  const account = req.params.account || "default";
  const body = req.body || {};

  // ждём формат v3 из твоего примера
  const p = body.payload?.value;
  if (body.payload?.type !== "message" || !p) {
    return res.json({ ok: true, skipped: "not a message event" });
  }
  // отрезаем системные сообщения
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
    // идемпотентно: создаём только если файла ещё нет
    const fd = fs.openSync(filePath, "wx");
    fs.writeFileSync(fd, JSON.stringify(task, null, 2), "utf8");
    fs.closeSync(fd);
  } catch (e) {
    if (e?.code === "EEXIST") return res.json({ ok: true, dedup: true });
    console.error("write task error:", e);
    return res.status(500).json({ ok: false, error: "write failed" });
  }

  res.json({ ok: true });
});

app.listen(PORT, () => console.log(`Webhook listening on :${PORT}`));
