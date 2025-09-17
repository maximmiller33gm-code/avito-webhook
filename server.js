// Новый /tasks/doneSafe: сверяем chat из запроса с taking и ищем в логах chat+author
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
    // Жёсткая привязка к переменной проекта ChatId
    return res.status(409).send({ ok:false, error:'chat mismatch', taking_chat: chatId, query_chat: chatQ });
  }

  // 2) author обязателен: берём из taking, если нет — из query
  const authorId = (authorFromTaking != null ? String(authorFromTaking) : null) || authQ;
  if (!authorId) return res.status(422).send({ ok:false, error:'author_id required (taking or query)' });

  // 3) Ищем подтверждение chat+author в двух последних логах (мы уже сделали функции tailContainsPair/hasConfirmationInTwoLogs)
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
