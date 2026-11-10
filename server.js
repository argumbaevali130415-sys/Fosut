// Б/У Рынок — онлайн-сервер без зависимостей (Node 16+).
// Запуск: node server.js   →  http://localhost:3000
// Данные хранятся в data.json рядом с файлом.
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto');
const PORT = process.env.PORT || 3000;
const DB = path.join(__dirname, 'data.json');
const PAGE = path.join(__dirname, 'bu-market.html');

let db = { listings: [], chats: [] };
try { db = Object.assign(db, JSON.parse(fs.readFileSync(DB, 'utf8'))); } catch (e) {}
let timer;
const save = () => {
  clearTimeout(timer);
  timer = setTimeout(() => fs.writeFile(DB + '.tmp', JSON.stringify(db), e => { if (!e) fs.rename(DB + '.tmp', DB, () => {}); }), 300);
};

const uidOf = t => crypto.createHash('sha256').update(t).digest('hex').slice(0, 16);
const rid = p => p + Date.now().toString(36) + crypto.randomBytes(3).toString('hex');
const fail = (code, msg) => Object.assign(new Error(msg), { code });
const str = (v, n) => String(v == null ? '' : v).trim().slice(0, n);

// Владелец хранится как хэш токена — токен знает только сам пользователь.
const lview = (l, uid, full) => ({
  id: l.id, title: l.title, desc: l.desc, price: l.price, phone: l.phone, ts: l.ts,
  mine: l.owner === uid, n: l.photos.length, photos: full ? l.photos : l.photos.slice(0, 1)
});
const cview = (c, uid) => ({
  id: c.id, lid: c.lid, asSeller: c.seller === uid,
  msgs: c.msgs.map(m => ({ me: m.from === uid, t: m.t, ts: m.ts }))
});

// ---- realtime (Server-Sent Events) ----
const clients = new Set();
const emit = (c, ev, d) => c.res.write(`event: ${ev}\ndata: ${JSON.stringify(d)}\n\n`);
setInterval(() => clients.forEach(c => c.res.write(': ping\n\n')), 25000);

const send = (res, code, obj) => {
  res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' });
  res.end(JSON.stringify(obj));
};
const readBody = req => new Promise((ok, no) => {
  const a = []; let n = 0;
  req.on('data', c => { n += c.length; if (n > 5e6) { no(fail(413, 'Слишком большой запрос')); req.destroy(); } else a.push(c); });
  req.on('end', () => { try { ok(JSON.parse(Buffer.concat(a).toString() || '{}')); } catch (e) { no(fail(400, 'Некорректный запрос')); } });
  req.on('error', no);
});

http.createServer(async (req, res) => {
  try {
    const u = new URL(req.url, 'http://x'), p = u.pathname, m = req.method;

    if (m === 'GET' && (p === '/' || p === '/index.html')) {
      return fs.readFile(PAGE, (e, b) => {
        if (e) return send(res, 500, { error: 'Не найден bu-market.html рядом с server.js' });
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' }); res.end(b);
      });
    }
    if (!p.startsWith('/api/')) return send(res, 404, { error: 'Не найдено' });

    const tok = req.headers['x-token'] || u.searchParams.get('tok') || '';
    if (!/^[a-z0-9]{16,40}$/.test(tok)) throw fail(401, 'Нет токена');
    const uid = uidOf(tok), r = p.slice(5).split('/'), body = m === 'POST' ? await readBody(req) : {};

    // GET /api/events — поток обновлений
    if (m === 'GET' && r[0] === 'events') {
      res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', Connection: 'keep-alive', 'X-Accel-Buffering': 'no' });
      res.write('retry: 2000\n\n');
      const c = { uid, res }; clients.add(c);
      req.on('close', () => clients.delete(c));
      return;
    }

    // ---- объявления ----
    if (r[0] === 'listings') {
      if (m === 'GET' && !r[1]) return send(res, 200, db.listings.slice().sort((a, b) => b.ts - a.ts).map(l => lview(l, uid)));
      if (m === 'GET') {
        const l = db.listings.find(x => x.id === r[1]);
        if (!l) throw fail(404, 'Объявление удалено');
        return send(res, 200, lview(l, uid, true));
      }
      if (m === 'POST') {
        const title = str(body.title, 80), desc = str(body.desc, 2000), phone = str(body.phone, 30), price = Number(body.price);
        const photos = Array.isArray(body.photos) ? body.photos.slice(0, 3) : [];
        if (!title) throw fail(400, 'Укажите название');
        if (!(price >= 0) || price > 1e9) throw fail(400, 'Укажите цену');
        if (phone.replace(/\D/g, '').length < 7) throw fail(400, 'Укажите номер телефона');
        if (!desc) throw fail(400, 'Добавьте описание');
        if (!photos.length || !photos.every(x => typeof x === 'string' && x.startsWith('data:image/') && x.length < 1.5e6)) throw fail(400, 'Добавьте хотя бы одно фото');
        if (db.listings.filter(l => l.owner === uid).length >= 20) throw fail(429, 'Не больше 20 объявлений');
        const l = { id: rid('l'), owner: uid, title, desc, price, phone, photos, ts: Date.now() };
        db.listings.push(l); save();
        clients.forEach(c => emit(c, 'listings', {}));
        return send(res, 200, lview(l, uid));
      }
      if (m === 'DELETE' && r[1]) {
        const l = db.listings.find(x => x.id === r[1]);
        if (!l) throw fail(404, 'Объявление не найдено');
        if (l.owner !== uid) throw fail(403, 'Это не ваше объявление');
        db.listings = db.listings.filter(x => x !== l);
        db.chats = db.chats.filter(c => c.lid !== l.id); save();
        clients.forEach(c => emit(c, 'listings', {}));
        return send(res, 200, { ok: true });
      }
    }

    // ---- чаты ----
    if (r[0] === 'chats') {
      if (m === 'GET' && !r[1]) return send(res, 200, db.chats.filter(c => c.buyer === uid || c.seller === uid).map(c => cview(c, uid)));
      if (m === 'POST' && !r[1]) {
        const l = db.listings.find(x => x.id === body.lid);
        if (!l) throw fail(404, 'Объявление удалено');
        if (l.owner === uid) throw fail(400, 'Это ваше объявление');
        let c = db.chats.find(x => x.lid === l.id && x.buyer === uid);
        if (!c) { c = { id: rid('c'), lid: l.id, buyer: uid, seller: l.owner, msgs: [] }; db.chats.push(c); save(); }
        return send(res, 200, cview(c, uid));
      }
      if (m === 'POST' && r[2] === 'messages') {
        const c = db.chats.find(x => x.id === r[1]);
        if (!c || (c.buyer !== uid && c.seller !== uid)) throw fail(404, 'Чат недоступен');
        const t = str(body.t, 500);
        if (!t) throw fail(400, 'Пустое сообщение');
        c.msgs.push({ from: uid, t, ts: Date.now() });
        if (c.msgs.length > 1000) c.msgs.shift();
        save();
        clients.forEach(x => { if (x.uid === c.buyer || x.uid === c.seller) emit(x, 'chat', cview(c, x.uid)); });
        return send(res, 200, cview(c, uid));
      }
    }
    throw fail(404, 'Не найдено');
  } catch (e) {
    if (!res.headersSent) send(res, typeof e.code === 'number' ? e.code : 500, { error: typeof e.code === 'number' ? e.message : 'Ошибка сервера' });
  }
}).listen(PORT, () => console.log(`Б/У Рынок: http://localhost:${PORT}`));
