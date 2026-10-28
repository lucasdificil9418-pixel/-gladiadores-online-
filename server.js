// Servidor de salas para el modo online de Gladiators: Arena of Honor.
// Uno crea una sala y recibe un código de 4 letras; el otro se une con ese código.
// El servidor solo reenvía los mensajes entre los dos jugadores de la sala.
// No necesita instalar nada: usa solo Node.js (WebSocket implementado a mano).
const http = require("http");
const crypto = require("crypto");

const PORT = process.env.PORT || 8080;
const LETTERS = "ABCDEFGHJKLMNPQRSTUVWXYZ"; // sin I ni O para no confundir
const rooms = new Map(); // código -> { host, guest, created }
const clients = new Set();

// ---------- WebSocket mínimo ----------
function wsSend(c, text) {
  if (c.closed) return;
  const data = Buffer.from(text, "utf8");
  let head;
  if (data.length < 126) {
    head = Buffer.from([0x81, data.length]);
  } else if (data.length < 65536) {
    head = Buffer.alloc(4); head[0] = 0x81; head[1] = 126; head.writeUInt16BE(data.length, 2);
  } else {
    head = Buffer.alloc(10); head[0] = 0x81; head[1] = 127; head.writeBigUInt64BE(BigInt(data.length), 2);
  }
  try { c.socket.write(Buffer.concat([head, data])); } catch (e) {}
}

function wsControl(c, op, payload = Buffer.alloc(0)) {
  try { c.socket.write(Buffer.concat([Buffer.from([0x80 | op, payload.length]), payload])); } catch (e) {}
}

function onData(c, chunk) {
  c.buf = Buffer.concat([c.buf, chunk]);
  while (c.buf.length >= 2) {
    const b0 = c.buf[0], b1 = c.buf[1];
    const fin = (b0 & 0x80) !== 0, op = b0 & 0x0f, masked = (b1 & 0x80) !== 0;
    let len = b1 & 0x7f, off = 2;
    if (len === 126) { if (c.buf.length < 4) return; len = c.buf.readUInt16BE(2); off = 4; }
    else if (len === 127) { if (c.buf.length < 10) return; len = Number(c.buf.readBigUInt64BE(2)); off = 10; }
    if (len > 1 << 20) return close(c);
    const mlen = masked ? 4 : 0;
    if (c.buf.length < off + mlen + len) return;
    const mask = masked ? c.buf.slice(off, off + 4) : null;
    let payload = c.buf.slice(off + mlen, off + mlen + len);
    if (mask) { payload = Buffer.from(payload); for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i & 3]; }
    c.buf = c.buf.slice(off + mlen + len);
    if (op === 0x8) return close(c);
    if (op === 0x9) { wsControl(c, 0xA, payload); continue; }
    if (op === 0xA) { c.alive = true; continue; }
    if (op === 0x0 || op === 0x1 || op === 0x2) {
      c.frag = c.frag ? Buffer.concat([c.frag, payload]) : payload;
      if (fin) { const text = c.frag.toString("utf8"); c.frag = null; onMessage(c, text); }
    }
  }
}

function close(c) {
  if (c.closed) return;
  c.closed = true;
  clients.delete(c);
  leave(c);
  try { c.socket.end(Buffer.from([0x88, 0])); } catch (e) {}
  try { c.socket.destroy(); } catch (e) {}
}

// ---------- salas ----------
function newCode() {
  for (let n = 0; n < 1000; n++) {
    let s = "";
    for (let i = 0; i < 4; i++) s += LETTERS[Math.floor(Math.random() * LETTERS.length)];
    if (!rooms.has(s)) return s;
  }
  return null;
}

function send(c, obj) { if (c) wsSend(c, JSON.stringify(obj)); }

function leave(c) {
  const code = c.room;
  if (!code || !rooms.has(code)) return;
  const r = rooms.get(code);
  const other = r.host === c ? r.guest : r.host;
  send(other, { t: "peer_left" });
  if (other) other.room = null;
  rooms.delete(code);
  c.room = null;
}

function onMessage(c, text) {
  let m;
  try { m = JSON.parse(text); } catch (e) { return; }
  if (m.t === "create") {
    leave(c);
    const code = newCode();
    if (!code) return send(c, { t: "error", msg: "No hay salas libres" });
    rooms.set(code, { host: c, guest: null, created: Date.now() });
    c.room = code;
    send(c, { t: "created", code });
  } else if (m.t === "join") {
    const code = String(m.code || "").toUpperCase().trim();
    const r = rooms.get(code);
    if (!r) return send(c, { t: "error", msg: "No existe la sala " + code });
    if (r.guest) return send(c, { t: "error", msg: "La sala ya está llena" });
    if (r.host === c) return send(c, { t: "error", msg: "Esa es tu propia sala" });
    leave(c);
    r.guest = c;
    c.room = code;
    send(c, { t: "joined", code });
    send(r.host, { t: "peer_joined" });
  } else if (m.t === "leave") {
    leave(c);
  } else if (m.t === "ping") {
    send(c, { t: "pong", c: m.c });
  } else {
    // mensaje del juego: se reenvía tal cual al otro jugador de la sala
    const r = c.room && rooms.get(c.room);
    if (!r) return;
    const other = r.host === c ? r.guest : r.host;
    if (other) wsSend(other, text);
  }
}

// ---------- HTTP + upgrade ----------
const server = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8" });
  res.end("Servidor de Gladiadores funcionando. Salas abiertas: " + rooms.size + " · Jugadores conectados: " + clients.size);
});

server.on("upgrade", (req, socket) => {
  const key = req.headers["sec-websocket-key"];
  if (!key) return socket.destroy();
  const accept = crypto.createHash("sha1").update(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").digest("base64");
  socket.write("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " + accept + "\r\n\r\n");
  socket.setNoDelay(true);
  const c = { socket, buf: Buffer.alloc(0), frag: null, room: null, alive: true, closed: false };
  clients.add(c);
  socket.on("data", (d) => onData(c, d));
  socket.on("close", () => close(c));
  socket.on("error", () => close(c));
});

// limpiar conexiones muertas y salas viejas
setInterval(() => {
  for (const c of clients) {
    if (!c.alive) { close(c); continue; }
    c.alive = false;
    wsControl(c, 0x9);
  }
  const now = Date.now();
  for (const [code, r] of rooms) {
    if (!r.guest && now - r.created > 30 * 60 * 1000) {
      send(r.host, { t: "error", msg: "La sala se cerró por inactividad" });
      if (r.host) r.host.room = null;
      rooms.delete(code);
    }
  }
}, 30000);

server.listen(PORT, () => console.log("Servidor de Gladiadores escuchando en el puerto " + PORT));
