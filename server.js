// CyberStrike lobby + relay server.
//
// Players connect with a WebSocket. The server keeps the lobby list (public and private),
// hands out lobby codes, and relays game messages between the players of a lobby. The
// lobby host's game runs the match; everyone else sends their moves to the host.
//
// Protocol: one text message per WebSocket frame, fields separated by '|'.
//   client -> server: hello|ver|name   list   create|code|public|mode|max|lobbyName
//                     join|code   leave   start   end   to|id|payload   all|payload   ping|t
//   server -> client: welcome|id   lobbies|entries   joined|code|hostId|mode|max|public|name
//                     roster|hostId|id:name;id:name   started   ended   m|fromId|payload
//                     err|text   closed|reason   pong|t
//
// Set TCP_PORT to also accept newline-separated messages over plain TCP (used by the
// automated tests; browsers always use the WebSocket).

'use strict';
const http = require('http');
const net = require('net');
const crypto = require('crypto');

const PORT = parseInt(process.env.PORT || '8080', 10);
const MODES = { duel: 2, dm: 10, casual: 10 };
const MAX_LOBBIES = 500;
const MAX_MSG = 64 * 1024;

let nextId = 1;
const lobbies = new Map();          // code -> lobby
const clients = new Map();          // id -> client

function clean(s, max) {
  return String(s || '').replace(/[|;,:\n\r\t]/g, ' ').replace(/[^\x20-\x7e]/g, '').trim().slice(0, max);
}
function cleanCode(s) { return String(s || '').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 12); }
function randomCode() {
  const A = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  for (;;) {
    let c = '';
    for (let i = 0; i < 5; i++) c += A[Math.floor(Math.random() * A.length)];
    if (!lobbies.has(c)) return c;
  }
}

function send(c, msg) { if (c && c.alive) c.send(msg); }
function lobbyOf(c) { return c.lobby ? lobbies.get(c.lobby) : null; }

function roster(l) {
  const list = l.members.map((id) => id + ':' + (clients.get(id) ? clients.get(id).name : '?')).join(';');
  return 'roster|' + l.host + '|' + list;
}
function broadcast(l, msg, except) {
  for (const id of l.members) if (id !== except) send(clients.get(id), msg);
}

function leaveLobby(c, reason) {
  const l = lobbyOf(c);
  c.lobby = null;
  if (!l) return;
  l.members = l.members.filter((id) => id !== c.id);
  if (l.host === c.id || l.members.length === 0) {
    // the host's game runs the match, so the lobby closes when the host leaves
    for (const id of l.members) {
      const o = clients.get(id);
      if (o) { o.lobby = null; send(o, 'closed|' + (reason || 'The host left the lobby')); }
    }
    lobbies.delete(l.code);
    return;
  }
  broadcast(l, roster(l));
}

function handle(c, msg) {
  if (msg.length > MAX_MSG) return;
  const bar = msg.indexOf('|');
  const cmd = bar < 0 ? msg : msg.slice(0, bar);
  const rest = bar < 0 ? '' : msg.slice(bar + 1);
  const f = rest.split('|');
  const l = lobbyOf(c);

  switch (cmd) {
    case 'hello':
      c.name = clean(f[1], 15) || ('Player' + c.id);
      send(c, 'welcome|' + c.id);
      break;

    case 'name':
      c.name = clean(f[0], 15) || c.name;
      if (l) broadcast(l, roster(l));
      break;

    case 'list': {
      const out = [];
      for (const lb of lobbies.values()) {
        if (!lb.isPublic) continue;
        out.push([lb.code, lb.mode, lb.members.length, lb.max, lb.started ? 1 : 0, lb.name].join(','));
        if (out.length >= 40) break;
      }
      send(c, 'lobbies|' + out.join(';'));
      break;
    }

    case 'create': {
      if (l) leaveLobby(c);
      if (lobbies.size >= MAX_LOBBIES) { send(c, 'err|The server is full, try again later'); break; }
      let code = cleanCode(f[0]);
      if (code && code.length < 3) { send(c, 'err|Codes need at least 3 letters or numbers'); break; }
      if (code && lobbies.has(code)) { send(c, 'err|That code is already taken'); break; }
      if (!code) code = randomCode();
      const mode = MODES[f[2]] ? f[2] : 'duel';
      let max = parseInt(f[3], 10) || MODES[mode];
      max = Math.max(2, Math.min(MODES[mode], max));
      const lb = {
        code, mode, max, isPublic: f[1] === '1', started: false,
        name: clean(f[4], 24) || (c.name + "'s lobby"),
        host: c.id, members: [c.id],
      };
      lobbies.set(code, lb);
      c.lobby = code;
      send(c, ['joined', code, lb.host, lb.mode, lb.max, lb.isPublic ? 1 : 0, lb.name].join('|'));
      send(c, roster(lb));
      break;
    }

    case 'join': {
      const code = cleanCode(f[0]);
      const lb = lobbies.get(code);
      if (!lb) { send(c, 'err|No lobby with that code'); break; }
      if (lb.members.includes(c.id)) break;
      if (lb.started) { send(c, 'err|That match has already started'); break; }
      if (lb.members.length >= lb.max) { send(c, 'err|That lobby is full'); break; }
      if (l) leaveLobby(c);
      lb.members.push(c.id);
      c.lobby = code;
      send(c, ['joined', code, lb.host, lb.mode, lb.max, lb.isPublic ? 1 : 0, lb.name].join('|'));
      broadcast(lb, roster(lb));
      break;
    }

    case 'leave':
      leaveLobby(c, 'The host closed the lobby');
      break;

    case 'start':
      if (!l || l.host !== c.id) break;
      l.started = true;
      broadcast(l, 'started');
      break;

    case 'end':
      if (!l || l.host !== c.id) break;
      l.started = false;
      broadcast(l, 'ended');
      break;

    case 'to': {
      if (!l) break;
      const to = parseInt(f[0], 10);
      if (!l.members.includes(to)) break;
      const payload = rest.slice(rest.indexOf('|') + 1);
      send(clients.get(to), 'm|' + c.id + '|' + payload);
      break;
    }

    case 'all':
      if (!l) break;
      broadcast(l, 'm|' + c.id + '|' + rest, c.id);
      break;

    case 'ping':
      send(c, 'pong|' + f[0]);
      break;
  }
}

function addClient(sendFn, closeFn) {
  const c = { id: nextId++, name: '', lobby: null, alive: true, send: sendFn, close: closeFn };
  clients.set(c.id, c);
  return c;
}
function dropClient(c) {
  if (!c.alive) return;
  c.alive = false;
  leaveLobby(c, 'The host disconnected');
  clients.delete(c.id);
}

// ---- minimal WebSocket server (RFC 6455): text frames, ping/pong, close. No packages needed.
const WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
function wsFrame(op, payload) {
  const len = payload.length;
  let head;
  if (len < 126) { head = Buffer.alloc(2); head[1] = len; }
  else if (len < 65536) { head = Buffer.alloc(4); head[1] = 126; head.writeUInt16BE(len, 2); }
  else { head = Buffer.alloc(10); head[1] = 127; head.writeBigUInt64BE(BigInt(len), 2); }
  head[0] = 0x80 | op;
  return Buffer.concat([head, payload]);
}
function acceptSocket(req, sock) {
  const key = req.headers['sec-websocket-key'];
  if (!key || (req.headers.upgrade || '').toLowerCase() !== 'websocket') { sock.destroy(); return; }
  const accept = crypto.createHash('sha1').update(key + WS_GUID).digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  sock.setNoDelay(true);
  let buf = Buffer.alloc(0), frag = [], fragLen = 0, open = true, alive = true;
  const c = addClient((m) => { if (open && !sock.destroyed) sock.write(wsFrame(1, Buffer.from(m))); }, () => sock.destroy());
  const finish = () => { if (!open) return; open = false; clearInterval(beat); dropClient(c); sock.destroy(); };
  const beat = setInterval(() => {                 // drop connections that stopped answering
    if (!alive) { finish(); return; }
    alive = false;
    if (!sock.destroyed) sock.write(wsFrame(9, Buffer.alloc(0)));
  }, 15000);
  sock.on('data', (d) => {
    buf = Buffer.concat([buf, d]);
    for (;;) {
      if (buf.length < 2) return;
      const fin = (buf[0] & 0x80) !== 0, op = buf[0] & 0x0f, masked = (buf[1] & 0x80) !== 0;
      let len = buf[1] & 0x7f, off = 2;
      if (len === 126) { if (buf.length < 4) return; len = buf.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (buf.length < 10) return; len = Number(buf.readBigUInt64BE(2)); off = 10; }
      if (len > MAX_MSG) { finish(); return; }
      const need = off + (masked ? 4 : 0) + len;
      if (buf.length < need) return;
      let payload = buf.slice(off + (masked ? 4 : 0), need);
      if (masked) {
        const mask = buf.slice(off, off + 4);
        payload = Buffer.from(payload);
        for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i & 3];
      }
      buf = buf.slice(need);
      alive = true;
      if (op === 8) { if (!sock.destroyed) sock.write(wsFrame(8, Buffer.alloc(0))); finish(); return; }
      if (op === 9) { if (!sock.destroyed) sock.write(wsFrame(10, payload)); continue; }
      if (op === 10) continue;
      if (op === 1 || op === 2 || op === 0) {
        frag.push(payload); fragLen += payload.length;
        if (fragLen > MAX_MSG) { finish(); return; }
        if (fin) {
          const msg = Buffer.concat(frag).toString();
          frag = []; fragLen = 0;
          try { handle(c, msg); } catch (e) { console.error(e); }
        }
      }
    }
  });
  sock.on('close', finish);
  sock.on('error', finish);
}

// ---- HTTP (health check / wake-up page) + WebSocket upgrade
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' });
  res.end('CyberStrike server is running. Lobbies: ' + lobbies.size + ', players online: ' + clients.size + '\n');
});
server.on('upgrade', (req, sock) => acceptSocket(req, sock));
server.listen(PORT, () => console.log('CyberStrike server listening on port ' + PORT));

// ---- optional plain TCP (tests)
if (process.env.TCP_PORT) {
  net.createServer((sock) => {
    let buf = '';
    const c = addClient((m) => { if (!sock.destroyed) sock.write(m + '\n'); }, () => sock.destroy());
    sock.setNoDelay(true);
    sock.on('data', (d) => {
      buf += d.toString();
      let i;
      while ((i = buf.indexOf('\n')) >= 0) {
        const line = buf.slice(0, i);
        buf = buf.slice(i + 1);
        if (line) { try { handle(c, line); } catch (e) { console.error(e); } }
      }
    });
    sock.on('close', () => dropClient(c));
    sock.on('error', () => dropClient(c));
  }).listen(parseInt(process.env.TCP_PORT, 10), () => console.log('TCP test port ' + process.env.TCP_PORT));
}
