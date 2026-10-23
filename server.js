// CyberStrike server: lobbies, match relay, accounts, stats, leaderboards and friends.
//
// Players connect with a WebSocket. The server keeps the lobby list (public and private),
// hands out lobby codes, and relays game messages between the players of a lobby. The
// lobby host's game runs the match; everyone else sends their moves to the host. When the host
// leaves (or hands off), the longest-connected player becomes the host and the match goes on.
//
// Accounts: the web page signs up / logs in over HTTPS (POST /api/signup, /api/login, /api/logout,
// /api/delete) and gets a session token; the game then sends auth|token on its WebSocket. Accounts, stats, friends and
// cloud saves are kept in Upstash Redis (free) when UPSTASH_REDIS_REST_URL / _TOKEN are set,
// otherwise in a local file (fine for testing; on Render that file is wiped on every restart).
//
// Protocol: one text message per WebSocket frame, fields separated by '|'.
//   client -> server: hello|ver|name   list   create|code|public|mode|max|lobbyName
//                     join|code   leave   handoff   away|0/1   start   end   to|id|payload   all|payload   ping|t
//                     auth|token   logout   st|kills|deaths|wins|matches|secs   sv|save   lb|kills/secs
//                     prof|name   flist   fadd|name   facc|name   fdec|name   frem|name   inv|name
//   server -> client: welcome|id   lobbies|entries   joined|code|hostId|mode|max|public|name
//                     roster|hostId|id:name;id:name   started   ended   m|fromId|payload
//                     err|text   closed|reason   pong|t
//                     me|name|owner|kills|deaths|wins|matches|secs   autherr|text   save|data
//                     lbd|board|myRank|myValue|name,value,owner;...
//                     profd|name|owner|kills|deaths|wins|matches|secs|status|mode|friend|kRank|hRank
//                     fl|requests(name,name)|friends(name:status:mode:code:owner;...)
//                     fnote|text   invd|fromName|code|mode|lobbyName
//
// Environment: PORT, UPSTASH_REDIS_REST_URL, UPSTASH_REDIS_REST_TOKEN, OWNER (your username: gets
// the OWNER tag), DATA_FILE (local storage file), TCP_PORT (plain TCP for the automated tests).

'use strict';
const http = require('http');
const net = require('net');
const crypto = require('crypto');
const fs = require('fs');

const PORT = parseInt(process.env.PORT || '8080', 10);
const MODES = { duel: 2, dm: 10, casual: 10 };
const MAX_LOBBIES = 500;
const MAX_MSG = 64 * 1024;
const OWNER = String(process.env.OWNER || '').trim().replace(/^@/, '').toLowerCase();

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

//==========================================================================================
// Storage: a few Redis commands, sent to Upstash over HTTPS, or run on a local copy
//==========================================================================================
class MemRedis {
  constructor(file) {
    this.file = file; this.dirty = false;
    this.str = new Map(); this.hash = new Map(); this.set = new Map(); this.zset = new Map(); this.exp = new Map();
    if (file && fs.existsSync(file)) {
      try {
        const d = JSON.parse(fs.readFileSync(file, 'utf8'));
        for (const [k, v] of d.str || []) this.str.set(k, v);
        for (const [k, v] of d.hash || []) this.hash.set(k, new Map(v));
        for (const [k, v] of d.set || []) this.set.set(k, new Set(v));
        for (const [k, v] of d.zset || []) this.zset.set(k, new Map(v));
        for (const [k, v] of d.exp || []) this.exp.set(k, v);
      } catch (e) { console.error('Could not read ' + file + ': ' + e.message); }
    }
    if (file) setInterval(() => this.save(), 3000).unref();
  }
  save() {
    if (!this.dirty || !this.file) return;
    this.dirty = false;
    const d = {
      str: [...this.str], hash: [...this.hash].map(([k, m]) => [k, [...m]]), set: [...this.set].map(([k, s]) => [k, [...s]]),
      zset: [...this.zset].map(([k, m]) => [k, [...m]]), exp: [...this.exp],
    };
    try { fs.writeFileSync(this.file + '.tmp', JSON.stringify(d)); fs.renameSync(this.file + '.tmp', this.file); }
    catch (e) { console.error('Could not write ' + this.file + ': ' + e.message); }
  }
  live(k) {
    const t = this.exp.get(k);
    if (t !== undefined && t <= Date.now()) { this.str.delete(k); this.hash.delete(k); this.set.delete(k); this.zset.delete(k); this.exp.delete(k); }
  }
  exec(a) {
    const cmd = String(a[0]).toUpperCase(), k = String(a[1]);
    this.live(k);
    const write = (v) => { this.dirty = true; return v; };
    const num = (v) => { const s = String(v); return s.includes('.') || s.includes('e') ? String(parseFloat(s)) : s; };
    switch (cmd) {
      case 'GET': return this.str.has(k) ? this.str.get(k) : null;
      case 'SET': {
        let nx = false, ex = 0;
        for (let i = 3; i < a.length; i++) {
          const o = String(a[i]).toUpperCase();
          if (o === 'NX') nx = true; else if (o === 'EX') ex = parseInt(a[++i], 10);
        }
        if (nx && this.str.has(k)) return null;
        this.str.set(k, String(a[2]));
        if (ex > 0) this.exp.set(k, Date.now() + ex * 1000); else this.exp.delete(k);
        return write('OK');
      }
      case 'DEL': {
        let n = 0;
        for (const key of a.slice(1).map(String)) {
          for (const m of [this.str, this.hash, this.set, this.zset]) if (m.delete(key)) n++;
          this.exp.delete(key);
        }
        return write(n);
      }
      case 'INCR': { const v = (parseInt(this.str.get(k) || '0', 10) || 0) + 1; this.str.set(k, String(v)); return write(v); }
      case 'EXPIRE': {
        if (!this.str.has(k) && !this.hash.has(k) && !this.set.has(k) && !this.zset.has(k)) return 0;
        this.exp.set(k, Date.now() + parseInt(a[2], 10) * 1000); return write(1);
      }
      case 'HSET': {
        let h = this.hash.get(k); if (!h) { h = new Map(); this.hash.set(k, h); }
        let n = 0;
        for (let i = 2; i + 1 < a.length; i += 2) { if (!h.has(String(a[i]))) n++; h.set(String(a[i]), String(a[i + 1])); }
        return write(n);
      }
      case 'HGETALL': { const h = this.hash.get(k); const out = []; if (h) for (const [f, v] of h) out.push(f, v); return out; }
      case 'HDEL': {
        const h = this.hash.get(k); if (!h) return 0;
        let n = 0; for (const f of a.slice(2).map(String)) if (h.delete(f)) n++;
        return write(n);
      }
      case 'HMGET': { const h = this.hash.get(k); return a.slice(2).map((f) => (h && h.has(String(f)) ? h.get(String(f)) : null)); }
      case 'HINCRBY': {
        let h = this.hash.get(k); if (!h) { h = new Map(); this.hash.set(k, h); }
        const v = (parseInt(h.get(String(a[2])) || '0', 10) || 0) + parseInt(a[3], 10);
        h.set(String(a[2]), String(v)); return write(v);
      }
      case 'SADD': {
        let s = this.set.get(k); if (!s) { s = new Set(); this.set.set(k, s); }
        let n = 0; for (const m of a.slice(2).map(String)) if (!s.has(m)) { s.add(m); n++; }
        return write(n);
      }
      case 'SREM': {
        const s = this.set.get(k); if (!s) return 0;
        let n = 0; for (const m of a.slice(2).map(String)) if (s.delete(m)) n++;
        if (s.size === 0) this.set.delete(k);
        return write(n);
      }
      case 'SMEMBERS': { const s = this.set.get(k); return s ? [...s] : []; }
      case 'SISMEMBER': { const s = this.set.get(k); return s && s.has(String(a[2])) ? 1 : 0; }
      case 'SCARD': { const s = this.set.get(k); return s ? s.size : 0; }
      case 'ZINCRBY': {
        let z = this.zset.get(k); if (!z) { z = new Map(); this.zset.set(k, z); }
        const v = (z.get(String(a[3])) || 0) + parseFloat(a[2]);
        z.set(String(a[3]), v); return write(num(v));
      }
      case 'ZREM': {
        const z = this.zset.get(k); if (!z) return 0;
        let n = 0; for (const m of a.slice(2).map(String)) if (z.delete(m)) n++;
        return write(n);
      }
      case 'ZSCORE': { const z = this.zset.get(k); return z && z.has(String(a[2])) ? num(z.get(String(a[2]))) : null; }
      case 'ZREVRANGE': case 'ZREVRANK': {
        const z = this.zset.get(k);
        const sorted = z ? [...z].sort((x, y) => (y[1] - x[1]) || (x[0] < y[0] ? 1 : (x[0] > y[0] ? -1 : 0))) : [];
        if (cmd === 'ZREVRANK') { const i = sorted.findIndex((e) => e[0] === String(a[2])); return i < 0 ? null : i; }
        let s = parseInt(a[2], 10), e = parseInt(a[3], 10);
        if (e < 0) e = sorted.length + e;
        const out = [];
        for (let i = Math.max(0, s); i <= e && i < sorted.length; i++) {
          out.push(sorted[i][0]);
          if (a[4] && String(a[4]).toUpperCase() === 'WITHSCORES') out.push(num(sorted[i][1]));
        }
        return out;
      }
      default: throw new Error('ERR unknown command ' + cmd);
    }
  }
}

const UPSTASH_URL = String(process.env.UPSTASH_REDIS_REST_URL || '').trim().replace(/\/+$/, '');
const UPSTASH_TOKEN = String(process.env.UPSTASH_REDIS_REST_TOKEN || '').trim();
const useUpstash = !!(UPSTASH_URL && UPSTASH_TOKEN);
const mem = useUpstash ? null : new MemRedis(process.env.DATA_FILE || 'cyberstrike-data.json');

// db([[cmd, ...args], ...]) -> results, in order. Throws if anything failed.
async function db(cmds) {
  if (!cmds.length) return [];
  const body = cmds.map((c) => c.map((x) => String(x)));
  if (!useUpstash) return body.map((c) => mem.exec(c));
  let lastErr = null;
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const res = await fetch(UPSTASH_URL + '/pipeline', {
        method: 'POST',
        headers: { Authorization: 'Bearer ' + UPSTASH_TOKEN, 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
        signal: AbortSignal.timeout(8000),
      });
      if (res.status === 401 || res.status === 403) throw Object.assign(new Error('Upstash rejected the token (check UPSTASH_REDIS_REST_TOKEN)'), { fatal: true });
      if (!res.ok) throw new Error('Upstash HTTP ' + res.status);
      const out = await res.json();
      if (!Array.isArray(out)) throw new Error('Upstash: unexpected reply');
      return out.map((r) => { if (r && r.error) throw Object.assign(new Error('Upstash: ' + r.error), { fatal: true }); return r ? r.result : null; });
    } catch (e) {
      lastErr = e;
      if (e.fatal) break;
      await new Promise((r) => setTimeout(r, 300 * (attempt + 1)));
    }
  }
  throw lastErr;
}
async function db1(cmd) { return (await db([cmd]))[0]; }

const K = {
  uid: 'cs:uid', email: (e) => 'cs:email:' + e, uname: (n) => 'cs:uname:' + n, user: (u) => 'cs:user:' + u,
  names: 'cs:names', sess: (h) => 'cs:sess:' + h, fr: (u) => 'cs:fr:' + u, rq: (u) => 'cs:rq:' + u,
  save: (u) => 'cs:save:' + u, lb: (b) => 'cs:lb:' + b,
};

//==========================================================================================
// Accounts
//==========================================================================================
const SESSION_DAYS = 60;
const RESERVED = new Set(['admin', 'administrator', 'owner', 'moderator', 'mod', 'support', 'staff', 'cyberstrike', 'system',
  'server', 'guest', 'null', 'undefined', 'anonymous', 'official', 'valve', 'everyone', 'host', 'player']);
class UserError extends Error {}

function hashToken(t) { return crypto.createHash('sha256').update(String(t)).digest('hex'); }
function newToken() { return crypto.randomBytes(32).toString('base64url'); }
function scrypt(pw, salt) {
  return new Promise((ok, bad) => crypto.scrypt(pw, salt, 32, { N: 16384, r: 8, p: 1, maxmem: 64 * 1024 * 1024 }, (e, k) => (e ? bad(e) : ok(k))));
}
async function hashPassword(pw) {
  const salt = crypto.randomBytes(16);
  return 's1$' + salt.toString('hex') + '$' + (await scrypt(pw, salt)).toString('hex');
}
async function checkPassword(pw, stored) {
  const p = String(stored || '').split('$');
  if (p.length !== 3 || p[0] !== 's1') return false;
  const want = Buffer.from(p[2], 'hex');
  const got = await scrypt(pw, Buffer.from(p[1], 'hex'));
  return want.length === got.length && crypto.timingSafeEqual(want, got);
}
function validEmail(e) { return e.length <= 254 && /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(e); }
function isOwner(name) { return !!OWNER && String(name).toLowerCase() === OWNER; }

// failed logins / sign-ups per IP and per account, kept in memory
const limits = new Map();
function limited(key, max, windowMs) {
  const now = Date.now(), e = limits.get(key);
  return !!(e && e.until > now && e.n >= max);
}
function bump(key, windowMs) {
  const now = Date.now();
  let e = limits.get(key);
  if (!e || e.until <= now) { e = { n: 0, until: now + windowMs }; limits.set(key, e); }
  e.n++;
}
setInterval(() => { const now = Date.now(); for (const [k, e] of limits) if (e.until <= now) limits.delete(k); }, 60000).unref();

async function newSession(uid) {
  const token = newToken();
  await db1(['SET', K.sess(hashToken(token)), uid, 'EX', SESSION_DAYS * 86400]);
  return token;
}

async function signup(ip, email, password, name) {
  email = String(email || '').trim().toLowerCase();
  name = String(name || '').trim().replace(/^@/, '');
  password = String(password || '');
  if (!validEmail(email)) throw new UserError('Enter a valid email address.');
  if (!/^[A-Za-z0-9_]{3,15}$/.test(name)) throw new UserError('Usernames are 3 to 15 letters, numbers or _ (no spaces).');
  if (RESERVED.has(name.toLowerCase()) || /^bot/i.test(name)) throw new UserError('That username is not available.');
  if (password.length < 8) throw new UserError('Use at least 8 characters for your password.');
  if (password.length > 128) throw new UserError('That password is too long.');
  if (limited('su:' + ip, 5)) throw new UserError('Too many new accounts from your network. Try again in an hour.');
  const pw = await hashPassword(password);
  const lname = name.toLowerCase();
  const [uid] = await db([['INCR', K.uid]]);
  const [okEmail] = await db([['SET', K.email(email), uid, 'NX']]);
  if (!okEmail) throw new UserError('There is already an account with this email. Log in instead.');
  const [okName] = await db([['SET', K.uname(lname), uid, 'NX']]);
  if (!okName) { await db([['DEL', K.email(email)]]); throw new UserError('That username is taken.'); }
  await db([
    ['HSET', K.user(uid), 'email', email, 'name', name, 'pw', pw, 'created', Date.now(), 'kills', 0, 'deaths', 0, 'wins', 0, 'matches', 0, 'secs', 0],
    ['HSET', K.names, uid, name],
  ]);
  bump('su:' + ip, 3600 * 1000);
  return { token: await newSession(uid), name };
}

async function login(ip, ident, password) {
  ident = String(ident || '').trim().replace(/^@/, '').toLowerCase();
  password = String(password || '');
  if (!ident || !password) throw new UserError('Enter your email and password.');
  if (limited('li:' + ip, 10) || limited('la:' + ident, 10)) throw new UserError('Too many tries. Wait 15 minutes and try again.');
  const uid = await db1(['GET', ident.includes('@') ? K.email(ident) : K.uname(ident)]);
  const user = uid ? hashOf(await db1(['HGETALL', K.user(uid)])) : null;
  if (!user || !(await checkPassword(password, user.pw))) {
    bump('li:' + ip, 15 * 60 * 1000); bump('la:' + ident, 15 * 60 * 1000);
    throw new UserError('Wrong email or password.');
  }
  return { token: await newSession(uid), name: user.name };
}

// delete an account and everything stored for it (asked for with the email/username and password)
async function deleteAccount(ip, ident, password) {
  ident = String(ident || '').trim().replace(/^@/, '').toLowerCase();
  password = String(password || '');
  if (!ident || !password) throw new UserError('Enter your email and password.');
  if (limited('li:' + ip, 10) || limited('la:' + ident, 10)) throw new UserError('Too many tries. Wait 15 minutes and try again.');
  const uid = await db1(['GET', ident.includes('@') ? K.email(ident) : K.uname(ident)]);
  const user = uid ? hashOf(await db1(['HGETALL', K.user(uid)])) : null;
  if (!user || !(await checkPassword(password, user.pw))) {
    bump('li:' + ip, 15 * 60 * 1000); bump('la:' + ident, 15 * 60 * 1000);
    throw new UserError('Wrong email or password.');
  }
  const id = String(uid);
  const u = users.get(id);
  if (u) u.deleted = true;
  const fr = await db1(['SMEMBERS', K.fr(id)]) || [];
  const cmds = fr.map((f) => ['SREM', K.fr(f), id]);
  cmds.push(['DEL', K.user(id), K.email(user.email), K.uname(String(user.name).toLowerCase()), K.save(id), K.fr(id), K.rq(id)],
    ['HDEL', K.names, id], ['ZREM', K.lb('kills'), id], ['ZREM', K.lb('secs'), id]);
  await db(cmds);
  nameCache.delete(id); lbCache.clear();
  for (const o of users.values()) {
    if (o.uid === id) continue;
    const had = o.friends.delete(id) | o.reqs.delete(id);
    if (had) sendFl(o.uid);
  }
  if (u) for (const c of [...u.conns]) { send(c, 'autherr|Your account was deleted.'); detach(c); }
  users.delete(id);
  return {};
}

function hashOf(arr) { const o = {}; if (Array.isArray(arr)) for (let i = 0; i + 1 < arr.length; i += 2) o[arr[i]] = arr[i + 1]; return o; }

//==========================================================================================
// Logged-in players: stats, friends, presence, cloud saves (kept in memory while online)
//==========================================================================================
const users = new Map();            // uid -> { uid, name, conns:Set, friends:Set, reqs:Set, base:{}, pend:{}, save, saveAt, lastFlush }
const nameCache = new Map();        // uid -> name
const STAT_KEYS = ['kills', 'deaths', 'wins', 'matches', 'secs'];
const FLUSH_MS = 3 * 60 * 1000, SAVE_FLUSH_MS = 45 * 1000;

function statusOf(u) {
  // 0 offline, 1 online, 2 in a lobby, 3 in a match. Also returns the mode and (public, waiting lobbies) the code.
  let best = { s: 0, mode: '', code: '' };
  if (!u) return best;
  for (const c of u.conns) {
    const l = lobbyOf(c);
    const s = l ? (l.started ? 3 : 2) : 1;
    if (s > best.s) best = { s, mode: l ? l.mode : '', code: l && l.isPublic && !l.started ? l.code : '' };
  }
  return best;
}
function totals(u) { const t = {}; for (const k of STAT_KEYS) t[k] = (u.base[k] || 0) + (u.pend[k] || 0); return t; }
function sendUser(u, msg) { for (const c of u.conns) send(c, msg); }
function meMsg(u) { const t = totals(u); return ['me', u.name, isOwner(u.name) ? 1 : 0, t.kills, t.deaths, t.wins, t.matches, Math.floor(t.secs)].join('|'); }

function flMsg(u) {
  const reqs = [...u.reqs].map((id) => nameCache.get(id)).filter(Boolean).join(',');
  const list = [...u.friends].map((id) => {
    const nm = nameCache.get(id);
    if (!nm) return null;
    const st = statusOf(users.get(id));
    return [nm, st.s, st.mode, st.code, isOwner(nm) ? 1 : 0].join(':');
  }).filter(Boolean).join(';');
  return 'fl|' + reqs + '|' + list;
}
function sendFl(uid) { const u = users.get(uid); if (u) sendUser(u, flMsg(u)); }

// presence: when a player's status changes, their online friends get a fresh list (batched)
const dirtyPresence = new Set();
let presenceTimer = null;
function touch(c) {
  if (!c || !c.uid) return;
  dirtyPresence.add(c.uid);
  if (!presenceTimer) presenceTimer = setTimeout(() => {
    presenceTimer = null;
    const notify = new Set();
    for (const uid of dirtyPresence) { const u = users.get(uid); const fr = u ? u.friends : offlineFriends.get(uid); if (fr) for (const f of fr) notify.add(f); offlineFriends.delete(uid); }
    dirtyPresence.clear();
    for (const f of notify) sendFl(f);
  }, 200);
}
const offlineFriends = new Map();   // uid -> friends, for the "went offline" notice after the last connection closed

async function namesFor(ids) {
  const miss = [...new Set(ids)].filter((id) => !nameCache.has(String(id)));
  if (miss.length) {
    const got = await db1(['HMGET', K.names, ...miss]);
    miss.forEach((id, i) => { if (got && got[i]) nameCache.set(String(id), got[i]); });
  }
}

async function attach(c, token) {
  const uid = await db1(['GET', K.sess(hashToken(token))]);
  if (!uid) { send(c, 'autherr|Your login expired. Please log in again.'); return; }
  if (!c.alive) return;
  if (c.uid && c.uid !== uid) detach(c);
  let u = users.get(uid);
  if (!u) {
    const [h, fr, rq, save] = await db([['HGETALL', K.user(uid)], ['SMEMBERS', K.fr(uid)], ['SMEMBERS', K.rq(uid)], ['GET', K.save(uid)], ['EXPIRE', K.sess(hashToken(token)), SESSION_DAYS * 86400]]);
    const o = hashOf(h);
    if (!o.name) { send(c, 'autherr|That account no longer exists.'); return; }
    u = users.get(uid);          // another connection may have loaded it meanwhile
    if (!u) {
      u = { uid, name: o.name, conns: new Set(), friends: new Set(fr || []), reqs: new Set(rq || []), base: {}, pend: {}, save: save || '', saveDirty: false, saveAt: 0, lastFlush: Date.now() };
      for (const k of STAT_KEYS) u.base[k] = parseInt(o[k] || '0', 10) || 0;
      users.set(uid, u);
    }
    nameCache.set(uid, o.name);
    await namesFor([...u.friends, ...u.reqs]);
  }
  if (!c.alive) return;
  c.uid = uid; c.statClock = Date.now();
  u.conns.add(c);
  c.name = u.name;
  send(c, meMsg(u));
  send(c, 'save|' + (u.save || ''));
  send(c, flMsg(u));
  const l = lobbyOf(c);
  if (l) broadcast(l, roster(l));
  touch(c);
}

function detach(c) {
  const uid = c.uid;
  if (!uid) return;
  touch(c);
  c.uid = null;
  const u = users.get(uid);
  if (!u) return;
  u.conns.delete(c);
  if (u.conns.size === 0) {
    offlineFriends.set(uid, u.friends);
    flushUser(u).catch((e) => console.error('flush', e.message)).finally(() => { if (u.conns.size === 0 && users.get(uid) === u) users.delete(uid); });
  }
}

async function flushUser(u) {
  if (u.deleted) return;
  const p = u.pend, cmds = [];
  const delta = {};
  for (const k of STAT_KEYS) { delta[k] = Math.floor(p[k] || 0); if (delta[k] > 0) cmds.push(['HINCRBY', K.user(u.uid), k, delta[k]]); }
  if (delta.kills > 0) cmds.push(['ZINCRBY', K.lb('kills'), delta.kills, u.uid]);
  if (delta.secs > 0) cmds.push(['ZINCRBY', K.lb('secs'), delta.secs, u.uid]);
  const saveNow = u.saveDirty ? u.save : null;
  if (saveNow !== null) cmds.push(['SET', K.save(u.uid), saveNow]);
  if (!cmds.length) return;
  // move what we're writing out of "pending" first, so new reports during the write aren't lost
  for (const k of STAT_KEYS) { p[k] = (p[k] || 0) - delta[k]; u.base[k] += delta[k]; }
  if (saveNow !== null) u.saveDirty = false;
  u.lastFlush = Date.now();
  try { await db(cmds); }
  catch (e) {
    for (const k of STAT_KEYS) { p[k] = (p[k] || 0) + delta[k]; u.base[k] -= delta[k]; }
    if (saveNow !== null && !u.saveDirty) u.saveDirty = true;
    throw e;
  }
  if (delta.kills > 0 || delta.secs > 0) lbCache.clear();
}
setInterval(() => {
  const now = Date.now();
  for (const u of users.values()) {
    const statsDue = now - u.lastFlush > FLUSH_MS && STAT_KEYS.some((k) => (u.pend[k] || 0) >= 1);
    const saveDue = u.saveDirty && now - u.saveAt > SAVE_FLUSH_MS;
    if (statsDue || saveDue) flushUser(u).catch((e) => console.error('flush', e.message));
  }
}, 15000).unref();

// a stats report from the game: what happened since the last report. Checked against the clock.
function addStats(c, f) {
  const u = users.get(c.uid);
  if (!u) return;
  const now = Date.now();
  const elapsed = (now - (c.statClock || now)) / 1000 + 3;
  c.statClock = now;
  const v = f.slice(0, 5).map((x) => Math.max(0, parseInt(x, 10) || 0));
  let [kills, deaths, wins, matches, secs] = v;
  secs = Math.min(secs, elapsed);
  kills = Math.min(kills, 3 + Math.ceil(secs));
  deaths = Math.min(deaths, 3 + Math.ceil(secs / 2));
  matches = Math.min(matches, 1 + Math.floor(secs / 30));
  wins = Math.min(wins, matches);
  const add = { kills, deaths, wins, matches, secs };
  for (const k of STAT_KEYS) u.pend[k] = (u.pend[k] || 0) + add[k];
  if (matches > 0) flushUser(u).catch((e) => console.error('flush', e.message));    // a match ended: save now
  sendUser(u, meMsg(u));
}

// leaderboards: top 50, cached for 30 seconds
const lbCache = new Map();
async function leaderboard(c, board) {
  if (board !== 'kills' && board !== 'secs') return;
  const asker = c.uid ? users.get(c.uid) : null;          // your own numbers are always current on the board
  if (asker && STAT_KEYS.some((k) => (asker.pend[k] || 0) >= 1)) await flushUser(asker).catch(() => {});
  let top = lbCache.get(board);
  if (!top || top.at < Date.now() - 30000) {
    const flat = await db1(['ZREVRANGE', K.lb(board), 0, 49, 'WITHSCORES']) || [];
    const ids = [], vals = [];
    for (let i = 0; i + 1 < flat.length; i += 2) { ids.push(String(flat[i])); vals.push(Math.floor(parseFloat(flat[i + 1]) || 0)); }
    await namesFor(ids);
    top = { at: Date.now(), rows: ids.map((id, i) => ({ id, name: nameCache.get(id) || '?', v: vals[i] })) };
    lbCache.set(board, top);
  }
  let myRank = 0, myVal = 0;
  if (c.uid) {
    const u = users.get(c.uid);
    const i = top.rows.findIndex((r) => r.id === c.uid);
    if (i >= 0) { myRank = i + 1; myVal = top.rows[i].v; }
    else {
      const [rk, sc] = await db([['ZREVRANK', K.lb(board), c.uid], ['ZSCORE', K.lb(board), c.uid]]);
      if (rk !== null && rk !== undefined) { myRank = parseInt(rk, 10) + 1; myVal = Math.floor(parseFloat(sc) || 0); }
    }
    if (u && !myRank) myVal = Math.floor(board === 'kills' ? totals(u).kills : totals(u).secs);
  }
  send(c, ['lbd', board, myRank, myVal, top.rows.map((r) => [r.name, r.v, isOwner(r.name) ? 1 : 0].join(',')).join(';')].join('|'));
}

async function uidByName(name) {
  const n = String(name || '').trim().replace(/^@/, '').toLowerCase();
  if (!/^[a-z0-9_]{3,15}$/.test(n)) return null;
  for (const [id, nm] of nameCache) if (nm.toLowerCase() === n) return id;
  const id = await db1(['GET', K.uname(n)]);
  if (id) await namesFor([id]);
  return id ? String(id) : null;
}

async function profile(c, name) {
  const id = await uidByName(name);
  if (!id) { send(c, 'fnote|No player called @' + clean(name, 20)); return; }
  let t;
  const on = users.get(id);
  if (on) t = totals(on);
  else { const o = hashOf(await db1(['HGETALL', K.user(id)])); t = {}; for (const k of STAT_KEYS) t[k] = parseInt(o[k] || '0', 10) || 0; }
  const [kr, hr] = await db([['ZREVRANK', K.lb('kills'), id], ['ZREVRANK', K.lb('secs'), id]]);
  const me = c.uid ? users.get(c.uid) : null;
  const st = statusOf(on);
  const nm = nameCache.get(id);
  send(c, ['profd', nm, isOwner(nm) ? 1 : 0, t.kills, t.deaths, t.wins, t.matches, Math.floor(t.secs), st.s, st.mode,
    me && me.friends.has(id) ? 1 : (id === c.uid ? 2 : 0), kr === null || kr === undefined ? 0 : parseInt(kr, 10) + 1,
    hr === null || hr === undefined ? 0 : parseInt(hr, 10) + 1].join('|'));
}

async function friendCmd(c, cmd, name) {
  const me = users.get(c.uid);
  if (!me) { send(c, 'fnote|Log in to add friends'); return; }
  const id = await uidByName(name);
  const nm = id ? nameCache.get(id) : clean(name, 20);
  if (!id) { send(c, 'fnote|No player called @' + nm.replace(/^@/, '')); return; }
  if (id === me.uid) { send(c, 'fnote|That is you'); return; }
  const them = users.get(id);
  if (cmd === 'fadd') {
    if (me.friends.has(id)) { send(c, 'fnote|@' + nm + ' is already your friend'); return; }
    if (me.reqs.has(id)) { await friendCmd(c, 'facc', nm); return; }      // they asked first: you're friends now
    if (me.friends.size >= 200) { send(c, 'fnote|You have the maximum of 200 friends'); return; }
    const n = await db1(['SCARD', K.rq(id)]);
    if (n >= 100) { send(c, 'fnote|@' + nm + ' has too many friend requests right now'); return; }
    await db1(['SADD', K.rq(id), me.uid]);
    if (them) { them.reqs.add(me.uid); sendFl(id); sendUser(them, 'fnote|@' + me.name + ' sent you a friend request'); }
    send(c, 'fnote|Friend request sent to @' + nm);
  } else if (cmd === 'facc') {
    if (!me.reqs.has(id)) { send(c, 'fnote|No friend request from @' + nm); return; }
    await db([['SREM', K.rq(me.uid), id], ['SREM', K.rq(id), me.uid], ['SADD', K.fr(me.uid), id], ['SADD', K.fr(id), me.uid]]);
    me.reqs.delete(id); me.friends.add(id);
    if (them) { them.reqs.delete(me.uid); them.friends.add(me.uid); sendFl(id); sendUser(them, 'fnote|@' + me.name + ' accepted your friend request'); }
    sendFl(me.uid);
    send(c, 'fnote|You and @' + nm + ' are now friends');
  } else if (cmd === 'fdec') {
    await db1(['SREM', K.rq(me.uid), id]);
    me.reqs.delete(id);
    sendFl(me.uid);
  } else if (cmd === 'frem') {
    await db([['SREM', K.fr(me.uid), id], ['SREM', K.fr(id), me.uid]]);
    me.friends.delete(id);
    if (them) { them.friends.delete(me.uid); sendFl(id); }
    sendFl(me.uid);
    send(c, 'fnote|Removed @' + nm + ' from your friends');
  }
}

function invite(c, name) {
  const me = users.get(c.uid);
  if (!me) { send(c, 'fnote|Log in to invite friends'); return; }
  const l = lobbyOf(c);
  if (!l) { send(c, 'fnote|Create or join a lobby first'); return; }
  if (l.started) { send(c, 'fnote|The match has already started'); return; }
  let id = null;
  for (const f of me.friends) if ((nameCache.get(f) || '').toLowerCase() === String(name).replace(/^@/, '').toLowerCase()) id = f;
  if (!id) { send(c, 'fnote|You can only invite friends'); return; }
  const them = users.get(id);
  if (!them || them.conns.size === 0) { send(c, 'fnote|@' + nameCache.get(id) + ' is offline'); return; }
  const key = 'inv:' + me.uid + ':' + id;
  if (limited(key, 1)) { send(c, 'fnote|You just invited @' + nameCache.get(id)); return; }
  bump(key, 5000);
  sendUser(them, ['invd', me.name, l.code, l.mode, l.name].join('|'));
  send(c, 'fnote|Invite sent to @' + nameCache.get(id));
}

//==========================================================================================
// Lobbies
//==========================================================================================
function send(c, msg) { if (c && c.alive) c.send(msg); }
function lobbyOf(c) { return c.lobby ? lobbies.get(c.lobby) : null; }

function roster(l) {
  const list = l.members.map((id) => id + ':' + (clients.get(id) ? clients.get(id).name : '?')).join(';');
  return 'roster|' + l.host + '|' + list;
}
function broadcast(l, msg, except) {
  for (const id of l.members) if (id !== except) send(clients.get(id), msg);
}
function touchLobby(l) { if (l) for (const id of l.members) touch(clients.get(id)); }

function leaveLobby(c, reason) {
  const l = lobbyOf(c);
  c.lobby = null;
  if (!l) return;
  touch(c);
  l.members = l.members.filter((id) => id !== c.id);
  if (l.members.length === 0) { lobbies.delete(l.code); return; }
  // the host's game runs the match: when the host goes, the longest-connected player takes over
  if (l.host === c.id) l.host = nextHost(l, c.id);
  broadcast(l, roster(l));
}

// next host: the longest-connected player whose game is in front (not in a background tab)
function nextHost(l, except) {
  const others = l.members.filter((id) => id !== except);
  const here = others.filter((id) => clients.get(id) && !clients.get(id).away);
  return (here.length ? here : others)[0];
}

// the host passes hosting to the next player but stays in the lobby (for example: their tab went to the background)
function handOff(c) {
  const l = lobbyOf(c);
  if (!l || l.host !== c.id || l.members.length < 2) return;
  const next = nextHost(l, c.id);
  if (clients.get(next) && clients.get(next).away) return;      // nobody else can take over right now
  l.host = next;
  broadcast(l, roster(l));
}

function handle(c, msg) {
  if (msg.length > MAX_MSG) return;
  const bar = msg.indexOf('|');
  const cmd = bar < 0 ? msg : msg.slice(0, bar);
  const rest = bar < 0 ? '' : msg.slice(bar + 1);
  const f = rest.split('|');
  const l = lobbyOf(c);
  const later = (p) => p.catch((e) => { console.error(cmd + ': ' + e.message); send(c, 'fnote|The server could not do that right now, try again'); });

  switch (cmd) {
    case 'hello':
      if (!c.uid) c.name = clean(f[1], 15) || ('Player' + c.id);
      send(c, 'welcome|' + c.id);
      break;

    case 'name':
      if (c.uid) break;                 // account names are fixed
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
      touch(c);
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
      touch(c);
      break;
    }

    case 'leave':
      leaveLobby(c);
      break;

    case 'handoff':
      handOff(c);
      break;

    case 'away':                    // away|1: tab in the background, away|0: back
      c.away = f[0] === '1';
      if (!l || !l.started) break;
      if (l.host === c.id) { if (c.away) handOff(c); }
      else send(clients.get(l.host), 'm|' + c.id + '|' + (c.away ? 'AW' : 'BK'));
      break;

    case 'start':
      if (!l || l.host !== c.id) break;
      l.started = true;
      broadcast(l, 'started');
      touchLobby(l);
      break;

    case 'end':
      if (!l || l.host !== c.id) break;
      l.started = false;
      broadcast(l, 'ended');
      touchLobby(l);
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

    // ---- accounts
    case 'auth':
      later(attach(c, f[0]));
      break;

    case 'logout':
      if (c.uid) { detach(c); c.name = clean(f[0], 15) || ('Player' + c.id); if (l) broadcast(l, roster(l)); }
      break;

    case 'st':
      if (c.uid) addStats(c, f);
      break;

    case 'sv': {
      const u = c.uid ? users.get(c.uid) : null;
      if (!u || rest.length > 48 * 1024) break;
      if (u.save !== rest) { u.save = rest; if (!u.saveDirty) u.saveAt = Date.now(); u.saveDirty = true; }
      break;
    }

    case 'lb':
      later(leaderboard(c, f[0]));
      break;

    case 'prof':
      later(profile(c, f[0]));
      break;

    case 'flist':
      if (c.uid && users.get(c.uid)) send(c, flMsg(users.get(c.uid)));
      break;

    case 'fadd': case 'facc': case 'fdec': case 'frem':
      later(friendCmd(c, cmd, f[0]));
      break;

    case 'inv':
      invite(c, f[0]);
      break;
  }
}

function addClient(sendFn, closeFn) {
  const c = { id: nextId++, name: '', lobby: null, alive: true, away: false, uid: null, send: sendFn, close: closeFn };
  clients.set(c.id, c);
  return c;
}
function dropClient(c) {
  if (!c.alive) return;
  c.alive = false;
  leaveLobby(c, 'The host disconnected');
  detach(c);
  clients.delete(c.id);
}

//==========================================================================================
// Minimal WebSocket server (RFC 6455): text frames, ping/pong, close. No packages needed.
//==========================================================================================
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
  // drop connections that stopped answering (a closed laptop, lost Wi-Fi) quickly, so the match can move to a new host.
  // Browsers answer pings even when the tab is in the background.
  const finish = () => { if (!open) return; open = false; clearInterval(beat); dropClient(c); sock.destroy(); };
  const beat = setInterval(() => {
    if (!alive) { finish(); return; }
    alive = false;
    if (!sock.destroyed) sock.write(wsFrame(9, Buffer.alloc(0)));
  }, 4000);
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

//==========================================================================================
// HTTP: status page, account API (sign up / log in / log out), WebSocket upgrade
//==========================================================================================
const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Access-Control-Max-Age': '86400',
};
function reply(res, status, obj) {
  res.writeHead(status, Object.assign({ 'Content-Type': 'application/json', 'Cache-Control': 'no-store' }, CORS));
  res.end(JSON.stringify(obj));
}
function clientIp(req) {
  const xf = String(req.headers['x-forwarded-for'] || '').split(',')[0].trim();
  return xf || req.socket.remoteAddress || '?';
}
function readJson(req) {
  return new Promise((ok, bad) => {
    let size = 0; const parts = [];
    req.on('data', (d) => { size += d.length; if (size > 8192) { bad(new UserError('Request too large.')); req.destroy(); } else parts.push(d); });
    req.on('end', () => { try { ok(JSON.parse(Buffer.concat(parts).toString() || '{}')); } catch (e) { bad(new UserError('Bad request.')); } });
    req.on('error', bad);
  });
}

const server = http.createServer(async (req, res) => {
  const url = (req.url || '/').split('?')[0];
  if (req.method === 'OPTIONS') { res.writeHead(204, CORS); res.end(); return; }
  if (url.startsWith('/api/')) {
    if (req.method !== 'POST') { reply(res, 405, { error: 'Use POST.' }); return; }
    try {
      const body = await readJson(req);
      const ip = clientIp(req);
      if (url === '/api/signup') reply(res, 200, Object.assign({ ok: true }, await signup(ip, body.email, body.password, body.username)));
      else if (url === '/api/login') reply(res, 200, Object.assign({ ok: true }, await login(ip, body.email, body.password)));
      else if (url === '/api/delete') reply(res, 200, Object.assign({ ok: true }, await deleteAccount(ip, body.email, body.password)));
      else if (url === '/api/logout') { if (body.token) await db1(['DEL', K.sess(hashToken(body.token))]); reply(res, 200, { ok: true }); }
      else reply(res, 404, { error: 'Not found.' });
    } catch (e) {
      if (e instanceof UserError) reply(res, 400, { error: e.message });
      else { console.error(url + ': ' + (e && e.message)); reply(res, 503, { error: 'The server could not reach its database. Try again in a minute.' }); }
    }
    return;
  }
  res.writeHead(200, Object.assign({ 'Content-Type': 'text/plain' }, CORS));
  res.end('CyberStrike server is running. Lobbies: ' + lobbies.size + ', players online: ' + clients.size + '\n' +
    (useUpstash ? 'Accounts are saved in Upstash.\n' : 'Accounts are saved in a local file. On Render, add UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN or accounts are wiped on every restart!\n') +
    (OWNER ? 'Owner: @' + OWNER + '\n' : ''));
});
server.on('upgrade', (req, sock) => acceptSocket(req, sock));
server.listen(PORT, () => console.log('CyberStrike server listening on port ' + PORT + (useUpstash ? ' (Upstash storage)' : ' (local file storage)')));

// save everything before the server is stopped (Render stops it on deploys and when it goes to sleep)
let stopping = false;
async function shutdown() {
  if (stopping) return;
  stopping = true;
  await Promise.allSettled([...users.values()].map((u) => flushUser(u)));
  if (mem) { mem.dirty = true; mem.save(); }
  process.exit(0);
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

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
