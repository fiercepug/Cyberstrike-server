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
//                     auth|token   logout   st|kills|deaths|wins|matches|secs   sv|save   lb|kills/secs/credits
//                     prof|name   flist   fadd|name   facc|name   fdec|name   frem|name   inv|name
//                     ac|kind|detail (the game's anti-cheat flagged something)   kick|id|reason (host only)
//   server -> client: welcome|id   lobbies|entries   joined|code|hostId|mode|max|public|name
//                     roster|hostId|id:name;id:name   started   ended   m|fromId|payload
//                     err|text   closed|reason   pong|t
//                     me|name|owner|kills|deaths|wins|matches|secs   autherr|text   save|data
//                     svx|reason|data (a save was refused: data is the last good save)
//                     staff|owner/tester/(empty) (after logging in: whether the account gets the owner panel)
//                     staffids|id,id (with every roster: lobby members logged in to staff accounts; the host trusts their panel)
//                     lbd|board|myRank|myValue|name,value,owner;...
//                     profd|name|owner|kills|deaths|wins|matches|secs|status|mode|friend|kRank|hRank
//                     fl|requests(name,name)|friends(name:status:mode:code:owner;...)
//                     fnote|text   invd|fromName|code|mode|lobbyName
//
// Environment: PORT, UPSTASH_REDIS_REST_URL, UPSTASH_REDIS_REST_TOKEN, OWNER (your username: gets
// the OWNER tag), DATA_FILE (local storage file), TCP_PORT (plain TCP for the automated tests),
// ADMIN_KEY (a password for POST /api/admin/unflag, which puts a wrongly flagged account back on the boards),
// STAFF (optional, replaces the built-in list fiercepug6:owner,PRO:owner): accounts that get the owner panel,
// email verification (optional): MAIL_FROM ("CyberStrike <you@gmail.com>") plus BREVO_API_KEY or RESEND_API_KEY.
//   With them set, new accounts get a 6-digit code by email and must enter it before they can log in.
//   Without them, sign-up works without verification (as before).
//
// Anti-cheat: stats reports are checked against the clock and against what a person can do; the game
// reports matches its own anti-cheat flagged (ac|...). An account with AC_LIMIT strikes is flagged:
// it keeps playing but leaves the leaderboards.
//
// Economy check: every cloud save is compared with the last accepted one. New credits, plus the price of the
// cases behind any new skins, minus what sold skins were worth, must fit what can be earned by playing in the
// time that passed. Saves that don't fit (credits or skins added from outside the game) are refused and the
// game gets the last good save back.

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
const ADMIN_KEY = String(process.env.ADMIN_KEY || '');
const AC_LIMIT = 3;

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
        let nx = false, ex = 0, keep = false;
        for (let i = 3; i < a.length; i++) {
          const o = String(a[i]).toUpperCase();
          if (o === 'NX') nx = true; else if (o === 'EX') ex = parseInt(a[++i], 10); else if (o === 'KEEPTTL') keep = true;
        }
        if (nx && this.str.has(k)) return null;
        this.str.set(k, String(a[2]));
        if (ex > 0) this.exp.set(k, Date.now() + ex * 1000); else if (!keep) this.exp.delete(k);
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
      case 'ZADD': {
        let z = this.zset.get(k); if (!z) { z = new Map(); this.zset.set(k, z); }
        const had = z.has(String(a[3])); z.set(String(a[3]), parseFloat(a[2])); return write(had ? 0 : 1);
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
  save: (u) => 'cs:save:' + u, lb: (b) => 'cs:lb:' + b, vcode: (u) => 'cs:vcode:' + u,
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
// Staff accounts get the owner panel in the game: 'owner' = everything, 'tester' = bug tools, credits and skins.
// Their saves skip the economy check (the panel gives credits and skins) and they're kept off the credits leaderboard.
// STAFF (env) replaces the list: "name:owner,name:tester".
const STAFF = (() => {
  const m = new Map([['fiercepug6', 'owner'], ['pro', 'owner']]);
  if (process.env.STAFF !== undefined) {
    m.clear();
    for (const part of String(process.env.STAFF).split(',')) {
      const [n, r] = part.split(':').map((x) => String(x || '').trim());
      if (n && (r === 'owner' || r === 'tester')) m.set(n.replace(/^@/, '').toLowerCase(), r);
    }
  }
  return m;
})();
function staffRole(name) { return STAFF.get(String(name || '').toLowerCase()) || ''; }

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

// ---- email verification (only when a mail service is configured)
const MAIL_FROM = String(process.env.MAIL_FROM || '').trim();
const BREVO_KEY = String(process.env.BREVO_API_KEY || '').trim();
const RESEND_KEY = String(process.env.RESEND_API_KEY || '').trim();
const mailOn = !!(MAIL_FROM && (BREVO_KEY || RESEND_KEY));
const CODE_MINUTES = 30, CODE_TRIES = 5, RESEND_SECONDS = 60;
function mailFrom() {
  const m = /^\s*(.*?)\s*<\s*([^>]+)\s*>\s*$/.exec(MAIL_FROM);
  return m ? { name: m[1].replace(/^"|"$/g, '') || 'CyberStrike', email: m[2] } : { name: 'CyberStrike', email: MAIL_FROM };
}
async function sendMail(to, subject, text, html) {
  const from = mailFrom();
  let res;
  if (BREVO_KEY) {
    res = await fetch(process.env.BREVO_API_URL || 'https://api.brevo.com/v3/smtp/email', {
      method: 'POST', headers: { 'api-key': BREVO_KEY, 'Content-Type': 'application/json', Accept: 'application/json' },
      body: JSON.stringify({ sender: from, to: [{ email: to }], subject, textContent: text, htmlContent: html }),
      signal: AbortSignal.timeout(10000),
    });
  } else {
    res = await fetch('https://api.resend.com/emails', {
      method: 'POST', headers: { Authorization: 'Bearer ' + RESEND_KEY, 'Content-Type': 'application/json' },
      body: JSON.stringify({ from: from.name + ' <' + from.email + '>', to: [to], subject, text, html }),
      signal: AbortSignal.timeout(10000),
    });
  }
  if (!res.ok) throw new Error('mail service HTTP ' + res.status + ' ' + (await res.text().catch(() => '')).slice(0, 200));
}
async function sendCode(uid, email, name) {
  const code = String(crypto.randomInt(0, 1000000)).padStart(6, '0');
  await db([['SET', K.vcode(uid), JSON.stringify({ h: hashToken(code), n: 0 }), 'EX', CODE_MINUTES * 60],
    ['HSET', K.user(uid), 'vsent', Date.now()]]);
  const text = 'Hi ' + name + ',\n\nYour CyberStrike verification code is ' + code + '\n\nEnter it in the game to finish creating your account. ' +
    'It works for ' + CODE_MINUTES + ' minutes.\n\nIf you didn\'t sign up for CyberStrike, you can ignore this email.';
  const html = '<div style="font-family:Segoe UI,Arial,sans-serif;font-size:15px;color:#222">' +
    '<p>Hi ' + name + ',</p><p>Your CyberStrike verification code is</p>' +
    '<p style="font-size:30px;font-weight:700;letter-spacing:6px;margin:8px 0 16px">' + code + '</p>' +
    '<p>Enter it in the game to finish creating your account. It works for ' + CODE_MINUTES + ' minutes.</p>' +
    '<p style="color:#888;font-size:13px">If you didn\'t sign up for CyberStrike, you can ignore this email.</p></div>';
  try { await sendMail(email, 'Your CyberStrike code: ' + code, text, html); }
  catch (e) { console.error('verification email to ' + email + ' failed: ' + e.message); throw new UserError("We couldn't send the email right now. Try again in a minute."); }
}
// a not yet verified account: send a fresh code unless one went out a moment ago
async function needVerify(uid, user) {
  const last = parseInt(user.vsent || '0', 10) || 0;
  if (Date.now() - last > RESEND_SECONDS * 1000) await sendCode(uid, user.email, user.name);
  return { needVerify: true, email: user.email };
}
async function uidByIdent(ident) {
  ident = String(ident || '').trim().replace(/^@/, '').toLowerCase();
  if (!ident) return null;
  return await db1(['GET', ident.includes('@') ? K.email(ident) : K.uname(ident)]);
}
async function verifyCode(ip, ident, code) {
  code = String(code || '').replace(/\D/g, '');
  if (code.length !== 6) throw new UserError('Enter the 6-digit code from the email.');
  if (limited('vf:' + ip, 20)) throw new UserError('Too many tries. Wait 15 minutes and try again.');
  const uid = await uidByIdent(ident);
  const user = uid ? hashOf(await db1(['HGETALL', K.user(uid)])) : null;
  if (!user) throw new UserError('That account no longer exists. Create it again.');
  if (user.verified !== '0') return { token: await newSession(uid), name: user.name };
  const raw = await db1(['GET', K.vcode(uid)]);
  let st = null; try { st = raw ? JSON.parse(raw) : null; } catch (e) { st = null; }
  if (!st) throw new UserError('That code has expired. Tap "Send a new code".');
  if (st.n >= CODE_TRIES) throw new UserError('Too many wrong codes. Tap "Send a new code".');
  if (st.h !== hashToken(code)) {
    bump('vf:' + ip, 15 * 60 * 1000);
    st.n++;
    await db1(['SET', K.vcode(uid), JSON.stringify(st), 'KEEPTTL']);
    throw new UserError(st.n >= CODE_TRIES ? 'Too many wrong codes. Tap "Send a new code".' : "That code isn't right. Check the email and try again.");
  }
  await db([['HSET', K.user(uid), 'verified', '1'], ['DEL', K.vcode(uid)]]);
  return { token: await newSession(uid), name: user.name };
}
async function resendCode(ip, ident) {
  if (limited('rs:' + ip, 6)) throw new UserError('Too many emails. Wait a while and try again.');
  const uid = await uidByIdent(ident);
  const user = uid ? hashOf(await db1(['HGETALL', K.user(uid)])) : null;
  if (!user || user.verified !== '0') throw new UserError('Nothing to verify. Log in instead.');
  const last = parseInt(user.vsent || '0', 10) || 0;
  const wait = Math.ceil((RESEND_SECONDS * 1000 - (Date.now() - last)) / 1000);
  if (wait > 0) throw new UserError('Wait ' + wait + ' seconds before asking for another code.');
  bump('rs:' + ip, 3600 * 1000);
  await sendCode(uid, user.email, user.name);
  return { sent: true, email: user.email };
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
  if (mailOn) {                      // the account works once the code from the email is entered
    await db1(['HSET', K.user(uid), 'verified', '0']);
    try { await sendCode(uid, email, name); } catch (e) { /* they can ask for a new code on the next screen */ }
    return { needVerify: true, email, name };
  }
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
  if (user.verified === '0' && mailOn) return await needVerify(uid, user);      // signed up but never entered the emailed code
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
  cmds.push(['DEL', K.user(id), K.email(user.email), K.uname(String(user.name).toLowerCase()), K.save(id), K.fr(id), K.rq(id), K.vcode(id)],
    ['HDEL', K.names, id], ['ZREM', K.lb('kills'), id], ['ZREM', K.lb('secs'), id], ['ZREM', K.lb('credits'), id]);
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
      u.acFlags = parseInt(o.acflags || '0', 10) || 0; u.flagged = o.flagged === '1';
      ecoLoad(u, o.eco);
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
  send(c, 'staff|' + staffRole(u.name));
  if (staffRole(u.name) && !u.staffLb) { u.staffLb = true; db1(['ZREM', K.lb('credits'), uid]).then(() => lbCache.clear()).catch(() => {}); }
  send(c, 'save|' + (u.save || ''));
  send(c, flMsg(u));
  const l = lobbyOf(c);
  if (l) broadcastRoster(l);
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
  if (delta.kills > 0 && !u.flagged) cmds.push(['ZINCRBY', K.lb('kills'), delta.kills, u.uid]);
  if (delta.secs > 0 && !u.flagged) cmds.push(['ZINCRBY', K.lb('secs'), delta.secs, u.uid]);
  const saveNow = u.saveDirty ? u.save : null;
  if (saveNow !== null) cmds.push(['SET', K.save(u.uid), saveNow]);
  const ecoNow = u.ecoDirty && u.eco ? ecoState(u) : null;
  if (ecoNow !== null) {
    cmds.push(['HSET', K.user(u.uid), 'eco', ecoNow]);
    if (!u.flagged && !staffRole(u.name)) cmds.push(['ZADD', K.lb('credits'), u.eco.credits, u.uid]);
  }
  if (!cmds.length) return;
  // move what we're writing out of "pending" first, so new reports during the write aren't lost
  for (const k of STAT_KEYS) { p[k] = (p[k] || 0) - delta[k]; u.base[k] += delta[k]; }
  if (saveNow !== null) u.saveDirty = false;
  if (ecoNow !== null) u.ecoDirty = false;
  u.lastFlush = Date.now();
  try { await db(cmds); }
  catch (e) {
    for (const k of STAT_KEYS) { p[k] = (p[k] || 0) + delta[k]; u.base[k] -= delta[k]; }
    if (saveNow !== null && !u.saveDirty) u.saveDirty = true;
    if (ecoNow !== null) u.ecoDirty = true;
    throw e;
  }
  if (delta.kills > 0 || delta.secs > 0 || ecoNow !== null) lbCache.clear();
}
setInterval(() => {
  const now = Date.now();
  for (const u of users.values()) {
    const statsDue = now - u.lastFlush > FLUSH_MS && STAT_KEYS.some((k) => (u.pend[k] || 0) >= 1);
    const saveDue = (u.saveDirty || u.ecoDirty) && now - u.saveAt > SAVE_FLUSH_MS;
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
  const asked = { kills, secs, matches };
  secs = Math.min(secs, elapsed);
  kills = Math.min(kills, 2 + Math.ceil(secs * 0.4));       // 24 kills a minute is already far past anyone real
  deaths = Math.min(deaths, 3 + Math.ceil(secs / 2));
  matches = Math.min(matches, 1 + Math.floor(secs / 30));
  wins = Math.min(wins, matches);
  // a report that is impossible on its own terms (not just early after a reconnect) is a strike
  if (asked.kills > 2 + asked.secs * 0.5 || asked.matches > 1 + asked.secs / 30 || v[2] > v[3])
    acStrike(c, u, 'stats', 'impossible stats report ' + v.join('/'));
  const add = { kills, deaths, wins, matches, secs };
  for (const k of STAT_KEYS) u.pend[k] = (u.pend[k] || 0) + add[k];
  if (matches > 0) flushUser(u).catch((e) => console.error('flush', e.message));    // a match ended: save now
  sendUser(u, meMsg(u));
}

//==========================================================================================
// Economy check on cloud saves (anti-cheat): credits and skins can only come from playing
//==========================================================================================
// Copied from the game by running it with CS_ECODUMP=1 (EcoDump in main.cpp): skins are [case, rarity],
// cases are [price, tier]. Update this when skins or cases are added to the game.
const ECO = {"startCredits":500,"maxInv":400,"skins":[[0,0],[0,0],[0,0],[0,0],[0,0],[0,1],[0,1],[0,1],[0,1],[0,2],[0,2],[0,2],[0,3],[0,3],[0,4],[0,4],[0,4],[0,4],[1,0],[1,0],[1,0],[1,0],[1,0],[1,1],[1,1],[1,1],[1,1],[1,2],[1,2],[1,2],[1,3],[1,3],[1,4],[1,4],[1,4],[1,4],[2,0],[2,0],[2,0],[2,0],[2,0],[2,1],[2,1],[2,1],[2,1],[2,2],[2,2],[2,2],[2,3],[2,3],[2,4],[2,4],[2,4],[2,4],[3,0],[3,0],[3,0],[3,0],[3,0],[3,1],[3,1],[3,1],[3,1],[3,2],[3,2],[3,2],[3,3],[3,3],[3,4],[3,4],[3,4],[3,4],[4,0],[4,0],[4,0],[4,0],[4,0],[4,1],[4,1],[4,1],[4,1],[4,2],[4,2],[4,2],[4,3],[4,3],[4,4],[4,4],[4,4],[4,4],[5,0],[5,0],[5,0],[5,0],[5,0],[5,1],[5,1],[5,1],[5,1],[5,2],[5,2],[5,2],[5,3],[5,3],[5,4],[5,4],[5,4],[5,4],[6,0],[6,0],[6,0],[6,0],[6,0],[6,1],[6,1],[6,1],[6,1],[6,2],[6,2],[6,2],[6,3],[6,3],[6,4],[6,4],[6,4],[6,4],[7,0],[7,0],[7,0],[7,0],[7,0],[7,1],[7,1],[7,1],[7,1],[7,2],[7,2],[7,2],[7,3],[7,3],[7,4],[7,4],[7,4],[7,4],[8,0],[8,0],[8,0],[8,0],[8,0],[8,1],[8,1],[8,1],[8,1],[8,2],[8,2],[8,2],[8,3],[8,3],[8,4],[8,4],[8,4],[8,4],[9,0],[9,0],[9,0],[9,0],[9,0],[9,1],[9,1],[9,1],[9,1],[9,2],[9,2],[9,2],[9,3],[9,3],[9,4],[9,4],[9,4],[9,4]],"cases":[[100,0],[250,1],[250,1],[250,1],[500,2],[500,2],[1000,3],[1000,3],[2000,4],[4000,5]],"tierValue":[0.5,1,1.60000002,2.5,4,7],"rarityValue":[30,100,300,900,2400],"tierOdds":[[84,13,2.4000001,0.449999988,0.150000006],[79.9199982,15.9799995,3.20000005,0.639999986,0.25999999],[72,19.5,6,1.79999995,0.699999988],[60,25,10,3.5999999,1.39999998],[45,30,16,6.5,2.5],[25,33,25,12,5]]};
const PROFILE_KEY = 'cyberstrike_profile_v1';
const ECO_RATE = 3;              // the allowance grows by this many credits per second of real time (a top player earns 1-3/s)
const ECO_BUCKET = 6000;         // the most allowance that builds up (a run of quick matches, or playing while the server was away)
const ECO_SLACK = 60;
const ECO_IMPORT_CAP = 40000;    // credits plus skin value an account from before this check may keep (one lucky knife fits); new accounts start fresh
const ECO_STRIKE = 5000;         // a refused save this far over is also an anti-cheat strike
const WEAR_MUL = [1.25, 1.10, 1.0, 0.90, 0.80].map(Math.fround);
const WEAR_EDGE = [0.07, 0.15, 0.38, 0.45].map(Math.fround);

function wearIndex(w) { return w < WEAR_EDGE[0] ? 0 : (w < WEAR_EDGE[1] ? 1 : (w < WEAR_EDGE[2] ? 2 : (w < WEAR_EDGE[3] ? 3 : 4))); }
function skinInfo(skin) {
  const s = ECO.skins[skin];
  const c = s ? ECO.cases[s[0]] : null;
  return c ? { rarity: s[1], price: c[0], tier: c[1] } : null;
}
// what the game pays when a skin is sold (ItemValue in main.cpp, in the same float arithmetic)
function itemValue(skin, wear) {
  const s = skinInfo(skin);
  if (!s) return 0;
  return Math.trunc(Math.fround(Math.fround(ECO.rarityValue[s.rarity] * WEAR_MUL[wearIndex(wear)]) * Math.fround(ECO.tierValue[s.tier])));
}
function unescapeSave(v) { return String(v).replace(/\\(.)/g, (m, ch) => (ch === 'n' ? '\n' : (ch === 't' ? '\t' : ch))); }
function escapeSave(v) { return String(v).replace(/\\/g, '\\\\').replace(/\n/g, '\\n').replace(/\t/g, '\\t').replace(/\r/g, ''); }
function saveFields(blob) { return String(blob || '').split('\t'); }    // rev, N, key, value, key, value...
function profileOf(blob) {
  const f = saveFields(blob);
  for (let i = 2; i + 1 < f.length; i += 2) if (f[i] === PROFILE_KEY) return unescapeSave(f[i + 1]);
  return '';
}
// credits and skins of a saved profile, read the way the game reads them (LoadProfile)
function parseEco(blob) {
  const e = { credits: ECO.startCredits, items: new Map(), n: 0 };
  for (const line of profileOf(blob).split('\n')) {
    let m;
    if ((m = /^credits (-?\d+)/.exec(line))) e.credits = Math.max(0, parseInt(m[1], 10) || 0);
    else if ((m = /^i (-?\d+) (-?\d+) (\S+)/.exec(line))) {
      const skin = parseInt(m[1], 10);
      if (e.n >= ECO.maxInv || !skinInfo(skin)) continue;     // the game skips unknown skins too (a newer game: update ECO)
      e.n++;
      const seed = (parseInt(m[2], 10) || 0) & 0xffff;
      const w = Math.fround(Math.min(1, Math.max(0, parseFloat(m[3]) || 0)));
      const key = skin + ':' + seed + ':' + w;
      const it = e.items.get(key);
      if (it) it.c++; else e.items.set(key, { skin, w, c: 1 });
    }
  }
  return e;
}
function ecoWorth(e) { let v = e.credits; for (const it of e.items.values()) v += itemValue(it.skin, it.w) * it.c; return v; }
// chance of k or more hits when lam are expected (Poisson)
function tailP(lam, k) {
  if (k <= 0) return 1;
  let term = Math.exp(-lam), sum = term;
  for (let i = 1; i < k; i++) { term *= lam / i; sum += term; }
  return Math.max(0, 1 - sum);
}
function tooLucky(st) { return (st.g >= 3 && tailP(st.eg, st.g) < 1e-6) || (st.r >= 4 && tailP(st.er, st.r) < 1e-6); }
// gold / red items among some items, and how many of each a fair case opening would have given
function luckOf(items, st) {
  for (const it of items) {
    const s = skinInfo(it.skin);
    if (!s) continue;
    const odds = ECO.tierOdds[s.tier];
    st.eg += it.c * odds[4] / 100; st.er += it.c * odds[3] / 100;
    if (s.rarity === 4) st.g += it.c; else if (s.rarity === 3) st.r += it.c;
  }
  return st;
}
// a save with the credits and skins put back to a new player's (level, rank and records stay)
function resetEconomy(blob) {
  const f = saveFields(blob);
  for (let i = 2; i + 1 < f.length; i += 2) {
    if (f[i] !== PROFILE_KEY) continue;
    const lines = unescapeSave(f[i + 1]).split('\n').filter((l) => !/^(i |eqg |sig )/.test(l))
      .map((l) => (/^credits /.test(l) ? 'credits ' + ECO.startCredits : (/^equip /.test(l) ? 'equip -1 -1' : l)));
    f[i + 1] = escapeSave(lines.join('\n'));      // unsigned: the game checks it like an old save and signs it again
  }
  return f.join('\t');
}
const ECO_SOLD_KEEP = 600;       // sold skins remembered per account (an exact skin can't come out of a case twice)
function freshEco(e, now, a) { return { credits: e.credits, items: e.items, t: now, a, eg: 0, er: 0, g: 0, r: 0, sold: [] }; }
function ecoState(u) { const x = u.eco; return JSON.stringify({ t: x.t, a: Math.round(x.a), eg: x.eg, er: x.er, g: x.g, r: x.r, sold: x.sold }); }

// a save from before (or without) a check: allowed if what it holds could be real
function importOk(e) {
  if (ecoWorth(e) > ECO_IMPORT_CAP) return 'more credits and skins than can be verified';
  const st = luckOf(e.items.values(), { eg: 0, er: 0, g: 0, r: 0 });
  if ((st.g >= 4 && tailP(st.eg, st.g) < 1e-9) || (st.r >= 6 && tailP(st.er, st.r) < 1e-9)) return 'far more rare skins than cases give';
  return '';
}
// when an account comes online: the last accepted save is the baseline
function ecoLoad(u, stateJson) {
  if (!u.save) { u.eco = null; return; }               // a new account: its first save is checked as an import
  const now = Date.now();
  if (staffRole(u.name)) { u.eco = freshEco(parseEco(u.save), now, ECO_BUCKET); u.ecoDirty = true; return; }   // staff: not checked
  let st = null;
  try { st = stateJson ? JSON.parse(stateJson) : null; } catch (e) { st = null; }
  const e = parseEco(u.save);
  if (st && typeof st.t === 'number') {
    u.eco = { credits: e.credits, items: e.items, t: st.t, a: Number(st.a) || 0, eg: Number(st.eg) || 0, er: Number(st.er) || 0, g: st.g | 0, r: st.r | 0,
      sold: Array.isArray(st.sold) ? st.sold.slice(-ECO_SOLD_KEEP).map(String) : [] };
    return;
  }
  const why = importOk(e);
  if (why) {
    console.log('economy: ' + u.name + "'s saved credits and skins were reset (" + why + ')');
    u.save = resetEconomy(u.save); u.saveDirty = true; u.saveAt = 0;
    u.eco = freshEco(parseEco(u.save), now, ECO_BUCKET);
  } else u.eco = freshEco(e, now, ECO_BUCKET);
  u.ecoDirty = true;
}
// a new save from the game: { ok } or { ok: false, why, strike }
function ecoCheck(u, blob) {
  const now = Date.now();
  const e = parseEco(blob);
  if (staffRole(u.name)) { u.eco = freshEco(e, now, ECO_BUCKET); u.ecoDirty = true; return { ok: true }; }    // staff: the panel gives credits and skins
  if (!u.eco) {
    // a new account's first save: it starts like a new player. Guest progress only ever lived in that browser, where
    // anyone can edit it, so none of it comes in (the game keeps it on the device for when they play as a guest).
    if (e.items.size > 0 || e.credits > ECO.startCredits) return { ok: false, why: 'a new account starts fresh, guest progress stays with the guest', strike: false, reset: true };
    u.eco = freshEco(e, now, ECO_BUCKET); u.ecoDirty = true;
    return { ok: true };
  }
  const o = u.eco;
  const allow = Math.min(ECO_BUCKET, o.a + ECO_RATE * Math.max(0, now - o.t) / 1000);
  let cost = 0, sold = 0, back = false;
  const added = [], soldKeys = [];
  const soldBefore = new Set(o.sold || []);
  for (const [k, it] of e.items) {
    const had = o.items.get(k), n = it.c - (had ? had.c : 0);
    if (n > 0) { cost += n * skinInfo(it.skin).price; added.push({ skin: it.skin, c: n }); if (soldBefore.has(k)) back = true; }
  }
  for (const [k, it] of o.items) {
    const now2 = e.items.get(k), n = it.c - (now2 ? now2.c : 0);
    if (n > 0) { sold += n * itemValue(it.skin, it.w); soldKeys.push(k); }
  }
  if (back) return { ok: false, why: 'a sold skin came back', strike: false };      // (or two devices out of step: no strike)
  const earned = (e.credits - o.credits) + cost - sold;      // what playing must have paid out for this save to be real
  if (earned > allow + ECO_SLACK)
    return { ok: false, why: 'credits or skins that were not earned', detail: Math.round(earned) + ' credits needed, ' + Math.round(allow) + ' possible', strike: earned > allow + ECO_STRIKE };
  const st = luckOf(added, { eg: o.eg, er: o.er, g: o.g, r: o.r });
  if (tooLucky(st)) return { ok: false, why: 'far more rare skins than cases give', strike: true };
  u.eco = { credits: e.credits, items: e.items, t: now, a: allow - Math.max(0, earned), eg: st.eg, er: st.er, g: st.g, r: st.r,
    sold: (o.sold || []).concat(soldKeys).slice(-ECO_SOLD_KEEP) };
  u.ecoDirty = true;
  return { ok: true };
}

// anti-cheat strikes: the game flagged a match / edited memory / edited saves, or sent impossible stats.
// At most one strike per connection per minute; AC_LIMIT strikes take the account off the leaderboards.
function acStrike(c, u, kind, detail) {
  const now = Date.now();
  if (c.acAt && now - c.acAt < 60000) return;
  c.acAt = now;
  u.acFlags = (u.acFlags || 0) + 1;
  console.log('anti-cheat: ' + u.name + ' ' + kind + ' (' + clean(detail, 100) + '), strike ' + u.acFlags);
  const cmds = [['HINCRBY', K.user(u.uid), 'acflags', 1]];
  if (u.acFlags >= AC_LIMIT && !u.flagged) {
    u.flagged = true;
    cmds.push(['HSET', K.user(u.uid), 'flagged', '1'], ['ZREM', K.lb('kills'), u.uid], ['ZREM', K.lb('secs'), u.uid], ['ZREM', K.lb('credits'), u.uid]);
    lbCache.clear();
    console.log('anti-cheat: ' + u.name + ' flagged, removed from the leaderboards');
  }
  db(cmds).catch((e) => console.error('acStrike', e.message));
}

async function adminUnflag(body) {
  if (!ADMIN_KEY || String(body.key || '') !== ADMIN_KEY) throw new UserError('Not allowed.');
  const uid = await uidByName(body.username);
  if (!uid) throw new UserError('No such player.');
  const o = hashOf(await db1(['HGETALL', K.user(uid)]));
  const u = users.get(uid);
  const kills = (parseInt(o.kills || '0', 10) || 0), secs = (parseInt(o.secs || '0', 10) || 0);
  const credits = u && u.eco ? u.eco.credits : parseEco(await db1(['GET', K.save(uid)]) || '').credits;
  await db([['HSET', K.user(uid), 'flagged', '0', 'acflags', 0], ['ZREM', K.lb('kills'), uid], ['ZREM', K.lb('secs'), uid],
            ['ZINCRBY', K.lb('kills'), kills, uid], ['ZINCRBY', K.lb('secs'), secs, uid]].concat(staffRole(o.name) ? [] : [['ZADD', K.lb('credits'), credits, uid]]));
  if (u) { u.flagged = false; u.acFlags = 0; }
  lbCache.clear();
  return { name: o.name, kills, secs };
}

// leaderboards: top 50, cached for 30 seconds
const lbCache = new Map();
async function leaderboard(c, board) {
  if (board !== 'kills' && board !== 'secs' && board !== 'credits') return;
  const asker = c.uid ? users.get(c.uid) : null;          // your own numbers are always current on the board
  if (asker && (STAT_KEYS.some((k) => (asker.pend[k] || 0) >= 1) || asker.ecoDirty)) await flushUser(asker).catch(() => {});
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
    if (u && !myRank) myVal = Math.floor(board === 'kills' ? totals(u).kills : (board === 'secs' ? totals(u).secs : (u.eco ? u.eco.credits : 0)));
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
// lobby members logged in to staff accounts (a guest can't call itself that: only an account's name counts)
function staffIds(l) {
  return 'staffids|' + l.members.filter((id) => { const m = clients.get(id); return m && m.uid && staffRole(m.name); }).join(',');
}
function broadcastRoster(l) { broadcast(l, roster(l)); broadcast(l, staffIds(l)); }
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
  broadcastRoster(l);
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
  broadcastRoster(l);
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
      if (l) broadcastRoster(l);
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
      send(c, roster(lb)); send(c, staffIds(lb));
      touch(c);
      break;
    }

    case 'join': {
      const code = cleanCode(f[0]);
      const lb = lobbies.get(code);
      if (!lb) { send(c, 'err|No lobby with that code'); break; }
      if (lb.members.includes(c.id)) break;
      if (lb.banned && (lb.banned.has(c.id) || (c.uid && lb.banned.has('u' + c.uid)))) { send(c, 'err|You were removed from that lobby'); break; }
      if (lb.started) { send(c, 'err|That match has already started'); break; }
      if (lb.members.length >= lb.max) { send(c, 'err|That lobby is full'); break; }
      if (l) leaveLobby(c);
      lb.members.push(c.id);
      c.lobby = code;
      send(c, ['joined', code, lb.host, lb.mode, lb.max, lb.isPublic ? 1 : 0, lb.name].join('|'));
      broadcastRoster(lb);
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
      if (c.uid) { detach(c); c.name = clean(f[0], 15) || ('Player' + c.id); if (l) broadcastRoster(l); }
      break;

    case 'st':
      if (c.uid) addStats(c, f);
      break;

    case 'ac': {                    // the game's anti-cheat: ac|match|why, ac|mem|what, ac|save|...
      const u = c.uid ? users.get(c.uid) : null;
      if (u && ['match', 'mem', 'save'].includes(f[0])) acStrike(c, u, f[0], f[1] || '');
      break;
    }

    case 'kick': {                  // host: the anti-cheat removed a player from the match
      if (!l || l.host !== c.id) break;
      const tid = parseInt(f[0], 10), tc = clients.get(tid);
      if (!tc || tc === c || tc.lobby !== l.code) break;
      l.banned = l.banned || new Set();
      l.banned.add(tid); if (tc.uid) l.banned.add('u' + tc.uid);
      send(tc, 'closed|Removed from the match by the anti-cheat' + (clean(f[1], 80) ? ': ' + clean(f[1], 80) : ''));
      leaveLobby(tc, 'kicked');
      break;
    }

    case 'sv': {
      const u = c.uid ? users.get(c.uid) : null;
      if (!u || rest.length > 48 * 1024 || u.save === rest) break;
      const r = ecoCheck(u, rest);
      if (r.ok) { u.save = rest; if (!u.saveDirty) u.saveAt = Date.now(); u.saveDirty = true; break; }
      console.log('economy: refused a save from ' + u.name + ': ' + r.why + (r.detail ? ' (' + r.detail + ')' : ''));
      if (r.reset) {                 // a new account's first save had guest progress in it: it starts with a new player's credits and skins (settings kept)
        u.save = resetEconomy(rest); u.saveDirty = true; u.saveAt = 0;
        u.eco = freshEco(parseEco(u.save), Date.now(), ECO_BUCKET); u.ecoDirty = true;
      }
      if (r.strike) acStrike(c, u, 'save', r.why + (r.detail ? ' (' + r.detail + ')' : ''));
      sendUser(u, 'svx|' + clean(r.why, 120) + '|' + u.save);
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
      else if (url === '/api/verify') reply(res, 200, Object.assign({ ok: true }, await verifyCode(ip, body.email, body.code)));
      else if (url === '/api/resend') reply(res, 200, Object.assign({ ok: true }, await resendCode(ip, body.email)));
      else if (url === '/api/logout') { if (body.token) await db1(['DEL', K.sess(hashToken(body.token))]); reply(res, 200, { ok: true }); }
      else if (url === '/api/admin/unflag') reply(res, 200, Object.assign({ ok: true }, await adminUnflag(body)));
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
    (OWNER ? 'Owner: @' + OWNER + '\n' : '') +
    (mailOn ? 'Email verification is on.\n' : 'Email verification is off (set MAIL_FROM and BREVO_API_KEY to turn it on).\n'));
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
