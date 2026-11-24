// Outbound address pool for upstream calls.
//
// veo-online.ai limits generation per IP per day (code 10304). This module
// keeps the request on the normal direct route, and only switches to a
// different address once the direct route is refused — then rotates through
// the pool until one gets through.
//
// Sources, in order: PROXY_URL / PROXY_LIST env, proxies.txt, cached list,
// then a free public list refreshed in the background.
'use strict';

const fs = require('fs');
const path = require('path');
const { fetch: ufetch, ProxyAgent } = require('undici');

const MODE = (process.env.PROXY_MODE || 'onlimit').toLowerCase(); // onlimit | always | off
const BLOCK_MS = Number(process.env.PROXY_BLOCK_MS || 10 * 60 * 1000);   // how long direct stays out after a limit
const BAD_MS = Number(process.env.PROXY_BAD_MS || 5 * 60 * 1000);         // how long a dead proxy stays out
const LIST_TTL = Number(process.env.PROXY_LIST_TTL_MS || 10 * 60 * 1000); // refresh interval for the remote list
const TARGET = Number(process.env.PROXY_TARGET || 200);                   // how many addresses to keep
const CONNECT_TIMEOUT = Number(process.env.PROXY_CONNECT_TIMEOUT_MS || 8000);
const VERIFY_BATCH = Number(process.env.PROXY_VERIFY_BATCH || 16);       // probed in parallel per pass
const VERIFY_TIMEOUT = Number(process.env.PROXY_VERIFY_TIMEOUT_MS || 7000);
const VERIFIED_TTL = Number(process.env.PROXY_VERIFIED_TTL_MS || 10 * 60 * 1000);
const VERIFY_EVERY = Number(process.env.PROXY_VERIFY_EVERY_MS || 15000);
const PROBE_URL = process.env.PROXY_PROBE_URL || ((process.env.UPSTREAM_BASE || 'https://veo-online.ai') + '/api/workflow/query');

const SOURCES = [
  'https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt',
  'https://api.proxyscrape.com/v2/?request=getproxies&protocol=http&timeout=4000&country=all&ssl=all&anonymity=all',
  'https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt',
];

const agents = new Map();
let entries = [];          // [{url, deadUntil, ok, fails, lastError}]
let source = 'none';
let directBlockedUntil = 0;
let cacheFile = null;
let refreshing = null;
let lastRefresh = 0;
let bootLoaded = false;

const now = () => Date.now();

// ---------- list handling ----------
function normalizeLine(line) {
  let s = String(line || '').trim();
  if (!s || s.startsWith('#')) return null;
  if (!/^https?:\/\//i.test(s)) {
    if (!/^\d{1,3}(\.\d{1,3}){3}:\d{2,5}$/.test(s)) return null; // no socks, no hostnames
    s = 'http://' + s;
  }
  try {
    const u = new URL(s);
    if (u.protocol !== 'http:' && u.protocol !== 'https:') return null;
    return u.origin;
  } catch { return null; }
}

function parseList(text) {
  const out = new Set();
  for (const line of String(text || '').split(/[\r\n]+/)) {
    const v = normalizeLine(line);
    if (v) out.add(v);
  }
  return [...out];
}

function merge(urls, srcName) {
  if (!urls.length) return 0;
  const seen = new Set(entries.map((e) => e.url));
  let added = 0;
  for (const u of urls) {
    if (seen.has(u) || entries.length >= TARGET) continue;
    seen.add(u);
    entries.push({ url: u, deadUntil: 0, ok: 0, fails: 0, lastError: null });
    added++;
  }
  if (added) source = srcName;
  return added;
}

function aliveList() {
  const t = now();
  return entries.filter((e) => e.deadUntil <= t);
}

// addresses whose pipe to upstream has actually been proven
function verifiedList() {
  const t = now();
  return entries.filter((e) => e.verifiedAt && t - e.verifiedAt <= VERIFIED_TTL && e.deadUntil <= t);
}

function entry(url) { return entries.find((e) => e.url === url); }

function markBad(url, err) {
  const e = entry(url);
  if (!e) return;
  e.fails++;
  e.deadUntil = now() + BAD_MS;
  e.lastError = String((err && err.message) || err || 'failed').slice(0, 120);
}

function markGood(url) {
  const e = entry(url);
  if (!e) return;
  e.ok++;
  e.fails = 0;
  e.deadUntil = 0;
  e.lastError = null;
}

// ---------- verification ----------
// A pipe being open is not the same as upstream answering. probing keeps a
// fast lane of addresses that are known to talk to the generator, so rotation
// on a limit does not burn minutes walking through dead entries.
let verifying = false;

async function probeOne(url) {
  try {
    const res = await doFetch(PROBE_URL, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'user-id': '0'.repeat(32) },
      body: JSON.stringify({ jobs: [{ id: 'ping' }] }),
    }, url, VERIFY_TIMEOUT);
    if (res.status !== 200) { try { await res.body.dump(); } catch { /* nothing to drain */ } return false; }
    const j = await res.json().catch(() => null);
    return !!(j && j.code === 10000);
  } catch { return false; }
}

async function verifyPass() {
  if (verifying) return verifiedList().length;
  verifying = true;
  try {
    const t = now();
    const stale = entries.filter((e) => e.deadUntil <= t && (!e.verifiedAt || t - e.verifiedAt > VERIFIED_TTL));
    const batch = stale.slice(0, VERIFY_BATCH);
    await Promise.all(batch.map(async (e) => {
      if (await probeOne(e.url)) {
        e.verifiedAt = now();
        e.ok++;
        e.deadUntil = 0;
        e.lastError = null;
      } else {
        e.fails++;
        e.deadUntil = now() + BAD_MS;
        e.lastError = 'probe failed';
      }
    }));
    return verifiedList().length;
  } finally {
    verifying = false;
  }
}

// ---------- loading ----------
function fromEnv() {
  const urls = [];
  if (process.env.PROXY_URL) urls.push(...parseList(process.env.PROXY_URL.replace(/,/g, '\n')));
  if (process.env.PROXY_LIST) urls.push(...parseList(process.env.PROXY_LIST.replace(/,/g, '\n')));
  return [...new Set(urls)];
}

function fromFile() {
  const f = process.env.PROXY_FILE || (cacheFile && path.join(path.dirname(cacheFile), 'proxies.txt'));
  if (!f || !fs.existsSync(f)) return [];
  try { return parseList(fs.readFileSync(f, 'utf8')); } catch { return []; }
}

function fromCache() {
  if (!cacheFile || !fs.existsSync(cacheFile)) return [];
  try {
    const c = JSON.parse(fs.readFileSync(cacheFile, 'utf8'));
    if (!Array.isArray(c.urls)) return [];
    if (now() - (c.at || 0) > 30 * 24 * 3600 * 1000) return []; // too old to trust
    return c.urls;
  } catch { return []; }
}

function writeCache() {
  if (!cacheFile) return;
  try { fs.writeFileSync(cacheFile, JSON.stringify({ at: now(), urls: entries.map((e) => e.url) })); } catch { /* cache is optional */ }
}

async function fetchRemote() {
  for (const src of SOURCES) {
    try {
      const res = await ufetch(src, { signal: AbortSignal.timeout(12000) });
      if (!res.ok) continue;
      const added = merge(parseList(await res.text()), 'public');
      if (added) { lastRefresh = now(); writeCache(); return added; }
    } catch { /* next source */ }
  }
  lastRefresh = now();
  return 0;
}

function ensureLoaded() {
  if (bootLoaded) return Promise.resolve(entries.length);
  bootLoaded = true;
  const env = fromEnv();
  if (env.length) merge(env, 'env');
  const file = fromFile();
  if (file.length) merge(file, 'file');
  const cache = fromCache();
  if (cache.length) merge(cache, 'cache');
  if (entries.length) { source = source === 'none' ? 'cache' : source; return Promise.resolve(entries.length); }
  return fetchRemote();
}

function backgroundRefresh() {
  if (refreshing) return refreshing;
  refreshing = (async () => {
    try { await fetchRemote(); } catch { /* keep whatever we have */ }
    refreshing = null;
  })();
  return refreshing;
}

// ---------- fetching ----------
function agentFor(url) {
  let a = agents.get(url);
  if (!a) {
    a = new ProxyAgent({ uri: url, connect: { timeout: CONNECT_TIMEOUT } });
    agents.set(url, a);
    return a;
  }
  return a;
}

function doFetch(url, opts, proxy, timeout) {
  const signal = AbortSignal.timeout(timeout);
  if (!proxy) return ufetch(url, { ...opts, signal });
  return ufetch(url, { ...opts, signal, dispatcher: agentFor(proxy) });
}

const PROXY_BAD_STATUS = new Set([407, 502, 503, 504, 522, 524]);

/**
 * Perform an upstream request.
 *   pin     – address already assigned to a job, used first if still alive
 *   timeout – ms, default 20s
 * Returns { res, proxy } where proxy is null for a direct call.
 */
async function request(url, opts = {}, { pin = null, timeout = 20000 } = {}) {
  const blocked = now() < directBlockedUntil;
  const useProxy = MODE === 'always' || blocked;

  const cands = [];
  const push = (v) => { if (v !== undefined && !cands.includes(v)) cands.push(v); };

  if (pin && (entry(pin) ? entry(pin).deadUntil <= now() : true)) push(pin);
  if (useProxy || MODE === 'always') {
    // proven addresses first — they answer in one shot instead of timing out
    const proven = verifiedList();
    const provenSet = new Set(proven.map((e) => e.url));
    for (const e of proven) push(e.url);
    for (const e of aliveList()) if (!provenSet.has(e.url)) push(e.url);
  }
  push(null); // direct is always the last resort

  const tried = new Set();
  let lastErr = null;

  // never lose the direct route: if it got cut off by the slice, put it back last
  const plan = cands.slice(0, 10);
  if (!plan.includes(null)) plan.push(null);

  for (const cand of plan) {
    tried.add(cand || null);
    try {
      const res = await doFetch(url, opts, cand, timeout);
      if (cand && PROXY_BAD_STATUS.has(res.status)) {
        markBad(cand, 'http ' + res.status);
        lastErr = new Error('proxy http ' + res.status);
        continue;
      }
      if (cand) markGood(cand);
      return { res, proxy: cand };
    } catch (e) {
      lastErr = e;
      if (cand) { markBad(cand, e); continue; }
      break; // direct transport failure: nothing else to try without a proxy
    }
  }
  throw lastErr || new Error('upstream unreachable');
}

/**
 * Direct route just answered with the per-IP daily limit.
 * Opens the address pool and starts preferring it.
 */
function noteLimit(proxy) {
  if (proxy) { markBad(proxy, 'daily limit'); return Promise.resolve(); }
  directBlockedUntil = now() + BLOCK_MS;
  // the retry is seconds away — make sure there is a proven address ready
  if (verifiedList().length) return Promise.resolve(verifiedList().length);
  const refresh = entries.length ? Promise.resolve(0) : backgroundRefresh();
  return refresh.then(() => verifyPass());
}

function resetDirect() { directBlockedUntil = 0; }

function stats() {
  const alive = aliveList().length;
  return {
    mode: MODE,
    source,
    total: entries.length,
    alive,
    proven: verifiedList().length,
    directBlocked: now() < directBlockedUntil,
    directBlockedFor: Math.max(0, directBlockedUntil - now()),
    lastRefresh,
  };
}

module.exports = { init, request, noteLimit, resetDirect, stats, ensureLoaded, aliveList, verifiedList, verifyPass };

function init(dataDir) {
  cacheFile = path.join(dataDir, 'proxies.json');
  if (MODE === 'off') { entries = []; return; }

  ensureLoaded().then(() => verifyPass()).catch(() => { /* pool is best effort */ });

  const listTimer = setInterval(() => { if (now() - lastRefresh > LIST_TTL) backgroundRefresh(); }, 60 * 1000);
  const verifyTimer = setInterval(() => { verifyPass().catch(() => {}); }, VERIFY_EVERY);
  listTimer.unref?.();
  verifyTimer.unref?.();
}

module.exports = { init, request, noteLimit, resetDirect, stats, ensureLoaded, aliveList };
