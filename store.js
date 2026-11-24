// Storage for keys, job history and video bytes.
//
// Three backends, picked automatically, best first:
//   pg  : DATABASE_URL            — Render Postgres, survives everything
//   kv  : REDIS_URL / REDIS_HOST  — Render Key Value, created by render.yaml
//   file: data/*.json             — local development, no services at all
//
// The Key Value route is what lets a personal project keep its data on
// Render's servers without provisioning a database: it is a separate
// instance, so the web service redeploying or spinning down never touches it.
const fs = require('fs');
const path = require('path');

let mode = 'file';
let file = null;
let videosDir = null;
let pool = null;      // postgres
let kv = null;        // redis/valkey client
let dbUrl = '';
let kvUrl = '';
let ready = null;
let degraded = false;  // wanted a remote store, fell back to the local file
let reconnectTimer = null;

const todayStamp = () => new Date().toISOString().slice(0, 10);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const K_KEYS = 'veo:keys';
const K_JOBS = 'veo:jobs';

// ---------- local json backend (development) ----------
const readFile = () => { try { const v = JSON.parse(fs.readFileSync(file, 'utf8')); return Array.isArray(v) ? v : []; } catch { return []; } };
const writeFile = (list) => fs.writeFileSync(file, JSON.stringify(list, null, 2), { mode: 0o600 });
const historyFile = () => path.join(path.dirname(file), 'history.json');
const readLocalJobs = () => { try { const v = JSON.parse(fs.readFileSync(historyFile(), 'utf8')); return Array.isArray(v) ? v : []; } catch { return []; } };

// ---------- row mapping ----------
const rowToRec = (r) => ({
  key: r.key,
  name: r.name,
  createdAt: Number(r.created_at),
  uses: r.uses,
  lastUsedAt: r.last_used_at === null || r.last_used_at === undefined ? null : Number(r.last_used_at),
  dayStamp: r.day_stamp,
  dayUses: r.day_uses,
  dailyLimit: r.daily_limit,
  revoked: !!r.revoked,
});

function jobToRow(j) {
  return {
    job_id: j.jobId, key_id: j.keyId || null, prompt: j.prompt || '', model: j.model || '',
    duration: j.duration ?? null, aspect_ratio: j.aspect_ratio || '', resolution: j.resolution || '',
    status: j.status || 'processing', output: j.output || null, remote: j.remote || null,
    proxy: j.proxy || null, created_at: Number(j.createdAt) || Date.now(),
    finished_at: j.finishedAt ?? null,
  };
}

function rowToJob(r) {
  const out = {
    jobId: r.job_id, keyId: r.key_id || null, prompt: r.prompt, model: r.model,
    duration: r.duration, aspect_ratio: r.aspect_ratio, resolution: r.resolution,
    status: r.status, output: r.output || null, remote: r.remote || null,
    proxy: r.proxy || null, createdAt: Number(r.created_at) || 0,
  };
  if (r.finished_at !== null && r.finished_at !== undefined) out.finishedAt = Number(r.finished_at);
  return out;
}

// ---------- postgres ----------
const DDL = `
CREATE TABLE IF NOT EXISTS api_keys (
  key          TEXT PRIMARY KEY,
  name         TEXT NOT NULL DEFAULT '',
  created_at   BIGINT NOT NULL,
  uses         INTEGER NOT NULL DEFAULT 0,
  last_used_at BIGINT,
  day_stamp    TEXT NOT NULL DEFAULT '',
  day_uses     INTEGER NOT NULL DEFAULT 0,
  daily_limit  INTEGER NOT NULL DEFAULT 100,
  revoked      BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS api_keys_revoked_idx ON api_keys (revoked);

CREATE TABLE IF NOT EXISTS jobs (
  job_id       TEXT PRIMARY KEY,
  key_id       TEXT,
  prompt       TEXT NOT NULL DEFAULT '',
  model        TEXT NOT NULL DEFAULT '',
  duration     INTEGER,
  aspect_ratio TEXT NOT NULL DEFAULT '',
  resolution   TEXT NOT NULL DEFAULT '',
  status       TEXT NOT NULL DEFAULT 'processing',
  output       TEXT,
  remote       TEXT,
  proxy        TEXT,
  created_at   BIGINT NOT NULL,
  finished_at  BIGINT
);
CREATE INDEX IF NOT EXISTS jobs_created_idx ON jobs (created_at DESC);

CREATE TABLE IF NOT EXISTS videos (
  job_id     TEXT PRIMARY KEY,
  data       BYTEA NOT NULL,
  size       INTEGER NOT NULL DEFAULT 0,
  created_at BIGINT NOT NULL
);`;

// retry a few times — Render can hand out a store that is still warming up
async function withRetry(fn, tries = 6) {
  let last;
  for (let i = 0; i < tries; i++) {
    try { return await fn(); } catch (e) {
      last = e;
      if (/ECONNREFUSED|ETIMEDOUT|ENOTFOUND|Connection terminated|terminating connection|Socket closed|reconnect/i.test(String(e.message))) {
        await sleep(500 * (i + 1));
        continue;
      }
      throw e;
    }
  }
  throw last;
}

async function initPg(url) {
  const { Pool } = require('pg');
  pool = new Pool({ connectionString: url, ssl: url.includes('localhost') ? false : { rejectUnauthorized: false }, max: 5 });
  await withRetry(() => pool.query(DDL));
  await withRetry(() => pool.query('SELECT 1'));
}

// ---------- key value ----------
async function initKv(url) {
  const { createClient } = require('redis');
  const client = createClient({
    url,
    socket: {
      connectTimeout: 6000,
      reconnectStrategy: (retries) => Math.min(retries * 500, 5000),
    },
  });
  client.on('error', () => { /* reported through the connect call */ });
  await client.connect();
  await client.ping();
  kv = client;
}

function resolveKvUrl() {
  if (process.env.REDIS_URL) return process.env.REDIS_URL;
  if (process.env.KV_URL) return process.env.KV_URL;
  const host = process.env.REDIS_HOST || process.env.KV_HOST;
  if (host) return `redis://${host}:${process.env.REDIS_PORT || process.env.KV_PORT || 6379}`;
  return '';
}

// ---------- init ----------
function init({ dataDir, databaseUrl, videosDir: vd } = {}) {
  if (ready) return ready;
  file = path.join(dataDir || path.join(__dirname, 'data'), 'api-keys.json');
  videosDir = vd || null;
  dbUrl = databaseUrl || '';
  kvUrl = resolveKvUrl();

  if (dbUrl) mode = 'pg';
  else if (kvUrl) mode = 'kv';
  else mode = 'file';

  if (mode === 'file') {
    if (!fs.existsSync(file)) writeFile([]);
    ready = Promise.resolve();
    return ready;
  }

  // NOTE: importFromDisk() must NOT run inside the `ready` chain — it reads
  // through listKeys()/listJobs(), which await `ready`, and that deadlocks.
  // It is scheduled as a separate continuation instead.
  const conn = (mode === 'pg' ? initPg(dbUrl) : initKv(kvUrl)).catch((e) => {
    console.error(`  ${mode === 'pg' ? 'postgres' : 'key value'} unavailable, falling back to local file: ${e.message}`);
    degraded = true; mode = 'file'; pool = null; kv = null;
    scheduleReconnect();
  });
  ready = conn;
  conn.then(() => {
    if (mode === 'file') return;
    return importFromDisk().catch((e) => console.error('  disk import failed: ' + e.message));
  }).catch(() => { /* already handled */ });
  return ready;
}

// the remote store can come back after the first failure — keep trying and
// move everything back into it as soon as it answers
function scheduleReconnect() {
  if (reconnectTimer) return;
  let attempt = 0;
  reconnectTimer = setInterval(async () => {
    if (mode !== 'file') { clearInterval(reconnectTimer); reconnectTimer = null; return; }
    if (!dbUrl && !kvUrl) { clearInterval(reconnectTimer); reconnectTimer = null; return; }
    attempt++;
    try {
      if (dbUrl) await initPg(dbUrl); else await initKv(kvUrl);
      mode = dbUrl ? 'pg' : 'kv';
      degraded = false;
      await importFromDisk();
      console.log(`  ${mode === 'pg' ? 'postgres' : 'key value'} reconnected — storage moved back to it`);
      clearInterval(reconnectTimer); reconnectTimer = null;
    } catch (e) {
      if (attempt % 6 === 0) console.log(`  storage still on the local file after ${attempt} tries: ${e.message}`);
    }
  }, 10000);
  reconnectTimer.unref?.();
}

// ---------- keys ----------
async function listKeys() {
  await ready;
  if (mode === 'pg') {
    const r = await withRetry(() => pool.query('SELECT * FROM api_keys ORDER BY created_at DESC'));
    return r.rows.map(rowToRec);
  }
  if (mode === 'kv') {
    const raw = await withRetry(() => kv.get(K_KEYS));
    try { return JSON.parse(raw || '[]'); } catch { return []; }
  }
  return readFile().slice().reverse();
}

async function getKey(k) {
  const all = await listKeys();
  return all.find((x) => x.key === k) || null;
}

async function createKey(rec) {
  await ready;
  if (mode === 'pg') {
    await withRetry(() => pool.query(
      `INSERT INTO api_keys (key,name,created_at,uses,last_used_at,day_stamp,day_uses,daily_limit,revoked)
       VALUES ($1,$2,$3,0,NULL,$4,0,$5,FALSE) ON CONFLICT (key) DO NOTHING`,
      [rec.key, rec.name, rec.createdAt, rec.dayStamp, rec.dailyLimit]
    ));
    mirrorKeys();
    return rec;
  }
  if (mode === 'kv') {
    const list = await listKeys();
    list.push(rec);
    await withRetry(() => kv.set(K_KEYS, JSON.stringify(list)));
    mirrorKeys();
    return rec;
  }
  const list = readFile();
  list.push(rec);
  writeFile(list);
  return rec;
}

async function revokeKey(k) {
  await ready;
  if (mode === 'pg') {
    const r = await withRetry(() => pool.query('UPDATE api_keys SET revoked = TRUE WHERE key = $1 RETURNING key', [k]));
    if (r.rowCount > 0) mirrorKeys();
    return r.rowCount > 0;
  }
  const list = await listKeys();
  const hit = list.find((x) => x.key === k);
  if (!hit) return false;
  hit.revoked = true;
  if (mode === 'kv') await withRetry(() => kv.set(K_KEYS, JSON.stringify(list)));
  else writeFile(list);
  mirrorKeys();
  return true;
}

// validate + consume one unit of the key's daily quota.
// returns { ok, rec, reason } where reason is missing|unknown|revoked|limit
async function touchKey(raw) {
  await ready;
  if (!raw) return { ok: false, reason: 'missing' };
  const today = todayStamp();

  const rec = await getKey(raw);
  if (!rec) return { ok: false, reason: 'unknown' };
  if (rec.revoked) return { ok: false, reason: 'revoked' };
  const sameDay = rec.dayStamp === today;
  if (!sameDay) { rec.dayStamp = today; rec.dayUses = 0; }
  if (rec.dailyLimit > 0 && rec.dayUses >= rec.dailyLimit) return { ok: false, reason: 'limit', rec };

  if (mode === 'pg') {
    const r = await withRetry(() => pool.query(
      `UPDATE api_keys
          SET uses = uses + 1,
              day_uses = CASE WHEN day_stamp = $2 THEN day_uses + 1 ELSE 1 END,
              day_stamp = $2,
              last_used_at = $3
        WHERE key = $1 AND revoked = FALSE
        RETURNING *`,
      [raw, today, Date.now()]
    ));
    if (!r.rows[0]) return { ok: false, reason: 'revoked' };
    return { ok: true, rec: rowToRec(r.rows[0]) };
  }

  const list = await listKeys();
  const hit = list.find((x) => x.key === raw);
  if (!hit) return { ok: false, reason: 'unknown' };
  hit.dayStamp = today;
  hit.dayUses = (sameDay ? rec.dayUses : 0) + 1;
  hit.uses = (hit.uses || 0) + 1;
  hit.lastUsedAt = Date.now();
  if (mode === 'kv') await withRetry(() => kv.set(K_KEYS, JSON.stringify(list)));
  else writeFile(list);
  return { ok: true, rec: hit };
}

// ---------- jobs ----------
async function listJobs(limit = 200) {
  await ready;
  if (mode === 'pg') {
    const r = await withRetry(() => pool.query('SELECT * FROM jobs ORDER BY created_at DESC LIMIT $1', [limit]));
    return r.rows.map(rowToJob);
  }
  if (mode === 'kv') {
    const raw = await withRetry(() => kv.get(K_JOBS));
    try { return JSON.parse(raw || '[]').slice(0, limit); } catch { return []; }
  }
  return readLocalJobs().slice(0, limit);
}

async function getJob(jobId) {
  return (await listJobs(500)).find((x) => x.jobId === jobId) || null;
}

async function saveJob(job) {
  await ready;
  if (mode === 'pg') {
    const row = jobToRow(job);
    await withRetry(() => pool.query(
      `INSERT INTO jobs (job_id,key_id,prompt,model,duration,aspect_ratio,resolution,status,output,remote,proxy,created_at,finished_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
       ON CONFLICT (job_id) DO UPDATE SET
         key_id=EXCLUDED.key_id, prompt=EXCLUDED.prompt, model=EXCLUDED.model,
         duration=EXCLUDED.duration, aspect_ratio=EXCLUDED.aspect_ratio, resolution=EXCLUDED.resolution,
         status=EXCLUDED.status, output=EXCLUDED.output, remote=EXCLUDED.remote,
         proxy=EXCLUDED.proxy, finished_at=EXCLUDED.finished_at`,
      [row.job_id, row.key_id, row.prompt, row.model, row.duration, row.aspect_ratio, row.resolution,
       row.status, row.output, row.remote, row.proxy, row.created_at, row.finished_at]
    ));
    mirrorJobs();
    return job;
  }

  const list = await listJobs(500);
  const i = list.findIndex((x) => x.jobId === job.jobId);
  if (i >= 0) list[i] = job; else list.unshift(job);
  const trimmed = list.slice(0, 200);
  if (mode === 'kv') await withRetry(() => kv.set(K_JOBS, JSON.stringify(trimmed)));
  else fs.writeFileSync(historyFile(), JSON.stringify(trimmed, null, 2));
  return job;
}

async function deleteJob(jobId) {
  await ready;
  if (mode === 'pg') {
    await withRetry(() => pool.query('DELETE FROM jobs WHERE job_id = $1', [jobId]));
    mirrorJobs();
    return;
  }
  const list = (await listJobs(500)).filter((x) => x.jobId !== jobId);
  if (mode === 'kv') await withRetry(() => kv.set(K_JOBS, JSON.stringify(list)));
  else fs.writeFileSync(historyFile(), JSON.stringify(list, null, 2));
  mirrorJobs();
}

// ---------- videos ----------
// Free Key Value is 25 MB, too small for mp4 payloads, so bytes only live in
// postgres. Everywhere else the local file is the cache and the upstream CDN
// URL stored on the job is the recovery path.
async function saveVideo(jobId, buf) {
  await ready;
  if (mode !== 'pg') return false;
  await withRetry(() => pool.query(
    `INSERT INTO videos (job_id, data, size, created_at) VALUES ($1, $2, $3, $4)
     ON CONFLICT (job_id) DO UPDATE SET data = EXCLUDED.data, size = EXCLUDED.size`,
    [jobId, buf, buf.length, Date.now()]
  ));
  return true;
}

async function getVideo(jobId) {
  await ready;
  if (mode !== 'pg') return null;
  const r = await withRetry(() => pool.query('SELECT data, size FROM videos WHERE job_id = $1', [jobId]));
  if (!r.rows[0]) return null;
  const data = r.rows[0].data;
  return { buf: Buffer.isBuffer(data) ? data : Buffer.from(data), size: Number(r.rows[0].size) };
}

// ---------- migration: local disk -> remote store ----------
// runs on every successful connect, ON CONFLICT DO NOTHING so it can never
// overwrite anything newer that is already in the store
async function importFromDisk() {
  if (mode === 'file') return { keys: 0, jobs: 0, videos: 0 };
  const out = { keys: 0, jobs: 0, videos: 0 };

  const localKeys = (() => { try { return JSON.parse(fs.readFileSync(file, 'utf8')); } catch { return []; } })();
  const localJobs = readLocalJobs();

  if (mode === 'pg') {
    for (const k of localKeys) {
      if (!k || !k.key) continue;
      await withRetry(() => pool.query(
        `INSERT INTO api_keys (key,name,created_at,uses,last_used_at,day_stamp,day_uses,daily_limit,revoked)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT (key) DO NOTHING`,
        [k.key, k.name || '', Number(k.createdAt) || Date.now(), k.uses | 0, k.lastUsedAt ?? null,
         k.dayStamp || '', k.dayUses | 0, k.dailyLimit || 100, !!k.revoked]
      ));
      out.keys++;
    }
    for (const j of localJobs) {
      if (!j || !j.jobId) continue;
      const r = jobToRow(j);
      await withRetry(() => pool.query(
        `INSERT INTO jobs (job_id,key_id,prompt,model,duration,aspect_ratio,resolution,status,output,remote,proxy,created_at,finished_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) ON CONFLICT (job_id) DO NOTHING`,
        [r.job_id, r.key_id, r.prompt, r.model, r.duration, r.aspect_ratio, r.resolution,
         r.status, r.output, r.remote, r.proxy, r.created_at, r.finished_at]
      ));
      out.jobs++;
    }
    // mp4 bytes, capped so we never blow the free 1 GB allowance
    if (videosDir && fs.existsSync(videosDir)) {
      let budget = 400 * 1024 * 1024;
      for (const name of fs.readdirSync(videosDir)) {
        if (!name.endsWith('.mp4')) continue;
        try {
          const buf = fs.readFileSync(path.join(videosDir, name));
          if (buf.length > budget) continue;
          const jobId = name.replace(/\.mp4$/, '');
          const r = await withRetry(() => pool.query('SELECT 1 FROM videos WHERE job_id = $1', [jobId]));
          if (r.rows.length) continue;
          await saveVideo(jobId, buf);
          budget -= buf.length;
          out.videos++;
        } catch { /* skip this file */ }
      }
    }
    return out;
  }

  // key value: merge, never overwrite what is already stored
  const storedKeys = await listKeys();
  const have = new Set(storedKeys.map((k) => k.key));
  const mergedKeys = storedKeys.slice();
  for (const k of localKeys) {
    if (!k || !k.key || have.has(k.key)) continue;
    mergedKeys.push(k); out.keys++;
  }
  if (out.keys) await withRetry(() => kv.set(K_KEYS, JSON.stringify(mergedKeys)));

  const storedJobs = await listJobs(500);
  const haveJob = new Set(storedJobs.map((j) => j.jobId));
  const mergedJobs = storedJobs.slice();
  for (const j of localJobs) {
    if (!j || !j.jobId || haveJob.has(j.jobId)) continue;
    mergedJobs.push(j); out.jobs++;
  }
  if (out.jobs) {
    mergedJobs.sort((a, b) => (b.createdAt || 0) - (a.createdAt || 0));
    await withRetry(() => kv.set(K_JOBS, JSON.stringify(mergedJobs.slice(0, 200))));
  }
  return out;
}

// Keep a local copy while running on a remote store, so anything can be
// re-imported later if that store is ever lost. Merges instead of overwriting:
// the local file may still hold rows that importFromDisk has not picked up yet.
function unionById(existing, incoming, id) {
  const byId = new Map();
  for (const x of existing) if (x && x[id]) byId.set(x[id], x);
  for (const x of incoming) if (x && x[id]) byId.set(x[id], x); // store wins
  return [...byId.values()];
}

async function mirrorKeys() {
  if (mode === 'file') return;
  try {
    const remote = mode === 'kv'
      ? await listKeys()
      : (await withRetry(() => pool.query('SELECT * FROM api_keys ORDER BY created_at DESC'))).rows.map(rowToRec);
    const merged = unionById(readFile(), remote, 'key');
    fs.writeFileSync(file, JSON.stringify(merged, null, 2), { mode: 0o600 });
  } catch { /* best effort */ }
}

async function mirrorJobs() {
  if (mode === 'file') return;
  try {
    const remote = mode === 'kv'
      ? await listJobs(500)
      : (await withRetry(() => pool.query('SELECT * FROM jobs ORDER BY created_at DESC LIMIT 500'))).rows.map(rowToJob);
    const merged = unionById(readLocalJobs(), remote, 'jobId');
    merged.sort((a, b) => (b.createdAt || 0) - (a.createdAt || 0));
    fs.writeFileSync(historyFile(), JSON.stringify(merged.slice(0, 500), null, 2));
  } catch { /* best effort */ }
}

// true when data written now will survive a redeploy and a spin-down
function durable() { return (mode === 'pg' || mode === 'kv') && !degraded; }

module.exports = {
  init, listKeys, getKey, createKey, revokeKey, touchKey,
  listJobs, getJob, saveJob, deleteJob, saveVideo, getVideo,
  importFromDisk, mirrorKeys, mirrorJobs, durable,
  get mode() { return mode; },
  isPg: () => mode === 'pg',
  wantsPg: () => !!dbUrl,
  wantsKv: () => !!kvUrl,
  isDegraded: () => degraded,
  pool: () => pool,
  kv: () => kv,
};