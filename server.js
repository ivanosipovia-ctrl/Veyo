const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

// ---------- config (env first so Render/Docker can mount a disk) ----------
const PORT = process.env.PORT || 3000;
const BASE = process.env.UPSTREAM_BASE || 'https://veo-online.ai';
const ROOT = __dirname;
const PUBLIC = path.join(ROOT, 'public');
const VIDEOS = process.env.VIDEOS_DIR ? path.resolve(process.env.VIDEOS_DIR) : path.join(ROOT, 'videos');
const DATA = process.env.DATA_DIR ? path.resolve(process.env.DATA_DIR) : path.join(ROOT, 'data');
const DAILY_LIMIT = Number(process.env.KEY_DAILY_LIMIT || 100);
const CORS_ORIGIN = process.env.CORS_ORIGIN || '*';
// Render sets RENDER=true on every instance — used to tell a real deployment
// from local development when reporting whether storage survives a restart
const ON_RENDER = !!process.env.RENDER;

fs.mkdirSync(VIDEOS, { recursive: true });
fs.mkdirSync(DATA, { recursive: true });

// ---------- stable visitor id (mimics their fingerprint visitorId) ----------
const uidPath = path.join(DATA, 'user-id');
let UID;
if (fs.existsSync(uidPath)) UID = fs.readFileSync(uidPath, 'utf8').trim();
if (!/^[0-9a-f]{32}$/.test(UID || '')) {
  UID = crypto.randomBytes(16).toString('hex');
  fs.writeFileSync(uidPath, UID);
}

// ---------- storage for keys / history / video bytes ----------
// picks a backend itself: Render Key Value (from render.yaml) > Postgres if
// DATABASE_URL is set > local json for development. Anything but the local
// json survives this service redeploying or spinning down on the free plan.
const store = require('./store');

const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/152.0.0.0 Safari/537.36';
const md5 = (s) => crypto.createHash('md5').update(s).digest('hex');
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// estimated_time: UI progress estimate, per-model seconds
const ESTIMATES = { 'veo3.1-fast': 220, 'veo3.1-lite': 240, 'veo3.1': 400, 'veo3': 300, 'veo2': 260 };

const MODELS = [
  { id: 'veo3.1-fast', name: 'Veo 3.1 Fast', durations: [4, 6, 8], resolutions: ['720P', '1080P', '4K'], modes: ['text-and-image', 'frames', 'full'], badge: 'Быстро' },
  { id: 'veo3.1', name: 'Veo 3.1 Quality', durations: [4, 6, 8], resolutions: ['720P', '1080P', '4K'], modes: ['text-and-image', 'frames'], badge: 'Лучшее качество' },
  { id: 'veo3.1-lite', name: 'Veo 3.1 Lite', durations: [4, 6, 8], resolutions: ['720P', '1080P', '4K'], modes: ['text-and-image', 'frames', 'full'], badge: 'Лёгкая' },
  { id: 'veo3', name: 'Veo 3', durations: [4, 6, 8], resolutions: ['720P', '1080P'], modes: ['text-and-image'], badge: 'Аудио' },
  { id: 'veo2', name: 'Veo 2', durations: [4, 6, 8], resolutions: ['720P'], modes: ['text-and-image'], badge: 'Классика' },
];
const RATIOS = ['16:9', '9:16'];
const MODE_LABELS = { 'text-and-image': 'Текст / Изображение', frames: 'Кадры', full: 'Референсы' };

// ---------- history ----------
// in postgres mode this survives every deploy and spin-down,
// in file mode it still reads data/history.json
const readHistory = () => store.listJobs(500);

// true when the video bytes are available locally or in the database
async function videoExists(jobId) {
  const f = path.join(VIDEOS, path.basename(jobId) + '.mp4');
  if (fs.existsSync(f)) return true;
  if (store.isPg() && (await store.getVideo(jobId))) return true;
  return false;
}

// ---------- api keys ----------
const newKey = () => 'veo_' + crypto.randomBytes(24).toString('base64url');
const maskKey = (k) => k.slice(0, 9) + '…' + k.slice(-4);

async function createKey(name, dailyLimit) {
  const rec = {
    key: newKey(),
    name: String(name || 'unnamed').slice(0, 60),
    createdAt: Date.now(),
    uses: 0, lastUsedAt: null,
    dayStamp: new Date().toISOString().slice(0, 10), dayUses: 0,
    dailyLimit: Number(dailyLimit) > 0 ? Number(dailyLimit) : DAILY_LIMIT,
    revoked: false,
  };
  return store.createKey(rec);
}

async function authenticateKey(req) {
  const hdr = String(req.headers['authorization'] || '');
  const raw = hdr.toLowerCase().startsWith('bearer ') ? hdr.slice(7).trim() : String(req.headers['x-api-key'] || '').trim();
  const r = await store.touchKey(raw);
  if (r.ok) return { ok: true, rec: r.rec };
  return { ok: false, reason: r.reason, rec: r.rec };
}

// ---------- upstream ----------
function jobHeaders(body, t) {
  const cid = md5(JSON.stringify({ job_type: body.job_type, estimated_time: body.estimated_time })) + md5(String(t)).slice(0, 16);
  return {
    'content-type': 'application/json',
    accept: 'application/json',
    'user-id': UID,
    'Request-Time': String(t),
    'Request-Create-Id': cid,
    origin: BASE,
    referer: BASE + '/',
    'user-agent': UA,
    'accept-language': 'ru,en;q=0.9',
  };
}

// human-readable upstream errors — never a dead end, the address pool retries
const ERR_MAP = {
  10304: 'узел перегружен, пробую другой адрес',
  10301: 'сервис перегружен, повторяю',
  10001: 'сессия не распознана, обнови страницу',
};

// upstream per-IP limits are handled by the address pool in proxies.js —
// no blocking state, the route just switches to another address and retries.
const pool = require('./proxies');

// last limit we saw upstream, kept for diagnostics only
const UPSTREAM = { lastLimitAt: null, lastCode: null, rotates: 0 };

async function createJob(opts) {
  const body = {
    job_type: 'home',
    estimated_time: ESTIMATES[opts.model] ?? 220,
    model: MODELS.some((m) => m.id === opts.model) ? opts.model : 'veo3.1-fast',
    prompt: String(opts.prompt || '').slice(0, 5000),
    duration: [4, 6, 8].includes(Number(opts.duration)) ? Number(opts.duration) : 8,
    aspect_ratio: RATIOS.includes(opts.aspect_ratio) ? opts.aspect_ratio : '16:9',
    resolution: ['720P', '1080P', '4K'].includes(String(opts.resolution || '').toUpperCase()) ? String(opts.resolution).toUpperCase() : '720P',
    generate_audio: false,
  };
  if (opts.first_frame_image) { body.first_frame_image = opts.first_frame_image; body.mode = 'text-and-image'; }
  if (opts.mode === 'frames') { body.mode = 'frames'; body.first_frame_image = opts.first_frame_image || ''; body.last_frame_image = opts.last_frame_image || ''; }
  if (opts.mode === 'full') { body.mode = 'full'; body.reference_images = opts.reference_images || []; }

  const MAX_ATTEMPTS = 8;
  let lastErr;

  for (let attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
    if (attempt) await sleep(1200 * Math.min(attempt, 4));
    const t = Date.now();
    let res, json, via;

    try {
      const r = await pool.request(BASE + '/api/video/job', {
        method: 'POST', headers: jobHeaders(body, t), body: JSON.stringify(body),
      }, { timeout: 15000 });
      res = r.res; via = r.proxy;
      json = await res.json().catch(() => null);
    } catch (e) {
      lastErr = Object.assign(new Error('Сеть недоступна, попробуй ещё раз.'), { status: 502 });
      continue;
    }

    if (json && json.code === 10000 && json.data?.job_id) {
      if (via) pool.resetDirect();
      return { jobId: json.data.job_id, proxy: via };
    }

    // the per-IP daily limit: switch address and go again — not an error
    if (json && json.code === 10304) {
      UPSTREAM.lastLimitAt = Date.now();
      UPSTREAM.lastCode = 10304;
      UPSTREAM.rotates++;
      await pool.noteLimit(via);
      lastErr = Object.assign(new Error('узел перегружен — переключаю адрес'), { status: 503, code: 10304 });
      continue;
    }

    const msg = ERR_MAP[json?.code] || json?.message || 'upstream ' + res.status;
    lastErr = Object.assign(new Error(msg), { status: 503, code: json?.code });
    if (json && json.code === 10001) throw lastErr;
    if (res.status !== 429 && json && json.code !== 10301) throw lastErr;
  }

  throw lastErr || Object.assign(new Error('Не удалось создать задание, попробуй ещё раз.'), { status: 502 });
}

async function queryJobs(ids, proxy) {
  const { res } = await pool.request(BASE + '/api/workflow/query', {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'user-id': UID, origin: BASE, referer: BASE + '/', 'user-agent': UA },
    body: JSON.stringify({ jobs: ids.map((id) => ({ id })) }),
  }, { pin: proxy, timeout: 15000 });
  return res.json();
}

async function uploadRemote(fileBuf, fileName) {
  const t = Math.floor(Date.now() / 1000);
  const payload = JSON.stringify({ file: fileName, type: 'upload' });
  const cid = md5(payload) + md5(String(t)).slice(0, 16);
  const { res } = await pool.request(BASE + '/api/upload', {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'user-id': UID, 'Request-Create-Id': cid, 'Request-Time': String(t), origin: BASE, referer: BASE + '/', 'user-agent': UA },
    body: JSON.stringify({ file: fileName }),
  }, { timeout: 30000 });
  const json = await res.json().catch(() => null);
  const putUrl = json?.data?.upload || json?.upload;
  if (!putUrl) throw Object.assign(new Error(json?.message || 'upload failed'), { status: 502 });
  const { res: put } = await pool.request(putUrl, { method: 'PUT', body: fileBuf, headers: { 'Content-Type': '' } }, { timeout: 90000 });
  if (!put.ok) throw Object.assign(new Error('upload PUT ' + put.status), { status: 502 });
  return decodeURIComponent(putUrl.split('?OSSAccessKeyId=')[0]);
}

async function queryHistory(jobId, proxy) {
  const { res } = await pool.request(BASE + '/api/workflow/history?page=1&page_size=20&job_type=home&result_type=video', {
    headers: { 'user-id': UID, accept: 'application/json', origin: BASE, referer: BASE + '/', 'user-agent': UA },
  }, { pin: proxy, timeout: 15000 });
  const json = await res.json().catch(() => null);
  const hit = (json?.data?.items || []).find((i) => i.taskId === jobId);
  if (!hit) return null;
  return { url: hit.url, error: !!hit.isError };
}

async function resolveJob(jobId, proxy) {
  try {
    const q = await queryJobs([jobId], proxy);
    const j = q?.data?.jobs?.[0];
    if (j && (j.status === 'succeeded' || j.status === 'failed')) return { status: j.status, url: j.output || null };
  } catch { /* fall through */ }
  try {
    const h = await queryHistory(jobId, proxy);
    if (h) return h.error ? { status: 'failed', url: null } : { status: 'succeeded', url: h.url };
  } catch { /* fall through */ }
  return { status: 'processing', url: null };
}

async function fetchVideo(url, jobId, proxy) {
  const { res } = await pool.request(url, {}, { pin: proxy, timeout: 120000 });
  if (!res.ok) throw new Error('download ' + res.status);
  const buf = Buffer.from(await res.arrayBuffer());
  const file = path.basename(jobId) + '.mp4';
  fs.writeFileSync(path.join(VIDEOS, file), buf);
  // when the store is postgres, mirror the bytes there too so the video
  // outlives a redeploy — the local copy is just the fast path for streaming
  if (store.isPg()) await store.saveVideo(jobId, buf);
  return { file, size: buf.length };
}

// serve a video. Resolution order when the instance disk was wiped by a
// redeploy: local file -> database bytes -> re-pull from the upstream CDN
// (the url is kept on the job record precisely for this).
async function serveVideo(res, jobId, req) {
  const file = path.join(VIDEOS, path.basename(jobId) + '.mp4');

  if (!fs.existsSync(file)) {
    // last resort: fetch it again from upstream and cache it locally
    try {
      const job = await store.getJob(jobId);
      if (job && job.remote) await fetchVideo(job.remote, jobId, job.proxy || null);
    } catch { /* falls through to the checks below */ }
  }

  if (fs.existsSync(file)) return serveFile(res, file, req);

  if (store.isPg()) {
    const v = await store.getVideo(jobId);
    if (v && v.buf) {
      // put it back on disk so the next request streams from disk again
      try { fs.writeFileSync(file, v.buf); } catch { /* disk is best effort */ }
      const mime = 'video/mp4';
      const range = req.headers.range;
      const base = { 'content-type': mime, 'accept-ranges': 'bytes', ...corsHeaders() };
      if (range) {
        const m = /bytes=(\d*)-(\d*)/.exec(range);
        if (m) {
          const start = m[1] === '' ? 0 : parseInt(m[1], 10);
          const end = m[2] === '' ? v.size - 1 : Math.min(parseInt(m[2], 10), v.size - 1);
          if (start > end || start >= v.size) { res.writeHead(416, { 'content-range': `bytes */${v.size}` }); res.end(); return; }
          res.writeHead(206, { ...base, 'content-length': end - start + 1, 'content-range': `bytes ${start}-${end}/${v.size}` });
          res.end(v.buf.subarray(start, end + 1));
          return;
        }
      }
      res.writeHead(200, { ...base, 'content-length': v.size });
      res.end(v.buf);
      return;
    }
  }
  return json(res, 404, { error: 'not found' });
}

// ---------- http helpers ----------
function corsHeaders() {
  return {
    'access-control-allow-origin': CORS_ORIGIN,
    'access-control-allow-methods': 'GET,POST,DELETE,OPTIONS',
    'access-control-allow-headers': 'content-type,authorization,x-api-key',
    'access-control-max-age': '86400',
    vary: 'origin',
  };
}

function json(res, code, obj) {
  res.writeHead(code, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store', ...corsHeaders() });
  res.end(JSON.stringify(obj));
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let n = 0;
    req.on('data', (c) => { n += c.length; if (n > 30 * 1024 * 1024) { reject(Object.assign(new Error('файл слишком большой'), { status: 413 })); req.destroy(); } else chunks.push(c); });
    req.on('end', () => resolve(Buffer.concat(chunks)));
    req.on('error', reject);
  });
}

const parseJson = (buf, fallback) => { try { return JSON.parse(buf.toString('utf8') || fallback); } catch { return null; } };

const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8', '.svg': 'image/svg+xml', '.png': 'image/png', '.webp': 'image/webp', '.mp4': 'video/mp4', '.json': 'application/json' };

function serveFile(res, filePath, req) {
  if (!filePath || !fs.existsSync(filePath)) return json(res, 404, { error: 'not found', file: path.basename(String(filePath || '')) });
  const stat = fs.statSync(filePath);
  const mime = MIME[path.extname(filePath).toLowerCase()] || 'application/octet-stream';
  const base = { 'content-type': mime, 'accept-ranges': 'bytes', ...corsHeaders() };
  const range = req.headers.range;
  if (range) {
    const m = /bytes=(\d*)-(\d*)/.exec(range);
    if (m) {
      const start = m[1] === '' ? 0 : parseInt(m[1], 10);
      const end = m[2] === '' ? stat.size - 1 : Math.min(parseInt(m[2], 10), stat.size - 1);
      if (start > end || start >= stat.size) { res.writeHead(416, { 'content-range': `bytes */${stat.size}` }); res.end(); return; }
      res.writeHead(206, { ...base, 'content-length': end - start + 1, 'content-range': `bytes ${start}-${end}/${stat.size}` });
      fs.createReadStream(filePath, { start, end }).pipe(res);
      return;
    }
  }
  res.writeHead(200, { ...base, 'content-length': stat.size });
  fs.createReadStream(filePath).pipe(res);
}

// register a job in history, optionally owned by an api key
function trackJob(body, keyId) {
  return {
    createdAt: Date.now(),
    status: 'processing',
    prompt: body.prompt,
    model: body.model,
    duration: body.duration,
    aspect_ratio: body.aspect_ratio,
    resolution: body.resolution,
    output: null,
    keyId: keyId || null,
    proxy: null, // address this job was opened through, kept for its polling
  };
}

function saveJob(item) {
  return store.saveJob(item);
}

function publicItem(it) {
  if (!it) return null;
  const { keyId, remote, proxy, ...rest } = it;
  return rest;
}

// ---------- routes ----------
const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://x');
  const p = url.pathname;

  if (req.method === 'OPTIONS') {
    res.writeHead(204, corsHeaders());
    return res.end();
  }

  try {
    // ---- health / meta ----
    if (req.method === 'GET' && p === '/health') {
      const k = await store.listKeys();
      return json(res, 200, {
        ok: true, uptime: process.uptime(), storage: store.mode,
        durable: store.durable(), onRender: ON_RENDER,
        proxy: pool.stats(), keys: k.filter((x) => !x.revoked).length,
      });
    }

    // ---- static site ----
    if (req.method === 'GET' && (p === '/' || p === '/index.html')) {
      return serveFile(res, path.join(PUBLIC, 'index.html'), req);
    }

    // ---- internal API (drives the site UI) ----
    if (req.method === 'GET' && p === '/api/models') return json(res, 200, { models: MODELS, ratios: RATIOS, modes: MODE_LABELS });
    if (req.method === 'GET' && p === '/api/history') return json(res, 200, { items: (await readHistory()).map(publicItem) });
    if (req.method === 'GET' && p === '/api/quota') {
      return json(res, 200, { addresses: pool.stats(), upstream: UPSTREAM });
    }

    if (req.method === 'GET' && p.startsWith('/api/history/')) {
      const item = (await readHistory()).find((x) => x.jobId === p.split('/')[3]);
      return item ? json(res, 200, publicItem(item)) : json(res, 404, { error: 'not found', jobId: p.split('/')[3] });
    }

    if (req.method === 'POST' && p.startsWith('/api/history/')) {
      await store.deleteJob(p.split('/')[3]);
      return json(res, 200, { ok: true });
    }

    if (req.method === 'POST' && p === '/api/generate') {
      const body = parseJson(await readBody(req), '{}') || {};
      if (!body.prompt || !String(body.prompt).trim()) return json(res, 400, { error: 'пустой промпт' });
      const { jobId, proxy } = await createJob(body);
      const item = { ...trackJob(body), jobId, proxy };
      saveJob(item);
      pollAndDownload(jobId, proxy);
      return json(res, 200, { jobId });
    }

    if (req.method === 'POST' && p === '/api/upload') {
      const body = parseJson(await readBody(req), '{}') || {};
      const match = /^data:image\/(png|jpe?g|webp);base64,(.+)$/.exec(body.dataUrl || '');
      if (!match) return json(res, 400, { error: 'нужно изображение' });
      const buf = Buffer.from(match[2], 'base64');
      if (buf.length > 10 * 1024 * 1024) return json(res, 400, { error: 'макс 10МБ' });
      const ext = match[1] === 'jpeg' ? 'jpg' : match[1];
      const remote = await uploadRemote(buf, `upload-${Date.now()}.${ext}`);
      return json(res, 200, { url: remote });
    }

    if (req.method === 'GET' && p.startsWith('/api/status/')) {
      const jobId = p.split('/')[3];
      const item = await store.getJob(jobId);
      const r = await resolveJob(jobId, item?.proxy || null);
      if (r.status === 'succeeded' && r.url) {
        let local = item?.output;
        if (!local || !(await videoExists(jobId))) {
          const dl = await fetchVideo(r.url, jobId, item?.proxy || null);
          local = '/videos/' + dl.file;
        }
        if (item) { item.status = 'succeeded'; item.output = local; item.remote = r.url; item.finishedAt = Date.now(); await store.saveJob(item); }
        return json(res, 200, { status: 'succeeded', output: local });
      }
      if (r.status === 'failed') { if (item) { item.status = 'failed'; await store.saveJob(item); } return json(res, 200, { status: 'failed' }); }
      return json(res, 200, { status: 'processing' });
    }

    // ---- key management ----
    if (p === '/api/keys' || p.startsWith('/api/keys/')) {
      if (req.method === 'GET') {
        const list = await store.listKeys();
        return json(res, 200, {
          storage: store.mode, durable: store.durable(), onRender: ON_RENDER,
          degraded: store.isDegraded(), wantsPg: store.wantsPg(),
          items: list.map((k) => ({ ...k, key: k.revoked ? maskKey(k.key) : k.key })),
        });
      }
      if (req.method === 'POST') {
        const body = parseJson(await readBody(req), '{}') || {};
        const rec = await createKey(body.name, body.dailyLimit);
        return json(res, 200, { key: rec.key, name: rec.name, dailyLimit: rec.dailyLimit });
      }
      if (req.method === 'DELETE') {
        const raw = decodeURIComponent(p.split('/')[3] || '');
        const hit = (await store.listKeys()).find((k) => k.key === raw || maskKey(k.key) === raw);
        if (!hit) return json(res, 404, { error: 'ключ не найден' });
        await store.revokeKey(hit.key);
        return json(res, 200, { ok: true });
      }
    }

    // ---- public video for the site itself ----
    if (req.method === 'GET' && p.startsWith('/videos/')) {
      return await serveVideo(res, path.basename(p).replace(/\.mp4$/, ''), req);
    }

    // ================= external API =================
    if (p.startsWith('/v1/')) {
      const auth = await authenticateKey(req);
      if (!auth.ok) {
        const status = auth.reason === 'limit' ? 429 : 401;
        const error = auth.reason === 'limit' ? 'daily key limit reached' : auth.reason === 'missing key' ? 'missing api key' : 'invalid api key';
        return json(res, status, { error, reason: auth.reason });
      }
      const key = auth.rec.key;
      const sub = p.slice(3);

      if (req.method === 'GET' && sub === '/models') return json(res, 200, { models: MODELS, ratios: RATIOS, modes: MODE_LABELS });
      if (req.method === 'GET' && sub === '/quota') return json(res, 200, { addresses: pool.stats(), upstream: UPSTREAM, key: { dayUses: auth.rec.dayUses, dailyLimit: auth.rec.dailyLimit } });

      if (req.method === 'POST' && sub === '/generate') {
        const body = parseJson(await readBody(req), '{}') || {};
        if (!body.prompt || !String(body.prompt).trim()) return json(res, 400, { error: 'prompt is required' });
        const { jobId, proxy } = await createJob(body);
        saveJob({ ...trackJob(body, key), jobId, proxy });
        pollAndDownload(jobId, proxy);
        return json(res, 200, { job_id: jobId, status: 'processing', status_url: `/v1/jobs/${jobId}` });
      }

      if (req.method === 'POST' && sub === '/upload') {
        const body = parseJson(await readBody(req), '{}') || {};
        const match = /^data:image\/(png|jpe?g|webp);base64,(.+)$/.exec(body.dataUrl || '');
        if (!match) return json(res, 400, { error: 'dataUrl must be a png/jpeg/webp data uri' });
        const buf = Buffer.from(match[2], 'base64');
        if (buf.length > 10 * 1024 * 1024) return json(res, 400, { error: 'max 10MB' });
        const ext = match[1] === 'jpeg' ? 'jpg' : match[1];
        return json(res, 200, { url: await uploadRemote(buf, `upload-${Date.now()}.${ext}`) });
      }

      if (req.method === 'GET' && sub === '/jobs') {
        const items = (await readHistory()).filter((x) => x.keyId === key).slice(0, 50);
        return json(res, 200, { items: items.map((x) => jobView(req, x)) });
      }

      if (sub.startsWith('/jobs/')) {
        const jobId = decodeURIComponent(sub.split('/')[2] || '');
        const item = await store.getJob(jobId);
        if (!item || item.keyId !== key) return json(res, 404, { error: 'job not found' });

        if (req.method === 'GET') {
          const r = await resolveJob(jobId, item.proxy || null);
          if (r.status === 'succeeded' && r.url) {
            let local = item.output;
            if (!local || !(await videoExists(jobId))) {
              const dl = await fetchVideo(r.url, jobId, item.proxy || null);
              local = '/videos/' + dl.file;
            }
            item.status = 'succeeded'; item.output = local; item.remote = r.url; item.finishedAt = Date.now();
            await store.saveJob(item);
            return json(res, 200, jobView(req, item));
          }
          if (r.status === 'failed') { item.status = 'failed'; await store.saveJob(item); return json(res, 200, jobView(req, item)); }
          return json(res, 200, jobView(req, item));
        }
        if (req.method === 'DELETE') {
          await store.deleteJob(jobId);
          return json(res, 200, { ok: true });
        }
      }

      // video bytes for external consumers (ownership enforced)
      if (req.method === 'GET' && sub.startsWith('/videos/')) {
        const file = path.basename(sub);
        const jobId = file.replace(/\.mp4$/, '');
        const item = await store.getJob(jobId);
        if (!item || item.keyId !== key) return json(res, 404, { error: 'video not found' });
        return await serveVideo(res, jobId, req);
      }

      return json(res, 404, { error: 'unknown endpoint' });
    }

    // ---- static ----
    if (req.method === 'GET') {
      const f = path.normalize(path.join(PUBLIC, p));
      if (f.startsWith(PUBLIC) && fs.existsSync(f) && fs.statSync(f).isFile()) return serveFile(res, f, req);
    }

    json(res, 404, {
      error: 'not found',
      path: p,
      method: req.method,
      served_from: PUBLIC,
      hint: 'working routes: GET /  ·  GET /health  ·  GET /api/models  ·  GET /api/keys  ·  GET /api/history  ·  POST /api/generate  ·  /v1/* (needs x-api-key)',
    });
  } catch (e) {
    json(res, e.status || 500, { error: String(e.message || e) });
  }
});

function absoluteUrl(req, p) {
  const host = req.headers['x-forwarded-host'] || req.headers.host;
  const proto = req.headers['x-forwarded-proto'] || 'http';
  return `${proto}://${host}${p}`;
}

function jobView(req, it) {
  const { keyId, remote, proxy, ...rest } = it;
  // external consumers must go through /v1/videos so the key is checked
  const rel = it.output ? it.output.replace(/^\/videos\//, '/v1/videos/') : null;
  return { ...rest, video_url: rel ? absoluteUrl(req, rel) : null };
}

// background: poll until done, then pull the video into local storage
async function pollAndDownload(jobId, proxy) {
  for (let i = 0; i < 240; i++) {
    await new Promise((r) => setTimeout(r, 5000));
    try {
      const r = await resolveJob(jobId, proxy || null);
      if (r.status === 'succeeded' && r.url) {
        const item = await store.getJob(jobId);
        if (!item) return;
        const dl = await fetchVideo(r.url, jobId, item.proxy || proxy || null);
        item.status = 'succeeded';
        item.output = '/videos/' + dl.file;
        item.remote = r.url;
        item.finishedAt = Date.now();
        await store.saveJob(item);
        return;
      }
      if (r.status === 'failed') {
        const item = await store.getJob(jobId);
        if (item) { item.status = 'failed'; await store.saveJob(item); }
        return;
      }
    } catch { /* retry */ }
  }
}

// resume jobs that were mid-flight when the process restarted (deploys, crashes)
async function resumePending() {
  try {
    const pending = (await readHistory()).filter((x) => x.status === 'processing');
    if (!pending.length) return;
    console.log(`  resuming ${pending.length} unfinished job(s)`);
    pending.forEach((x) => pollAndDownload(x.jobId, x.proxy || null));
  } catch (e) {
    console.error('  resume skipped: ' + e.message);
  }
}

(async () => {
  try {
    await store.init({ dataDir: DATA, databaseUrl: process.env.DATABASE_URL || '', videosDir: VIDEOS });
  } catch (e) {
    console.error('  storage init failed, using local file: ' + e.message);
  }

  pool.init(DATA);

  server.listen(PORT, () => {
    console.log(`veo proxy → http://localhost:${PORT}`);
    console.log(`  uid          ${UID}`);
    console.log(`  data         ${DATA}`);
    console.log(`  videos       ${VIDEOS}`);
    const label = store.mode === 'pg' ? 'postgres (persistent)'
      : store.mode === 'kv' ? 'key value (persistent)'
      : 'local json';
    console.log(`  storage      ${label}${store.durable() ? '' : '  ⚠ NOT DURABLE'}`);
    if (ON_RENDER && !store.durable()) {
      console.log('');
      console.log('  !!! STORAGE NOT DURABLE — keys are on the local disk and WILL be wiped on every restart or redeploy.');
      console.log('  !!! REDIS_HOST is not set, so this repo was not deployed as a Blueprint.');
      console.log('  !!! Render -> New -> Blueprint -> connect this repo. It creates the veo-store');
      console.log('  !!! instance and wires REDIS_HOST/REDIS_PORT by itself. No values to paste.');
      console.log('');
    }
    console.log(`  addresses    ${pool.stats().mode} — pool fills up in background`);
    console.log(`  health       /health`);
    resumePending().catch((e) => console.error('  resume failed: ' + e.message));
  });
})();
