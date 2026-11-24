const crypto = require('crypto');

const UID = process.env.VEO_UID || '64ea8bbff1854dfc5dbf0a138804a7f4';
const BASE = 'https://veo-online.ai';

function md5(s) { return crypto.createHash('md5').update(s).digest('hex'); }

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
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/152.0.0.0 Safari/537.36',
    'accept-language': 'ru,en;q=0.9',
  };
}

async function createJob(opts) {
  const body = {
    job_type: 'home',
    estimated_time: opts.estimated_time ?? 220,
    model: opts.model,
    prompt: opts.prompt,
    duration: opts.duration,
    aspect_ratio: opts.aspect_ratio,
    resolution: opts.resolution,
  };
  if (opts.first_frame_image) body.first_frame_image = opts.first_frame_image;
  const t = Date.now();
  const res = await fetch(BASE + '/api/video/job', { method: 'POST', headers: jobHeaders(body, t), body: JSON.stringify(body) });
  const text = await res.text();
  return { status: res.status, text };
}

async function query(ids) {
  const res = await fetch(BASE + '/api/workflow/query', {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'user-id': UID,
      origin: BASE,
      referer: BASE + '/',
      'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/152.0.0.0 Safari/537.36',
    },
    body: JSON.stringify({ jobs: ids.map(id => ({ id })) }),
  });
  return res.json();
}

(async () => {
  const c = await createJob({ model: 'veo3.1-fast', prompt: 'a red fox running through snow, cinematic', duration: 4, aspect_ratio: '16:9', resolution: '720P' });
  console.log('CREATE', c.status, c.text);
  let jobId;
  try { jobId = JSON.parse(c.text).data.job_id; } catch {}
  if (!jobId) return;
  for (let i = 0; i < 60; i++) {
    await new Promise(r => setTimeout(r, 5000));
    const q = await query([jobId]);
    const j = q?.data?.jobs?.[0];
    console.log(i, j?.status, j?.output);
    if (j && j.status !== 'processing' && j.status !== 'pending') { console.log('DONE', JSON.stringify(j)); break; }
  }
})();
