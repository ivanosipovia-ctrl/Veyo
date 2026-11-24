const crypto = require('crypto');
const BASE = 'https://veo-online.ai';
const md5 = (s) => crypto.createHash('md5').update(s).digest('hex');
const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/152.0.0.0 Safari/537.36';

async function tryUid(uid) {
  const body = { job_type: 'home', estimated_time: 220, model: 'veo3.1-fast', prompt: 'quota probe ' + uid.slice(0, 6), duration: 4, aspect_ratio: '16:9', resolution: '720P' };
  const t = Date.now();
  const cid = md5(JSON.stringify({ job_type: body.job_type, estimated_time: body.estimated_time })) + md5(String(t)).slice(0, 16);
  const res = await fetch(BASE + '/api/video/job', {
    method: 'POST',
    headers: {
      'content-type': 'application/json', accept: 'application/json', 'user-id': uid,
      'Request-Time': String(t), 'Request-Create-Id': cid,
      origin: BASE, referer: BASE + '/', 'user-agent': UA, 'accept-language': 'ru,en;q=0.9',
    },
    body: JSON.stringify(body),
  });
  const j = await res.json().catch(() => null);
  return { uid, code: j?.code, msg: j?.message || j?.id, jobId: j?.data?.job_id };
}

(async () => {
  const known = process.argv[2];
  const fresh = crypto.randomBytes(16).toString('hex');
  if (known) console.log('existing:', JSON.stringify(await tryUid(known)));
  console.log('fresh   :', JSON.stringify(await tryUid(fresh)));
})();
