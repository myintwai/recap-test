#!/usr/bin/env node
/*
 * Mini n8n runner for the "Movie Recap Workflow (PC)" main chain (recap mode, uploaded video).
 * Runs the workflow's own Code-node JavaScript and SSH-node bash scripts UNCHANGED, locally.
 *
 * usage: node run_pipeline.js workflow.json input.json [--until "Node Name"]
 *   input.json = { "url":"local:KEY", "lang":"my", "chatId":"123", "geminiKey":"...",
 *                  "voicePref":"my-MM-ThihaNeural", "tonePref":"natural", "modePref":"recap", "job_id":"abc" }
 *   (geminiKey may be omitted; then env GEMINI_KEY is used)
 */
const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');

const wfPath = process.argv[2];
const inPath = process.argv[3];
const untilIdx = process.argv.indexOf('--until');
const UNTIL = untilIdx > -1 ? process.argv[untilIdx + 1] : null;
if (!wfPath || !inPath) { console.error('usage: node run_pipeline.js workflow.json input.json [--until "Node"]'); process.exit(2); }

const wf = JSON.parse(fs.readFileSync(wfPath, 'utf8'));
const NODES = Object.fromEntries(wf.nodes.map(n => [n.name, n]));
const body = JSON.parse(fs.readFileSync(inPath, 'utf8'));
if (!body.geminiKey) body.geminiKey = process.env.GEMINI_KEY || '';

const OUT = process.env.OUT_DIR || path.join(process.cwd(), 'out');
fs.mkdirSync(OUT, { recursive: true });

const LOCK_DIR = '/media/dub/.pipeline.lock';   // Download Video's queue lock; one runner = one job, so clear it at start/end
const clearLock = () => { try { fs.rmSync(LOCK_DIR, { recursive: true, force: true }); } catch (e) {} };
clearLock();
const out = {};            // node name -> [{json, binary?}]
const log = (...a) => console.log('[runner]', ...a);
const sleep = ms => new Promise(r => setTimeout(r, ms));

function nodeRef(name) {
  const items = out[name];
  if (!items) throw new Error(`Node "${name}" has not run yet`);
  return { first: () => items[0], last: () => items[items.length - 1], all: () => items, item: items[0], isExecuted: true };
}
function makeCtx(inputItems) {
  const input = { first: () => inputItems[0], last: () => inputItems[inputItems.length - 1], all: () => inputItems, item: inputItems[0] };
  const $ = (name) => nodeRef(name);
  return { $, $input: input, $json: (inputItems[0] || {}).json || {}, $items: () => inputItems };
}

// ---- n8n-style expression handling -------------------------------------------------
function evalExpr(expr, ctx) {
  const fn = new Function('$', '$json', '$input', '$env', 'Buffer', `return (${expr});`);
  return fn(ctx.$, ctx.$json, ctx.$input, process.env, Buffer);
}
function resolveTemplate(str, ctx) {
  if (typeof str !== 'string') return str;
  if (!str.startsWith('=')) return str;           // plain (non-expression) value
  const s = str.slice(1);
  return s.replace(/\{\{([\s\S]*?)\}\}/g, (_, e) => {
    const v = evalExpr(e.trim(), ctx);
    return v === undefined || v === null ? '' : (typeof v === 'object' ? JSON.stringify(v) : String(v));
  });
}

// ---- executors ---------------------------------------------------------------------
async function runCode(node, inputItems) {
  const ctx = makeCtx(inputItems);
  const code = node.parameters.jsCode;
  const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;
  const fn = new AsyncFunction('$', '$input', '$json', '$items', 'Buffer', 'require', 'console', 'process', code);
  const res = await fn(ctx.$, ctx.$input, ctx.$json, ctx.$items, Buffer, require, console, process);
  if (!Array.isArray(res)) throw new Error(`Code node "${node.name}" did not return an array`);
  return res;
}

function runBash(command, extraEnv = {}) {
  return new Promise((resolve) => {
    const child = spawn('bash', ['-c', command], { env: { ...process.env, ...extraEnv }, stdio: ['ignore', 'pipe', 'pipe'] });
    let stdout = '', stderr = '';
    child.stdout.on('data', d => { const s = d.toString(); stdout += s; process.stdout.write(s); });
    child.stderr.on('data', d => { const s = d.toString(); stderr += s; process.stderr.write(s); });
    child.on('close', (code, signal) => resolve({ code: code === null ? -1 : code, signal, stdout, stderr }));
  });
}
async function runSsh(node, inputItems) {
  const ctx = makeCtx(inputItems);
  const cmd = resolveTemplate(node.parameters.command, ctx);
  const r = await runBash(cmd);
  const item = { json: { code: r.code, signal: r.signal, stdout: r.stdout, stderr: r.stderr } };
  if (r.code !== 0) item.json.error = `Command exited with code ${r.code}`;
  return [item];
}

async function runGemini(node, inputItems) {
  if (process.env.STUB_GEMINI) {                                   // testing only: fake Gemini answer from the parsed segments
    const segs = out['Parse Transcript'][0].json.segments || [];
    const lines = ['<<N>> ဒီမှာ ဇာတ်လမ်းစတယ် <<M>> မင်း ဘာလုပ်နေတာလဲ <<F>> ငါ့ကို မမေးနဲ့', '<<N>> ပြီးတော့ ဘာဖြစ်လဲဆိုတော့ <<OM>> ကလေးတွေ လာကြ'];
    const body = { seo: { title: 'test', hashtags: '#test' }, segments: segs.map((x, i) => ({ id: x.id, burmese: lines[i % 2] })) };
    return [{ json: { candidates: [{ content: { parts: [{ text: JSON.stringify(body) }] } }] } }];
  }
  const p = node.parameters;
  const maxTries = node.retryOnFail ? (node.maxTries || 3) : 1;
  const wait = node.waitBetweenTries || 5000;
  const ctx = makeCtx(inputItems);
  const url = resolveTemplate(p.url, ctx);
  const bodyStr = resolveTemplate(p.jsonBody, ctx);
  let last = null;
  for (let t = 1; t <= maxTries; t++) {
    try {
      const resp = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: bodyStr, signal: AbortSignal.timeout(15 * 60 * 1000) });
      const text = await resp.text();
      let json; try { json = JSON.parse(text); } catch (e) { json = { raw: text }; }
      if (resp.ok && !json.error) return [{ json }];
      const msg = (json.error && (json.error.message || JSON.stringify(json.error))) || `HTTP ${resp.status}`;
      last = { error: `${node.name}: ${msg}`.slice(0, 600) };
      log(`${node.name} try ${t}/${maxTries} failed: ${last.error}`);
    } catch (e) {
      last = { error: `${node.name}: ${e.message}` };
      log(`${node.name} try ${t}/${maxTries} threw: ${e.message}`);
    }
    if (t < maxTries) await sleep(wait * t);
  }
  return [{ json: last }];
}

const STUBS = process.env.STUBS ? JSON.parse(fs.readFileSync(process.env.STUBS, 'utf8')) : {};   // testing only

async function execNode(name, inputItems) {
  if (STUBS[name]) { out[name] = STUBS[name]; log(`>>> ${name} (STUBBED for testing)`); return out[name]; }
  const node = NODES[name];
  if (!node) throw new Error(`Workflow has no node named "${name}"`);
  const type = node.type.split('.').pop();
  const maxTries = (node.retryOnFail && type === 'ssh') ? (node.maxTries || 2) : 1;
  log(`>>> ${name} (${type})`);
  const t0 = Date.now();
  let res, lastErr;
  for (let t = 1; t <= maxTries; t++) {
    try {
      if (type === 'code') res = await runCode(node, inputItems);
      else if (type === 'ssh') res = await runSsh(node, inputItems);
      else if (type === 'httpRequest') res = await runGemini(node, inputItems);
      else throw new Error(`Unsupported node type ${type}`);
      lastErr = null;
      if (type === 'ssh' && res[0].json.error && t < maxTries) { log(`${name} failed, retry ${t + 1}/${maxTries}`); await sleep(5000); continue; }
      break;
    } catch (e) {
      lastErr = e;
      log(`${name} threw: ${e.message}`);
      if (t < maxTries) await sleep(5000);
    }
  }
  if (lastErr) {
    if (node.onError === 'continueRegularOutput') res = [{ json: { error: lastErr.message } }];
    else { out[name] = [{ json: { error: lastErr.message } }]; throw lastErr; }
  }
  out[name] = res;
  log(`<<< ${name} done in ${((Date.now() - t0) / 1000).toFixed(1)}s`);
  if (UNTIL === name) { log(`stopping after "${name}" (--until)`); finish(0, true); }
  return res;
}

function fatalIfError(name, label) {
  const j = (out[name] && out[name][0] && out[name][0].json) || {};
  if (j.error) {
    const msg = typeof j.error === 'string' ? j.error : JSON.stringify(j.error);
    log(`FAILED at ${label || name}: ${msg}`);
    fs.writeFileSync(path.join(OUT, 'error.txt'), `${label || name}: ${msg}\n`);
    finish(1);
  }
}

let finished = false;
function finish(code, soft) {
  if (finished) return; finished = true;
  clearLock();
  try {
    const summary = {};
    for (const [k, v] of Object.entries(out)) {
      const j = (v[0] && v[0].json) || {};
      summary[k] = Object.fromEntries(Object.entries(j).filter(([kk, vv]) => typeof vv !== 'object' && String(vv).length < 200));
    }
    fs.writeFileSync(path.join(OUT, 'run_summary.json'), JSON.stringify(summary, null, 2));
  } catch (e) {}
  process.exit(code);
}

// ---- main chain --------------------------------------------------------------------
(async () => {
  let cur = [{ json: body }];
  const step = async (name) => { cur = await execNode(name, cur); return cur; };

  await step('Validate URL');
  if (cur[0].json.valid === false) { log('Invalid input: ' + cur[0].json.error); fs.writeFileSync(path.join(OUT, 'error.txt'), String(cur[0].json.error)); finish(1); }

  for (const n of ['Download Video', 'Parse Video Info']) { await step(n); }
  fatalIfError('Parse Video Info', 'Download/Probe');

  for (const n of ['Extract Audio & Transcribe', 'Parse Transcript']) { await step(n); }
  fatalIfError('Parse Transcript', 'Transcribe');

  for (const n of ['Extract Gap Frames', 'Parse Gap Frames', 'Build Gemini Prompt', 'Add Length Contract',
                   'Add Tone And Gender', 'Add Voice Tags', 'Long Segment Budgets']) { await step(n); }

  // Gemini with the same fallback chain as the workflow
  let g = await execNode('Call Gemini API', cur);
  if (g[0].json.error) {
    g = await execNode('Call Gemini Fallback', cur);
    if (g[0].json.error) g = await execNode('Call Gemini Fallback 2', cur);
  }
  if (g[0].json.error) { fatalIfError(g === out['Call Gemini Fallback 2'] ? 'Call Gemini Fallback 2' : 'Call Gemini API', 'Gemini'); }

  cur = await execNode('Build Plan', g);
  fatalIfError('Build Plan', 'Build Plan');

  for (const n of ['Tag Voices', 'Generate Voiceover', 'Render']) { await step(n); }

  const wd = out['Build Plan'][0].json.work_dir;
  const finalPath = path.join(wd, 'final.mp4');
  if (!fs.existsSync(finalPath) || fs.statSync(finalPath).size < 1000) {
    const rs = (out['Render'][0].json.stdout || '').split('\n').filter(l => l.includes('ERROR')).join('; ');
    log('FAILED: final.mp4 missing. ' + rs);
    fs.writeFileSync(path.join(OUT, 'error.txt'), 'Render: ' + (rs || 'final.mp4 missing') + '\n');
    finish(1);
  }
  fs.copyFileSync(finalPath, path.join(OUT, 'final.mp4'));
  const bp = out['Build Plan'][0].json;
  const tv = out['Tag Voices'] && out['Tag Voices'][0].json;
  try { if (tv && tv.burmese_srt_b64) fs.writeFileSync(path.join(OUT, 'script.srt'), Buffer.from(tv.burmese_srt_b64, 'base64')); } catch (e) {}
  try { if (tv && tv.voiceover_text_b64) fs.writeFileSync(path.join(OUT, 'voiceover.txt'), Buffer.from(tv.voiceover_text_b64, 'base64')); } catch (e) {}
  for (const f of ['vo_full.mp3']) { try { fs.copyFileSync(path.join(wd, f), path.join(OUT, f)); } catch (e) {} }
  log(`SUCCESS: ${path.join(OUT, 'final.mp4')} (${(fs.statSync(path.join(OUT, 'final.mp4')).size / 1048576).toFixed(1)} MB)`);
  finish(0);
})().catch(e => { log('FATAL: ' + (e && e.stack || e)); try { fs.writeFileSync(path.join(OUT, 'error.txt'), String(e && e.message || e)); } catch (x) {} finish(1); });
