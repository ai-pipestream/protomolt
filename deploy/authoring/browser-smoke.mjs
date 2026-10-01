#!/usr/bin/env node
// Release qualification uses Node 22+ and Chromium; users do not need these to run Compose.
import {spawn, execFileSync} from 'node:child_process';
import {mkdtempSync, writeFileSync, rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {randomUUID} from 'node:crypto';

const directory = process.env.STARTER_DIR ?? process.cwd();
const base = process.env.HTTP_BASE ?? 'http://127.0.0.1:8080';
const screenshotPath = process.env.SCREENSHOT_PATH ?? join(directory, 'authoring-browser.png');
const profile = mkdtempSync(join(tmpdir(), 'protomolt-authoring-browser-'));
const port = process.env.CHROME_DEBUG_PORT ?? '9238';
const browser = spawn(process.env.CHROME_BIN ?? 'google-chrome', [
  '--headless=new', '--no-sandbox', '--disable-gpu', `--remote-debugging-port=${port}`,
  `--user-data-dir=${profile}`, '--window-size=1280,1200', 'about:blank',
], {stdio: 'ignore'});
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
let socket;
try {
  let page;
  for (let n = 0; n < 100; n++) {
    try {
      const pages = await (await fetch(`http://127.0.0.1:${port}/json/list`)).json();
      page = pages.find(candidate => candidate.type === 'page' && candidate.webSocketDebuggerUrl);
      if (page) break;
    } catch { /* The debug endpoint may not be listening yet. */ }
    await sleep(100);
  }
  if (!page) throw new Error('Chromium did not expose a debuggable page');
  socket = new WebSocket(page.webSocketDebuggerUrl);
  await new Promise((resolve, reject) => { socket.onopen = resolve; socket.onerror = reject; });
  let sequence = 0;
  const pending = new Map();
  socket.onmessage = event => {
    const message = JSON.parse(event.data);
    if (!message.id) return;
    const request = pending.get(message.id);
    if (!request) return;
    pending.delete(message.id);
    clearTimeout(request.timer);
    message.error ? request.reject(new Error(message.error.message)) : request.resolve(message.result);
  };
  const call = (method, params = {}) => new Promise((resolve, reject) => {
    const id = ++sequence;
    const timer = setTimeout(() => { pending.delete(id); reject(new Error(`Browser command timed out: ${method}`)); }, 15000);
    pending.set(id, {resolve, reject, timer});
    socket.send(JSON.stringify({id, method, params}));
  });
  const evaluate = async expression => {
    const result = await call('Runtime.evaluate', {expression, returnByValue: true, awaitPromise: true});
    if (result.exceptionDetails) throw new Error('Browser interaction failed');
    return result.result?.value;
  };
  const until = async (expression, action) => {
    const deadline = Date.now() + 180000;
    while (Date.now() < deadline) {
      if (await evaluate(expression)) return;
      if (action) await evaluate(action);
      await sleep(500);
    }
    throw new Error('Authoring browser expectation timed out');
  };
  const button = label => `[...document.querySelectorAll('button')].find(x => x.textContent.trim() === ${JSON.stringify(label)} && !x.disabled)`;
  const click = async label => { await until(`!!(${button(label)})`); await evaluate(`${button(label)}.click()`); };
  await call('Page.enable');
  const navigation = await call('Page.navigate', {url: `${base}/console/tasks`});
  if (navigation.errorText) throw new Error('Browser navigation failed');
  await until(`!!document.querySelector('input[type="password"]')`);
  const token = execFileSync('docker', ['compose', 'exec', '-T', 'serve', 'cat', '/run/browser/token'],
    {cwd: directory, encoding: 'utf8'}).trim();
  await evaluate(`(() => { const input = document.querySelector('input[type="password"]');
    Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value').set.call(input, ${JSON.stringify(token)});
    input.dispatchEvent(new Event('input', {bubbles:true})); })()`);
  await click('Connect');
  await until(`!!document.querySelector('.worker-card .presence-dot.online')`,
    `document.querySelector('button[aria-label="Refresh tasks"]')?.click()`);
  await click('Author a workflow');
  const previousSelection = await evaluate(`document.querySelector('.task-list .v-list-item--active .text-mono')?.textContent ?? ''`);
  await click('Start workflow authoring');
  await until(`(() => { const selected = document.querySelector('.task-list .v-list-item--active .text-mono')?.textContent;
    return !!selected && selected !== ${JSON.stringify(previousSelection)}; })()`);
  await until(`!!document.querySelector('[aria-label="Accepted workflow launch"] textarea')`);
  const operationId = randomUUID();
  const inputJson = JSON.stringify({operationId, content: '  browser\r\n workflow  '});
  await evaluate(`(() => { const input = document.querySelector('[aria-label="Accepted workflow launch"] textarea');
    Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, 'value').set.call(input, ${JSON.stringify(inputJson)});
    input.dispatchEvent(new Event('input', {bubbles:true})); })()`);
  await click('Prepare input');
  await click('Launch workflow');
  await until(`document.querySelector('.saved-launch')?.innerText.includes('Execution completed')`,
    `${button('Refresh status')}?.click()`);
  const summary = await evaluate(`document.querySelector('.saved-launch').innerText`);
  if (!summary.includes('Authorization SHA-256:') || !summary.includes('Job ID:')) {
    throw new Error('Completed launch lacks its authorization or job identity');
  }
  const screenshot = await call('Page.captureScreenshot', {format: 'png', captureBeyondViewport: true});
  writeFileSync(screenshotPath, Buffer.from(screenshot.data, 'base64'));
  console.log(JSON.stringify({browser: 'Chromium', authoring: 'accepted', job: 'completed',
    operationId, screenshot: screenshotPath, provider: 'scripted', liveModel: false}));
} finally {
  socket?.close();
  browser.kill();
  await sleep(500);
  rmSync(profile, {recursive: true, force: true});
}
