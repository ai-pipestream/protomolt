#!/usr/bin/env node
// Test-owned Chromium interaction; the credential is supplied through the environment.
import {spawn} from 'node:child_process';
import {mkdtempSync, writeFileSync, rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';

const phase = process.env.REVIEW_PHASE;
const taskId = process.env.REVIEW_TASK_ID;
const token = process.env.REVIEW_BROWSER_TOKEN;
const base = process.env.HTTP_BASE;
const screenshotPath = process.env.SCREENSHOT_PATH;
if (!['start', 'failed', 'retry', 'accepted'].includes(phase) || !token || !base
    || !screenshotPath || (phase !== 'start' && !taskId)) {
  throw new Error('Review browser test inputs are incomplete');
}
const profile = mkdtempSync(join(tmpdir(), 'protomolt-review-browser-'));
const port = String(20000 + Math.floor(Math.random() * 30000));
const browser = spawn(process.env.CHROME_BIN ?? 'google-chrome', [
  '--headless=new', '--no-sandbox', '--disable-gpu', `--remote-debugging-port=${port}`,
  `--user-data-dir=${profile}`, '--window-size=1280,1200', 'about:blank',
], {stdio: 'ignore'});
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
let socket;
try {
  let pages;
  for (let n = 0; n < 100; n++) {
    try { pages = await (await fetch(`http://127.0.0.1:${port}/json/list`)).json(); break; }
    catch { await sleep(100); }
  }
  if (!pages) throw new Error('Chromium did not start');
  socket = new WebSocket(pages.find(page => page.type === 'page').webSocketDebuggerUrl);
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
    const timer = setTimeout(() => {
      pending.delete(id); reject(new Error(`Browser command timed out: ${method}`));
    }, 15000);
    pending.set(id, {resolve, reject, timer});
    socket.send(JSON.stringify({id, method, params}));
  });
  const evaluate = async expression => {
    const result = await call('Runtime.evaluate', {expression, returnByValue: true, awaitPromise: true});
    if (result.exceptionDetails) throw new Error('Browser interaction failed');
    return result.result?.value;
  };
  const until = async (expression, action) => {
    const deadline = Date.now() + 120000;
    while (Date.now() < deadline) {
      if (await evaluate(expression)) return;
      if (action) await evaluate(action);
      await sleep(400);
    }
    throw new Error(`Browser ${phase} expectation timed out`);
  };
  const button = label => `[...document.querySelectorAll('button')].find(x => x.textContent.trim() === ${JSON.stringify(label)} && !x.disabled)`;
  const click = async label => { await until(`!!(${button(label)})`); await evaluate(`${button(label)}.click()`); };
  await call('Page.enable');
  const navigation = await call('Page.navigate', {url: `${base}/console/tasks`});
  if (navigation.errorText) throw new Error('Browser navigation failed');
  await until(`!!document.querySelector('input[type="password"]')`);
  await evaluate(`(() => { const input = document.querySelector('input[type="password"]');
    Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value').set.call(input, ${JSON.stringify(token)});
    input.dispatchEvent(new Event('input', {bubbles:true})); })()`);
  await click('Connect');
  if (phase === 'start') {
    await until(`!!document.querySelector('.worker-card .presence-dot.online')`,
      `document.querySelector('button[aria-label="Refresh tasks"]')?.click()`);
    await click('Author a workflow');
    await click('Start workflow authoring');
    await until(`!!document.querySelector('.task-list .v-list-item--active')`);
  } else {
    const shortId = taskId.slice(0, 8);
    const item = `[...document.querySelectorAll('.task-list .v-list-item')].find(x => x.textContent.includes(${JSON.stringify(shortId)}))`;
    await until(`!!(${item})`, `document.querySelector('button[aria-label="Refresh tasks"]')?.click()`);
    await evaluate(`${item}.click()`);
    if (phase === 'failed') {
      await until(`document.querySelector('.v-alert')?.innerText.includes('Review could not finish (')
        && !!(${button('Retry review')})`);
    } else if (phase === 'retry') {
      await click('Retry review');
      await until(`document.querySelector('.task-list .v-list-item--active')?.innerText.includes('accepted')`);
    } else {
      await until(`document.querySelector('.task-list .v-list-item--active')?.innerText.includes('accepted')`);
      const visibleRetry = await evaluate(`!!(${button('Retry review')})`);
      if (visibleRetry) throw new Error('Accepted task still offers review retry');
    }
  }
  const screenshot = await call('Page.captureScreenshot', {format: 'png', captureBeyondViewport: true});
  writeFileSync(screenshotPath, Buffer.from(screenshot.data, 'base64'));
  console.log(JSON.stringify({browser: 'Chromium', phase, taskId: taskId ?? null, screenshot: screenshotPath}));
} catch (failure) {
  if (socket?.readyState === WebSocket.OPEN) {
    try {
      const screenshot = await new Promise((resolve, reject) => {
        const id = 999999;
        const timer = setTimeout(() => reject(new Error('Screenshot timed out')), 5000);
        const handler = event => {
          const message = JSON.parse(event.data);
          if (message.id !== id) return;
          clearTimeout(timer);
          socket.removeEventListener('message', handler);
          message.error ? reject(new Error('Screenshot failed')) : resolve(message.result);
        };
        socket.addEventListener('message', handler);
        socket.send(JSON.stringify({id, method: 'Page.captureScreenshot', params: {format: 'png'}}));
      });
      writeFileSync(screenshotPath, Buffer.from(screenshot.data, 'base64'));
    } catch { /* Preserve the original browser failure. */ }
  }
  throw failure;
} finally {
  socket?.close();
  browser.kill();
  await sleep(500);
  rmSync(profile, {recursive: true, force: true});
}
