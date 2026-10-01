#!/usr/bin/env node
// Rehearse the service-workspace tutorial against the published authoring starter.
// Node 22+ is needed for this check, not by users connecting their own MCP/ACP client.
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {createInterface} from 'node:readline';
import {randomUUID} from 'node:crypto';

const base = process.env.HTTP_BASE ?? 'http://127.0.0.1:8080';
const token = process.env.PROTOMOLT_API_TOKEN;
const launcher = process.env.ACP_LAUNCHER;
assert(token, 'Set PROTOMOLT_API_TOKEN to an authorized coordinator credential');
assert(launcher, 'Set ACP_LAUNCHER to the installed protomolt-acp-agent executable');
const profile = `tutorial-${randomUUID()}`;
const method = 'ai.protomolt.proto.samples.authoring.v1.AuthoringFixtureService/NormalizeText';
const headers = {'content-type': 'application/json', accept: 'application/json, text/event-stream', api_token: token};
let sequence = 0;
async function mcp(method, params, notification = false) {
  const response = await fetch(`${base}/mcp`, {method: 'POST', headers,
    body: JSON.stringify({jsonrpc: '2.0', ...(notification ? {} : {id: ++sequence}), method, params}),
    signal: AbortSignal.timeout(15000)});
  assert(response.ok, `MCP HTTP status ${response.status}`);
  const session = response.headers.get('mcp-session-id');
  if (session) headers['mcp-session-id'] = session;
  if (notification) return;
  const message = await response.json();
  assert(!message.error, 'MCP returned a protocol error');
  return message.result;
}
const tool = (name, args) => mcp('tools/call', {name, arguments: args});
const input = text => ({name: profile, endpoint: 'local', method, request: {text}, deadlineMs: 5000});
let child;
try {
  const initialized = await mcp('initialize', {protocolVersion: '2025-03-26',
    capabilities: {}, clientInfo: {name: 'protomolt-service-tutorial', version: '1'}});
  headers['mcp-protocol-version'] = initialized.protocolVersion;
  await mcp('notifications/initialized', {}, true);
  const registered = await tool('service-register', {
    profile: {name: profile, endpoints: [{name: 'local', host: 'fixture', port: 9778,
      transport: 'TRANSPORT_PLAINTEXT'}]}, endpoint: 'local', deadlineMs: 5000});
  assert.equal(registered.isError, false);
  assert.equal(registered.structuredContent.ok, true);
  const fingerprint = registered.structuredContent.profile.schemaSource.descriptorFingerprint;
  const inspected = await tool('service-inspect', {name: profile});
  assert.equal(inspected.isError, false);
  const valid = await tool('service-invoke', input('  Hello from MCP  '));
  assert.equal(valid.isError, false);
  assert.equal(valid.structuredContent.responses[0].text, 'Hello from MCP');
  const invalid = await tool('service-invoke', input(''));
  assert.equal(invalid.isError, true);
  assert.equal(invalid.structuredContent.error, 'invalid-input');

  // ACP forwards to the same coordinator, so its workspace must contain the MCP registration.
  child = spawn(launcher, ['--remote-target', process.env.GRPC_TARGET ?? '127.0.0.1:9090'],
    {stdio: ['pipe', 'pipe', 'inherit']});
  const pending = new Map();
  let chunks = '';
  const rejectAll = error => { for (const call of pending.values()) call.reject(error); pending.clear(); };
  child.on('error', rejectAll);
  child.on('exit', code => rejectAll(new Error(`ACP exited: ${code}`)));
  const lines = createInterface({input: child.stdout});
  lines.on('line', line => {
    let message;
    try { message = JSON.parse(line); } catch { rejectAll(new Error('ACP emitted non-JSON stdout')); return; }
    if (message.method === 'session/update') {
      const update = message.params?.update;
      if (update?.sessionUpdate === 'agent_message_chunk') chunks += update.content?.text ?? '';
    }
    if (message.id !== undefined) {
      const call = pending.get(message.id);
      if (!call) return;
      pending.delete(message.id);
      message.error ? call.reject(new Error('ACP returned a protocol error')) : call.resolve(message.result);
    }
  });
  function acp(method, params) {
    return new Promise((resolve, reject) => {
      const id = ++sequence;
      const timer = setTimeout(() => { pending.delete(id); reject(new Error(`ACP ${method} timed out`)); }, 15000);
      pending.set(id, {resolve: result => {clearTimeout(timer); resolve(result);},
        reject: error => {clearTimeout(timer); reject(error);}});
      child.stdin.write(JSON.stringify({jsonrpc: '2.0', id, method, params}) + '\n');
    });
  }
  await acp('initialize', {protocolVersion: 1, clientCapabilities: {fs: {readTextFile: false, writeTextFile: false}, terminal: false}});
  const session = await acp('session/new', {cwd: process.cwd(), mcpServers: []});
  async function prompt(name, args) {
    chunks = '';
    await acp('session/prompt', {sessionId: session.sessionId,
      prompt: [{type: 'text', text: `${name} ${JSON.stringify(args)}`}]});
    return chunks;
  }
  const acpInspect = JSON.parse(await prompt('service-inspect', {name: profile}));
  assert.equal(acpInspect.profile.name, profile);
  assert.equal(acpInspect.profile.schemaSource.descriptorFingerprint, fingerprint);
  const acpValid = JSON.parse(await prompt('service-invoke', input('  Hello from ACP  ')));
  assert.equal(acpValid.ok, true);
  assert.equal(acpValid.responses[0].text, 'Hello from ACP');
  const acpInvalid = await prompt('service-invoke', input(''));
  assert.match(acpInvalid, /INVALID_ARGUMENT|invalid-input/);
  console.log(JSON.stringify({profile, fingerprint, mcp: 'registered, inspected, invoked, rejected invalid input',
    acp: 'inspected same profile, invoked, rejected invalid input'}));
} finally {
  child?.stdin.end();
  child?.kill();
  if (headers['mcp-session-id']) {
    await fetch(`${base}/mcp`, {method: 'DELETE', headers, signal: AbortSignal.timeout(5000)});
  }
}
