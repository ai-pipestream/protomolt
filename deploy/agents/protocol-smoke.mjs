#!/usr/bin/env node
// Local protocol qualification. Run against a disposable agent starter stack.
import assert from 'node:assert/strict'
import { execFileSync, spawn, spawnSync } from 'node:child_process'
import { createHash, randomBytes } from 'node:crypto'
import { readFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { setTimeout as delay } from 'node:timers/promises'

const assetDir = dirname(fileURLToPath(import.meta.url))
const composeDir = process.env.PROTOMOLT_COMPOSE_DIR ?? process.cwd()
const httpBase = process.env.PROTOMOLT_HTTP_BASE ?? 'http://127.0.0.1:29832'
const grpcTarget = process.env.PROTOMOLT_GRPC_TARGET ?? '127.0.0.1:29833'
const catalogService = 'ai.protomolt.proto.grpc.service.v1.ProtoMoltService'
const delegationService = 'ai.protomolt.proto.delegation.v1.DelegationService'
const customType = 'ai.protomolt.proto.starter.protocol.v1.CustomProtocolReport'
const bundledType = 'ai.protomolt.proto.samples.starter.v1.CoordinationReport'
const customDescriptor = readFileSync(join(assetDir, 'custom-report.binpb')).toString('base64')
const nonce = randomBytes(5).toString('hex')
const results = { provider: 'protocol-fixture', liveModel: false, tests: [], tasks: {} }

function composeRead(serviceName, path) {
  return execFileSync('docker', ['compose', 'exec', '-T', serviceName, 'cat', path],
    { cwd: composeDir, encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'], timeout: 15000 }).trim()
}
const operatorToken = composeRead('serve', '/run/operator/token')
const coordinateToken = composeRead('fixture-worker', '/run/coordination/token')
assert.ok(operatorToken && coordinateToken && operatorToken !== coordinateToken)

function rpc(method, payload, service = delegationService) {
  const args = ['-plaintext', '-expand-headers', '-H',
    'api_token: ${PROTOMOLT_RPC_TOKEN}', '-d', '@', grpcTarget, `${service}/${method}`]
  const run = spawnSync('grpcurl', args, { input: JSON.stringify(payload), encoding: 'utf8',
    env: { ...process.env, PROTOMOLT_RPC_TOKEN: operatorToken }, timeout: 30000,
    maxBuffer: 8 * 1024 * 1024 })
  if (run.error || run.status !== 0) {
    throw new Error(`gRPC ${method} failed (${run.status ?? 'process error'}): `
      + (run.stderr ?? run.error?.message ?? '').trim().slice(0, 600))
  }
  return JSON.parse(run.stdout || '{}')
}

class Mcp {
  constructor(token) { this.token = token; this.id = 0; this.session = null; this.version = null }
  async send(method, params, notification = false) {
    const body = { jsonrpc: '2.0', ...(notification ? {} : { id: ++this.id }), method,
      ...(params === undefined ? {} : { params }) }
    const headers = { authorization: `Bearer ${this.token}`, 'content-type': 'application/json',
      accept: 'application/json, text/event-stream' }
    if (this.session) {
      headers['Mcp-Session-Id'] = this.session
      headers['MCP-Protocol-Version'] = this.version
    }
    const response = await fetch(`${httpBase}/mcp`, { method: 'POST', headers,
      body: JSON.stringify(body), signal: AbortSignal.timeout(30000) })
    if (notification) {
      assert.equal(response.status, 202, `MCP ${method} notification`)
      return null
    }
    assert.equal(response.status, 200, `MCP ${method} HTTP status`)
    const envelope = await response.json()
    assert.ok(!envelope.error, `MCP ${method}: ${envelope.error?.message ?? ''}`)
    if (method === 'initialize') {
      this.session = response.headers.get('mcp-session-id')
      this.version = envelope.result?.protocolVersion
      assert.ok(this.session && this.version, 'MCP session and version')
    }
    return envelope.result
  }
  async initialize() {
    await this.send('initialize', { protocolVersion: '2025-06-18', capabilities: {},
      clientInfo: { name: 'protomolt-protocol-smoke', version: '1' } })
    await this.send('notifications/initialized', undefined, true)
  }
  async tool(name, args, expectedError = false) {
    const result = await this.send('tools/call', { name, arguments: args })
    assert.equal(Boolean(result?.isError), expectedError,
      `${name}: ${result?.structuredContent?.message ?? 'unexpected tool result'}`)
    assert.ok(result.structuredContent && typeof result.structuredContent === 'object')
    return result.structuredContent
  }
}

const mcp = new Mcp(coordinateToken)
await mcp.initialize()
results.tests.push('authenticated MCP initialize')
const grpcCustomValid = rpc('ValidateMessage', { schema: { descriptorSetBase64: customDescriptor },
  type: customType, message: { title: 'Protocol gRPC qualification', observations: ['Direct validation'],
    observationCount: 1, evidenceDigest: 'a'.repeat(64) } }, catalogService)
assert.equal(grpcCustomValid.valid, true)
const grpcCustomInvalid = rpc('ValidateMessage', { schema: { descriptorSetBase64: customDescriptor },
  type: customType, message: { title: 'Protocol gRPC qualification', observations: ['Direct validation'],
    observationCount: 2, evidenceDigest: 'a'.repeat(64) } }, catalogService)
assert.notEqual(grpcCustomInvalid.valid, true)
assert.ok(grpcCustomInvalid.violations?.length > 0)
results.tests.push('authenticated direct ProtoMoltService gRPC contract validation')
assert.equal(rpc('ReadTranscript', { maxEntries: 1 }).ok, true)
results.tests.push('authenticated native DelegationService gRPC transcript read')

function workerId(suffix) { return `protocol-${suffix}-${nonce}` }
async function registerWorker(suffix) {
  const worker = workerId(suffix)
  const response = await mcp.tool('delegation-worker-register', {
    workerId: worker, provider: 'protocol-fixture',
    capabilities: [{ name: 'protocol-report', description: 'scripted harness only' }],
  })
  assert.equal(response.admitted, true)
  return worker
}
async function transcript(taskId) {
  return mcp.tool('delegation-transcript', { taskId, maxEntries: 500 })
}
async function waitFor(taskId, predicate) {
  for (let attempt = 0; attempt < 30; attempt++) {
    const current = await transcript(taskId)
    if (predicate(current.events ?? [])) return current
    await delay(200)
  }
  throw new Error(`task ${taskId} did not reach the expected transcript event`)
}
function sha256(bytes) { return createHash('sha256').update(bytes).digest('hex') }
function artifact(taskId, check) {
  const bytes = Buffer.from(JSON.stringify({ kind: 'protocol-harness-evidence', taskId,
    check, observedAt: new Date().toISOString(),
    statement: 'The harness wrote these bytes and verified the shared evidence volume.' }) + '\n')
  const digest = sha256(bytes)
  const target = `/workspace/artifacts/${digest}`
  execFileSync('docker', ['compose', 'exec', '-T', 'fixture-worker', 'sh', '-ec',
    `umask 077; cat > '${target}'`], { cwd: composeDir, input: bytes,
    stdio: ['pipe', 'ignore', 'pipe'], timeout: 15000 })
  const held = execFileSync('docker', ['compose', 'exec', '-T', 'serve', 'cat',
    `/data/evidence/${digest}`], { cwd: composeDir, encoding: null,
    stdio: ['ignore', 'pipe', 'pipe'], timeout: 15000 })
  assert.equal(sha256(held), digest)
  assert.deepEqual(held, bytes)
  return { sha256: digest, mediaType: 'application/json', sizeBytes: bytes.length,
    redacted: false }
}
function candidate(attempt, revision, check, result, reference, evidence = true) {
  return { attempt, revision, summary: 'Scripted protocol harness report; no model or build run.',
    evidence: evidence ? [{ checkName: check, verdict: 'CHECK_VERDICT_PASSED',
      ranAt: new Date().toISOString(),
      detail: 'Harness checked report values and verified referenced bytes in the serve mount.',
      artifacts: [reference] }] : [],
    artifacts: [reference], result }
}
async function offerMcp(worker, typeName, descriptorSet, check, objective) {
  const response = await mcp.tool('delegation-offer', { workerId: worker, leaseSeconds: 300,
    spec: { objective, allowedScope: ['protocol-harness/**'],
      constraints: ['Scripted local qualification; no code execution or model inference'],
      requiredChecks: [{ name: check, description: 'Harness report and artifact check' }],
      contract: { descriptorSet, typeName } } })
  assert.equal(response.ok, true)
  return response.taskId
}
function offerGrpc(worker, typeName, descriptorSet, check, objective) {
  const response = rpc('OfferTask', { workerId: worker, leaseSeconds: 300,
    spec: { objective, allowedScope: ['protocol-harness/**'],
      constraints: ['Scripted local qualification; no code execution or model inference'],
      requiredChecks: [{ name: check, description: 'Harness report and artifact check' }],
      contract: { descriptorSet, typeName } } })
  assert.equal(response.ok, true)
  return response.taskId
}
async function accept(worker, taskId) {
  const response = await mcp.tool('delegation-accept', { workerId: worker, taskId, attempt: 1 })
  assert.equal(response.ok, true)
}
async function submit(worker, taskId, report, expectedError = false) {
  return mcp.tool('delegation-candidate', { workerId: worker, taskId, candidate: report },
    expectedError)
}
function offeredCandidate(events) {
  return events.find(event => event.entry?.workerFrame?.completion)?.entry.workerFrame.completion
}

const customWorker = await registerWorker('custom')
const customCheck = 'protocol-report-valid'
const customTask = offerGrpc(customWorker, customType, customDescriptor, customCheck,
  'Produce a caller-owned custom protocol report from a scripted worker.')
results.tasks.custom = customTask
await accept(customWorker, customTask)
const customArtifact = artifact(customTask, customCheck)
const customResult = { '@type': `type.googleapis.com/${customType}`,
  title: 'Custom protocol qualification',
  observations: ['MCP worker submitted a caller-owned typed report.'],
  observationCount: 1, evidenceDigest: customArtifact.sha256 }
assert.equal(customResult.observations.length, customResult.observationCount)
assert.equal(customResult.evidenceDigest, customArtifact.sha256)
await submit(customWorker, customTask,
  candidate(1, 1, customCheck, customResult, customArtifact))
const customRecorded = await waitFor(customTask, events => Boolean(offeredCandidate(events)))
assert.equal(offeredCandidate(customRecorded.events).result?.['@type'],
  `type.googleapis.com/${customType}`)
assert.equal(offeredCandidate(customRecorded.events).result?.evidenceDigest,
  customArtifact.sha256)
results.tests.push('caller-owned descriptor via MCP and actual content-addressed artifact')
const customReview = rpc('ReviewCandidate', { taskId: customTask, attempt: 1, revision: 1,
  decision: 'REVIEW_DECISION_ACCEPT',
  verdict: 'Automated protocol harness inspected the custom typed result and shared artifact bytes; no model or external build was run.' })
assert.equal(customReview.ok, true)
await waitFor(customTask, events => events.some(event => event.entry?.coordinatorFrame?.accepted))
results.tests.push('native gRPC review bound to accepted MCP candidate identity')

// A fresh ACP process proves the packaged adapter can recover caller-owned Any types
// from the coordinator's recorded offer, then mutate a separate task over native gRPC.
async function runAcpQualification() {
  const child = spawn('docker', ['compose', '--profile', 'acp', 'run', '--rm', '--no-deps',
    '-T', '-i', 'acp-agent'], { cwd: composeDir, stdio: ['pipe', 'pipe', 'ignore'] })
  let buffer = ''
  let nextId = 0
  let failure = null
  const pending = new Map()
  const transcripts = new Map()
  function fail(message) {
    failure = failure ?? message
    for (const [id, waiter] of pending) {
      pending.delete(id)
      clearTimeout(waiter.timer)
      waiter.reject(new Error(message))
    }
  }
  child.stdout.setEncoding('utf8')
  child.stdout.on('data', chunk => {
    buffer += chunk
    for (;;) {
      const newline = buffer.indexOf('\n')
      if (newline < 0) break
      const line = buffer.slice(0, newline).trim()
      buffer = buffer.slice(newline + 1)
      if (!line) continue
      let message
      try { message = JSON.parse(line) } catch { continue }
      if (message.method === 'session/update') {
        const session = message.params?.sessionId
        const text = message.params?.update?.content?.text
        if (session && text) transcripts.set(session, (transcripts.get(session) ?? '') + text)
      }
      const waiter = pending.get(message.id)
      if (waiter) {
        pending.delete(message.id)
        clearTimeout(waiter.timer)
        waiter.resolve(message)
      }
    }
  })
  child.on('error', () => fail('ACP Compose process could not start'))
  child.stdin.on('error', () => fail('ACP input closed early'))
  child.on('close', code => fail(`ACP process closed (${code ?? 'unknown'})`))
  async function call(method, params) {
    if (failure) throw new Error(failure)
    const id = ++nextId
    const reply = new Promise((resolve, reject) => {
      const timer = setTimeout(() => { pending.delete(id); reject(new Error(`ACP ${method} timed out`)) }, 30000)
      pending.set(id, { timer, resolve: value => { clearTimeout(timer); resolve(value) }, reject })
    })
    child.stdin.write(`${JSON.stringify({ jsonrpc: '2.0', id, method, params })}\n`)
    const message = await reply
    assert.ok(!message.error, `ACP ${method} returned a JSON-RPC error`)
    return message
  }
  try {
    await call('initialize', { protocolVersion: 1, clientCapabilities: {},
      clientInfo: { name: 'protomolt-agent-protocol-smoke', version: '1' } })
    const created = await call('session/new', { cwd: '/workspace', mcpServers: [] })
    const sessionId = created.result?.sessionId
    assert.ok(sessionId)
    async function prompt(method, request) {
      transcripts.set(sessionId, '')
      await call('session/prompt', { sessionId,
        prompt: [{ type: 'text', text: `delegation/${method} ${JSON.stringify(request)}` }] })
      return (transcripts.get(sessionId) ?? '').trim()
    }
    const historical = JSON.parse(await prompt('ReadTranscript',
      { taskId: customTask, maxEntries: 100 }))
    assert.ok(historical.events?.some(event =>
      event.entry?.workerFrame?.completion?.result?.['@type'] ===
        `type.googleapis.com/${customType}`))
    results.tests.push('cold ACP transcript renders caller-owned historical Any')

    const acpWorker = await registerWorker('acp')
    const acpCheck = 'acp-report-valid'
    const offered = JSON.parse(await prompt('OfferTask', { workerId: acpWorker,
      leaseSeconds: 300, spec: { objective: 'Return a caller-owned ACP protocol report',
        allowedScope: ['protocol-harness/**'], requiredChecks: [
          { name: acpCheck, description: 'Harness inspected ACP report and bytes' }],
        contract: { descriptorSet: customDescriptor, typeName: customType } } }))
    assert.equal(offered.ok, true)
    const acpTask = offered.taskId
    results.tasks.acp = acpTask
    assert.equal(rpc('AcceptTask', { workerId: acpWorker, taskId: acpTask, attempt: 1 }).ok, true)
    const acpArtifact = artifact(acpTask, acpCheck)
    const acpResult = { '@type': `type.googleapis.com/${customType}`,
      title: 'ACP protocol qualification', observations: ['ACP submitted this typed report.'],
      observationCount: 1, evidenceDigest: acpArtifact.sha256 }
    const before = await transcript(acpTask)
    const invalid = await prompt('SubmitCandidate', { workerId: acpWorker, taskId: acpTask,
      candidate: candidate(1, 1, acpCheck, { ...acpResult, observationCount: 2 }, acpArtifact) })
    assert.ok(invalid.startsWith('worker-stream-failed:'),
      'ACP invalid CEL report did not receive the coordinator contract refusal')
    const after = await transcript(acpTask)
    assert.equal(after.cursor, before.cursor)
    // The coordinator closes the invalid worker stream; reconnect before correcting it.
    const reconnected = JSON.parse(await prompt('RegisterWorker', {
      workerId: acpWorker, provider: 'protocol-fixture' }))
    assert.equal(reconnected.admitted, true)
    const submitted = JSON.parse(await prompt('SubmitCandidate', { workerId: acpWorker,
      taskId: acpTask, candidate: candidate(1, 1, acpCheck, acpResult, acpArtifact) }))
    assert.equal(submitted.ok, true)
    await waitFor(acpTask, events => Boolean(offeredCandidate(events)))
    results.tests.push('ACP typed custom candidate crossed native gRPC and MCP state')
    results.acp_delegation = true
    child.stdin.end()
  } finally {
    if (child.exitCode === null && child.signalCode === null) child.kill('SIGTERM')
  }
}
await runAcpQualification()

const bundledResponse = await fetch(`${httpBase}/console/contracts/coordination-report.binpb`,
  { signal: AbortSignal.timeout(15000) })
assert.equal(bundledResponse.status, 200)
const bundledDescriptor = Buffer.from(await bundledResponse.arrayBuffer()).toString('base64')
const bundledCheck = 'fixture-report-valid'
const validBundled = { '@type': `type.googleapis.com/${bundledType}`,
  headline: 'Protocol harness coordination report',
  findings: ['Scripted harness verified report values and referenced bytes.'],
  findingCount: 1 }
const invalidCases = [
  { name: 'wrong-count CEL', result: { ...validBundled, findingCount: 2 }, evidence: true },
  { name: 'wrong Any type', result: { ...customResult }, evidence: true },
  { name: 'missing required evidence', result: validBundled, evidence: false },
]
for (const [index, testCase] of invalidCases.entries()) {
  const worker = await registerWorker(`invalid-${index}`)
  const taskId = offerGrpc(worker, bundledType, bundledDescriptor, bundledCheck,
    `Validate the bundled report rejection case: ${testCase.name}.`)
  results.tasks[`invalid${index}`] = taskId
  await accept(worker, taskId)
  const reference = artifact(taskId, testCase.name)
  const before = await transcript(taskId)
  const rejected = await submit(worker, taskId,
    candidate(1, 1, bundledCheck, testCase.result, reference, testCase.evidence), true)
  assert.ok(rejected.code || rejected.message, `${testCase.name} had a structured error`)
  const after = await transcript(taskId)
  assert.equal(after.cursor, before.cursor, `${testCase.name}: cursor advanced`)
  assert.equal(after.events.length, before.events.length, `${testCase.name}: frame appended`)
  assert.ok(!offeredCandidate(after.events), `${testCase.name}: reached review`)
  const cancelled = rpc('CancelTask',
    { taskId, reason: `Protocol harness cleaned up ${testCase.name}` })
  assert.equal(cancelled.ok, true)
  results.tests.push(`${testCase.name} refused before task transcript append`)
}

const validWorker = await registerWorker('bundled-valid')
const validTask = await offerMcp(validWorker, bundledType, bundledDescriptor, bundledCheck,
  'Produce a bundled coordination report for protocol qualification.')
results.tasks.bundledValid = validTask
assert.equal(rpc('AcceptTask', { workerId: validWorker, taskId: validTask, attempt: 1 }).ok, true)
const validArtifact = artifact(validTask, bundledCheck)
await submit(validWorker, validTask,
  candidate(1, 1, bundledCheck, validBundled, validArtifact))
await waitFor(validTask, events => Boolean(offeredCandidate(events)))
results.tests.push('bundled report valid candidate available for human review')
results.manualReviewPending = validTask

console.log(JSON.stringify(results))
