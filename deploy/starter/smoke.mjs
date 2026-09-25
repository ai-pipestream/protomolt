#!/usr/bin/env node
// End-to-end acceptance for an already running starter stack. Requires Node 22+
// and Docker Compose; grpcurl adds the direct gRPC leg when installed.
import { execFileSync, spawn } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import { setTimeout as delay } from 'node:timers/promises'

const major = Number(process.versions.node.split('.')[0])
if (major < 22) throw new Error(`Node 22 or newer is required (found ${process.versions.node})`)

const httpBase = (process.env.HTTP_BASE ?? 'http://127.0.0.1:8080').replace(/\/$/, '')
const grpcTarget = process.env.GRPC_TARGET ?? '127.0.0.1:9090'
const timeoutMs = 360_000
const httpTimeoutMs = Number(process.env.HTTP_TIMEOUT_MS ?? 360_000)

function timedFetch(url, options = {}) {
  return fetch(url, { ...options, signal: options.signal ?? AbortSignal.timeout(httpTimeoutMs) })
}

function composeToken(secretPath) {
  const value = execFileSync('docker', ['compose', 'exec', '-T', 'serve', 'cat', secretPath], {
    encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'], timeout: 15_000,
  }).trim()
  if (!value) throw new Error(`Could not read required Compose credential at ${secretPath}`)
  return value
}

function assert(condition, message) {
  if (!condition) throw new Error(message)
}

async function jsonResponse(response, description) {
  const body = await response.text()
  try { return JSON.parse(body) } catch {
    throw new Error(`${description} returned non-JSON (HTTP ${response.status})`)
  }
}

const consoleToken = composeToken('/run/console-secret/token')
const operatorToken = composeToken('/run/operator-secret/token')
const source = {
  recordId: 'contact-1',
  contactText: 'Ada Lovelace; ada [at] example.org',
  internalNotes: 'private-source-note',
}
const requestFor = (runId) => ({ workflowName: 'correct-contact', runId, source })
const serviceMethod = 'ai.protomolt.proto.correction.v1.CorrectionService/'
const invoke = (rpc, request) => ({
  name: 'correction', endpoint: 'default', method: serviceMethod + rpc, request,
})
const accepted = (value) => value?.outcome?.assessment?.disposition === 'ASSESSMENT_DISPOSITION_ACCEPTED'

// Browser-console authentication uses the console-only secret and an HttpOnly session cookie.
const unauthenticated = await timedFetch(`${httpBase}/api/correction`)
assert(unauthenticated.status === 401, `Unauthenticated console API should return 401 (got ${unauthenticated.status})`)
const login = await timedFetch(`${httpBase}/api/task-session`, {
  method: 'POST', headers: { 'content-type': 'application/json' },
  body: JSON.stringify({ token: consoleToken }),
})
assert(login.ok, `Console login failed (HTTP ${login.status})`)
const cookie = login.headers.get('set-cookie')?.split(';', 1)[0]
assert(cookie, 'Console login did not issue a session cookie')
const available = await timedFetch(`${httpBase}/api/correction`, { headers: { cookie } })
assert(available.ok, `Authenticated correction availability check failed (HTTP ${available.status})`)

// After the offline helper restarts the stack, check the old outcome through
// the public API without creating another run.
if (process.env.VERIFY_SAVED_RUN) {
  const run = process.env.VERIFY_SAVED_RUN
  const response = await timedFetch(`${httpBase}/api/correction/runs/${encodeURIComponent(run)}`, { headers: { cookie } })
  assert(response.ok, `Saved run retrieval failed after restart (HTTP ${response.status})`)
  const result = await jsonResponse(response, 'Saved run retrieval')
  assert(accepted(result) && result.outcome.runId === run, 'Saved accepted outcome did not survive restart')
  console.log(JSON.stringify({ run, retrieved_after_restart: true }))
  process.exit(0)
}

const httpRunId = `contact-${randomUUID()}`
const httpRunResponse = await timedFetch(`${httpBase}/api/correction/runs`, {
  method: 'POST', headers: { cookie, 'content-type': 'application/json' },
  body: JSON.stringify(requestFor(httpRunId)), signal: AbortSignal.timeout(timeoutMs),
})
assert(httpRunResponse.ok, `HTTP correction run failed (HTTP ${httpRunResponse.status})`)
const httpOutcome = await jsonResponse(httpRunResponse, 'HTTP correction run')
assert(accepted(httpOutcome), 'HTTP correction run did not record an accepted assessment')

const storedResponse = await timedFetch(`${httpBase}/api/correction/runs/${encodeURIComponent(httpRunId)}`, { headers: { cookie } })
assert(storedResponse.ok, `HTTP retrieval failed (HTTP ${storedResponse.status})`)
const stored = await jsonResponse(storedResponse, 'HTTP correction retrieval')
assert(JSON.stringify(stored) === JSON.stringify(httpOutcome), 'Retrieved HTTP outcome differs from the completed result')

const duplicateResponse = await timedFetch(`${httpBase}/api/correction/runs`, {
  method: 'POST', headers: { cookie, 'content-type': 'application/json' },
  body: JSON.stringify(requestFor(httpRunId)),
})
assert(duplicateResponse.status === 409, `Duplicate run ID should return 409 (got ${duplicateResponse.status})`)
const invalidHttp = await timedFetch(`${httpBase}/api/correction/runs`, {
  method: 'POST', headers: { cookie, 'content-type': 'application/json' },
  body: JSON.stringify(requestFor('INVALID-ID')),
})
assert(invalidHttp.status === 400, `Invalid HTTP request should return 400 (got ${invalidHttp.status})`)

async function mcpRpc(method, params, state, notify = false) {
  const headers = {
    authorization: `Bearer ${operatorToken}`,
    'content-type': 'application/json',
    accept: 'application/json, text/event-stream',
  }
  if (state.session) headers['mcp-session-id'] = state.session
  if (state.version) headers['mcp-protocol-version'] = state.version
  const body = { jsonrpc: '2.0', method, params }
  if (!notify) body.id = ++state.sequence
  const response = await timedFetch(`${httpBase}/mcp`, {
    method: 'POST', headers, body: JSON.stringify(body), signal: AbortSignal.timeout(timeoutMs),
  })
  assert([200, 202].includes(response.status), `MCP ${method} failed (HTTP ${response.status})`)
  state.session = response.headers.get('mcp-session-id') ?? state.session
  if (notify) return null
  const text = await response.text()
  let message
  try { message = JSON.parse(text) } catch {
    const data = text.split(/\r?\n/).filter((line) => line.startsWith('data:')).map((line) => line.slice(5).trim()).at(-1)
    if (data) message = JSON.parse(data)
  }
  assert(message, `MCP ${method} returned no JSON-RPC response`)
  assert(!message.error, `MCP ${method} returned a JSON-RPC error`)
  return message
}

const mcp = { sequence: 0, session: null, version: null }
const initialized = await mcpRpc('initialize', {
  protocolVersion: '2025-06-18', capabilities: {}, clientInfo: { name: 'protomolt-starter-smoke', version: '1' },
}, mcp)
mcp.version = initialized.result?.protocolVersion
assert(mcp.version, 'MCP initialize did not return a protocol version')
await mcpRpc('notifications/initialized', {}, mcp, true)
const listed = await mcpRpc('tools/list', {}, mcp)
const tools = listed.result?.tools ?? []
const reflectedTool = tools.find((tool) =>
  String(tool.description).includes('CorrectionService.RunCorrection')
  || (tool.name.startsWith('correction-') && tool.name.includes('run-correction')))
assert(reflectedTool, 'MCP tools/list did not expose the reflected correction RunCorrection verb')

async function mcpTool(name, args) {
  const envelope = await mcpRpc('tools/call', { name, arguments: args }, mcp)
  const result = envelope.result
  assert(result, `MCP tool ${name} returned no tool result`)
  const text = result.content?.find((item) => item.type === 'text')?.text
  let value = null
  try { if (text) value = JSON.parse(text) } catch { /* protocol error text may be plain text */ }
  return { result, value, detail: `${text ?? ''} ${JSON.stringify(result)}` }
}

function assertMcpInvalid(result, description) {
  assert(result.result.isError, `${description} did not reject an invalid correction request`)
  assert(/invalid-input|INVALID_ARGUMENT|invalid argument|contract violation/i.test(result.detail),
    `${description} did not expose the invalid-input contract error`)
}

const mcpWrappedId = `contact-${randomUUID()}`
const wrapped = await mcpTool('service-invoke', invoke('RunCorrection', requestFor(mcpWrappedId)))
assert(!wrapped.result.isError && wrapped.value?.ok === true
  && accepted(wrapped.value.responses?.[0]), 'MCP service-invoke correction was not accepted')

const mcpReflectedId = `contact-${randomUUID()}`
const direct = await mcpTool(reflectedTool.name, requestFor(mcpReflectedId))
assert(!direct.result.isError && accepted(direct.value), 'Direct reflected MCP correction was not accepted')

const invalidWrapped = await mcpTool('service-invoke', invoke('RunCorrection', requestFor('INVALID-ID')))
assertMcpInvalid(invalidWrapped, 'MCP service-invoke')
const invalidDirect = await mcpTool(reflectedTool.name, requestFor('INVALID-ID'))
assertMcpInvalid(invalidDirect, 'Direct reflected MCP correction')

// ACP's stdio transcript contains the action result and session updates; its secret is mounted
// into the on-demand container and is never read or printed by this host-side script.
async function runAcp() {
  const child = spawn('docker', ['compose', 'run', '--rm', '--no-deps', '-T', '-i', 'acp'], {
    stdio: ['pipe', 'pipe', 'ignore'],
  })
  let stdoutBuffer = ''
  let nextId = 0
  const pending = new Map()
  const transcripts = new Map()
  let childFailure = null
  child.stdout.setEncoding('utf8')
  child.stdout.on('data', (chunk) => {
    stdoutBuffer += chunk
    for (;;) {
      const newline = stdoutBuffer.indexOf('\n')
      if (newline < 0) break
      const line = stdoutBuffer.slice(0, newline).trim()
      stdoutBuffer = stdoutBuffer.slice(newline + 1)
      if (!line) continue
      let message
      try { message = JSON.parse(line) } catch { continue }
      if (message.method === 'session/update') {
        const sessionId = message.params?.sessionId
        const text = message.params?.update?.content?.text
        if (sessionId && text) transcripts.set(sessionId, (transcripts.get(sessionId) ?? '') + text)
      }
      const waiter = pending.get(message.id)
      if (waiter) {
        pending.delete(message.id)
        waiter.resolve(message)
      }
    }
  })
  const rejectPending = (message) => {
    childFailure = childFailure ?? message
    for (const [id, waiter] of pending) {
      clearTimeout(waiter.timer)
      pending.delete(id)
      waiter.reject(new Error(message))
    }
  }
  child.on('error', () => rejectPending('ACP Compose process could not start'))
  child.stdin.on('error', () => rejectPending('ACP input closed before request completed'))
  child.on('close', (code, signal) => {
    rejectPending(`ACP process closed (code ${code ?? 'none'}, signal ${signal ?? 'none'})`)
  })
  const call = async (method, params) => {
    if (childFailure) throw new Error(childFailure)
    const id = ++nextId
    const reply = new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        pending.delete(id)
        reject(new Error(`ACP ${method} timed out`))
      }, timeoutMs)
      pending.set(id, {
        timer,
        resolve: (message) => { clearTimeout(timer); resolve(message) },
        reject: (error) => { clearTimeout(timer); reject(error) },
      })
    })
    try { child.stdin.write(`${JSON.stringify({ jsonrpc: '2.0', id, method, params })}\n`) }
    catch { rejectPending('ACP input closed before request was sent') }
    const message = await reply
    assert(!message.error, `ACP ${method} returned a JSON-RPC error`)
    return message
  }
  try {
    await call('initialize', {
      protocolVersion: 1, clientCapabilities: {}, clientInfo: { name: 'protomolt-starter-smoke', version: '1' },
    })
    const created = await call('session/new', { cwd: '/workspace', mcpServers: [] })
    const sessionId = created.result?.sessionId
    assert(sessionId, 'ACP session/new returned no session ID')
    const prompt = async (request) => {
      transcripts.set(sessionId, '')
      await call('session/prompt', {
        sessionId, prompt: [{ type: 'text', text: `service-invoke ${JSON.stringify(invoke('RunCorrection', request))}` }],
      })
      return transcripts.get(sessionId) ?? ''
    }
    const acceptedText = await prompt(requestFor(`contact-${randomUUID()}`))
    const acpOutcome = JSON.parse(acceptedText)
    assert(acpOutcome.ok === true && accepted(acpOutcome.responses?.[0]), 'ACP correction run was not accepted')
    const invalidText = await prompt(requestFor('INVALID-ID'))
    assert(invalidText.startsWith('invalid-input:'), 'ACP did not reject an invalid correction request')
    const exited = new Promise((resolve) => child.once('close', resolve))
    child.stdin.end()
    await Promise.race([exited, delay(15_000, undefined, { ref: false })])
    if (child.exitCode === null && child.signalCode === null) child.kill('SIGTERM')
  } catch (error) {
    child.kill('SIGTERM')
    throw error
  }
}
await runAcp()

// grpcurl is optional locally; release CI sets REQUIRE_GRPCURL=1 to make it mandatory.
let grpcurlStatus = 'skipped (grpcurl not installed)'
try {
  execFileSync('grpcurl', ['-version'], { stdio: 'ignore' })
  const grpcRunId = `contact-${randomUUID()}`
  const callGrpc = (request) => execFileSync('grpcurl', [
    '-plaintext', '-expand-headers', '-H', 'api_token: ${SMOKE_API_TOKEN}',
    '-d', JSON.stringify(invoke('RunCorrection', request)), grpcTarget,
    'ai.protomolt.proto.grpc.service.v1.ProtoMoltService/ServiceInvoke',
  ], {
    encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'], timeout: timeoutMs,
    env: { ...process.env, SMOKE_API_TOKEN: operatorToken },
  })
  const good = JSON.parse(callGrpc(requestFor(grpcRunId)))
  assert(good.ok === true && accepted(good.responses?.[0]), 'gRPC ServiceInvoke correction was not accepted')
  let grpcError
  try { callGrpc(requestFor('INVALID-ID')) } catch (error) { grpcError = error }
  assert(grpcError, 'gRPC ServiceInvoke did not reject an invalid correction request')
  const grpcDetail = `${grpcError.stderr ?? ''} ${grpcError.stdout ?? ''}`
  assert(/INVALID_ARGUMENT|InvalidArgument|invalid argument/i.test(grpcDetail)
    && /invalid-input|invalid input|contract violation/i.test(grpcDetail),
  `gRPC ServiceInvoke rejection had unexpected status/contract text (status=${/INVALID_ARGUMENT|InvalidArgument|invalid argument/i.test(grpcDetail)}, contract=${/invalid-input|invalid input|contract violation/i.test(grpcDetail)})`)
  grpcurlStatus = 'accepted + invalid rejected'
} catch (error) {
  if (error?.code !== 'ENOENT') throw error
  if (process.env.REQUIRE_GRPCURL === '1') throw new Error('grpcurl is required but not installed')
}

console.log(JSON.stringify({
  run: httpRunId,
  outcome: httpOutcome.outcome.assessment.disposition,
  authenticated_console: true,
  retrieval: true,
  duplicate_rejected: true,
  invalid_rejected: true,
  mcp: 'service-invoke and reflected accepted; invalid rejected',
  acp: 'remote Compose stdio accepted; invalid rejected',
  grpcurl: grpcurlStatus,
}))
