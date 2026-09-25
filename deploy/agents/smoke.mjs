#!/usr/bin/env node
// Qualification harness: independently checks the scripted worker's protocol behavior.
// Run in the extracted Compose directory with Node 22+ and Docker available.
import { execFileSync } from 'node:child_process'
import assert from 'node:assert/strict'
import { setTimeout as delay } from 'node:timers/promises'

const base = process.env.HTTP_BASE ?? 'http://127.0.0.1:8080'
const token = execFileSync('docker', ['compose', 'exec', '-T', 'serve', 'cat', '/run/console/token'],
  { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'], timeout: 15000 }).trim()
const unauthenticated = await fetch(`${base}/api/tasks`)
assert.equal(unauthenticated.status, 401)
const login = await fetch(`${base}/api/task-session`, { method: 'POST',
  headers: { 'content-type': 'application/json' }, body: JSON.stringify({ token }) })
assert.equal(login.status, 200)
const cookie = login.headers.get('set-cookie')?.split(';', 1)[0]
assert.ok(cookie)
async function request(path, body, status = 200) {
  const response = await fetch(`${base}/api/tasks${path}`, {
    headers: { cookie, 'content-type': 'application/json' },
    ...(body === undefined ? {} : { method: 'POST', body: JSON.stringify(body) }),
    signal: AbortSignal.timeout(30000),
  })
  const result = await response.json()
  assert.equal(response.status, status, `${path}: ${result.error ?? 'unexpected HTTP status'}`)
  return result
}
async function until(taskId, predicate) {
  for (let attempt = 0; attempt < 90; attempt++) {
    const detail = await request(`/${taskId}`)
    if (predicate(detail)) return detail
    await delay(1000)
  }
  throw new Error(`Task ${taskId} did not reach the expected state`)
}
if (process.env.VERIFY_SAVED_TASK) {
  const taskId = process.env.VERIFY_SAVED_TASK
  const detail = await request(`/${taskId}`)
  assert.equal(detail.task.phase, 'accepted')
  assert.equal(detail.task.candidateRevision, 2)
  const question = await request(`/${taskId}/messages`, { recipient: 'fixture-worker',
    kind: 'question', text: 'After restart, explain the scope of this fixture task.' }, 201)
  const messageId = question.message?.messageId
  assert.ok(messageId)
  const resumed = await until(taskId, current => current.events.some(event =>
    event.entry.workerFrame?.taskMessage?.replyTo === messageId))
  assert.equal(resumed.task.phase, 'accepted')
  assert.equal(resumed.task.candidateRevision, 2)
  const snapshot = await request(`/${taskId}/record`, {})
  assert.ok(snapshot.recordBase64 && snapshot.transcriptBase64)
  console.log(JSON.stringify({ taskId, retrieved_after_restart: true,
    conversation_after_restart: true, post_acceptance_record: true, cursor: resumed.cursor }))
  process.exit(0)
}
for (let attempt = 0; ; attempt++) {
  const workers = await request('/workers')
  if (workers.workers.some((worker) => worker.workerId === 'fixture-worker' && worker.admitted && worker.connected)) break
  assert.ok(attempt < 60, 'Fixture AgentHost did not register')
  await delay(1000)
}
const descriptorResponse = await fetch(`${base}/console/contracts/coordination-report.binpb`)
assert.equal(descriptorResponse.status, 200)
const contract = {
  descriptorSet: Buffer.from(await descriptorResponse.arrayBuffer()).toString('base64'),
  typeName: 'ai.protomolt.proto.samples.starter.v1.CoordinationReport',
}
const offered = await request('/offer', {
  workerId: 'fixture-worker', objective: 'Produce a coordination report about this starter task.',
  requiredChecks: [{ name: 'fixture-report-valid', description: 'Reported fixture validation only' }],
  leaseMinutes: 5, contract,
}, 201)
const taskId = offered.taskId
const first = await until(taskId, (detail) => detail.task.phase === 'candidate')
const result = first.events.find((event) => event.entry.workerFrame?.completion)?.entry.workerFrame.completion.result
assert.equal(result?.['@type'], `type.googleapis.com/${contract.typeName}`)
assert.equal(result.findingCount, result.findings.length)
await request(`/${taskId}/messages`, { recipient: 'fixture-worker', kind: 'question',
  text: 'What work did this fixture perform?' }, 201)
await until(taskId, (detail) => detail.events.some((event) =>
  event.entry.workerFrame?.taskMessage?.text.includes('Fixture worker')))
await request(`/${taskId}/review`, { decision: 'revise', attempt: 1, revision: 1,
  feedback: 'Include this revision request in the report.' })
const replacement = await until(taskId, (detail) => detail.task.phase === 'candidate' && detail.task.candidateRevision === 2)
const beforeStale = replacement.events.length
await request(`/${taskId}/review`, { decision: 'accept', attempt: 1, revision: 1,
  verdict: 'This delayed review must be refused.' }, 409)
const afterStale = await request(`/${taskId}`)
assert.equal(afterStale.events.length, beforeStale)
assert.equal(afterStale.task.phase, 'candidate')
await request(`/${taskId}/review`, { decision: 'accept', attempt: 1, revision: 2,
  verdict: 'Harness inspected the typed report and fixture evidence; no live model or external tests claimed.' })
const exported = await request(`/${taskId}/record`, {})
assert.ok(exported.recordBase64 && exported.transcriptBase64)
const rejected = await request('/offer', { workerId: 'fixture-worker', objective: 'Unsupported fixture task',
  requiredChecks: [{ name: 'unknown-check' }], leaseMinutes: 5 }, 201)
await until(rejected.taskId, (detail) => detail.task.phase === 'rejected')
assert.ok((await request(`/${rejected.taskId}/record`, {})).recordBase64)
console.log(JSON.stringify({ taskId, rejectedTaskId: rejected.taskId, authenticated_console: true,
  fixture_agent_host: true, typed_candidate: true, revision_bound_review: true,
  question_reply: true, unsupported_offer_rejected: true, receipt_snapshot: true,
  provider: 'fixture', live_model: false }))
