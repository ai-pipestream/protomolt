import { beforeEach, describe, expect, it, vi } from 'vitest'
import {
  WorkflowLaunchApi, WorkflowLaunchIntentStore, type LaunchIntent,
} from './workflowLaunch'

class MemoryStorage {
  private readonly values = new Map<string, string>()
  get length() { return this.values.size }
  key(index: number) { return [...this.values.keys()][index] ?? null }
  getItem(key: string) { return this.values.get(key) ?? null }
  setItem(key: string, value: string) { this.values.set(key, value) }
}

const acceptance = {
  taskId: '00000000-0000-4000-8000-000000000001', attempt: 1, revision: 1,
  taskSpecSha256: 'a'.repeat(64), candidateSha256: 'b'.repeat(64),
  acceptedEntrySha256: 'c'.repeat(64),
}
const intent: LaunchIntent = {
  launchId: '00000000-0000-4000-8000-000000000002', acceptance,
  input: { sha256: 'd'.repeat(64), mediaType: 'application/x-protobuf', sizeBytes: 4 },
}
const result = { jobId: intent.launchId,
  authorization: { sha256: 'e'.repeat(64), mediaType: 'application/x-protobuf', sizeBytes: 50 } }

function response(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), { status,
    headers: { 'Content-Type': 'application/json' } })
}

describe('WorkflowLaunchApi', () => {
  let storage: MemoryStorage
  let intents: WorkflowLaunchIntentStore

  beforeEach(() => {
    storage = new MemoryStorage()
    intents = new WorkflowLaunchIntentStore(storage)
  })

  it('persists the complete request before a lost reply and retries that UUID after reload', async () => {
    const firstFetch = vi.fn(() => {
      expect(intents.list(acceptance.taskId)[0]?.intent).toEqual(intent)
      return Promise.reject(new Error('connection closed after send'))
    })
    const first = new WorkflowLaunchApi(firstFetch, intents)
    await expect(first.launch(intent)).rejects.toThrow('connection closed')
    expect(intents.list(acceptance.taskId)).toMatchObject([{ intent }])

    const retryFetch = vi.fn().mockResolvedValue(response(result))
    const reloaded = new WorkflowLaunchApi(retryFetch, new WorkflowLaunchIntentStore(storage))
    const saved = reloaded.intents.list(acceptance.taskId)[0]!.intent
    await expect(reloaded.launch(saved)).resolves.toEqual(result)
    expect(JSON.parse(String(retryFetch.mock.calls[0]?.[1]?.body))).toEqual(intent)
    expect(reloaded.intents.list(acceptance.taskId)[0]?.result).toEqual(result)
    expect(retryFetch.mock.calls[0]?.[1]).toMatchObject({ credentials: 'same-origin' })
  })

  it('refuses changed intent under an already persisted UUID before network', async () => {
    intents.persist(intent)
    const fetchFn = vi.fn().mockResolvedValue(response(result))
    const api = new WorkflowLaunchApi(fetchFn, intents)
    await expect(api.launch({ ...intent, input: { ...intent.input, sha256: 'f'.repeat(64) } }))
      .rejects.toThrow('another request')
    expect(fetchFn).not.toHaveBeenCalled()
  })

  it('does not send a launch if local persistence fails', async () => {
    const deniedStorage = {
      length: 0,
      key: () => null,
      getItem: () => null,
      setItem: () => { throw new Error('storage denied') },
    }
    const fetchFn = vi.fn().mockResolvedValue(response(result))
    const api = new WorkflowLaunchApi(fetchFn, new WorkflowLaunchIntentStore(deniedStorage))
    await expect(api.launch(intent)).rejects.toThrow('storage denied')
    expect(fetchFn).not.toHaveBeenCalled()
  })

  it.each([
    { ...result.authorization, redacted: true },
    { ...result.authorization, redacted: 0 },
    { ...result.authorization, redacted: '' },
    { ...result.authorization, mediaType: 'text/plain' },
    { ...result.authorization, sizeBytes: 4 * 1024 * 1024 + 1 },
    { ...result.authorization, sha256: 'not-a-hash' },
  ])('does not record malformed 200 authorization evidence: %j', async (badAuthorization) => {
    const api = new WorkflowLaunchApi(vi.fn().mockResolvedValue(response({
      jobId: intent.launchId, authorization: badAuthorization,
    })), intents)
    await expect(api.launch(intent)).rejects.toThrow('does not match')
    expect(intents.list(acceptance.taskId)).toMatchObject([{ intent }])
    expect(intents.list(acceptance.taskId)[0]?.result).toBeUndefined()
  })

  it('keeps an unresolved request when result persistence silently fails', async () => {
    const writeStore = new class extends MemoryStorage {
      override setItem(key: string, value: string) {
        if (value.includes('"result"')) return
        super.setItem(key, value)
      }
    }()
    const saved = new WorkflowLaunchIntentStore(writeStore)
    const api = new WorkflowLaunchApi(vi.fn().mockResolvedValue(response(result)), saved)
    await expect(api.launch(intent)).rejects.toThrow('result could not be saved')
    expect(saved.list(acceptance.taskId)).toMatchObject([{ intent }])
    expect(saved.list(acceptance.taskId)[0]?.result).toBeUndefined()
  })

  it('encodes JSON bytes for the scoped prepare route without a browser token', async () => {
    const fetchFn = vi.fn().mockResolvedValue(response({ acceptance, input: intent.input }))
    const api = new WorkflowLaunchApi(fetchFn, intents)
    await api.prepare(acceptance, '{"text":"é"}')
    const [path, init] = fetchFn.mock.calls[0]!
    expect(path).toBe('/api/workflow-launch/prepare')
    expect(init).toMatchObject({ method: 'POST', credentials: 'same-origin' })
    expect(JSON.parse(String(init.body))).toEqual({ acceptance,
      inputJson: Buffer.from('{"text":"é"}', 'utf8').toString('base64') })
    expect(String(init.body)).not.toContain('api_token')
  })

  it.each([
    [{ request: intent, notAuthorized: true }, 'notAuthorized'],
    [{ request: intent, authorizedNotQueued: true }, 'authorizedNotQueued'],
    [{ request: intent, job: {
      jobId: intent.launchId, state: 'WORKFLOW_LAUNCH_JOB_STATE_QUEUED',
      attempt: 0, maxAttempts: 3, createdAt: '2026-10-01T12:00:00Z',
      updatedAt: '2026-10-01T12:00:00Z',
    } }, 'job'],
    ...(['RUNNING', 'WAITING'] as const).map((name) => [{ request: intent, job: {
      jobId: intent.launchId, state: `WORKFLOW_LAUNCH_JOB_STATE_${name}`,
      attempt: 1, maxAttempts: 3, createdAt: '2026-10-01T12:00:00Z',
      updatedAt: '2026-10-01T12:01:00Z',
    } }, 'job'] as const),
    [{ request: intent, job: {
      jobId: intent.launchId, state: 'WORKFLOW_LAUNCH_JOB_STATE_COMPLETED',
      attempt: 1, maxAttempts: 3, createdAt: '2026-10-01T12:00:00Z',
      updatedAt: '2026-10-01T12:02:00Z', completedAt: '2026-10-01T12:02:00Z',
    } }, 'job'],
    ...(['FAILED', 'DEAD'] as const).map((name) => [{ request: intent, job: {
      jobId: intent.launchId, state: `WORKFLOW_LAUNCH_JOB_STATE_${name}`,
      attempt: 1, maxAttempts: 3, createdAt: '2026-10-01T12:00:00Z',
      updatedAt: '2026-10-01T12:02:00Z', completedAt: '2026-10-01T12:02:00Z',
    } }, 'job'] as const),
  ].map(([status, label]) => ({ status, label })))('reads a bound status without launching or changing the saved intent', async ({ status }) => {
    intents.persist(intent)
    const fetchFn = vi.fn().mockResolvedValue(response(status))
    const api = new WorkflowLaunchApi(fetchFn, intents)
    await expect(api.status(intent)).resolves.toEqual(status)
    expect(fetchFn).toHaveBeenCalledTimes(1)
    expect(fetchFn.mock.calls[0]?.[0]).toBe('/api/workflow-launch/status')
    expect(JSON.parse(String(fetchFn.mock.calls[0]?.[1]?.body))).toEqual({ request: intent })
    expect(intents.list(acceptance.taskId)[0]?.intent).toEqual(intent)
    expect(intents.list(acceptance.taskId)[0]?.result).toBeUndefined()
  })

  it.each([
    { request: { ...intent, input: { ...intent.input, sha256: '0'.repeat(64) } }, notAuthorized: true },
    { request: { ...intent, acceptance: { ...acceptance, revision: 2 } }, notAuthorized: true },
    { request: intent, notAuthorized: true, authorizedNotQueued: true },
    { request: intent, job: { jobId: '00000000-0000-4000-8000-000000000099',
      state: 'WORKFLOW_LAUNCH_JOB_STATE_RUNNING', attempt: 1, maxAttempts: 3,
      createdAt: '2026-10-01T12:00:00Z', updatedAt: '2026-10-01T12:01:00Z' } },
    { request: intent, job: { jobId: intent.launchId,
      state: 'WORKFLOW_LAUNCH_JOB_STATE_COMPLETED', attempt: 1, maxAttempts: 3,
      createdAt: '2026-10-01T12:00:00Z', updatedAt: '2026-10-01T12:01:00Z' } },
    { request: intent, job: { jobId: intent.launchId,
      state: 'WORKFLOW_LAUNCH_JOB_STATE_UNSPECIFIED', attempt: 1, maxAttempts: 3,
      createdAt: '2026-10-01T12:00:00Z', updatedAt: '2026-10-01T12:01:00Z' } },
  ])('refuses malformed or mismatched status before displaying success: %s', async (status) => {
    const api = new WorkflowLaunchApi(vi.fn().mockResolvedValue(response(status)), intents)
    await expect(api.status(intent)).rejects.toThrow('does not match')
  })
})
