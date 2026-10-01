// @vitest-environment jsdom
import { beforeEach, describe, expect, it, vi } from 'vitest'
import {
  WorkflowAuthoringApi,
  WorkflowAuthoringStartStore,
  type AuthoringStartRequest,
} from './workflowAuthoring'

const request: AuthoringStartRequest = {
  taskId: '00000000-0000-4000-8000-000000000001',
  workerId: 'author-one',
  templateSha256: 'a'.repeat(64),
  objective: 'Build the approved workflow',
}
const offer = { attempt: 1, spec: { objective: request.objective },
  startBindingSha256: 'b'.repeat(64) }
const starts = () => new WorkflowAuthoringStartStore('coordinator-one')

function response(body: unknown): Response {
  return new Response(JSON.stringify(body), { status: 200,
    headers: { 'Content-Type': 'application/json' } })
}

describe('WorkflowAuthoringApi', () => {
  beforeEach(() => localStorage.clear())

  it('saves the exact request before sending and recovers a lost reply with the same UUID', async () => {
    let calls = 0
    const fetchFn = vi.fn(async (_path: string, init?: RequestInit) => {
      calls++
      expect(JSON.parse(String(init?.body))).toEqual(request)
      expect(starts().list()[0]?.request).toEqual(request)
      if (calls === 1) throw new Error('reply lost')
      return response({ request, offer })
    })
    const first = new WorkflowAuthoringApi(starts(), fetchFn)
    await expect(first.start(request)).rejects.toThrow('reply lost')
    const reloaded = new WorkflowAuthoringApi(starts(), fetchFn)
    expect(reloaded.starts.list()).toHaveLength(1)
    await expect(reloaded.start(reloaded.starts.list()[0]!.request))
      .resolves.toEqual({ request, offer })
    expect(reloaded.starts.list()[0]?.response).toEqual({ request, offer })
    expect(fetchFn).toHaveBeenCalledTimes(2)
  })

  it('refuses UUID reuse with changed guidance before a network call', async () => {
    const fetchFn = vi.fn().mockResolvedValue(response({ request, offer }))
    const api = new WorkflowAuthoringApi(starts(), fetchFn)
    api.starts.persist(request)
    await expect(api.start({ ...request, objective: 'Different guidance' }))
      .rejects.toThrow('belongs to another request')
    expect(fetchFn).not.toHaveBeenCalled()
    expect(api.starts.list()[0]?.request).toEqual(request)
  })

  it.each([
    { request: { ...request, workerId: 'other-author' }, offer },
    { request, offer: { ...offer, attempt: 2 } },
    { request, offer: { ...offer, spec: { objective: 'Other objective' } } },
    { request, offer: { ...offer, startBindingSha256: '' } },
  ])('refuses an unbound response and keeps the start pending', async (bad) => {
    const api = new WorkflowAuthoringApi(starts(), vi.fn().mockResolvedValue(response(bad)))
    await expect(api.start(request)).rejects.toThrow('does not match')
    expect(api.starts.list()[0]?.response).toBeUndefined()
  })

  it('does not surface another principal\'s saved objective or migrate unscoped data', () => {
    starts().persist(request)
    localStorage.setItem(`protomolt.workflow-authoring.start.v1.${request.taskId}`,
      JSON.stringify({ request, createdAt: Date.now() }))
    expect(new WorkflowAuthoringStartStore('coordinator-two').list()).toEqual([])
    expect(starts().list()).toHaveLength(1)
  })

  it('counts supplementary Unicode characters as one protobuf character', () => {
    const store = starts()
    store.persist({ ...request, objective: '🧪'.repeat(4096) })
    expect(store.list()).toHaveLength(1)
    expect(() => store.persist({ ...request, taskId: '00000000-0000-4000-8000-000000000002',
      objective: '🧪'.repeat(4097) })).toThrow('incomplete')
  })
})
