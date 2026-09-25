import { beforeEach, describe, expect, it, vi } from 'vitest'
import { TaskApi, TaskApiError } from './tasks'

type FetchLike = (input: string, init?: RequestInit) => Promise<Response>

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json' },
  })
}

describe('TaskApi', () => {
  let fetchMock: ReturnType<typeof vi.fn<FetchLike>>
  let api: TaskApi

  beforeEach(() => {
    fetchMock = vi.fn<FetchLike>()
    api = new TaskApi('/api/tasks', '/api/task-session', fetchMock)
  })

  it('treats an unauthenticated session status as data, not an exception', async () => {
    fetchMock.mockResolvedValue(json({ authenticated: false, loginRequired: true }, 401))
    await expect(api.sessionStatus()).resolves.toEqual({
      authenticated: false,
      loginRequired: true,
    })
    expect(fetchMock).toHaveBeenCalledWith('/api/task-session', {
      method: 'GET',
      credentials: 'same-origin',
    })
  })

  it('logs in through a same-origin cookie flow without retaining the token', async () => {
    fetchMock.mockResolvedValue(json({ authenticated: true, loginRequired: true }))
    await api.login('browser-secret')
    expect(fetchMock.mock.calls[0]?.[0]).toBe('/api/task-session')
    expect(fetchMock.mock.calls[0]?.[1]).toMatchObject({
      method: 'POST',
      credentials: 'same-origin',
      body: JSON.stringify({ token: 'browser-secret' }),
    })
  })

  it('builds a cursor-resumable long poll', async () => {
    fetchMock.mockResolvedValue(json({ events: [], cursor: 71, truncated: false }))
    const controller = new AbortController()
    await api.watchEvents(71, 'task/id', 12_000, 32, controller.signal)
    const [path, init] = fetchMock.mock.calls[0] ?? []
    expect(path).toContain('/api/tasks/events?')
    const url = new URL(path as string, 'http://example.test')
    expect(url.searchParams.get('after')).toBe('71')
    expect(url.searchParams.get('taskId')).toBe('task/id')
    expect(url.searchParams.get('timeoutMs')).toBe('12000')
    expect(url.searchParams.get('maxEvents')).toBe('32')
    expect(init).toMatchObject({ credentials: 'same-origin', signal: controller.signal })
  })

  it('sends only the structured task-message fields', async () => {
    fetchMock.mockResolvedValue(json({ message: { messageId: 'm1' } }, 201))
    await api.sendMessage('t1', 'kimi-worker', 'guidance', 'run the focused test', 'm0')
    expect(fetchMock.mock.calls[0]?.[0]).toBe('/api/tasks/t1/messages')
    expect(JSON.parse(String(fetchMock.mock.calls[0]?.[1]?.body))).toEqual({
      recipient: 'kimi-worker',
      kind: 'guidance',
      text: 'run the focused test',
      replyTo: 'm0',
    })
  })

  it('binds review decisions to the candidate attempt and revision the reviewer saw', async () => {
    fetchMock.mockResolvedValue(json({ decision: 'accept', phase: 'accepted' }))

    await api.reviewAccept('t1', 4, 7, 'done to my satisfaction')

    expect(fetchMock.mock.calls[0]?.[0]).toBe('/api/tasks/t1/review')
    expect(JSON.parse(String(fetchMock.mock.calls[0]?.[1]?.body))).toEqual({
      decision: 'accept',
      attempt: 4,
      revision: 7,
      verdict: 'done to my satisfaction',
    })
  })

  it('binds revision requests to the candidate attempt and revision the reviewer saw', async () => {
    fetchMock.mockResolvedValue(json({ decision: 'revise', phase: 'working' }))

    await api.reviewRevise('t1', 4, 7, 'lint still complains', ['lint-clean'])

    expect(JSON.parse(String(fetchMock.mock.calls[0]?.[1]?.body))).toEqual({
      decision: 'revise',
      attempt: 4,
      revision: 7,
      feedback: 'lint still complains',
      failedChecks: ['lint-clean'],
    })
  })

  it('surfaces bounded server errors with their status', async () => {
    fetchMock.mockResolvedValue(json({ error: 'authentication required' }, 401))
    const failure = await api.listTasks().catch((error) => error)
    expect(failure).toBeInstanceOf(TaskApiError)
    expect(failure).toMatchObject({ status: 401, message: 'authentication required' })
  })
})
