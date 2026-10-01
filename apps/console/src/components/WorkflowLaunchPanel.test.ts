// @vitest-environment jsdom
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
import WorkflowLaunchPanel from './WorkflowLaunchPanel.vue'
import { installDomStubs, vuetifyForTests } from '../componentTestKit'

installDomStubs()

const taskId = '00000000-0000-4000-8000-000000000001'
const accepted = {
  taskId, attempt: 1, revision: 1,
  taskSpecSha256: 'a'.repeat(64), candidateSha256: 'b'.repeat(64),
  acceptedEntrySha256: 'c'.repeat(64),
}
const descriptors = { sha256: 'd'.repeat(64), mediaType: 'application/x-protobuf', sizeBytes: 64 }
const input = { sha256: 'e'.repeat(64), mediaType: 'application/x-protobuf', sizeBytes: 3 }
const authorization = { sha256: 'f'.repeat(64), mediaType: 'application/x-protobuf', sizeBytes: 40 }

function response(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), { status,
    headers: { 'Content-Type': 'application/json' } })
}

function mountPanel(id = taskId) {
  return mount(WorkflowLaunchPanel, {
    props: { taskId: id },
    global: { plugins: [vuetifyForTests()] },
  })
}

function button(wrapper: ReturnType<typeof mountPanel>, label: string) {
  return wrapper.findAll('button').find((item) => item.text().includes(label))!
}

describe('WorkflowLaunchPanel', () => {
  beforeEach(() => {
    localStorage.clear()
    vi.unstubAllGlobals()
  })

  it('retains the immutable launch after a lost response and retries it after reload', async () => {
    const launches: Record<string, unknown>[] = []
    let loseReply = true
    vi.stubGlobal('fetch', vi.fn(async (path: string, init: RequestInit) => {
      const body = JSON.parse(String(init.body)) as Record<string, unknown>
      if (path.endsWith('/accepted')) return response(accepted)
      if (path.endsWith('/contract')) return response({ acceptance: accepted,
        inputType: 'test.Input', descriptors, descriptorSet: 'AQID' })
      if (path.endsWith('/prepare')) return response({ acceptance: accepted, input })
      if (path.endsWith('/launch')) {
        launches.push(body)
        if (loseReply) { loseReply = false; throw new Error('reply lost') }
        return response({ jobId: body.launchId, authorization })
      }
      throw new Error(`unexpected ${path}`)
    }))
    const first = mountPanel()
    await flushPromises()
    await button(first, 'Prepare input').trigger('click')
    await flushPromises()
    await button(first, 'Launch workflow').trigger('click')
    await flushPromises()
    expect(first.text()).toContain('reply lost')
    expect(first.text()).toContain('Retry saved launch')
    expect(launches).toHaveLength(1)
    const original = launches[0]!
    expect(localStorage.length).toBe(1)
    first.unmount()

    const reloaded = mountPanel()
    await flushPromises()
    await button(reloaded, 'Retry saved launch').trigger('click')
    await flushPromises()
    expect(launches).toHaveLength(2)
    expect(launches[1]).toEqual(original)
    expect(reloaded.text()).toContain(`Job ID: ${original.launchId}`)
    expect(reloaded.text()).toContain(authorization.sha256)
    expect(reloaded.text()).toContain('Launch submitted. Refresh to read job progress.')
    reloaded.unmount()
  })

  it('reuses a pending UUID when identical input is prepared and launched again', async () => {
    const launches: Record<string, unknown>[] = []
    let loseReply = true
    vi.stubGlobal('fetch', vi.fn(async (path: string, init: RequestInit) => {
      const body = JSON.parse(String(init.body)) as Record<string, unknown>
      if (path.endsWith('/accepted')) return response(accepted)
      if (path.endsWith('/contract')) return response({ acceptance: accepted,
        inputType: 'test.Input', descriptors, descriptorSet: 'AQID' })
      if (path.endsWith('/prepare')) return response({ acceptance: accepted, input })
      if (path.endsWith('/launch')) {
        launches.push(body)
        if (loseReply) { loseReply = false; throw new Error('reply lost') }
        return response({ jobId: body.launchId, authorization })
      }
      throw new Error(`unexpected ${path}`)
    }))
    const wrapper = mountPanel()
    await flushPromises()
    await button(wrapper, 'Prepare input').trigger('click')
    await flushPromises()
    await button(wrapper, 'Launch workflow').trigger('click')
    await flushPromises()
    expect(wrapper.text()).toContain('reply lost')
    await button(wrapper, 'Prepare input').trigger('click')
    await flushPromises()
    await button(wrapper, 'Launch workflow').trigger('click')
    await flushPromises()
    expect(launches).toHaveLength(2)
    expect(launches[1]).toEqual(launches[0])
    expect(localStorage.length).toBe(1)
    wrapper.unmount()
  })

  it('invalidates preparation on edit and creates a fresh launch UUID after preparing again', async () => {
    const launches: Record<string, unknown>[] = []
    let preparations = 0
    vi.stubGlobal('fetch', vi.fn(async (path: string, init: RequestInit) => {
      const body = JSON.parse(String(init.body)) as Record<string, unknown>
      if (path.endsWith('/accepted')) return response(accepted)
      if (path.endsWith('/contract')) return response({ acceptance: accepted,
        inputType: 'test.Input', descriptors, descriptorSet: 'AQID' })
      if (path.endsWith('/prepare')) {
        preparations++
        return response({ acceptance: accepted, input: { ...input,
          sha256: preparations === 1 ? 'e'.repeat(64) : '1'.repeat(64) } })
      }
      if (path.endsWith('/launch')) {
        launches.push(body)
        return response({ jobId: body.launchId, authorization })
      }
      throw new Error(`unexpected ${path}`)
    }))
    const wrapper = mountPanel()
    await flushPromises()
    await button(wrapper, 'Prepare input').trigger('click')
    await flushPromises()
    await button(wrapper, 'Launch workflow').trigger('click')
    await flushPromises()
    await wrapper.find('textarea').setValue('{"text":"edited"}')
    expect(button(wrapper, 'Launch workflow').attributes('disabled')).toBeDefined()
    await button(wrapper, 'Prepare input').trigger('click')
    await flushPromises()
    await button(wrapper, 'Launch workflow').trigger('click')
    await flushPromises()
    expect(launches).toHaveLength(2)
    expect(launches[0]?.launchId).not.toBe(launches[1]?.launchId)
    expect(launches[0]?.input).not.toEqual(launches[1]?.input)
    expect(localStorage.length).toBe(2)
    wrapper.unmount()
  })

  it('ignores a late response for a task that was left and an edited input', async () => {
    const otherId = '00000000-0000-4000-8000-000000000009'
    let releaseAccepted!: (value: Response) => void
    let releasePrepare!: (value: Response) => void
    vi.stubGlobal('fetch', vi.fn((path: string, init: RequestInit) => {
      const body = JSON.parse(String(init.body)) as Record<string, unknown>
      if (path.endsWith('/accepted') && body.taskId === taskId) {
        return new Promise<Response>((resolve) => { releaseAccepted = resolve })
      }
      if (path.endsWith('/accepted')) return Promise.resolve(response({ ...accepted, taskId: otherId }))
      if (path.endsWith('/contract')) return Promise.resolve(response({
        acceptance: { ...accepted, taskId: otherId }, inputType: 'other.Input', descriptors,
        descriptorSet: 'AQID',
      }))
      if (path.endsWith('/prepare')) return new Promise<Response>((resolve) => { releasePrepare = resolve })
      throw new Error(`unexpected ${path}`)
    }))
    const wrapper = mountPanel()
    await flushPromises()
    await wrapper.setProps({ taskId: otherId })
    await flushPromises()
    releaseAccepted(response(accepted))
    await flushPromises()
    expect(wrapper.text()).toContain('other.Input')
    expect(wrapper.text()).not.toContain('test.Input')

    await button(wrapper, 'Prepare input').trigger('click')
    await wrapper.find('textarea').setValue('{"text":"changed while preparing"}')
    releasePrepare(response({ acceptance: { ...accepted, taskId: otherId }, input }))
    await flushPromises()
    expect(button(wrapper, 'Launch workflow').attributes('disabled')).toBeDefined()
    wrapper.unmount()
  })

  it.each([[403, 'permission'], [412, 'not a launchable authored workflow']])(
    'handles accepted-task lookup status %s', async (status, expected) => {
      vi.stubGlobal('fetch', vi.fn().mockResolvedValue(response({ error: 'denied' }, status)))
      const wrapper = mountPanel()
      await flushPromises()
      expect(wrapper.text()).toContain(expected)
      expect(wrapper.find('textarea').exists()).toBe(false)
      wrapper.unmount()
    })

  it('shows explicit job progress and terminal outcomes without treating submission as completion', async () => {
    let state = 'WORKFLOW_LAUNCH_JOB_STATE_RUNNING'
    vi.stubGlobal('fetch', vi.fn(async (path: string, init: RequestInit) => {
      const body = JSON.parse(String(init.body)) as Record<string, unknown>
      if (path.endsWith('/accepted')) return response(accepted)
      if (path.endsWith('/contract')) return response({ acceptance: accepted,
        inputType: 'test.Input', descriptors, descriptorSet: 'AQID' })
      if (path.endsWith('/prepare')) return response({ acceptance: accepted, input })
      if (path.endsWith('/launch')) return response({ jobId: body.launchId, authorization })
      if (path.endsWith('/status')) {
        const request = body.request as Record<string, unknown>
        return response({ request, job: { jobId: request.launchId, state,
          attempt: 1, maxAttempts: 3, createdAt: '2026-10-01T12:00:00Z',
          updatedAt: '2026-10-01T12:02:00Z',
          ...(state === 'WORKFLOW_LAUNCH_JOB_STATE_COMPLETED'
            || state === 'WORKFLOW_LAUNCH_JOB_STATE_FAILED'
            || state === 'WORKFLOW_LAUNCH_JOB_STATE_DEAD'
            ? { completedAt: '2026-10-01T12:02:00Z' } : {}) } })
      }
      throw new Error(`unexpected ${path}`)
    }))
    const wrapper = mountPanel()
    await flushPromises()
    await button(wrapper, 'Prepare input').trigger('click')
    await flushPromises()
    await button(wrapper, 'Launch workflow').trigger('click')
    await flushPromises()
    expect(wrapper.text()).toContain('Launch submitted. Refresh to read job progress.')
    expect(wrapper.text()).not.toContain('Execution completed')
    await button(wrapper, 'Refresh status').trigger('click')
    await flushPromises()
    expect(wrapper.text()).toContain('Running')
    state = 'WORKFLOW_LAUNCH_JOB_STATE_COMPLETED'
    await button(wrapper, 'Refresh status').trigger('click')
    await flushPromises()
    expect(wrapper.text()).toContain('Execution completed')
    state = 'WORKFLOW_LAUNCH_JOB_STATE_FAILED'
    await button(wrapper, 'Refresh status').trigger('click')
    await flushPromises()
    expect(wrapper.text()).toContain('Execution failed')
    expect(wrapper.text()).not.toContain('private backend error')
    wrapper.unmount()
  })

  it('keeps a saved launch observation when the draft input changes during refresh', async () => {
    let releaseStatus!: (value: Response) => void
    vi.stubGlobal('fetch', vi.fn((path: string, init: RequestInit) => {
      const body = JSON.parse(String(init.body)) as Record<string, unknown>
      if (path.endsWith('/accepted')) return Promise.resolve(response(accepted))
      if (path.endsWith('/contract')) return Promise.resolve(response({ acceptance: accepted,
        inputType: 'test.Input', descriptors, descriptorSet: 'AQID' }))
      if (path.endsWith('/prepare')) return Promise.resolve(response({ acceptance: accepted, input }))
      if (path.endsWith('/launch')) return Promise.resolve(response({ jobId: body.launchId, authorization }))
      if (path.endsWith('/status')) return new Promise<Response>((resolve) => { releaseStatus = resolve })
      throw new Error(`unexpected ${path}`)
    }))
    const wrapper = mountPanel()
    await flushPromises()
    await button(wrapper, 'Prepare input').trigger('click')
    await flushPromises()
    await button(wrapper, 'Launch workflow').trigger('click')
    await flushPromises()
    await button(wrapper, 'Refresh status').trigger('click')
    expect(button(wrapper, 'Refresh status').attributes('disabled')).toBeDefined()
    await wrapper.find('textarea').setValue('{"text":"a new draft"}')
    expect(button(wrapper, 'Refresh status').attributes('disabled')).toBeDefined()
    const intent = JSON.parse(localStorage.getItem(localStorage.key(0)!)!).intent
    releaseStatus(response({ request: intent, authorizedNotQueued: true }))
    await flushPromises()
    expect(wrapper.text()).toContain('Launch authorized; no job was observed')
    expect(button(wrapper, 'Refresh status').attributes('disabled')).toBeUndefined()
    wrapper.unmount()
  })

  it('clears prior task observations when the selected task changes', async () => {
    const otherId = '00000000-0000-4000-8000-000000000009'
    let releaseStatus!: (value: Response) => void
    vi.stubGlobal('fetch', vi.fn((path: string, init: RequestInit) => {
      const body = JSON.parse(String(init.body)) as Record<string, unknown>
      if (path.endsWith('/accepted')) return Promise.resolve(response({ ...accepted, taskId: body.taskId }))
      if (path.endsWith('/contract')) return Promise.resolve(response({
        acceptance: body.acceptance, inputType: 'test.Input', descriptors, descriptorSet: 'AQID',
      }))
      if (path.endsWith('/prepare')) return Promise.resolve(response({ acceptance: accepted, input }))
      if (path.endsWith('/launch')) return Promise.resolve(response({ jobId: body.launchId, authorization }))
      if (path.endsWith('/status')) return new Promise<Response>((resolve) => { releaseStatus = resolve })
      throw new Error(`unexpected ${path}`)
    }))
    const wrapper = mountPanel()
    await flushPromises()
    await button(wrapper, 'Prepare input').trigger('click')
    await flushPromises()
    await button(wrapper, 'Launch workflow').trigger('click')
    await flushPromises()
    await button(wrapper, 'Refresh status').trigger('click')
    const oldIntent = JSON.parse(localStorage.getItem(localStorage.key(0)!)!).intent
    await wrapper.setProps({ taskId: otherId })
    await flushPromises()
    releaseStatus(response({ request: oldIntent, authorizedNotQueued: true }))
    await flushPromises()
    expect(wrapper.text()).not.toContain('Launch authorized; no job was observed')
    await wrapper.setProps({ taskId })
    await flushPromises()
    expect(button(wrapper, 'Refresh status').attributes('disabled')).toBeUndefined()
    expect(wrapper.text()).not.toContain('Launch authorized; no job was observed')
    wrapper.unmount()
  })

  it.each([
    ['notAuthorized', 'No launch authorization was observed'],
    ['authorizedNotQueued', 'Launch authorized; no job was observed'],
  ])('keeps the exact saved retry available for %s', async (outcome, label) => {
    const launches: Record<string, unknown>[] = []
    vi.stubGlobal('fetch', vi.fn(async (path: string, init: RequestInit) => {
      const body = JSON.parse(String(init.body)) as Record<string, unknown>
      if (path.endsWith('/accepted')) return response(accepted)
      if (path.endsWith('/contract')) return response({ acceptance: accepted,
        inputType: 'test.Input', descriptors, descriptorSet: 'AQID' })
      if (path.endsWith('/prepare')) return response({ acceptance: accepted, input })
      if (path.endsWith('/launch')) {
        launches.push(body)
        if (launches.length === 1) throw new Error('reply lost')
        return response({ jobId: body.launchId, authorization })
      }
      if (path.endsWith('/status')) return response({ request: body.request, [outcome]: true })
      throw new Error(`unexpected ${path}`)
    }))
    const wrapper = mountPanel()
    await flushPromises()
    await button(wrapper, 'Prepare input').trigger('click')
    await flushPromises()
    await button(wrapper, 'Launch workflow').trigger('click')
    await flushPromises()
    await button(wrapper, 'Refresh status').trigger('click')
    await flushPromises()
    expect(wrapper.text()).toContain(label)
    await button(wrapper, 'Retry saved launch').trigger('click')
    await flushPromises()
    expect(launches).toHaveLength(2)
    expect(launches[1]).toEqual(launches[0])
    wrapper.unmount()
  })

  it('ignores a status reply that arrives after the saved launch is retried', async () => {
    let releaseStatus!: (value: Response) => void
    let launches = 0
    vi.stubGlobal('fetch', vi.fn((path: string, init: RequestInit) => {
      const body = JSON.parse(String(init.body)) as Record<string, unknown>
      if (path.endsWith('/accepted')) return Promise.resolve(response(accepted))
      if (path.endsWith('/contract')) return Promise.resolve(response({ acceptance: accepted,
        inputType: 'test.Input', descriptors, descriptorSet: 'AQID' }))
      if (path.endsWith('/prepare')) return Promise.resolve(response({ acceptance: accepted, input }))
      if (path.endsWith('/launch')) {
        launches++
        return launches === 1 ? Promise.reject(new Error('reply lost'))
          : Promise.resolve(response({ jobId: body.launchId, authorization }))
      }
      if (path.endsWith('/status')) return new Promise<Response>((resolve) => { releaseStatus = resolve })
      throw new Error(`unexpected ${path}`)
    }))
    const wrapper = mountPanel()
    await flushPromises()
    await button(wrapper, 'Prepare input').trigger('click')
    await flushPromises()
    await button(wrapper, 'Launch workflow').trigger('click')
    await flushPromises()
    await button(wrapper, 'Refresh status').trigger('click')
    await button(wrapper, 'Retry saved launch').trigger('click')
    await flushPromises()
    expect(button(wrapper, 'Refresh status').attributes('disabled')).toBeUndefined()
    const intent = JSON.parse(localStorage.getItem(localStorage.key(0)!)!).intent
    releaseStatus(response({ request: intent, authorizedNotQueued: true }))
    await flushPromises()
    expect(wrapper.text()).toContain(`Job ID: ${intent.launchId}`)
    expect(wrapper.text()).not.toContain('Launch authorized; no job was observed')
    wrapper.unmount()
  })
})
