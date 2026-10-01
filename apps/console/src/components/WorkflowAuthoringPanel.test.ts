// @vitest-environment jsdom
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
import WorkflowAuthoringPanel from './WorkflowAuthoringPanel.vue'
import { installDomStubs, vuetifyForTests } from '../componentTestKit'

installDomStubs()

const principal = 'coordinator-one'
const objective = 'Create a bounded workflow and verify its contract.'
const template = {
  templateSha256: 'a'.repeat(64),
  template: {
    leaseSeconds: 600,
    spec: {
      objective,
      requiredChecks: [{ name: 'workflow-valid', description: 'Compile and validate.' }],
      context: [{ sha256: 'b'.repeat(64) }],
      contract: { typeName: 'example.v1.WorkflowAuthoringDeliverable' },
    },
  },
}

function response(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json' },
  })
}

function startResponse(request: Record<string, string>) {
  return {
    request,
    offer: {
      attempt: 1,
      spec: { objective: request.objective },
      startBindingSha256: 'c'.repeat(64),
    },
  }
}

function mountPanel(whichPrincipal = principal) {
  return mount(WorkflowAuthoringPanel, {
    props: {
      principal: whichPrincipal,
      workers: [
        { workerId: 'author-one', admitted: true, connected: true, provider: 'fixture', model: '', capabilities: [] },
        { workerId: 'offline-worker', admitted: true, connected: false, provider: 'fixture', model: '', capabilities: [] },
        { workerId: 'unadmitted-worker', admitted: false, connected: true, provider: 'fixture', model: '', capabilities: [] },
      ],
    },
    global: { plugins: [vuetifyForTests()] },
  })
}

function button(wrapper: ReturnType<typeof mountPanel>, label: string) {
  return wrapper.findAll('button').find((item) => item.text().includes(label))!
}

function deferred<T>() {
  let resolve!: (value: T) => void
  let reject!: (reason?: unknown) => void
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no })
  return { promise, resolve, reject }
}

describe('WorkflowAuthoringPanel', () => {
  beforeEach(() => {
    localStorage.clear()
    vi.unstubAllGlobals()
  })

  it('retries the exact persisted request after a lost start reply and component remount', async () => {
    const starts: Record<string, string>[] = []
    let loseFirstReply = true
    vi.stubGlobal('fetch', vi.fn(async (path: string, init?: RequestInit) => {
      if (path.endsWith('/template')) return response(template)
      if (path.endsWith('/start')) {
        const request = JSON.parse(String(init?.body)) as Record<string, string>
        starts.push(request)
        if (loseFirstReply) {
          loseFirstReply = false
          throw new Error('reply lost')
        }
        return response(startResponse(request))
      }
      throw new Error(`unexpected ${path}`)
    }))

    const first = mountPanel()
    await flushPromises()
    expect(first.text()).toContain('workflow-valid')
    expect(first.find('textarea').element.value).toBe(objective)
    await button(first, 'Start workflow authoring').trigger('click')
    await flushPromises()
    expect(first.text()).toContain('reply lost')
    expect(first.text()).toContain('Retry saved start')
    expect(starts).toHaveLength(1)
    const original = starts[0]!
    expect(original).toEqual({
      taskId: expect.any(String), workerId: 'author-one', templateSha256: template.templateSha256, objective,
    })
    expect(localStorage.length).toBe(1)
    first.unmount()

    const reloaded = mountPanel()
    await flushPromises()
    expect(reloaded.text()).toContain('Saved authoring starts')
    await button(reloaded, 'Retry saved start').trigger('click')
    await flushPromises()
    expect(starts).toHaveLength(2)
    expect(starts[1]).toEqual(original)
    expect(reloaded.text()).toContain('Original offer recorded. See the task for current progress.')
    expect(reloaded.emitted('started')?.[0]).toEqual([original.taskId])
    reloaded.unmount()
  })

  it('persists the request snapshot and suppresses navigation when the draft changes during the reply', async () => {
    const pending = deferred<Response>()
    let sent: Record<string, string> | undefined
    vi.stubGlobal('fetch', vi.fn((path: string, init?: RequestInit) => {
      if (path.endsWith('/template')) return Promise.resolve(response(template))
      if (path.endsWith('/start')) {
        sent = JSON.parse(String(init?.body)) as Record<string, string>
        return pending.promise
      }
      return Promise.reject(new Error(`unexpected ${path}`))
    }))

    const wrapper = mountPanel()
    await flushPromises()
    await button(wrapper, 'Start workflow authoring').trigger('click')
    await flushPromises()
    expect(sent?.objective).toBe(objective)
    expect(JSON.parse(localStorage.getItem(localStorage.key(0)!)!).request).toEqual(sent)

    const edited = 'Keep the same contract but clarify the authoring objective.'
    await wrapper.find('textarea').setValue(edited)
    pending.resolve(response(startResponse(sent!)))
    await flushPromises()

    expect(wrapper.find('textarea').element.value).toBe(edited)
    expect(wrapper.emitted('started')).toBeUndefined()
    expect(wrapper.text()).toContain('Original offer recorded. See the task for current progress.')
    expect(JSON.parse(localStorage.getItem(localStorage.key(0)!)!).response.request.objective).toBe(objective)
    wrapper.unmount()
  })

  it('ignores a pending response after principal changes and keeps saved starts isolated', async () => {
    const pending = deferred<Response>()
    let sent: Record<string, string> | undefined
    vi.stubGlobal('fetch', vi.fn((path: string, init?: RequestInit) => {
      if (path.endsWith('/template')) return Promise.resolve(response(template))
      if (path.endsWith('/start')) {
        sent = JSON.parse(String(init?.body)) as Record<string, string>
        return pending.promise
      }
      return Promise.reject(new Error(`unexpected ${path}`))
    }))

    const wrapper = mountPanel()
    await flushPromises()
    await button(wrapper, 'Start workflow authoring').trigger('click')
    await flushPromises()
    const priorTask = sent!.taskId
    await wrapper.setProps({ principal: 'coordinator-two' })
    await flushPromises()
    pending.resolve(response(startResponse(sent!)))
    await flushPromises()

    expect(wrapper.text()).not.toContain(priorTask)
    expect(wrapper.text()).not.toContain('Original offer recorded.')
    expect(wrapper.emitted('started')).toBeUndefined()
    expect(localStorage.getItem(
      `protomolt.workflow-authoring.start.v1.${encodeURIComponent('coordinator-two')}.${priorTask}`,
    )).toBeNull()
    wrapper.unmount()
  })

  it('ignores a successful response that arrives after the panel is unmounted', async () => {
    const pending = deferred<Response>()
    let sent: Record<string, string> | undefined
    vi.stubGlobal('fetch', vi.fn((path: string, init?: RequestInit) => {
      if (path.endsWith('/template')) return Promise.resolve(response(template))
      if (path.endsWith('/start')) {
        sent = JSON.parse(String(init?.body)) as Record<string, string>
        return pending.promise
      }
      return Promise.reject(new Error(`unexpected ${path}`))
    }))
    const wrapper = mountPanel()
    await flushPromises()
    await button(wrapper, 'Start workflow authoring').trigger('click')
    await flushPromises()
    wrapper.unmount()
    pending.resolve(response(startResponse(sent!)))
    await flushPromises()
    expect(localStorage.length).toBe(1)
    expect(wrapper.emitted('started')).toBeUndefined()
  })

  it('rejects a mismatched response and leaves the saved exact request retryable', async () => {
    vi.stubGlobal('fetch', vi.fn(async (path: string, init?: RequestInit) => {
      if (path.endsWith('/template')) return response(template)
      if (path.endsWith('/start')) {
        const request = JSON.parse(String(init?.body)) as Record<string, string>
        return response({ ...startResponse(request), request: { ...request, workerId: 'other-worker' } })
      }
      throw new Error(`unexpected ${path}`)
    }))
    const wrapper = mountPanel()
    await flushPromises()
    await button(wrapper, 'Start workflow authoring').trigger('click')
    await flushPromises()
    expect(wrapper.text()).toContain('does not match the saved request')
    expect(wrapper.text()).toContain('Retry saved start')
    const saved = JSON.parse(localStorage.getItem(localStorage.key(0)!)!)
    expect(saved.response).toBeUndefined()
    wrapper.unmount()
  })
})
