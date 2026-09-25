// @vitest-environment jsdom
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
import CorrectionStarterView from './CorrectionStarterView.vue'
import { installDomStubs, vuetifyForTests } from '../componentTestKit'

const { sessionApi } = vi.hoisted(() => ({
  sessionApi: { sessionStatus: vi.fn(), login: vi.fn(), logout: vi.fn() },
}))

vi.mock('../services/tasks', () => ({ TaskApi: class { constructor() { return sessionApi } } }))

installDomStubs()

const accepted = {
  outcome: {
    runId: 'contact-saved-1',
    assessment: { disposition: 'ASSESSMENT_DISPOSITION_ACCEPTED', reason: 'Contact is valid.' },
  },
  receipts: [{ receiptId: 'receipt-1' }],
}

function response(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } })
}

async function mountView() {
  const wrapper = mount(CorrectionStarterView, { global: { plugins: [vuetifyForTests()] } })
  await flushPromises()
  return wrapper
}

function input(wrapper: ReturnType<typeof mount>, label: string): HTMLInputElement {
  const field = wrapper.findAll('.v-input').find((candidate) => candidate.text().includes(label))
  if (!field) throw new Error(`Missing field: ${label}`)
  return field.find('input').element as HTMLInputElement
}

beforeEach(() => {
  vi.clearAllMocks()
  sessionApi.sessionStatus.mockResolvedValue({ authenticated: false, loginRequired: true })
  sessionApi.login.mockResolvedValue({ authenticated: true, loginRequired: true })
  sessionApi.logout.mockResolvedValue(undefined)
  vi.stubGlobal('fetch', vi.fn().mockResolvedValue(response({ available: true })))
})

describe('CorrectionStarterView', () => {
  it('logs in via the console session and keeps the token out of correction requests and storage', async () => {
    const fetchMock = vi.mocked(fetch)
    const wrapper = await mountView()
    await wrapper.get('input[type="password"]').setValue('console-secret')
    await wrapper.get('form').trigger('submit')
    await flushPromises()

    expect(sessionApi.login).toHaveBeenCalledWith('console-secret')
    expect(wrapper.find('input[type="password"]').exists()).toBe(false)
    expect(fetchMock).toHaveBeenCalledWith('/api/correction', expect.objectContaining({
      method: 'GET', credentials: 'same-origin',
    }))
    expect(JSON.stringify(fetchMock.mock.calls)).not.toContain('console-secret')
    expect(localStorage.length).toBe(0)
    expect(sessionStorage.length).toBe(0)
    expect(wrapper.text()).toContain('Run correction')
  })

  it('submits the deterministic fixture, displays the verified status and receipt, then retrieves a saved run', async () => {
    const fetchMock = vi.mocked(fetch)
    sessionApi.sessionStatus.mockResolvedValue({ authenticated: true, loginRequired: true })
    fetchMock.mockResolvedValueOnce(response({ available: true }))
    fetchMock.mockResolvedValueOnce(response(accepted))
    const wrapper = await mountView()

    await wrapper.findAll('.v-input').find((candidate) => candidate.text().includes('Run ID'))!
      .find('input').setValue('contact-saved-1')
    await wrapper.get('form').trigger('submit')
    await flushPromises()
    expect(fetchMock.mock.calls[1]?.[0]).toBe('/api/correction/runs')
    expect(JSON.parse(String(fetchMock.mock.calls[1]?.[1]?.body))).toEqual({
      workflowName: 'correct-contact',
      runId: input(wrapper, 'Run ID').value,
      source: {
        recordId: 'contact-1',
        contactText: 'Ada Lovelace; ada [at] example.org',
        internalNotes: 'Private source note; excluded from provider evidence.',
      },
    })
    expect(wrapper.text()).toContain('ACCEPTED')
    expect(wrapper.text()).toContain('Contact is valid.')
    expect(wrapper.text()).toContain('receipt-1')

    expect(input(wrapper, 'Saved run ID').value).toBe('contact-saved-1')
    // A new page instance represents a browser restart: only the user-saved ID is carried forward.
    wrapper.unmount()
    sessionApi.sessionStatus.mockResolvedValue({ authenticated: true, loginRequired: true })
    fetchMock.mockResolvedValueOnce(response({ available: true }))
    fetchMock.mockResolvedValueOnce(response(accepted))
    const afterRestart = await mountView()
    const lookup = afterRestart.findAll('.v-input').find((candidate) => candidate.text().includes('Saved run ID'))!
    await lookup.find('input').setValue('contact-saved-1')
    await afterRestart.findAll('button').find((button) => button.text().includes('Retrieve'))!.trigger('click')
    await flushPromises()
    expect(fetchMock.mock.calls.at(-1)?.[0]).toBe('/api/correction/runs/contact-saved-1')
    expect(afterRestart.text()).toContain('receipt-1')
  })

  it('surfaces duplicate, invalid, and missing saved-run errors without retrying automatically', async () => {
    const fetchMock = vi.mocked(fetch)
    sessionApi.sessionStatus.mockResolvedValue({ authenticated: true, loginRequired: true })
    fetchMock.mockResolvedValueOnce(response({ available: true }))
    fetchMock.mockResolvedValueOnce(response({ error: 'ALREADY_EXISTS' }, 409))
    fetchMock.mockResolvedValueOnce(response({ error: 'INVALID_ARGUMENT' }, 400))
    fetchMock.mockResolvedValueOnce(response({ error: 'NOT_FOUND' }, 404))
    const wrapper = await mountView()

    await wrapper.get('form').trigger('submit')
    await flushPromises()
    expect(wrapper.text()).toContain('already been used')
    expect(fetchMock).toHaveBeenCalledTimes(2)

    await wrapper.get('form').trigger('submit')
    await flushPromises()
    expect(fetchMock).toHaveBeenCalledTimes(3)
    expect(wrapper.text()).toContain('does not satisfy the correction contract')

    const lookup = wrapper.findAll('.v-input').find((candidate) => candidate.text().includes('Saved run ID'))!
    await lookup.find('input').setValue('missing-run')
    await wrapper.findAll('button').find((button) => button.text().includes('Retrieve'))!.trigger('click')
    await flushPromises()
    expect(wrapper.text()).toContain('No completed outcome exists')
  })
})
