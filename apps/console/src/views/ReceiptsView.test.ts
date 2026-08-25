// @vitest-environment jsdom
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
import ReceiptsView from './ReceiptsView.vue'
import * as receipts from '../services/receipts'
import { installDomStubs, routerForTests, vuetifyForTests } from '../componentTestKit'

vi.mock('../services/receipts', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../services/receipts')>()
  return {
    ...actual,
    exportRecord: vi.fn(),
    verifyRecord: vi.fn(),
    evaluateRecord: vi.fn(),
  }
})

const api = receipts as unknown as {
  exportRecord: ReturnType<typeof vi.fn>
  verifyRecord: ReturnType<typeof vi.fn>
  evaluateRecord: ReturnType<typeof vi.fn>
}

installDomStubs()

async function mountView() {
  const router = routerForTests()
  await router.push('/receipts')
  await router.isReady()
  const wrapper = mount(ReceiptsView, {
    global: { plugins: [vuetifyForTests(), router] },
  })
  await flushPromises()
  return wrapper
}

beforeEach(() => {
  vi.clearAllMocks()
})

describe('ReceiptsView', () => {
  it('exports a run into the record box, then verifies it', async () => {
    api.exportRecord.mockResolvedValue({ recordBase64: 'AAAA', recordId: 'record-run-1' })
    api.verifyRecord.mockResolvedValue({
      verified: true,
      manifestDigest: 'sha256:abcdef',
      checks: [
        { id: 'manifest-digest', status: 'PASSED', detail: 'digest matches' },
        { id: 'artifact-bytes', status: 'SKIPPED', detail: 'no artifact bytes supplied' },
      ],
      nonClaims: ['does not claim the inputs were correct'],
    })
    const wrapper = await mountView()

    await wrapper.find('input').setValue('run-1')
    await wrapper.findAll('button').find((b) => b.text().includes('Export from the run'))!
      .trigger('click')
    await flushPromises()
    expect(api.exportRecord).toHaveBeenCalledWith('run-1')

    await wrapper.findAll('button').find((b) => b.text() === 'Verify')!.trigger('click')
    await flushPromises()
    expect(api.verifyRecord).toHaveBeenCalledWith('AAAA')
    const text = wrapper.text()
    expect(text).toContain('the record holds')
    expect(text).toContain('manifest-digest')
    expect(text).toContain('What this record does not claim')
    expect(text).toContain('does not claim the inputs were correct')
  })

  it('shows an evaluation with its replayed steps and failures', async () => {
    api.evaluateRecord.mockResolvedValue({
      accepted: false,
      policyId: 'default',
      checks: [{ id: 'workflow-replay', status: 'FAILED', detail: 'step order differs' }],
      replaySteps: [
        { stepName: 'chunk', ok: true },
        { stepName: 'embed', ok: false, detail: 'recorded FAILED, workflow requires success' },
      ],
      nonClaims: [],
    })
    const wrapper = await mountView()
    await wrapper.find('textarea').setValue('AAAA')
    await wrapper.findAll('button')
      .find((b) => b.text().includes('Evaluate against its workflow'))!.trigger('click')
    await flushPromises()
    const text = wrapper.text()
    expect(text).toContain('not accepted')
    expect(text).toContain('The recorded steps, replayed')
    expect(text).toContain('recorded FAILED, workflow requires success')
  })

  it('surfaces an export refusal without clearing the page', async () => {
    api.exportRecord.mockRejectedValue(new Error("no evidence stored for run 'nope'"))
    const wrapper = await mountView()
    await wrapper.find('input').setValue('nope')
    await wrapper.findAll('button').find((b) => b.text().includes('Export from the run'))!
      .trigger('click')
    await flushPromises()
    expect(wrapper.text()).toContain("no evidence stored for run 'nope'")
  })
})
