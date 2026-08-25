// @vitest-environment jsdom
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
import MetricsView from './MetricsView.vue'
import * as metricsApi from '../services/metrics'
import { installDomStubs, routerForTests, vuetifyForTests } from '../componentTestKit'

vi.mock('../services/metrics', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../services/metrics')>()
  return {
    ...actual,
    findMetricsProfile: vi.fn(),
    describeMapping: vi.fn(),
    queryMetrics: vi.fn(),
  }
})

const api = metricsApi as unknown as {
  findMetricsProfile: ReturnType<typeof vi.fn>
  describeMapping: ReturnType<typeof vi.fn>
  queryMetrics: ReturnType<typeof vi.fn>
}

installDomStubs()

async function mountView() {
  const router = routerForTests()
  await router.push('/metrics')
  await router.isReady()
  const wrapper = mount(MetricsView, {
    global: { plugins: [vuetifyForTests(), router] },
  })
  await flushPromises()
  return wrapper
}

beforeEach(() => {
  vi.clearAllMocks()
})

describe('MetricsView', () => {
  it('says how to connect a metrics node when no profile has the contract', async () => {
    api.findMetricsProfile.mockResolvedValue(null)
    const wrapper = await mountView()
    expect(wrapper.text()).toContain('No registered service exposes the metric contract')
  })

  it('describes a subject, fills the pickers, and renders queried rows', async () => {
    api.findMetricsProfile.mockResolvedValue('lake')
    api.describeMapping.mockResolvedValue({
      members: [
        { name: 'region', role: 'MEMBER_ROLE_DIMENSION' },
        { name: 'orders', role: 'MEMBER_ROLE_MEASURE' },
      ],
    })
    api.queryMetrics.mockResolvedValue({
      rows: [
        { dimensions: { region: 'EU' }, measures: { orders: 1200 } },
        { dimensions: { region: 'US' }, measures: { orders: 300 } },
      ],
      physicalPlan: 'SELECT region, count(*) …',
    })
    const wrapper = await mountView()

    await wrapper.find('input').setValue('orders-value')
    await wrapper.findAll('button').find((b) => b.text() === 'Describe')!.trigger('click')
    await flushPromises()
    expect(api.describeMapping).toHaveBeenCalledWith('lake', 'orders-value')

    await wrapper.findAll('button').find((b) => b.text() === 'Query')!.trigger('click')
    await flushPromises()
    // The first measure was pre-chosen by the describe.
    expect(api.queryMetrics).toHaveBeenCalledWith('lake', expect.objectContaining({
      mappingSubject: 'orders-value', measures: ['orders'],
    }))
    const text = wrapper.text()
    expect(text).toContain('2 rows')
    expect(text).toContain('1,200')
    expect(text).toContain('How the engine ran it')
  })

  it('surfaces a refused subject as its own message', async () => {
    api.findMetricsProfile.mockResolvedValue('lake')
    api.describeMapping.mockRejectedValue(
      new Error("unknown subject 'x'; served: orders-value"))
    const wrapper = await mountView()
    await wrapper.find('input').setValue('x')
    await wrapper.findAll('button').find((b) => b.text() === 'Describe')!.trigger('click')
    await flushPromises()
    expect(wrapper.text()).toContain("unknown subject 'x'; served: orders-value")
  })
})
