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

describe('MetricsView frozen columns', () => {
  it('keeps the table on the queried measures until the next Query click', async () => {
    api.findMetricsProfile.mockResolvedValue('lake')
    api.describeMapping.mockResolvedValue({
      members: [
        { name: 'region', role: 'MEMBER_ROLE_DIMENSION' },
        { name: 'orders', role: 'MEMBER_ROLE_MEASURE' },
        { name: 'revenue', role: 'MEMBER_ROLE_MEASURE' },
      ],
    })
    api.queryMetrics.mockResolvedValueOnce({
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

    // The describe pre-chose the first measure only.
    await wrapper.findAll('button').find((b) => b.text() === 'Query')!.trigger('click')
    await flushPromises()
    expect(api.queryMetrics).toHaveBeenCalledWith('lake', expect.objectContaining({
      measures: ['orders'],
    }))

    const headers = () => wrapper.findAll('thead th').map((th) => th.text())
    expect(headers()).toEqual(['orders'])

    // Grow the picker WITHOUT re-querying: the table's columns must stay frozen.
    const measuresSelect = wrapper
      .findAllComponents({ name: 'VSelect' })
      .find((c) => c.props('label') === 'Measures')!
    await measuresSelect.setValue(['orders', 'revenue'])
    await flushPromises()
    expect(headers()).toEqual(['orders'])
    expect(headers()).not.toContain('revenue')

    // A second Query with both measures chosen renders both columns.
    api.queryMetrics.mockResolvedValueOnce({
      rows: [
        { dimensions: { region: 'EU' }, measures: { orders: 1200, revenue: 55000 } },
      ],
      physicalPlan: 'SELECT region, count(*), sum(revenue) …',
    })
    await wrapper.findAll('button').find((b) => b.text() === 'Query')!.trigger('click')
    await flushPromises()
    expect(api.queryMetrics).toHaveBeenLastCalledWith('lake', expect.objectContaining({
      measures: ['orders', 'revenue'],
    }))
    expect(headers()).toEqual(['orders', 'revenue'])
  })
})
