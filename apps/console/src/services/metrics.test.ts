import { describe, expect, it, vi } from 'vitest'
import {
  describeMapping,
  dimensions,
  findMetricsProfile,
  measures,
  queryMetrics,
  renderMeasure,
} from './metrics'

function bridge(handler: (url: string, body?: Record<string, unknown>) => unknown) {
  return vi.fn(async (url: string, init?: RequestInit) => {
    const body = init?.body ? JSON.parse(String(init.body)) : undefined
    return { ok: true, status: 200, text: async () => JSON.stringify(handler(url, body)) }
  }) as never
}

describe('finding the metrics profile', () => {
  it('picks the profile whose contract is the metric service', async () => {
    const fetchFn = bridge((url, body) => {
      if (url.endsWith('/ServiceList')) {
        return { services: [{ name: 'search-node' }, { name: 'lake' }] }
      }
      return body!.name === 'lake'
        ? { services: [{ name: 'ai.pipestream.proto.metric.v1.MetricService' }] }
        : { services: [{ name: 'ai.pipestream.proto.search.v1.SearchService' }] }
    })
    expect(await findMetricsProfile(fetchFn)).toBe('lake')
  })
})

describe('mapping members', () => {
  it('splits members by their declared role', () => {
    const mapping = {
      members: [
        { name: 'region', role: 'MEMBER_ROLE_DIMENSION' },
        { name: 'orders', role: 'MEMBER_ROLE_MEASURE', aggregate: 'AGGREGATE_COUNT' },
        { name: 'unroled' },
      ],
    }
    expect(measures(mapping).map((m) => m.name)).toEqual(['orders'])
    expect(dimensions(mapping).map((m) => m.name)).toEqual(['region'])
  })
})

describe('metric calls through ServiceInvoke', () => {
  it('describes a subject and returns the first streamed response', async () => {
    const fetchFn = bridge((url, body) => {
      expect(body!.method).toBe('ai.pipestream.proto.metric.v1.MetricService/DescribeMapping')
      expect(body!.request).toEqual({ mappingSubject: 'orders-value' })
      return { ok: true, status: 'OK', responses: [{ members: [{ name: 'orders' }] }] }
    })
    const mapping = await describeMapping('lake', 'orders-value', fetchFn)
    expect(mapping.members).toEqual([{ name: 'orders' }])
  })

  it('wraps chosen dimension names as MemberRefs and keeps the plan', async () => {
    const fetchFn = bridge((url, body) => {
      const request = body!.request as Record<string, unknown>
      expect(request.dimensions).toEqual([{ name: 'region' }])
      expect(request.limit).toBe(50)
      return {
        ok: true, status: 'OK',
        responses: [{ rows: [{ dimensions: { region: 'EU' }, measures: { orders: 7 } }],
          physicalPlan: 'GROUP BY region' }],
      }
    })
    const result = await queryMetrics('lake', {
      mappingSubject: 'orders-value', measures: ['orders'], dimensions: ['region'], limit: 50,
    }, fetchFn)
    expect(result.rows[0].measures).toEqual({ orders: 7 })
    expect(result.physicalPlan).toBe('GROUP BY region')
  })

  it('surfaces a refusal by its description', async () => {
    const fetchFn = bridge(() =>
      ({ ok: false, status: 'INVALID_ARGUMENT', description: "unknown subject 'x'" }))
    await expect(queryMetrics('lake',
        { mappingSubject: 'x', measures: ['orders'], limit: 10 }, fetchFn))
      .rejects.toThrow("unknown subject 'x'")
  })
})

describe('measure rendering', () => {
  it('keeps counts whole and ratios precise', () => {
    expect(renderMeasure(120000)).toBe('120,000')
    expect(renderMeasure(0.12345678)).toBe('0.1235')
  })
})
