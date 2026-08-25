/**
 * The metrics page's client. Metrics are served by a document-platform metrics
 * node, reached like search: through a registered service profile and the
 * ServiceInvoke verb, found by its contract rather than its name.
 */
import { inspectService, invokeMethod, listServices } from './services'

export const METRIC_SERVICE = 'ai.pipestream.proto.metric.v1.MetricService'

export interface MappingMember {
  name: string
  /** 'MEMBER_ROLE_DIMENSION' or 'MEMBER_ROLE_MEASURE'. */
  role?: string
  aggregate?: string
  fieldPath?: string
  description?: string
}

export interface MetricMapping {
  mappingSubject?: string
  messageType?: string
  members?: MappingMember[]
  backends?: string[]
}

export function measures(mapping: MetricMapping): MappingMember[] {
  return (mapping.members ?? []).filter((m) => m.role === 'MEMBER_ROLE_MEASURE')
}

export function dimensions(mapping: MetricMapping): MappingMember[] {
  return (mapping.members ?? []).filter((m) => m.role === 'MEMBER_ROLE_DIMENSION')
}

export interface MetricRow {
  dimensions?: Record<string, string>
  measures?: Record<string, number>
}

export interface MetricsResult {
  rows: MetricRow[]
  physicalPlan?: string
}

/** The registered profile exposing the metric contract, or null when none does. */
export async function findMetricsProfile(
  fetchFn: typeof fetch = fetch,
): Promise<string | null> {
  for (const summary of await listServices(fetchFn)) {
    try {
      const inspection = await inspectService(summary.name, fetchFn)
      if ((inspection.services ?? []).some((s) => s.name === METRIC_SERVICE)) {
        return summary.name
      }
    } catch {
      // A profile whose endpoint is down is not the metrics profile today.
    }
  }
  return null
}

export async function describeMapping(
  profile: string,
  mappingSubject: string,
  fetchFn: typeof fetch = fetch,
): Promise<MetricMapping> {
  const result = await invokeMethod(profile, `${METRIC_SERVICE}/DescribeMapping`,
      { mappingSubject }, fetchFn)
  if (!result.ok) {
    throw new Error(result.description || result.status || 'DescribeMapping failed')
  }
  return (result.responses?.[0] ?? {}) as MetricMapping
}

export async function queryMetrics(
  profile: string,
  query: {
    mappingSubject: string
    measures: string[]
    dimensions?: string[]
    limit: number
  },
  fetchFn: typeof fetch = fetch,
): Promise<MetricsResult> {
  const request: Record<string, unknown> = {
    mappingSubject: query.mappingSubject,
    measures: query.measures,
    limit: query.limit,
  }
  if (query.dimensions?.length) {
    request.dimensions = query.dimensions.map((name) => ({ name }))
  }
  const result = await invokeMethod(profile, `${METRIC_SERVICE}/QueryMetrics`,
      request, fetchFn)
  if (!result.ok) {
    throw new Error(result.description || result.status || 'QueryMetrics failed')
  }
  const response = (result.responses?.[0] ?? {}) as
      { rows?: MetricRow[]; physicalPlan?: string }
  return { rows: response.rows ?? [], physicalPlan: response.physicalPlan }
}

/** A readable measure value: counts stay integers, ratios keep their precision. */
export function renderMeasure(value: number): string {
  if (Number.isInteger(value)) return value.toLocaleString('en-US')
  return value.toLocaleString('en-US', { maximumFractionDigits: 4 })
}
