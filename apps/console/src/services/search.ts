/**
 * The search page's client. Search is served by a document-platform node, not by
 * protomolt-serve, so the console reaches it the way it reaches any remote gRPC
 * service: through a registered service profile and the ServiceInvoke verb. This
 * module finds the profile that exposes the search contract and shapes its calls.
 */
import { inspectService, invokeMethod, listServices } from './services'

export const SEARCH_SERVICE = 'ai.pipestream.proto.search.v1.SearchService'

export interface SearchSubject {
  subject: string
  docIdField?: string
  textFields?: string[]
  hasVectorLane?: boolean
}

export type SearchLane = 'SEARCH_LANE_LEXICAL' | 'SEARCH_LANE_VECTOR' | 'SEARCH_LANE_HYBRID'

export interface SearchQuery {
  mappingSubject: string
  query: string
  k: number
  lane: SearchLane
  fields?: string[]
}

/** One stored field value; exactly one arm is set, typed by the mapping. */
export interface StoredValue {
  stringValue?: string
  int64Value?: string | number
  doubleValue?: number
  boolValue?: boolean
  timestampValue?: string
  bytesValue?: string
}

export interface SearchHit {
  docId: string
  chunkId?: string
  score?: number
  stored?: Record<string, StoredValue>
}

/**
 * The registered profile exposing the search contract, or null when none does.
 * Found by inspecting each profile's reflected services — the profile's name is
 * whatever the operator chose, so the contract is the only reliable marker.
 */
export async function findSearchProfile(
  fetchFn: typeof fetch = fetch,
): Promise<string | null> {
  for (const summary of await listServices(fetchFn)) {
    try {
      const inspection = await inspectService(summary.name, fetchFn)
      if ((inspection.services ?? []).some((s) => s.name === SEARCH_SERVICE)) {
        return summary.name
      }
    } catch {
      // A profile whose endpoint is down is not the search profile today.
    }
  }
  return null
}

export async function listSubjects(
  profile: string,
  fetchFn: typeof fetch = fetch,
): Promise<SearchSubject[]> {
  const result = await invokeMethod(profile, `${SEARCH_SERVICE}/ListSubjects`, {}, fetchFn)
  if (!result.ok) throw new Error(result.description || result.status || 'ListSubjects failed')
  const response = (result.responses?.[0] ?? {}) as { subjects?: SearchSubject[] }
  return response.subjects ?? []
}

export async function search(
  profile: string,
  query: SearchQuery,
  fetchFn: typeof fetch = fetch,
): Promise<SearchHit[]> {
  const request: Record<string, unknown> = {
    mappingSubject: query.mappingSubject,
    query: query.query,
    k: query.k,
    lane: query.lane,
  }
  if (query.fields?.length) request.fields = query.fields
  const result = await invokeMethod(profile, `${SEARCH_SERVICE}/Search`, request, fetchFn)
  if (!result.ok) throw new Error(result.description || result.status || 'Search failed')
  const response = (result.responses?.[0] ?? {}) as { hits?: SearchHit[] }
  return response.hits ?? []
}

/** The stored value's one set arm, as text a result list can show. */
export function renderStored(value: StoredValue): string {
  if (value.stringValue !== undefined) return value.stringValue
  if (value.int64Value !== undefined) return String(value.int64Value)
  if (value.doubleValue !== undefined) return String(value.doubleValue)
  if (value.boolValue !== undefined) return String(value.boolValue)
  if (value.timestampValue !== undefined) return value.timestampValue
  if (value.bytesValue !== undefined) return `${value.bytesValue.length} bytes (base64)`
  return ''
}
