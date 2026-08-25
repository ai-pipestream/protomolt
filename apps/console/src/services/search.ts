/**
 * The search page's client. Search is served by a document-platform node, not by
 * protomolt-serve, so the console reaches it the way it reaches any remote gRPC
 * service: through a registered service profile and the ServiceInvoke verb. This
 * module finds the profile that exposes the search contract and shapes its calls.
 */
import { findProfileByContract, invokeUnary } from './services'

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

/** The registered profile exposing the search contract, or null when none does. */
export async function findSearchProfile(
  fetchFn: typeof fetch = fetch,
): Promise<string | null> {
  return findProfileByContract(SEARCH_SERVICE, fetchFn)
}

export async function listSubjects(
  profile: string,
  fetchFn: typeof fetch = fetch,
): Promise<SearchSubject[]> {
  const response = await invokeUnary<{ subjects?: SearchSubject[] }>(
      profile, `${SEARCH_SERVICE}/ListSubjects`, {}, fetchFn)
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
  const response = await invokeUnary<{ hits?: SearchHit[] }>(
      profile, `${SEARCH_SERVICE}/Search`, request, fetchFn)
  return response.hits ?? []
}

/** The stored value's one set arm, as text a result list can show. */
export function renderStored(value: StoredValue): string {
  if (value.stringValue !== undefined) return value.stringValue
  if (value.int64Value !== undefined) return String(value.int64Value)
  if (value.doubleValue !== undefined) return String(value.doubleValue)
  if (value.boolValue !== undefined) return String(value.boolValue)
  if (value.timestampValue !== undefined) return value.timestampValue
  if (value.bytesValue !== undefined) {
    // The JSON arm is base64; the person is told how many bytes the field holds.
    const encoded = value.bytesValue.replace(/=+$/, '')
    const count = Math.floor((encoded.length * 3) / 4)
    return `${count} byte${count === 1 ? '' : 's'}`
  }
  return ''
}
