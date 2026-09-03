import { describe, expect, it, vi } from 'vitest'
import { findSearchProfile, listSubjects, renderStored, search } from './search'

type Handler = (url: string, body?: Record<string, unknown>) => unknown

/** A serve bridge whose ProtoMoltService verbs answer from the handler. */
function bridge(handler: Handler) {
  return vi.fn(async (url: string, init?: RequestInit) => {
    const body = init?.body ? JSON.parse(String(init.body)) : undefined
    return { ok: true, status: 200, text: async () => JSON.stringify(handler(url, body)) }
  }) as never
}

describe('finding the search profile', () => {
  it('picks the profile whose contract is the search service, whatever its name', async () => {
    const fetchFn = bridge((url, body) => {
      if (url.endsWith('/ServiceList')) {
        return { services: [{ name: 'billing' }, { name: 'westcoast-node' }] }
      }
      if (url.endsWith('/ServiceInspect')) {
        return body!.name === 'westcoast-node'
          ? { services: [{ name: 'ai.protomolt.proto.search.v1.SearchService' }] }
          : { services: [{ name: 'shop.Billing' }] }
      }
      throw new Error(`unexpected ${url}`)
    })
    expect(await findSearchProfile(fetchFn)).toBe('westcoast-node')
  })

  it('answers null when no profile exposes the contract', async () => {
    const fetchFn = bridge((url) =>
      url.endsWith('/ServiceList') ? { services: [{ name: 'billing' }] }
          : { services: [{ name: 'shop.Billing' }] })
    expect(await findSearchProfile(fetchFn)).toBeNull()
  })

  it('skips a profile whose endpoint refuses inspection', async () => {
    const calls: string[] = []
    const fetchFn = vi.fn(async (url: string, init?: RequestInit) => {
      const body = init?.body ? JSON.parse(String(init.body)) : undefined
      if (url.endsWith('/ServiceList')) {
        return { ok: true, status: 200, text: async () => JSON.stringify(
            { services: [{ name: 'down' }, { name: 'search' }] }) }
      }
      calls.push(String(body!.name))
      if (body!.name === 'down') {
        return { ok: false, status: 502, text: async () => JSON.stringify(
            { message: 'unreachable' }) }
      }
      return { ok: true, status: 200, text: async () => JSON.stringify(
          { services: [{ name: 'ai.protomolt.proto.search.v1.SearchService' }] }) }
    }) as never
    expect(await findSearchProfile(fetchFn)).toBe('search')
    expect(calls).toEqual(['down', 'search'])
  })
})

describe('search calls through ServiceInvoke', () => {
  it('lists subjects from the first streamed response', async () => {
    const fetchFn = bridge((url, body) => {
      expect(url.endsWith('/ServiceInvoke')).toBe(true)
      expect(body!.method).toBe('ai.protomolt.proto.search.v1.SearchService/ListSubjects')
      return { ok: true, status: 'OK', responses: [{ subjects: [{ subject: 'people' }] }] }
    })
    expect(await listSubjects('search', fetchFn)).toEqual([{ subject: 'people' }])
  })

  it('sends the query and surfaces a refusal by its description', async () => {
    const fetchFn = bridge((url, body) => {
      const request = body!.request as Record<string, unknown>
      expect(request).toEqual({
        mappingSubject: 'people', query: 'ada', k: 5, lane: 'SEARCH_LANE_HYBRID',
      })
      return { ok: false, status: 'INVALID_ARGUMENT', description: "unknown subject 'people'" }
    })
    await expect(search('search', {
      mappingSubject: 'people', query: 'ada', k: 5, lane: 'SEARCH_LANE_HYBRID',
    }, fetchFn)).rejects.toThrow("unknown subject 'people'")
  })
})

describe('stored values', () => {
  it('renders whichever arm the mapping typed', () => {
    expect(renderStored({ stringValue: 'Ada' })).toBe('Ada')
    expect(renderStored({ int64Value: '42' })).toBe('42')
    expect(renderStored({ doubleValue: 1.5 })).toBe('1.5')
    expect(renderStored({ boolValue: false })).toBe('false')
    expect(renderStored({ timestampValue: '2026-08-24T00:00:00Z' }))
      .toBe('2026-08-24T00:00:00Z')
    // 'AAAA' encodes 3 bytes; 'AA==' encodes 1. The label counts decoded bytes.
    expect(renderStored({ bytesValue: 'AAAA' })).toBe('3 bytes')
    expect(renderStored({ bytesValue: 'AA==' })).toBe('1 byte')
    expect(renderStored({})).toBe('')
  })
})
