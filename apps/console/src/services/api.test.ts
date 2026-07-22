import { beforeEach, describe, expect, it, vi } from 'vitest'
import {
  COMPATIBILITY_MODES,
  RegistryApi,
  RegistryError,
  errorMessage,
  parseViolations,
} from './api'

const CONTENT_TYPE = 'application/vnd.schemaregistry.v1+json'

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': CONTENT_TYPE },
  })
}

type FetchLike = (input: string, init?: RequestInit) => Promise<Response>

interface SentRequest {
  method: string
  headers: Record<string, string>
  body: string
}

function sent(fetchMock: ReturnType<typeof vi.fn<FetchLike>>, call = 0): SentRequest {
  return (fetchMock.mock.calls[call]?.[1] ?? {}) as SentRequest
}

describe('RegistryApi', () => {
  let fetchMock: ReturnType<typeof vi.fn<FetchLike>>
  let api: RegistryApi

  beforeEach(() => {
    fetchMock = vi.fn<FetchLike>()
    api = new RegistryApi('/api/protomolt', fetchMock)
  })

  it('lists subjects', async () => {
    fetchMock.mockResolvedValue(jsonResponse(['a', 'b/c.proto']))
    await expect(api.listSubjects()).resolves.toEqual(['a', 'b/c.proto'])
    expect(fetchMock).toHaveBeenCalledWith('/api/protomolt/subjects', expect.anything())
  })

  it('percent-encodes subjects containing slashes in every endpoint', async () => {
    fetchMock.mockResolvedValue(jsonResponse([1, 2]))
    await api.listVersions('example/person.proto')
    expect(fetchMock.mock.calls[0]?.[0]).toBe('/api/protomolt/subjects/example%2Fperson.proto/versions')

    fetchMock.mockResolvedValue(jsonResponse({ compatibility: 'FULL' }))
    await api.setSubjectConfig('example/person.proto', 'FULL')
    expect(fetchMock.mock.calls[1]?.[0]).toBe('/api/protomolt/config/example%2Fperson.proto')
  })

  it('fetches a version envelope and normalizes omitted references', async () => {
    fetchMock.mockResolvedValue(
      jsonResponse({
        subject: 's',
        id: 3,
        version: 2,
        schemaType: 'PROTOBUF',
        schema: 'syntax = "proto3";',
      }),
    )
    const envelope = await api.getVersion('s', 'latest')
    expect(fetchMock.mock.calls[0]?.[0]).toBe('/api/protomolt/subjects/s/versions/latest')
    expect(envelope.references).toEqual([])
    expect(envelope.schema).toBe('syntax = "proto3";')
  })

  it('registers with schemaType PROTOBUF and the registry content type', async () => {
    fetchMock.mockResolvedValue(jsonResponse({ id: 7 }))
    const refs = [{ name: 'core.proto', subject: 'core.proto', version: 1 }]
    await expect(api.register('s', 'syntax = "proto3";', refs)).resolves.toEqual({ id: 7 })
    expect(fetchMock.mock.calls[0]?.[0]).toBe('/api/protomolt/subjects/s/versions')
    const init = sent(fetchMock)
    expect(init.method).toBe('POST')
    expect(init.headers['Content-Type']).toBe(CONTENT_TYPE)
    expect(JSON.parse(init.body)).toEqual({
      schema: 'syntax = "proto3";',
      schemaType: 'PROTOBUF',
      references: refs,
    })
  })

  it('looks up by content via POST /subjects/{s}', async () => {
    fetchMock.mockResolvedValue(
      jsonResponse({ subject: 's', id: 1, version: 1, schemaType: 'PROTOBUF', schema: 'x' }),
    )
    const match = await api.lookup('s', 'x')
    expect(fetchMock.mock.calls[0]?.[0]).toBe('/api/protomolt/subjects/s')
    expect(sent(fetchMock).method).toBe('POST')
    expect(match.version).toBe(1)
  })

  it('fetches schemas by global id', async () => {
    fetchMock.mockResolvedValue(jsonResponse({ schema: 'x', schemaType: 'PROTOBUF' }))
    const byId = await api.getById(42)
    expect(fetchMock.mock.calls[0]?.[0]).toBe('/api/protomolt/schemas/ids/42')
    expect(byId.references).toEqual([])
  })

  describe('the config key quirk', () => {
    it('reads compatibilityLevel from GET /config', async () => {
      fetchMock.mockResolvedValue(jsonResponse({ compatibilityLevel: 'BACKWARD' }))
      await expect(api.globalConfig()).resolves.toBe('BACKWARD')
    })

    it('writes {compatibility} in PUT /config and reads the compatibility echo', async () => {
      fetchMock.mockResolvedValue(jsonResponse({ compatibility: 'NONE' }))
      await expect(api.setGlobalConfig('NONE')).resolves.toBe('NONE')
      expect(fetchMock.mock.calls[0]?.[0]).toBe('/api/protomolt/config')
      const init = sent(fetchMock)
      expect(init.method).toBe('PUT')
      expect(JSON.parse(init.body)).toEqual({ compatibility: 'NONE' })
    })

    it('maps the 40408 "no subject-level config" error to null', async () => {
      fetchMock.mockResolvedValue(
        jsonResponse({ error_code: 40408, message: 'not configured' }, 404),
      )
      await expect(api.subjectConfig('s')).resolves.toBeNull()
    })

    it('returns the subject-level override when set', async () => {
      fetchMock.mockResolvedValue(jsonResponse({ compatibilityLevel: 'FULL' }))
      await expect(api.subjectConfig('s')).resolves.toBe('FULL')
    })
  })

  it('exposes all seven compatibility modes', () => {
    expect(COMPATIBILITY_MODES).toHaveLength(7)
    expect(COMPATIBILITY_MODES).toContain('BACKWARD_TRANSITIVE')
  })

  describe('error envelopes', () => {
    it('parses {error_code, message} into a RegistryError', async () => {
      fetchMock.mockResolvedValue(
        jsonResponse({ error_code: 40401, message: "Subject 'nope' not found." }, 404),
      )
      const error = await api.listVersions('nope').catch((e) => e)
      expect(error).toBeInstanceOf(RegistryError)
      expect(error.status).toBe(404)
      expect(error.errorCode).toBe(40401)
      expect(error.isUnknownSubject).toBe(true)
      expect(error.message).toBe("Subject 'nope' not found.")
      expect(errorMessage(error)).toBe("Subject 'nope' not found.")
    })

    it('classifies 40403 (schema not found) and 409 (incompatible)', async () => {
      fetchMock.mockResolvedValue(jsonResponse({ error_code: 40403, message: 'Schema not found' }, 404))
      const notFound = await api.lookup('s', 'x').catch((e) => e)
      expect(notFound.isSchemaNotFound).toBe(true)

      fetchMock.mockResolvedValue(jsonResponse({ error_code: 409, message: 'nope' }, 409))
      const conflict = await api.register('s', 'x').catch((e) => e)
      expect(conflict.isIncompatible).toBe(true)
    })

    it('survives non-JSON error bodies', async () => {
      fetchMock.mockResolvedValue(new Response('<html>bad gateway</html>', { status: 502 }))
      const error = await api.listSubjects().catch((e) => e)
      expect(error).toBeInstanceOf(RegistryError)
      expect(error.status).toBe(502)
      expect(error.message).toBe('HTTP 502')
    })
  })

  it('fetches the descriptor set as raw bytes', async () => {
    const bytes = new Uint8Array([10, 1, 2, 3])
    fetchMock.mockResolvedValue(
      new Response(bytes, { status: 200, headers: { 'Content-Type': 'application/x-protobuf' } }),
    )
    const result = await api.descriptorSet('example/person.proto')
    expect(fetchMock.mock.calls[0]?.[0]).toBe(
      '/api/protomolt/protomolt/subjects/example%2Fperson.proto/descriptor-set',
    )
    expect(result).toEqual(bytes)
  })

  it('throws the envelope for descriptor-set errors too', async () => {
    fetchMock.mockResolvedValue(jsonResponse({ error_code: 40401, message: 'nope' }, 404))
    await expect(api.descriptorSet('s')).rejects.toMatchObject({ errorCode: 40401 })
  })
})

describe('parseViolations', () => {
  it('parses the standard 409 message into rule/path/detail rows', () => {
    const message =
      'Schema being registered is incompatible with an earlier schema: ' +
      'v1: FIELD_TYPE_CHANGED at example.Person.age: field 1 changed type from int32 to string; ' +
      'v2: FIELD_REMOVED at example.Person.email: field 2 was removed'
    const violations = parseViolations(message)
    expect(violations).toHaveLength(2)
    expect(violations[0]).toMatchObject({
      version: 'v1',
      rule: 'FIELD_TYPE_CHANGED',
      path: 'example.Person.age',
      detail: 'field 1 changed type from int32 to string',
    })
    expect(violations[1]).toMatchObject({ version: 'v2', rule: 'FIELD_REMOVED' })
  })

  it('keeps unparseable lines as raw detail', () => {
    const violations = parseViolations(
      'Schema being registered is incompatible with an earlier schema: ' +
        'v3: comparison failed: boom; candidate does not resolve: missing import',
    )
    expect(violations).toHaveLength(2)
    expect(violations[0]).toMatchObject({ version: 'v3', rule: '', detail: 'comparison failed: boom' })
    expect(violations[1]).toMatchObject({ rule: '', detail: 'candidate does not resolve: missing import' })
  })

  it('handles messages without the standard prefix', () => {
    const violations = parseViolations('v1: FIELD_REMOVED at a.B.c: gone')
    expect(violations).toHaveLength(1)
    expect(violations[0].rule).toBe('FIELD_REMOVED')
  })
})
