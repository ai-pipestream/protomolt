import { describe, expect, it } from 'vitest'
import { RegistryError, type RegistryApi } from './api'
import { checkCandidate, registerCandidate } from './compatCheck'

function apiWith(overrides: Partial<Record<'lookup' | 'register', unknown>>): RegistryApi {
  return overrides as unknown as RegistryApi
}

describe('checkCandidate', () => {
  it('reports unchanged when the exact content is already registered', async () => {
    const api = apiWith({
      lookup: async () => ({ subject: 's', id: 5, version: 2, schemaType: 'PROTOBUF', schema: 'x', references: [] }),
    })
    await expect(checkCandidate(api, 's', 'x', [])).resolves.toEqual({
      kind: 'unchanged',
      version: 2,
      id: 5,
    })
  })

  it('reports new-subject on 40401', async () => {
    const api = apiWith({
      lookup: async () => {
        throw new RegistryError(404, 40401, "Subject 's' not found.")
      },
    })
    await expect(checkCandidate(api, 's', 'x', [])).resolves.toEqual({ kind: 'new-subject' })
  })

  it('reports will-register on 40403 (content differs from all versions)', async () => {
    const api = apiWith({
      lookup: async () => {
        throw new RegistryError(404, 40403, 'Schema not found')
      },
    })
    await expect(checkCandidate(api, 's', 'x', [])).resolves.toEqual({ kind: 'will-register' })
  })

  it('reports invalid on 422', async () => {
    const api = apiWith({
      lookup: async () => {
        throw new RegistryError(422, 42201, 'Invalid schema references: nope')
      },
    })
    await expect(checkCandidate(api, 's', 'x', [])).resolves.toEqual({
      kind: 'invalid',
      message: 'Invalid schema references: nope',
    })
  })

  it('rethrows transport-level failures', async () => {
    const api = apiWith({
      lookup: async () => {
        throw new TypeError('network down')
      },
    })
    await expect(checkCandidate(api, 's', 'x', [])).rejects.toThrow('network down')
  })
})

describe('registerCandidate', () => {
  it('returns the new global id on success', async () => {
    const api = apiWith({ register: async () => ({ id: 9 }) })
    await expect(registerCandidate(api, 's', 'x', [])).resolves.toEqual({
      kind: 'registered',
      id: 9,
    })
  })

  it('parses 409 violations into rows', async () => {
    const api = apiWith({
      register: async () => {
        throw new RegistryError(
          409,
          409,
          'Schema being registered is incompatible with an earlier schema: ' +
            'v1: FIELD_TYPE_CHANGED at example.Person.age: field 1 changed type',
        )
      },
    })
    const outcome = await registerCandidate(api, 's', 'x', [])
    expect(outcome.kind).toBe('incompatible')
    if (outcome.kind === 'incompatible') {
      expect(outcome.violations).toHaveLength(1)
      expect(outcome.violations[0]).toMatchObject({
        version: 'v1',
        rule: 'FIELD_TYPE_CHANGED',
        path: 'example.Person.age',
      })
    }
  })

  it('maps 422 to invalid', async () => {
    const api = apiWith({
      register: async () => {
        throw new RegistryError(422, 42201, 'Invalid schema: empty schema')
      },
    })
    await expect(registerCandidate(api, 's', '', [])).resolves.toEqual({
      kind: 'invalid',
      message: 'Invalid schema: empty schema',
    })
  })
})
