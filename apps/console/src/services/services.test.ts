import { describe, expect, it, vi } from 'vitest'
import {
  findProfileByContract,
  invokeMethod,
  invokeUnary,
  kebab,
  listServices,
  registerService,
  requestSkeleton,
  splitTarget,
  unwrap,
  verbName,
  type ReflectedMethod,
  type ReflectedService,
} from './services'

function answering(body: unknown, ok = true) {
  return vi.fn(async () => ({
    ok, status: ok ? 200 : 400, text: async () => JSON.stringify(body),
  })) as never
}

describe('the workspace client', () => {
  it('lists registered services through ServiceList', async () => {
    const fetchFn = answering({ services: [{ name: 'billing', endpoints: ['default'] }] })
    expect(await listServices(fetchFn)).toEqual([{ name: 'billing', endpoints: ['default'] }])
    expect((fetchFn as ReturnType<typeof vi.fn>).mock.calls[0][0])
      .toBe('/api/serve/grpc-json/ProtoMoltService/ServiceList')
  })

  it('registers a profile with the target split into host and port', async () => {
    const fetchFn = answering({ ok: true })
    await registerService('billing', 'billing-host:9000', true, '', fetchFn)
    const body = JSON.parse((fetchFn as ReturnType<typeof vi.fn>).mock.calls[0][1].body)
    expect(body.profile.endpoints).toEqual([
      { name: 'default', host: 'billing-host', port: 9000, transport: 'TRANSPORT_TLS' },
    ])
  })

  it('refuses a target without a port, naming what is missing', () => {
    expect(() => splitTarget('billing-host')).toThrow(/host:port/)
    expect(() => splitTarget('host:99999')).toThrow(/host:port/)
    expect(splitTarget('[::1]:9000')).toEqual(['[::1]', 9000])
  })

  it('invokes package.Service/Method with the request as the body', async () => {
    const fetchFn = answering({ ok: true, status: 'OK', responses: [{}] })
    const result = await invokeMethod('billing', 'shop.Billing/Charge', { amount: 3 }, fetchFn)
    expect(result.ok).toBe(true)
    const body = JSON.parse((fetchFn as ReturnType<typeof vi.fn>).mock.calls[0][1].body)
    expect(body).toEqual({ name: 'billing', method: 'shop.Billing/Charge', request: { amount: 3 } })
  })

  it('surfaces the serve error body as the thrown message', async () => {
    const fetchFn = answering({ error: 'unknown-service', message: "no profile 'x'" }, false)
    await expect(listServices(fetchFn)).rejects.toThrow("no profile 'x'")
  })

  it('answers an outage with its HTTP status, not a parse error', async () => {
    // A proxy fronting an outage answers HTML; the status is the story.
    const gateway = {
      ok: false, status: 502,
      text: async () => '<html>Bad Gateway</html>',
    } as unknown as Response
    await expect(unwrap(gateway)).rejects.toThrow('HTTP 502')
  })

  it('carries gate findings on the thrown error', async () => {
    const refused = {
      ok: false, status: 409,
      text: async () => JSON.stringify({
        message: 'not stored', findings: [{ step: '', kind: 'workflow', error: 'no steps' }],
      }),
    } as unknown as Response
    await expect(unwrap(refused)).rejects.toSatisfy((e: Error & { findings?: unknown[] }) =>
      e.message === 'not stored' && e.findings?.length === 1)
  })

  it('finds a profile by its contract with the slow endpoint not consulted last', async () => {
    const inspected: string[] = []
    const fetchFn = vi.fn(async (url: string, init?: RequestInit) => {
      const body = init?.body ? JSON.parse(String(init.body)) : undefined
      if (url.endsWith('/ServiceList')) {
        return { ok: true, status: 200, text: async () => JSON.stringify(
            { services: [{ name: 'down' }, { name: 'lake' }] }) }
      }
      inspected.push(String(body!.name))
      if (body!.name === 'down') {
        return { ok: false, status: 502, text: async () => 'unreachable' }
      }
      return { ok: true, status: 200, text: async () => JSON.stringify(
          { services: [{ name: 'x.v1.Wanted' }] }) }
    }) as never
    expect(await findProfileByContract('x.v1.Wanted', fetchFn)).toBe('lake')
    // Both inspections were issued; the failure did not stop the sweep.
    expect(inspected.sort()).toEqual(['down', 'lake'])
  })

  it('invokeUnary answers the first reply and throws the call description', async () => {
    const ok = answering({ ok: true, status: 'OK', responses: [{ subjects: [] }] })
    expect(await invokeUnary('p', 's.S/List', {}, ok)).toEqual({ subjects: [] })
    const refused = answering({ ok: false, status: 'INVALID_ARGUMENT',
      description: 'unknown subject' })
    await expect(invokeUnary('p', 's.S/List', {}, refused)).rejects.toThrow('unknown subject')
  })
})

describe('verb naming, mirrored from the server', () => {
  const charge: ReflectedMethod = {
    name: 'Charge', fullName: 'shop.Billing/Charge',
    inputType: 'shop.ChargeRequest', outputType: 'shop.ChargeResponse',
  }

  it('kebabs the way the server does, one dash per uppercase', () => {
    expect(kebab('ListOrders')).toBe('list-orders')
    expect(kebab('already-kebab')).toBe('already-kebab')
    expect(kebab('with_underscore.dot')).toBe('with-underscore-dot')
    // Uppercase runs dash every letter — the server's rule, not the pretty one.
    expect(kebab('HTTPGet')).toBe('h-t-t-p-get')
  })

  it('is profile-qualified alone until two services collide on a method name', () => {
    const services: ReflectedService[] = [
      { name: 'shop.Billing', methods: [charge] },
    ]
    expect(verbName('billing', charge, services)).toBe('billing-charge')

    const refunds: ReflectedMethod = { ...charge, fullName: 'shop.Refunds/Charge' }
    const colliding: ReflectedService[] = [
      { name: 'shop.Billing', methods: [charge] },
      { name: 'shop.Refunds', methods: [refunds] },
    ]
    expect(verbName('billing', charge, colliding)).toBe('billing-billing-charge')
    expect(verbName('billing', refunds, colliding)).toBe('billing-refunds-charge')
  })

  it('does not count client-streaming methods toward a collision', () => {
    const streaming: ReflectedMethod = {
      ...charge, fullName: 'shop.Bulk/Charge', clientStreaming: true,
    }
    const services: ReflectedService[] = [
      { name: 'shop.Billing', methods: [charge] },
      { name: 'shop.Bulk', methods: [streaming] },
    ]
    expect(verbName('billing', charge, services)).toBe('billing-charge')
  })
})

describe('request skeletons', () => {
  it('states one zero value per input field under its JSON name', () => {
    const method: ReflectedMethod = {
      name: 'Check', fullName: 's.S/Check', inputType: 'i', outputType: 'o',
      inputFields: [
        { name: 'doc_id', jsonName: 'docId', type: 'string' },
        { name: 'k', type: 'int32' },
        { name: 'exact', type: 'bool' },
        { name: 'lane', type: 'enum', typeName: 's.Lane' },
        { name: 'filters', type: 'message', cardinality: 'repeated', typeName: 's.F' },
        { name: 'query', type: 'message', typeName: 's.Q' },
        // A map is repeated on the wire but an object in proto3 JSON.
        { name: 'metadata', type: 'message', cardinality: 'map', typeName: 's.M' },
      ],
    }
    expect(requestSkeleton(method)).toEqual({
      docId: '', k: 0, exact: false, lane: 0, filters: [], query: {}, metadata: {},
    })
  })
})
