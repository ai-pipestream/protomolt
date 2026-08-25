import { describe, expect, it, vi } from 'vitest'
import {
  invokeMethod,
  kebab,
  listServices,
  registerService,
  requestSkeleton,
  splitTarget,
  verbName,
  type ReflectedMethod,
  type ReflectedService,
} from './services'

function answering(body: unknown, ok = true) {
  return vi.fn(async () => ({ ok, status: ok ? 200 : 400, json: async () => body })) as never
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
      { name: 'default', host: 'billing-host', port: 9000, tls: true },
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
      ],
    }
    expect(requestSkeleton(method)).toEqual({
      docId: '', k: 0, exact: false, lane: 0, filters: [], query: {},
    })
  })
})
