import { describe, expect, it } from 'vitest'
import {
  defaultResolutions,
  importsOf,
  isHardClash,
  mergedNameFor,
  mergedReferences,
  mergeRequest,
  scopeNameFor,
  unresolvedFields,
  type MergeClash,
} from './mergeWorkbench'

const CLASHES: MergeClash[] = [
  {
    field: 'id',
    kind: 'coalesced',
    origins: [
      { source: 'order', type: 'string' },
      { source: 'ticket', type: 'string' },
    ],
    suggested: { action: 'coalesce' },
  },
  {
    field: 'status',
    kind: 'type-clash',
    origins: [
      { source: 'order', type: 'string' },
      { source: 'ticket', type: 'support.v1.Status' },
    ],
    suggested: { action: 'rename', names: { order: 'order_status', ticket: 'ticket_status' } },
  },
]

describe('merge workbench logic', () => {
  it('builds the envelope with resolutions and reportOnly only when present', () => {
    const bare = mergeRequest('derived.v1.Case', [
      { name: 'order', type: 'shop.v1.Order', descriptorSetBase64: 'AAA' },
    ], {}, true) as Record<string, unknown>
    expect(bare).toMatchObject({
      name: 'derived.v1.Case',
      reportOnly: true,
      sources: [{ name: 'order', type: 'shop.v1.Order', schema: { descriptorSetBase64: 'AAA' } }],
    })
    expect('resolutions' in bare).toBe(false)

    const resolved = mergeRequest('derived.v1.Case', [], {
      status: { action: 'prefer', source: 'ticket' },
    }, false) as Record<string, unknown>
    expect(resolved.resolutions).toEqual({ status: { action: 'prefer', source: 'ticket' } })
    expect('reportOnly' in resolved).toBe(false)
  })

  it('classifies clashes and pre-fills only the hard ones', () => {
    expect(isHardClash(CLASHES[0])).toBe(false)
    expect(isHardClash(CLASHES[1])).toBe(true)
    const defaults = defaultResolutions(CLASHES)
    expect(Object.keys(defaults)).toEqual(['status'])
    expect(defaults.status.names).toEqual({ order: 'order_status', ticket: 'ticket_status' })
  })

  it('tracks unresolved hard clashes', () => {
    expect(unresolvedFields(CLASHES, {})).toEqual(['status'])
    expect(unresolvedFields(CLASHES, { status: { action: 'rename' } })).toEqual([])
  })

  it('derives registration references from the emitted imports', () => {
    const source = [
      'syntax = "proto3";',
      '',
      'package derived.v1;',
      '',
      'import "shop/v1/order.proto";',
      'import "support/v1/ticket.proto";',
      '',
      'message Case {',
      '  string id = 1;',
      '}',
    ].join('\n')
    expect(importsOf(source)).toEqual(['shop/v1/order.proto', 'support/v1/ticket.proto'])
    expect(mergedReferences(source, () => 1)).toEqual([
      { name: 'shop/v1/order.proto', subject: 'shop/v1/order.proto', version: 1 },
      { name: 'support/v1/ticket.proto', subject: 'support/v1/ticket.proto', version: 1 },
    ])
  })

  it('suggests scope and merged names', () => {
    expect(scopeNameFor('shop.v1.Order', [])).toBe('order')
    expect(scopeNameFor('crm.v1.Order', ['order'])).toBe('order_2')
    expect(mergedNameFor([
      { name: 'order', type: 'shop.v1.Order', descriptorSetBase64: '' },
      { name: 'ticket', type: 'support.v1.Ticket', descriptorSetBase64: '' },
    ])).toBe('derived.v1.OrderTicket')
  })
})
