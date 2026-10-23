import { create } from '@bufbuild/protobuf'
import { describe, expect, it } from 'vitest'
import { IntakeWriterScopeSchema } from '@/gen/ai/protomolt/proto/authz/v1/account_bindings_pb'
import { coveringScope, scopeLabel, scopesIn } from './sessionWritersCore'

const files = create(IntakeWriterScopeSchema, { accountId: 'acct-a', datasourceId: 'ds-files', allowCreate: true })
const crawler = create(IntakeWriterScopeSchema, { accountId: 'acct-a', datasourceId: 'ds-web', connectorId: 'crawler' })
const other = create(IntakeWriterScopeSchema, { accountId: 'acct-b', datasourceId: 'ds-files', allowCreate: true })
const scopes = [files, crawler, other]

describe('coveringScope', () => {
  const target = (datasourceId: string, connectorId = '', creating = true, accountId = 'acct-a') => ({
    accountId,
    datasourceId,
    connectorId,
    creating,
  })

  it('applies the intake grant rule: account, datasource, connector and creation', () => {
    expect(coveringScope(scopes, target('ds-files'))).toBe(files)
    expect(coveringScope(scopes, target('ds-files', 'any-connector'))).toBe(files)
    expect(coveringScope(scopes, target('ds-web', 'crawler', false))).toBe(crawler)
    expect(coveringScope(scopes, target('ds-web', 'crawler', true))).toBeNull()
    expect(coveringScope(scopes, target('ds-web', 'other', false))).toBeNull()
    expect(coveringScope(scopes, target('ds-web', '', false))).toBeNull()
    expect(coveringScope(scopes, target('ds-files', '', true, 'acct-c'))).toBeNull()
    expect(coveringScope([], target('ds-files'))).toBeNull()
  })
})

describe('labels', () => {
  it('names the owner and whether the scope creates', () => {
    expect(scopeLabel(files)).toBe('ds-files (create and update)')
    expect(scopeLabel(crawler)).toBe('ds-web via crawler (update only)')
    expect(scopesIn(scopes, 'acct-b')).toEqual([other])
  })
})
