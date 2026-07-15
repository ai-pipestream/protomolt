import { describe, expect, it, vi } from 'vitest'
import {
  chainSummary,
  checkChain,
  listChains,
  parseDefinition,
  putChain,
  runChain,
  type ChainFinding,
} from './chains'

function fetchOk(body: unknown): typeof fetch {
  return vi.fn(async () => new Response(JSON.stringify(body), { status: 200 })) as never
}

describe('chains service', () => {
  it('lists, checks, and runs through the two bridges', async () => {
    const list = fetchOk(['compile-and-list'])
    expect(await listChains(list)).toEqual(['compile-and-list'])
    expect((list as ReturnType<typeof vi.fn>).mock.calls[0][0])
      .toBe('/api/protomolt/protomolt/chains')

    const check = fetchOk({ ok: true, findings: [] })
    expect((await checkChain({ name: 'x' }, check)).ok).toBe(true)
    expect((check as ReturnType<typeof vi.fn>).mock.calls[0][0])
      .toBe('/api/serve/grpc-json/ProtoMoltService/CheckChain')

    const run = fetchOk({ ok: true, outputType: 't.T', steps: [] })
    const result = await runChain('compile-and-list', { a: 1 }, run)
    expect(result.outputType).toBe('t.T')
    const runBody = JSON.parse(
      ((run as ReturnType<typeof vi.fn>).mock.calls[0][1] as RequestInit).body as string)
    expect(runBody).toEqual({ chainName: 'compile-and-list', input: { a: 1 } })
  })

  it('surfaces gate findings from a rejected save', async () => {
    const findings: ChainFinding[] = [{ step: 'embed', kind: 'rule', error: 'no field' }]
    const rejected = vi.fn(async () => new Response(
      JSON.stringify({ message: 'Chain does not verify', findings }),
      { status: 422 })) as never
    await expect(putChain('bad', {}, rejected)).rejects.toMatchObject({
      message: 'Chain does not verify',
      findings,
    })
  })

  it('summarizes and parses definitions defensively', () => {
    expect(chainSummary({
      inputType: 'a.In',
      steps: [{ name: 'one' }, { name: 'two' }],
    })).toBe('a.In → one → two')
    expect(() => parseDefinition('{oops')).toThrow(/Not valid JSON/)
    expect(() => parseDefinition('[1]')).toThrow(/JSON object/)
    expect(parseDefinition('{"name": "x"}')).toEqual({ name: 'x' })
  })
})
