import { describe, expect, it, vi } from 'vitest'
import {
  workflowSummary,
  checkWorkflow,
  listWorkflows,
  parseDefinition,
  putWorkflow,
  runWorkflow,
  type WorkflowFinding,
} from './workflows'

function fetchOk(body: unknown): typeof fetch {
  return vi.fn(async () => new Response(JSON.stringify(body), { status: 200 })) as never
}

describe('workflows service', () => {
  it('lists, checks, and runs through the two bridges', async () => {
    const list = fetchOk(['compile-and-list'])
    expect(await listWorkflows(list)).toEqual(['compile-and-list'])
    expect((list as ReturnType<typeof vi.fn>).mock.calls[0][0])
      .toBe('/api/protomolt/protomolt/workflows')

    const check = fetchOk({ ok: true, findings: [] })
    expect((await checkWorkflow({ name: 'x' }, check)).ok).toBe(true)
    expect((check as ReturnType<typeof vi.fn>).mock.calls[0][0])
      .toBe('/api/serve/grpc-json/ProtoMoltService/CheckWorkflow')

    const run = fetchOk({ ok: true, outputType: 't.T', steps: [] })
    const result = await runWorkflow('compile-and-list', { a: 1 }, run)
    expect(result.outputType).toBe('t.T')
    const runBody = JSON.parse(
      ((run as ReturnType<typeof vi.fn>).mock.calls[0][1] as RequestInit).body as string)
    expect(runBody).toEqual({ workflowName: 'compile-and-list', input: { a: 1 } })
  })

  it('surfaces gate findings from a rejected save', async () => {
    const findings: WorkflowFinding[] = [{ step: 'embed', kind: 'rule', error: 'no field' }]
    const rejected = vi.fn(async () => new Response(
      JSON.stringify({ message: 'Workflow does not verify', findings }),
      { status: 422 })) as never
    await expect(putWorkflow('bad', {}, rejected)).rejects.toMatchObject({
      message: 'Workflow does not verify',
      findings,
    })
  })

  it('summarizes and parses definitions defensively', () => {
    expect(workflowSummary({
      inputType: 'a.In',
      steps: [{ name: 'one' }, { name: 'two' }],
    })).toBe('a.In → one → two')
    expect(() => parseDefinition('{oops')).toThrow(/Not valid JSON/)
    expect(() => parseDefinition('[1]')).toThrow(/JSON object/)
    expect(parseDefinition('{"name": "x"}')).toEqual({ name: 'x' })
  })
})
