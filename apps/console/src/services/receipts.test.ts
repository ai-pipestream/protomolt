import { describe, expect, it, vi } from 'vitest'
import { evaluateRecord, exportRecord, passed, skipped, verifyRecord } from './receipts'

function answering(body: unknown, ok = true) {
  return vi.fn(async () => ({
    ok, status: ok ? 200 : 400, text: async () => JSON.stringify(body),
  })) as never
}

describe('the receipts client', () => {
  it('exports a record by run id through ExportWorkRecord', async () => {
    const fetchFn = answering({ recordBase64: 'AAAA', recordId: 'r-1' })
    expect(await exportRecord('run-7', fetchFn)).toEqual({ recordBase64: 'AAAA', recordId: 'r-1' })
    const calls = (fetchFn as ReturnType<typeof vi.fn>).mock.calls
    expect(calls[0][0]).toBe('/api/serve/grpc-json/ProtoMoltService/ExportWorkRecord')
    expect(JSON.parse(calls[0][1].body)).toEqual({ runId: 'run-7' })
  })

  it('normalizes a sparse verification to booleans and empty arrays', async () => {
    const fetchFn = answering({ manifestDigest: 'sha256:d' })
    expect(await verifyRecord('AAAA', fetchFn)).toEqual({
      verified: false, manifestDigest: 'sha256:d', checks: [], nonClaims: [],
    })
  })

  it('keeps a full verification intact, verified strictly boolean', async () => {
    const fetchFn = answering({
      verified: true,
      checks: [{ id: 'signature', status: 'PASSED' }],
      nonClaims: ['no wall-clock ordering'],
    })
    const result = await verifyRecord('AAAA', fetchFn)
    expect(result.verified).toBe(true)
    expect(result.checks).toEqual([{ id: 'signature', status: 'PASSED' }])
    expect(result.nonClaims).toEqual(['no wall-clock ordering'])
  })

  it('evaluates with the record, workflow, and schema all in the body', async () => {
    const fetchFn = answering({ accepted: true })
    const workflow = { steps: [{ name: 'parse' }] }
    const schema = { type: 'object' }
    const result = await evaluateRecord('AAAA', workflow, schema, fetchFn)
    const calls = (fetchFn as ReturnType<typeof vi.fn>).mock.calls
    expect(calls[0][0]).toBe('/api/serve/grpc-json/ProtoMoltService/EvaluateWorkRecord')
    expect(JSON.parse(calls[0][1].body)).toEqual({ recordBase64: 'AAAA', workflow, schema })
    expect(result).toEqual({
      accepted: true, manifestDigest: undefined, policyId: undefined,
      evaluatedAt: undefined, checks: [], replaySteps: [], nonClaims: [],
    })
  })

  it('normalizes absent replaySteps and nonClaims on an evaluation', async () => {
    const fetchFn = answering({
      accepted: false, policyId: 'p-1',
      checks: [{ id: 'replay', status: 'FAILED', detail: 'step out of order' }],
    })
    const result = await evaluateRecord('AAAA', {}, {}, fetchFn)
    expect(result.accepted).toBe(false)
    expect(result.policyId).toBe('p-1')
    expect(result.replaySteps).toEqual([])
    expect(result.nonClaims).toEqual([])
  })

  it('answers passed and skipped by exact status', () => {
    expect(passed({ id: 'a', status: 'PASSED' })).toBe(true)
    expect(passed({ id: 'a', status: 'SKIPPED' })).toBe(false)
    expect(skipped({ id: 'a', status: 'SKIPPED' })).toBe(true)
    expect(skipped({ id: 'a', status: 'FAILED' })).toBe(false)
  })

  it('surfaces the serve error body as the thrown message', async () => {
    const fetchFn = answering({ error: 'not-found', message: "no run 'run-9'" }, false)
    await expect(exportRecord('run-9', fetchFn)).rejects.toThrow("no run 'run-9'")
  })
})
