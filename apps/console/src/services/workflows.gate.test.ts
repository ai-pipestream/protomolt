import { describe, expect, it, vi } from 'vitest'
import {
  checkWorkflow,
  compileWorkflow,
  putWorkflow,
  type WorkflowFinding,
} from './workflows'

/** A fetch double answering with a JSON body; unwrap reads text(), never json(). */
function fetchJson(body: unknown, status = 200): typeof fetch {
  return vi.fn(async () => ({
    ok: status >= 200 && status < 300,
    status,
    text: async () => JSON.stringify(body),
  })) as never
}

describe('workflows save gate', () => {
  it('compileWorkflow posts the definition and returns the compiled workflow', async () => {
    const definition = { name: 'w', steps: [{ name: 'one' }] }
    const compiled = { inputType: 'a.In', steps: [] }
    const fetchFn = fetchJson({ workflow: compiled })
    expect(await compileWorkflow(definition, fetchFn)).toEqual(compiled)

    const calls = (fetchFn as ReturnType<typeof vi.fn>).mock.calls
    expect(calls[0][0]).toBe('/api/serve/grpc-json/ProtoMoltService/CompileWorkflow')
    const init = calls[0][1] as RequestInit
    expect(init.method).toBe('POST')
    expect(JSON.parse(init.body as string)).toEqual({ workflow: definition })
  })

  it('compileWorkflow refuses a reply without a workflow', async () => {
    await expect(compileWorkflow({}, fetchJson({ ok: true })))
      .rejects.toThrow('the compiler answered without a workflow')
  })

  it('putWorkflow surfaces gate findings from a 409 refusal', async () => {
    const findings: WorkflowFinding[] = [
      { step: 'embed', kind: 'rule', error: 'no such field' },
    ]
    const refused = fetchJson({ message: 'Workflow does not verify', findings }, 409)
    const thrown = await putWorkflow('bad', { name: 'bad' }, refused)
      .then(() => { throw new Error('the save gate did not refuse') })
      .catch((e: Error & { findings?: unknown[] }) => e)
    expect(thrown.message).toBe('Workflow does not verify')
    expect(thrown.findings).toEqual(findings)
  })

  it('checkWorkflow returns the {ok, findings} body as-is', async () => {
    const body = {
      ok: false,
      findings: [{ step: '', kind: 'workflow', error: 'no steps' }],
    }
    expect(await checkWorkflow({ name: 'x' }, fetchJson(body))).toEqual(body)
  })

  it('a non-JSON 502 surfaces as its HTTP status, not a parse error', async () => {
    const outage = vi.fn(async () => ({
      ok: false,
      status: 502,
      text: async () => '<html>Bad Gateway</html>',
    })) as never
    await expect(checkWorkflow({}, outage)).rejects.toThrow('HTTP 502')
  })
})
