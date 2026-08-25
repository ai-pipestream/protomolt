// @vitest-environment jsdom
import { describe, expect, it } from 'vitest'
import { mount } from '@vue/test-utils'
import WorkflowFindingsPanel from './WorkflowFindingsPanel.vue'
import { installDomStubs, vuetifyForTests } from '../componentTestKit'

installDomStubs()

function panel(findings: Array<{ step: string; kind: string; error: string }>,
    source: 'check' | 'save' = 'check') {
  return mount(WorkflowFindingsPanel, {
    props: { findings, source },
    global: { plugins: [vuetifyForTests()] },
  })
}

describe('WorkflowFindingsPanel', () => {
  it('groups findings by step with workflow-level ones first', () => {
    const wrapper = panel([
      { step: 'chunk', kind: 'method', error: 'no such method' },
      { step: '', kind: 'workflow', error: 'no steps' },
      { step: 'chunk', kind: 'output', error: 'unmapped output' },
    ])
    const headers = wrapper.findAll('.text-caption.font-weight-medium')
      .map((h) => h.text())
    expect(headers).toEqual(['The workflow itself', 'Step chunk'])
    // Both chunk findings render under the one step header.
    expect(wrapper.findAll('.finding')).toHaveLength(3)
  })

  it('says kinds in plain words, contract findings included', () => {
    const wrapper = panel([
      { step: 'embed', kind: 'contract', error: 'steps[1].method: required' },
      { step: 'embed', kind: 'celRule', error: 'does not compile' },
    ])
    const text = wrapper.text()
    expect(text).toContain('declared contract')
    expect(text).toContain('CEL rule')
    // An unknown kind still renders, as itself.
    expect(panel([{ step: '', kind: 'novel', error: 'x' }]).text()).toContain('novel')
  })

  it('states what a save-gate refusal means', () => {
    const wrapper = panel([{ step: '', kind: 'workflow', error: 'no steps' }], 'save')
    expect(wrapper.text()).toContain('Not stored')
    // A check headline instead frames the findings as work-to-do.
    expect(panel([{ step: '', kind: 'workflow', error: 'no steps' }]).text())
      .toContain('does not verify yet')
  })
})
