// @vitest-environment jsdom
import { describe, expect, it } from 'vitest'
import { mount } from '@vue/test-utils'
import WorkRecordCheckList from './WorkRecordCheckList.vue'
import WorkRecordNonClaims from './WorkRecordNonClaims.vue'
import { installDomStubs, vuetifyForTests } from '../componentTestKit'
import type { WorkRecordCheck } from '../services/receipts'

installDomStubs()

function checkList(checks: WorkRecordCheck[]) {
  return mount(WorkRecordCheckList, {
    props: { checks },
    global: { plugins: [vuetifyForTests()] },
  })
}

function nonClaims(claims: string[]) {
  return mount(WorkRecordNonClaims, {
    props: { claims },
    global: { plugins: [vuetifyForTests()] },
  })
}

describe('WorkRecordCheckList', () => {
  it('renders one row per check with its id and detail', () => {
    const wrapper = checkList([
      { id: 'signature', status: 'PASSED', detail: 'all envelopes verified' },
      { id: 'digest-chain', status: 'FAILED', detail: 'chunk 3 digest mismatch' },
    ])
    const rows = wrapper.findAll('li')
    expect(rows).toHaveLength(2)
    expect(rows[0].text()).toContain('signature')
    expect(rows[0].text()).toContain('all envelopes verified')
    expect(rows[1].text()).toContain('digest-chain')
    expect(rows[1].text()).toContain('chunk 3 digest mismatch')
  })

  it('marks status through the icon: success color, minus, or error color', () => {
    const wrapper = checkList([
      { id: 'a', status: 'PASSED', detail: 'ok' },
      { id: 'b', status: 'SKIPPED', detail: 'no workflow given' },
      { id: 'c', status: 'FAILED', detail: 'broken' },
    ])
    const passedIcon = wrapper.find('[aria-label="PASSED"]')
    expect(passedIcon.exists()).toBe(true)
    expect(passedIcon.classes()).toContain('text-success')
    expect(passedIcon.classes()).toContain('mdi-check-circle-outline')

    const skippedIcon = wrapper.find('[aria-label="SKIPPED"]')
    expect(skippedIcon.exists()).toBe(true)
    expect(skippedIcon.classes()).toContain('mdi-minus-circle-outline')
    expect(skippedIcon.classes()).not.toContain('text-success')
    expect(skippedIcon.classes()).not.toContain('text-error')

    const failedIcon = wrapper.find('[aria-label="FAILED"]')
    expect(failedIcon.exists()).toBe(true)
    expect(failedIcon.classes()).toContain('text-error')
    expect(failedIcon.classes()).toContain('mdi-close-circle-outline')
  })
})

describe('WorkRecordNonClaims', () => {
  it('renders nothing at all when there are no claims', () => {
    const wrapper = nonClaims([])
    expect(wrapper.html()).not.toContain('does not claim')
    expect(wrapper.findAll('li')).toHaveLength(0)
  })

  it('renders the header and each claim line when claims exist', () => {
    const wrapper = nonClaims([
      'that the workflow produced correct output',
      'that the model weights were unchanged',
    ])
    expect(wrapper.text()).toContain('What this record does not claim')
    const lines = wrapper.findAll('li').map((li) => li.text())
    expect(lines).toEqual([
      'that the workflow produced correct output',
      'that the model weights were unchanged',
    ])
  })
})
