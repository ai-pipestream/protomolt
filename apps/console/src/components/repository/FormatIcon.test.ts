// @vitest-environment jsdom
import { mount } from '@vue/test-utils'
import { describe, expect, it } from 'vitest'
import { vuetifyForTests } from '@/componentTestKit'
import FormatIcon from './FormatIcon.vue'

function mountFormatIcon(props: InstanceType<typeof FormatIcon>['$props']) {
  return mount(FormatIcon, {
    props,
    global: {
      plugins: [vuetifyForTests()],
    },
  })
}

describe('FormatIcon', () => {
  it('marks icon decorative when paired with visible format text and renders accessible text chips', () => {
    const wrapper = mountFormatIcon({
      mediaType: 'application/vnd.apache.parquet',
      derived: true,
      stale: true,
      staleReason: 'Derived from r1; current source is r2.',
      showMediaType: true,
    })

    const root = wrapper.get('[data-testid="format-icon"]')
    expect(root.attributes('data-format-family')).toBe('parquet')
    expect(root.attributes('role')).toBeUndefined()
    expect(wrapper.find('.v-icon').attributes('aria-hidden')).toBe('true')
    expect(root.text()).toContain('Parquet')
    expect(root.text()).toContain('application/vnd.apache.parquet')
    expect(wrapper.get('[data-testid="format-badge-derived"]').text()).toBe('Derived')
    const staleChip = wrapper.get('[data-testid="format-badge-stale"]')
    expect(staleChip.text()).toBe('Stale')
    expect(staleChip.attributes('title')).toBe('Derived from r1; current source is r2.')
    wrapper.unmount()
  })

  it('provides role="img" and explicit aria-label in compact icon-only mode', () => {
    const wrapper = mountFormatIcon({
      mediaType: 'application/x-protobuf',
      authoritativeProtobuf: true,
      compact: true,
    })

    const root = wrapper.get('[data-testid="format-icon"]')
    expect(root.attributes('role')).toBe('img')
    expect(root.attributes('aria-label')).toContain('Protobuf')
    expect(root.attributes('aria-label')).toContain('authoritative protobuf')
    expect(wrapper.find('.v-icon').attributes('aria-hidden')).toBe('true')
    wrapper.unmount()
  })

  it('labels filename extension hints explicitly and escapes hostile mediaType strings as plain text', () => {
    const hinted = mountFormatIcon({
      mediaType: 'application/octet-stream',
      filename: 'table.parquet',
      showMediaType: true,
    })
    expect(hinted.get('[data-testid="format-extension-hint"]').text()).toBe('(filename hint)')
    hinted.unmount()

    const hostile = mountFormatIcon({
      mediaType: '<img src=x onerror=alert(1)>',
      showMediaType: true,
    })
    expect(hostile.find('img').exists()).toBe(false)
    expect(hostile.text()).toContain('Unknown format')
    expect(hostile.text()).toContain('<img src=x onerror=alert(1)>')
    hostile.unmount()
  })
})
