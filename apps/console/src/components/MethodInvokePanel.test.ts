// @vitest-environment jsdom
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
import MethodInvokePanel from './MethodInvokePanel.vue'
import * as workspace from '../services/services'
import type { ReflectedMethod } from '../services/services'
import { installDomStubs, vuetifyForTests } from '../componentTestKit'

vi.mock('../services/services', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../services/services')>()
  return {
    ...actual,
    invokeMethod: vi.fn(),
  }
})

const api = workspace as unknown as {
  invokeMethod: ReturnType<typeof vi.fn>
}

installDomStubs()

const method: ReflectedMethod = {
  name: 'Score',
  fullName: 'demo.v1.Scorer/Score',
  inputType: 'demo.v1.ScoreRequest',
  outputType: 'demo.v1.ScoreReply',
  inputFields: [
    { name: 'query', type: 'string' },
    { name: 'note', type: 'string' },
    { name: 'strict', type: 'bool', cardinality: 'singular' },
    { name: 'labels', type: 'message', cardinality: 'map', typeName: 'demo.v1.ScoreRequest.LabelsEntry' },
  ],
}

function panel() {
  return mount(MethodInvokePanel, {
    props: { profile: 'demo', method },
    global: { plugins: [vuetifyForTests()] },
  })
}

function buttonByText(wrapper: ReturnType<typeof panel>, label: string) {
  const button = wrapper.findAll('button').find((b) => b.text() === label)
  expect(button, `button '${label}'`).toBeTruthy()
  return button!
}

beforeEach(() => {
  vi.clearAllMocks()
})

describe('MethodInvokePanel', () => {
  it('renders a text field for strings, a switch for bools, a textarea for maps', async () => {
    const wrapper = panel()
    // Two string fields edit as text, the bool as a switch, the map as a textarea.
    expect(wrapper.findAll('input[type="text"]')).toHaveLength(2)
    expect(wrapper.findAll('input[type="checkbox"]')).toHaveLength(1)
    expect(wrapper.findAll('textarea').length).toBeGreaterThanOrEqual(1)

    // The JSON tab's skeleton states the map as an object, not a list.
    await buttonByText(wrapper, 'JSON').trigger('click')
    const raw = (wrapper.find('textarea').element as HTMLTextAreaElement).value
    expect(JSON.parse(raw).labels).toEqual({})
    expect(raw).not.toContain('[]')
  })

  it('invokes with blanks and false bools omitted and the map parsed as JSON', async () => {
    api.invokeMethod.mockResolvedValue({ ok: true, status: 'OK', responses: [{}] })
    const wrapper = panel()

    await wrapper.findAll('input[type="text"]')[0].setValue('hello')
    // 'note' stays blank, the switch stays off.
    await wrapper.find('textarea').setValue('{"k": "v"}')
    await buttonByText(wrapper, 'Call').trigger('click')
    await flushPromises()

    expect(api.invokeMethod).toHaveBeenCalledWith('demo', 'demo.v1.Scorer/Score', {
      query: 'hello',
      labels: { k: 'v' },
    })
  })

  it('renders the description of a failed invocation', async () => {
    api.invokeMethod.mockResolvedValue({
      ok: false,
      status: 'INVALID_ARGUMENT',
      description: 'k must be positive',
    })
    const wrapper = panel()

    await buttonByText(wrapper, 'Call').trigger('click')
    await flushPromises()

    expect(wrapper.text()).toContain('INVALID_ARGUMENT')
    expect(wrapper.text()).toContain('k must be positive')
  })
})
