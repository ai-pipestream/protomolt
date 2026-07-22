// @vitest-environment jsdom
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { flushPromises, mount, type VueWrapper } from '@vue/test-utils'
import CompatCheckPanel from './CompatCheckPanel.vue'
import { RegistryError, registryApi } from '../services/api'
import { installDomStubs, routerForTests, vuetifyForTests } from '../componentTestKit'

vi.mock('../services/api', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../services/api')>()
  return {
    ...actual,
    registryApi: {
      lookup: vi.fn(),
      register: vi.fn(),
      setSubjectConfig: vi.fn(),
    },
  }
})

const api = registryApi as unknown as {
  lookup: ReturnType<typeof vi.fn>
  register: ReturnType<typeof vi.fn>
  setSubjectConfig: ReturnType<typeof vi.fn>
}

installDomStubs()

const LATEST = 'syntax = "proto3";\npackage example;\nmessage Person { string name = 1; }\n'

async function mountPanel() {
  const router = routerForTests()
  await router.push('/')
  await router.isReady()
  const wrapper = mount(CompatCheckPanel, {
    props: {
      subject: 'example/person.proto',
      latestSchema: LATEST,
      latestReferences: [],
      effectiveMode: 'BACKWARD' as const,
      modeInherited: true,
    },
    global: { plugins: [vuetifyForTests(), router] },
  })
  await flushPromises()
  return wrapper
}

function buttonByText(wrapper: VueWrapper, text: string) {
  const button = wrapper.findAll('button').find((b) => b.text().includes(text))
  expect(button, `button "${text}"`).toBeTruthy()
  return button!
}

beforeEach(() => vi.clearAllMocks())

describe('CompatCheckPanel', () => {
  it('seeds the editor with the latest schema', async () => {
    const wrapper = await mountPanel()
    const editor = wrapper.find('textarea')
    expect((editor.element as HTMLTextAreaElement).value).toBe(LATEST)
  })

  it('reports unchanged when the content lookup hits', async () => {
    api.lookup.mockResolvedValue({
      subject: 'example/person.proto',
      id: 4,
      version: 2,
      schemaType: 'PROTOBUF',
      schema: LATEST,
      references: [],
    })
    const wrapper = await mountPanel()

    await buttonByText(wrapper, 'Check').trigger('click')
    await flushPromises()

    expect(api.lookup).toHaveBeenCalledWith('example/person.proto', LATEST, [])
    expect(wrapper.text()).toContain('Unchanged')
    expect(wrapper.text()).toContain('v2')
    expect(api.register).not.toHaveBeenCalled()
  })

  it('requires explicit confirmation before registering a new version', async () => {
    api.lookup.mockRejectedValue(new RegistryError(404, 40403, 'Schema not found'))
    const wrapper = await mountPanel()

    await buttonByText(wrapper, 'Check').trigger('click')
    await flushPromises()

    expect(wrapper.text()).toContain('WILL register')
    const registerButton = buttonByText(wrapper, 'Register now')
    expect(registerButton.attributes('disabled')).toBeDefined()

    await wrapper.find('input[type="checkbox"]').setValue(true)
    expect(buttonByText(wrapper, 'Register now').attributes('disabled')).toBeUndefined()
  })

  it('renders parsed 409 violations as rule/path/detail rows', async () => {
    api.lookup.mockRejectedValue(new RegistryError(404, 40403, 'Schema not found'))
    api.register.mockRejectedValue(
      new RegistryError(
        409,
        409,
        'Schema being registered is incompatible with an earlier schema: ' +
          'v1: FIELD_TYPE_CHANGED at example.Person.age: field 2 changed type from int32 to string',
      ),
    )
    const wrapper = await mountPanel()

    await buttonByText(wrapper, 'Check').trigger('click')
    await flushPromises()
    await wrapper.find('input[type="checkbox"]').setValue(true)
    await buttonByText(wrapper, 'Register now').trigger('click')
    await flushPromises()

    expect(wrapper.text()).toContain('Incompatible — 1 violation')
    expect(wrapper.text()).toContain('FIELD_TYPE_CHANGED')
    expect(wrapper.text()).toContain('example.Person.age')
    expect(wrapper.text()).toContain('field 2 changed type from int32 to string')
    expect(wrapper.text()).toContain('v1')
  })

  it('registers after confirmation and emits registered', async () => {
    api.lookup.mockRejectedValue(new RegistryError(404, 40401, "Subject 'x' not found."))
    api.register.mockResolvedValue({ id: 11 })
    const wrapper = await mountPanel()

    await buttonByText(wrapper, 'Check').trigger('click')
    await flushPromises()
    expect(wrapper.text()).toContain('New subject')

    await wrapper.find('input[type="checkbox"]').setValue(true)
    await buttonByText(wrapper, 'Register now').trigger('click')
    await flushPromises()

    expect(api.register).toHaveBeenCalledWith('example/person.proto', LATEST, [])
    expect(wrapper.emitted('registered')).toBeTruthy()
  })

  it('surfaces 422 invalid-schema outcomes without a register path', async () => {
    api.lookup.mockRejectedValue(new RegistryError(422, 42201, 'Invalid schema: empty schema'))
    const wrapper = await mountPanel()

    await wrapper.find('textarea').setValue('x')
    await buttonByText(wrapper, 'Check').trigger('click')
    await flushPromises()

    expect(wrapper.text()).toContain('Invalid schema')
    expect(wrapper.findAll('button').some((b) => b.text().includes('Register now'))).toBe(false)
  })

  it('sends cleaned references with the check', async () => {
    api.lookup.mockRejectedValue(new RegistryError(404, 40403, 'Schema not found'))
    const wrapper = await mountPanel()

    await buttonByText(wrapper, 'add').trigger('click')
    const inputs = wrapper.findAll('input[type="text"]')
    // name + subject fields of the new reference row (version is type=number)
    await inputs[0].setValue('core.proto')
    await inputs[1].setValue('core.proto')
    await buttonByText(wrapper, 'Check').trigger('click')
    await flushPromises()

    expect(api.lookup).toHaveBeenCalledWith('example/person.proto', LATEST, [
      { name: 'core.proto', subject: 'core.proto', version: 1 },
    ])
  })
})
