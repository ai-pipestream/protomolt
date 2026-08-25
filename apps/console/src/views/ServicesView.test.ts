// @vitest-environment jsdom
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
import ServicesView from './ServicesView.vue'
import * as workspace from '../services/services'
import { installDomStubs, routerForTests, vuetifyForTests } from '../componentTestKit'

vi.mock('../services/services', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../services/services')>()
  return {
    ...actual,
    listServices: vi.fn(),
    inspectService: vi.fn(),
    registerService: vi.fn(),
  }
})

const api = workspace as unknown as {
  listServices: ReturnType<typeof vi.fn>
  inspectService: ReturnType<typeof vi.fn>
  registerService: ReturnType<typeof vi.fn>
}

installDomStubs()

async function mountView(path = '/services') {
  const router = routerForTests()
  await router.push(path)
  await router.isReady()
  const wrapper = mount(ServicesView, {
    global: { plugins: [vuetifyForTests(), router] },
  })
  await flushPromises()
  return wrapper
}

beforeEach(() => {
  vi.clearAllMocks()
})

describe('ServicesView', () => {
  it('explains itself when nothing is registered', async () => {
    api.listServices.mockResolvedValue([])
    const wrapper = await mountView()
    expect(wrapper.text()).toContain('Nothing registered yet')
    expect(wrapper.text()).toContain('every method it declares')
  })

  it('shows a selected profile as methods with their verb names', async () => {
    api.listServices.mockResolvedValue([{ name: 'billing', endpoints: ['default'] }])
    api.inspectService.mockResolvedValue({
      services: [
        {
          name: 'shop.Billing',
          methods: [{
            name: 'Charge', fullName: 'shop.Billing/Charge',
            inputType: 'shop.ChargeRequest', outputType: 'shop.ChargeResponse',
            inputFields: [{ name: 'amount', type: 'int32' }],
          }],
        },
        // The reflection protocol's service is contract noise, not a workbench entry.
        { name: 'grpc.reflection.v1.ServerReflection', methods: [] },
      ],
    })
    const wrapper = await mountView('/services?profile=billing')
    expect(api.inspectService).toHaveBeenCalledWith('billing')
    expect(wrapper.text()).toContain('shop.Billing')
    expect(wrapper.text()).toContain('billing-charge')
    expect(wrapper.text()).toContain('ChargeRequest → ChargeResponse')
    expect(wrapper.text()).not.toContain('ServerReflection')
  })

  it('registers, then reloads and selects the new profile', async () => {
    api.listServices.mockResolvedValue([])
    api.registerService.mockResolvedValue({ ok: true })
    api.inspectService.mockResolvedValue({ services: [] })
    const wrapper = await mountView('/services?target=billing-host:9000')

    const nameField = wrapper.findAll('input[type="text"]')
      .find((i) => (i.element as HTMLInputElement).value === '')
    await nameField!.setValue('billing')
    // The target deep link prefilled the other field.
    api.listServices.mockResolvedValue([{ name: 'billing' }])
    await wrapper.findAll('button').find((b) => b.text().includes('Reflect and register'))!
      .trigger('click')
    await flushPromises()

    expect(api.registerService).toHaveBeenCalledWith('billing', 'billing-host:9000', false, '')
    expect(api.inspectService).toHaveBeenCalledWith('billing')
  })

  it('says why a registration was refused', async () => {
    api.listServices.mockResolvedValue([])
    api.registerService.mockResolvedValue({ ok: false, error: 'the endpoint did not answer' })
    const wrapper = await mountView('/services?target=nope:1')
    const nameField = wrapper.findAll('input[type="text"]')
      .find((i) => (i.element as HTMLInputElement).value === '')
    await nameField!.setValue('nope')
    await wrapper.findAll('button').find((b) => b.text().includes('Reflect and register'))!
      .trigger('click')
    await flushPromises()
    expect(wrapper.text()).toContain('the endpoint did not answer')
  })
})
