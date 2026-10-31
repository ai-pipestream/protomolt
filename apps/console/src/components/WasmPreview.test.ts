// @vitest-environment jsdom
import { mount, flushPromises } from '@vue/test-utils'
import { create, fromBinary } from '@bufbuild/protobuf'
import { AnySchema } from '@bufbuild/protobuf/wkt'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { installDomStubs, vuetifyForTests } from '@/componentTestKit'
import { runPreview } from '@/wasm/preview'
import WasmPreview from './WasmPreview.vue'

vi.mock('@/wasm/preview', () => ({ runPreview: vi.fn() }))
afterEach(() => vi.clearAllMocks())

const bytes = new Uint8Array([8, 127, 0xa0, 6, 1]) // includes an unknown field; must not be reserialized
const descriptorSet = new Uint8Array([10, 0])
function view(unavailable: string | null = null) {
  installDomStubs()
  return mount(WasmPreview, { props: { typeUrl: 'example/Preview', bytes, descriptorSet, unavailable }, global: { plugins: [vuetifyForTests()] } })
}
async function choose(wrapper: ReturnType<typeof view>) {
  const element = wrapper.get('input').element as HTMLInputElement
  Object.defineProperty(element, 'files', { configurable: true, value: [{ size: 8, arrayBuffer: async () => new Uint8Array([0, 97, 115, 109, 1, 0, 0, 0]).buffer }] })
  await wrapper.get('input').trigger('change')
  await flushPromises()
}

describe('WASM preview controls', () => {
  it('passes exact original bytes and descriptor artifact, and renders guest text without HTML', async () => {
    vi.mocked(runPreview).mockResolvedValue('<script>alert(1)</script>')
    const wrapper = view()
    await choose(wrapper)
    await wrapper.get('[data-testid="run-preview"]').trigger('click')
    await flushPromises()
    const input = vi.mocked(runPreview).mock.calls[0][0]
    expect(fromBinary(AnySchema, input.payload)).toEqual(create(AnySchema, { typeUrl: 'example/Preview', value: bytes }))
    expect(input.descriptorSet).toEqual(descriptorSet)
    expect(wrapper.get('[data-testid="preview-text"]').text()).toBe('<script>alert(1)</script>')
    expect(wrapper.find('script').exists()).toBe(false)
    wrapper.unmount()
  })
  it('does not offer execution for an opaque part', async () => {
    const wrapper = view('Custom previews are unavailable for parts containing opaque payloads.')
    expect(wrapper.find('input').exists()).toBe(false)
    expect(wrapper.text()).toContain('opaque')
    expect(runPreview).not.toHaveBeenCalled()
    wrapper.unmount()
  })
  it.each(['cancel', 'replace', 'unmount'])('aborts on %s and ignores a late answer', async (action) => {
    let resolve!: (text: string) => void
    vi.mocked(runPreview).mockImplementation(() => new Promise(done => { resolve = done }))
    const wrapper = view()
    await choose(wrapper)
    await wrapper.get('[data-testid="run-preview"]').trigger('click')
    const signal = vi.mocked(runPreview).mock.calls[0][1]
    if (action === 'cancel') await wrapper.get('[data-testid="cancel-preview"]').trigger('click')
    else if (action === 'replace') await wrapper.setProps({ bytes: new Uint8Array([8, 1]) })
    else wrapper.unmount()
    expect(signal.aborted).toBe(true)
    resolve('Old revision')
    await flushPromises()
    if (action !== 'unmount') {
      expect(wrapper.find('[data-testid="preview-text"]').exists()).toBe(false)
      wrapper.unmount()
    }
  })
  it('reports a module failure and permits another run', async () => {
    vi.mocked(runPreview).mockRejectedValueOnce(new Error('WASM trap')).mockResolvedValueOnce('Recovered')
    const wrapper = view()
    await choose(wrapper)
    await wrapper.get('[data-testid="run-preview"]').trigger('click')
    await flushPromises()
    expect(wrapper.get('[data-testid="preview-failure"]').text()).toContain('WASM trap')
    await wrapper.get('[data-testid="run-preview"]').trigger('click')
    await flushPromises()
    expect(wrapper.find('[data-testid="preview-failure"]').exists()).toBe(false)
    expect(wrapper.get('[data-testid="preview-text"]').text()).toBe('Recovered')
    wrapper.unmount()
  })
})
