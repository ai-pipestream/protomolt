// @vitest-environment jsdom
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
import SubjectsView from './SubjectsView.vue'
import { registryApi } from '../services/api'
import { installDomStubs, routerForTests, vuetifyForTests } from '../componentTestKit'

vi.mock('../services/api', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../services/api')>()
  return {
    ...actual,
    registryApi: {
      health: vi.fn(),
      globalConfig: vi.fn(),
      listSubjects: vi.fn(),
      listVersions: vi.fn(),
    },
  }
})

const api = registryApi as unknown as {
  health: ReturnType<typeof vi.fn>
  globalConfig: ReturnType<typeof vi.fn>
  listSubjects: ReturnType<typeof vi.fn>
  listVersions: ReturnType<typeof vi.fn>
}

installDomStubs()

async function mountView() {
  const router = routerForTests()
  await router.push('/schema-registry')
  await router.isReady()
  const wrapper = mount(SubjectsView, {
    global: { plugins: [vuetifyForTests(), router] },
  })
  await flushPromises()
  return wrapper
}

beforeEach(() => {
  vi.clearAllMocks()
  api.health.mockResolvedValue(true)
  api.globalConfig.mockResolvedValue('BACKWARD')
})

describe('SubjectsView', () => {
  it('lists subjects with version counts and latest version', async () => {
    api.listSubjects.mockResolvedValue(['example/person.proto', 'orders/order.proto'])
    api.listVersions.mockImplementation(async (subject: string) =>
      subject === 'example/person.proto' ? [1, 2, 3] : [1],
    )

    const wrapper = await mountView()

    expect(wrapper.text()).toContain('example/person.proto')
    expect(wrapper.text()).toContain('orders/order.proto')
    const rows = wrapper.findAll('tbody tr')
    expect(rows).toHaveLength(2)
    expect(rows[0].text()).toContain('3') // version count
    expect(rows[0].text()).toContain('v3') // latest
    // Subject links carry the percent-encoded subject as a single segment.
    const link = rows[0].find('a')
    expect(link.attributes('href')).toContain('/schema-registry/subjects/example%2Fperson.proto')
  })

  it('shows the registry health and global mode in the header', async () => {
    api.listSubjects.mockResolvedValue([])
    const wrapper = await mountView()
    expect(wrapper.text()).toContain('Registry up')
    expect(wrapper.text()).toContain('BACKWARD')
  })

  it('filters subjects by search text', async () => {
    api.listSubjects.mockResolvedValue(['example/person.proto', 'orders/order.proto'])
    api.listVersions.mockResolvedValue([1])
    const wrapper = await mountView()

    const search = wrapper.find('input[type="text"]')
    await search.setValue('orders')
    await flushPromises()

    expect(wrapper.text()).toContain('orders/order.proto')
    expect(wrapper.text()).not.toContain('example/person.proto')
  })

  it('shows a no-match empty state with a clear action', async () => {
    api.listSubjects.mockResolvedValue(['example/person.proto'])
    api.listVersions.mockResolvedValue([1])
    const wrapper = await mountView()

    await wrapper.find('input[type="text"]').setValue('zzz-no-such-subject')
    await flushPromises()

    expect(wrapper.text()).toContain('No subjects match')
    expect(wrapper.text()).toContain('Clear search')
  })

  it('shows an empty-registry state when there are no subjects', async () => {
    api.listSubjects.mockResolvedValue([])
    const wrapper = await mountView()
    expect(wrapper.text()).toContain('No subjects yet')
  })

  it('renders the error envelope message when the listing fails', async () => {
    api.listSubjects.mockRejectedValue(
      Object.assign(new Error('Error in the backend datastore: boom'), { errorCode: 50001 }),
    )
    const wrapper = await mountView()
    expect(wrapper.text()).toContain('Could not load subjects')
    expect(wrapper.text()).toContain('Error in the backend datastore: boom')
    expect(wrapper.text()).toContain('Retry')
  })
})
