// @vitest-environment jsdom
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
import SearchView from './SearchView.vue'
import * as searchApi from '../services/search'
import { installDomStubs, routerForTests, vuetifyForTests } from '../componentTestKit'

vi.mock('../services/search', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../services/search')>()
  return {
    ...actual,
    findSearchProfile: vi.fn(),
    listSubjects: vi.fn(),
    search: vi.fn(),
  }
})

const api = searchApi as unknown as {
  findSearchProfile: ReturnType<typeof vi.fn>
  listSubjects: ReturnType<typeof vi.fn>
  search: ReturnType<typeof vi.fn>
}

installDomStubs()

async function mountView() {
  const router = routerForTests()
  await router.push('/search')
  await router.isReady()
  const wrapper = mount(SearchView, {
    global: { plugins: [vuetifyForTests(), router] },
  })
  await flushPromises()
  return wrapper
}

beforeEach(() => {
  vi.clearAllMocks()
})

describe('SearchView', () => {
  it('says how to connect a search node when no profile has the contract', async () => {
    api.findSearchProfile.mockResolvedValue(null)
    const wrapper = await mountView()
    expect(wrapper.text()).toContain('No registered service exposes the search contract')
    expect(wrapper.text()).toContain('Register a service')
  })

  it('loads subjects and renders hits with their typed stored fields', async () => {
    api.findSearchProfile.mockResolvedValue('westcoast-node')
    api.listSubjects.mockResolvedValue([
      { subject: 'people', textFields: ['name'], hasVectorLane: false },
    ])
    api.search.mockResolvedValue([
      {
        docId: 'person-7',
        score: 1.25,
        stored: { name: { stringValue: 'Ada Lovelace' }, age: { int64Value: '36' } },
      },
    ])
    const wrapper = await mountView()
    expect(wrapper.text()).toContain('via westcoast-node')
    expect(wrapper.text()).toContain('no vector lane')

    const query = wrapper.findAll('input').find((i) =>
      i.attributes('type') !== 'number' && (i.element as HTMLInputElement).value === '')
    await query!.setValue('ada')
    await wrapper.findAll('button').find((b) => b.text() === 'Search')!.trigger('click')
    await flushPromises()

    expect(api.search).toHaveBeenCalledWith('westcoast-node', expect.objectContaining({
      mappingSubject: 'people', query: 'ada', lane: 'SEARCH_LANE_LEXICAL',
    }))
    const text = wrapper.text()
    expect(text).toContain('person-7')
    expect(text).toContain('Ada Lovelace')
    expect(text).toContain('36')
    expect(text).toContain('score 1.2500')
  })

  it('states an empty result instead of showing nothing', async () => {
    api.findSearchProfile.mockResolvedValue('node')
    api.listSubjects.mockResolvedValue([{ subject: 'people' }])
    api.search.mockResolvedValue([])
    const wrapper = await mountView()
    const query = wrapper.findAll('input').find((i) =>
      i.attributes('type') !== 'number' && (i.element as HTMLInputElement).value === '')
    await query!.setValue('nobody')
    await wrapper.findAll('button').find((b) => b.text() === 'Search')!.trigger('click')
    await flushPromises()
    expect(wrapper.text()).toContain('Nothing matched')
  })
})
