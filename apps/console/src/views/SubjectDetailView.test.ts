// @vitest-environment jsdom
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
import { createMemoryHistory, createRouter } from 'vue-router'
import SubjectDetailView from './SubjectDetailView.vue'
import { RegistryError, registryApi } from '../services/api'
import { installDomStubs, vuetifyForTests } from '../componentTestKit'

vi.mock('../services/api', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../services/api')>()
  return {
    ...actual,
    registryApi: {
      listVersions: vi.fn(),
      getVersion: vi.fn(),
      subjectConfig: vi.fn(),
      globalConfig: vi.fn(),
      descriptorSet: vi.fn(),
    },
  }
})

const api = registryApi as unknown as Record<string, ReturnType<typeof vi.fn>>

installDomStubs()

const SCHEMA = 'syntax = "proto3";\npackage example;\n\nmessage Person {\n  string name = 1;\n}\n'

function envelope(version: number) {
  return {
    subject: 'example/person.proto',
    id: version,
    version,
    schemaType: 'PROTOBUF',
    schema: SCHEMA,
    references: [{ name: 'core.proto', subject: 'shared/core.proto', version: 1 }],
  }
}

async function mountDetail(subject = 'example/person.proto') {
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [
      { path: '/schema-registry', name: 'schema-registry-subjects', component: { template: '<div />' } },
      {
        path: '/schema-registry/subjects/:subject(.*)',
        name: 'schema-registry-subject',
        component: SubjectDetailView,
      },
    ],
  })
  await router.push(`/schema-registry/subjects/${encodeURIComponent(subject)}`)
  await router.isReady()
  const wrapper = mount(SubjectDetailView, {
    global: { plugins: [vuetifyForTests(), router] },
  })
  await flushPromises()
  return wrapper
}

beforeEach(() => {
  vi.clearAllMocks()
  api.listVersions.mockResolvedValue([1, 2])
  api.getVersion.mockImplementation(async (_s: string, v: number | 'latest') =>
    envelope(v === 'latest' ? 2 : v),
  )
  api.subjectConfig.mockResolvedValue(null)
  api.globalConfig.mockResolvedValue('BACKWARD')
})

describe('SubjectDetailView', () => {
  it('decodes the slash-bearing subject from the route and loads its versions', async () => {
    const wrapper = await mountDetail()
    expect(api.listVersions).toHaveBeenCalledWith('example/person.proto')
    expect(wrapper.text()).toContain('example/person.proto')
    // Version timeline, latest first.
    expect(wrapper.text()).toContain('v2')
    expect(wrapper.text()).toContain('latest')
  })

  it('shows the effective compatibility mode as inherited from global', async () => {
    const wrapper = await mountDetail()
    expect(wrapper.text()).toContain('BACKWARD')
    expect(wrapper.text()).toContain('(global)')
  })

  it('renders the highlighted schema with id chips and reference links', async () => {
    const wrapper = await mountDetail()
    expect(wrapper.text()).toContain('global id 2')
    expect(wrapper.html()).toContain('pshl-keyword')
    // Reference chip links to the referenced subject, percent-encoded.
    const referenceLink = wrapper
      .findAll('a')
      .find((a) => (a.attributes('href') ?? '').includes('shared%2Fcore.proto'))
    expect(referenceLink).toBeTruthy()
    expect(referenceLink!.text()).toContain('core.proto → shared/core.proto v1')
  })

  it('renders the 40401 envelope when the subject is unknown', async () => {
    api.listVersions.mockRejectedValue(new RegistryError(404, 40401, "Subject 'x' not found."))
    const wrapper = await mountDetail('x')
    expect(wrapper.text()).toContain('Could not load subject')
    expect(wrapper.text()).toContain("Subject 'x' not found.")
  })

  it('ignores stale responses from a previous subject when navigating between subjects', async () => {
    let resolveSlowVersions!: (versions: number[]) => void
    api.listVersions.mockImplementation((subj: string) => {
      if (subj === 'slow/subject.proto') {
        return new Promise<number[]>((resolve) => {
          resolveSlowVersions = resolve
        })
      }
      return Promise.resolve([10])
    })
    api.getVersion.mockImplementation(async (subj: string, v: number | 'latest') => ({
      subject: subj,
      id: v === 'latest' ? 99 : v,
      version: v === 'latest' ? 10 : v,
      schemaType: 'PROTOBUF',
      schema: `// ${subj} v${v}`,
      references: [],
    }))

    const router = createRouter({
      history: createMemoryHistory(),
      routes: [
        { path: '/schema-registry', name: 'schema-registry-subjects', component: { template: '<div />' } },
        {
          path: '/schema-registry/subjects/:subject(.*)',
          name: 'schema-registry-subject',
          component: SubjectDetailView,
        },
      ],
    })
    await router.push(`/schema-registry/subjects/${encodeURIComponent('slow/subject.proto')}`)
    await router.isReady()
    const wrapper = mount(SubjectDetailView, {
      global: { plugins: [vuetifyForTests(), router] },
    })
    await flushPromises()

    // Navigate to fast/subject.proto before slow/subject.proto resolves
    await router.push(`/schema-registry/subjects/${encodeURIComponent('fast/subject.proto')}`)
    await flushPromises()
    expect(wrapper.text()).toContain('fast/subject.proto')
    expect(wrapper.text()).toContain('v10')

    // Now resolve the stale slow/subject.proto request
    resolveSlowVersions([1, 2, 3])
    await flushPromises()
    expect(wrapper.text()).toContain('fast/subject.proto')
    expect(wrapper.text()).toContain('v10')
    expect(wrapper.text()).not.toContain('v3')
    wrapper.unmount()
  })
})
