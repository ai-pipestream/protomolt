// @vitest-environment jsdom
import { flushPromises, mount } from '@vue/test-utils'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { defineComponent, h, onMounted } from 'vue'
import { createMemoryHistory, createRouter } from 'vue-router'
import { create, toBinary } from '@bufbuild/protobuf'
import { ListDrivesResponseSchema, DriveSchema } from '@/gen/ai/protomolt/proto/repo/v1/drive_pb'
import { installDomStubs } from '@/componentTestKit'
import RepositoryShell from '@/views/repository/RepositoryShell.vue'
import { useRepositoryClients } from '@/repository/clients'

/** A child screen that makes one call through the shell's clients. */
function probe(result: { names?: string[]; error?: unknown }) {
  return defineComponent({
    setup() {
      const clients = useRepositoryClients()
      onMounted(async () => {
        try {
          const page = await clients.drives.listDrives({ accountId: 'acct-a' })
          result.names = page.drives.map((d) => d.name)
        } catch (error) {
          result.error = error
        }
      })
      return () => h('div')
    },
  })
}

async function mountShell(child: ReturnType<typeof probe>) {
  installDomStubs()
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [{ path: '/repository', component: RepositoryShell, children: [{ path: '', component: child }] }],
  })
  await router.push('/repository')
  await router.isReady()
  const wrapper = mount({ template: '<router-view />' }, { global: { plugins: [router] } })
  for (let i = 0; i < 5; i++) await flushPromises()
  return wrapper
}

afterEach(() => vi.unstubAllGlobals())

describe('RepositoryShell', () => {
  it('calls the gateway same-origin under /connect with binary protobuf and the session cookie', async () => {
    const fetch = vi.fn(async () =>
      new Response(toBinary(ListDrivesResponseSchema, create(ListDrivesResponseSchema, { drives: [create(DriveSchema, { name: 'intake' })] })), {
        status: 200,
        headers: { 'content-type': 'application/proto' },
      }),
    )
    vi.stubGlobal('fetch', fetch)
    const result: { names?: string[]; error?: unknown } = {}
    const wrapper = await mountShell(probe(result))
    expect(result.names).toEqual(['intake'])
    const [url, init] = fetch.mock.calls[0] as unknown as [string, RequestInit]
    expect(url).toBe('/connect/ai.protomolt.proto.repo.v1.DriveService/ListDrives')
    expect(new Headers(init.headers).get('content-type')).toBe('application/proto')
    expect(init.credentials).toBe('same-origin')
    wrapper.unmount()
  })

  it('starts sign-in, returning to the open page, when the gateway answers UNAUTHENTICATED', async () => {
    // Connect's unary error envelope, as the gateway writes it for HTTP 401.
    vi.stubGlobal('fetch', vi.fn(async () =>
      new Response(new TextEncoder().encode('{"code":"unauthenticated","message":"session expired"}'), {
        status: 401,
        headers: { 'content-type': 'application/json' },
      }),
    ))
    const assign = vi.fn()
    vi.stubGlobal('location', { ...window.location, pathname: '/console/repository/accounts/acct-a', search: '?x=1', assign })
    const result: { names?: string[]; error?: unknown } = {}
    const wrapper = await mountShell(probe(result))
    expect(assign).toHaveBeenCalledWith('/console/sign-in?return=%2Fconsole%2Frepository%2Faccounts%2Facct-a%3Fx%3D1')
    expect(result.error).toBeDefined()
    wrapper.unmount()
  })
})
