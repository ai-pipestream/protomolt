// @vitest-environment jsdom
import { flushPromises, mount } from '@vue/test-utils'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { defineComponent, h, onMounted } from 'vue'
import { createMemoryHistory, createRouter } from 'vue-router'
import { create, fromBinary, toBinary } from '@bufbuild/protobuf'
import { ListDrivesResponseSchema, DriveSchema } from '@/gen/ai/protomolt/proto/repo/v1/drive_pb'
import {
  ArchiveMutationKind,
  ArchiveMutationReceiptSchema,
  ArchiveMutationRequestSchema,
  ArchiveMutationState,
} from '@/gen/ai/protomolt/proto/repo/archive/v1/archive_mutation_pb'
import { ArchiveMutationResponseSchema } from '@/gen/ai/protomolt/proto/repo/archive/v1/archive_mutation_service_pb'
import { ArchiveSchema, ArchiveStatsSchema } from '@/gen/ai/protomolt/proto/repo/archive/v1/archive_pb'
import {
  GetArchiveResponseSchema,
  GetArchiveStatsResponseSchema,
  GetEntryManifestResponseSchema,
  ListEntriesResponseSchema,
  ListVersionsResponseSchema,
} from '@/gen/ai/protomolt/proto/repo/archive/v1/archive_service_pb'
import { EntryAddressSchema, EntryInfoSchema, VersionManifestSchema } from '@/gen/ai/protomolt/proto/repo/archive/v1/entry_pb'
import { installDomStubs, vuetifyForTests } from '@/componentTestKit'
import { archiveDeleteDigest } from '@/repository/archiveDeleteCore'
import { settle } from '@/repository/repositoryViewKit'
import RepositoryArchiveEntryView from '@/views/repository/RepositoryArchiveEntryView.vue'
import RepositoryArchiveView from '@/views/repository/RepositoryArchiveView.vue'
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

  it('retains and displays an arrived archive delete receipt across RepositoryShell route navigation (#871)', async () => {
    installDomStubs()
    const addr = create(EntryAddressSchema, { accountId: 'acct-a', archive: 'papers', entryId: 'e-1' })
    const stub = { template: '<div />' }
    const router = createRouter({
      history: createMemoryHistory(),
      routes: [
        {
          path: '/repository',
          component: RepositoryShell,
          children: [
            { path: '', name: 'repository', component: stub },
            { path: 'accounts/:accountId', name: 'repository-account', component: stub },
            { path: 'document', name: 'repository-document', component: stub },
            { path: 'upload', name: 'repository-upload', component: stub },
            { path: 'archive', name: 'repository-archive', component: RepositoryArchiveView },
            { path: 'archive-entry', name: 'repository-archive-entry', component: RepositoryArchiveEntryView },
            { path: 'lookup', name: 'repository-lookup', component: stub },
          ],
        },
      ],
    })
    let recordedOp = ''
    let recordedDigest = ''
    vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input)
      const protoResponse = (bytes: Uint8Array) =>
        new Response(bytes as Uint8Array<ArrayBuffer>, { status: 200, headers: { 'content-type': 'application/proto' } })
      if (url.endsWith('/GetEntryManifest')) {
        return protoResponse(
          toBinary(
            GetEntryManifestResponseSchema,
            create(GetEntryManifestResponseSchema, {
              info: create(EntryInfoSchema, { address: addr, currentVersion: 3n, title: 'Paper 1' }),
              manifest: create(VersionManifestSchema, { address: addr, version: 3n }),
            }),
          ),
        )
      }
      if (url.endsWith('/ListVersions')) {
        return protoResponse(toBinary(ListVersionsResponseSchema, create(ListVersionsResponseSchema)))
      }
      if (url.endsWith('/GetArchive')) {
        return protoResponse(
          toBinary(GetArchiveResponseSchema, create(GetArchiveResponseSchema, { archive: create(ArchiveSchema, { name: 'papers', accountId: 'acct-a' }) })),
        )
      }
      if (url.endsWith('/GetArchiveStats')) {
        return protoResponse(
          toBinary(GetArchiveStatsResponseSchema, create(GetArchiveStatsResponseSchema, { stats: create(ArchiveStatsSchema, { archive: 'papers' }) })),
        )
      }
      if (url.endsWith('/ListEntries')) {
        return protoResponse(toBinary(ListEntriesResponseSchema, create(ListEntriesResponseSchema)))
      }
      if (url.endsWith('/ArchiveMutation')) {
        const bodyBytes = init?.body instanceof Uint8Array ? init.body : new Uint8Array(await new Response(init?.body).arrayBuffer())
        const req = fromBinary(ArchiveMutationRequestSchema, bodyBytes)
        recordedOp = req.operationId
        recordedDigest = archiveDeleteDigest(req)
        const res = protoResponse(
          toBinary(
            ArchiveMutationResponseSchema,
            create(ArchiveMutationResponseSchema, {
              receipt: create(ArchiveMutationReceiptSchema, {
                operationId: req.operationId,
                address: addr,
                kind: ArchiveMutationKind.DELETE_ENTRY,
                state: ArchiveMutationState.COMPLETED,
                entryDeleted: true,
                commandSha256: recordedDigest,
              }),
            }),
          ),
        )
        void router.push({ name: 'repository-archive', query: { account: 'acct-a', archive: 'papers' } })
        return res
      }
      throw new Error(`Unexpected RPC URL: ${url}`)
    }))

    await router.push({ name: 'repository-archive-entry', query: { account: 'acct-a', archive: 'papers', entry: 'e-1' } })
    await router.isReady()
    const wrapper = mount({ template: '<router-view />' }, {
      global: { plugins: [vuetifyForTests(), router] },
      attachTo: document.body,
    })
    try {
      await settle()
      await wrapper.get('[data-testid="delete-archive-entry"]').trigger('click')
      await settle()
      ;(document.body.querySelector('[data-testid="delete-archive-confirm"]') as HTMLElement).click()
      await settle()

      expect(router.currentRoute.value.name).toBe('repository-archive')
      const banners = wrapper.findAll('[data-testid="archive-delete-receipt-banner"]')
      expect(banners).toHaveLength(1)
      expect(banners[0].text()).toContain('acct-a / papers / e-1')
      expect(banners[0].text()).toContain(recordedOp)
      expect(banners[0].text()).toContain('COMPLETED')
      expect(banners[0].text()).toContain(recordedDigest)
    } finally {
      wrapper.unmount()
      document.body.innerHTML = ''
    }
  })
})
