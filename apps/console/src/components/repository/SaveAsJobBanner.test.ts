// @vitest-environment jsdom
import { create } from '@bufbuild/protobuf'
import { Code, ConnectError } from '@connectrpc/connect'
import { flushPromises, mount } from '@vue/test-utils'
import { describe, expect, it } from 'vitest'
import { EntryAddressSchema } from '@/gen/ai/protomolt/proto/repo/archive/v1/entry_pb'
import {
  CancelRepositorySinkJobResponseSchema,
  PublishRepositorySinkRequestSchema,
  PublishRepositorySinkResponseSchema,
  RepositorySinkJobSchema,
  RepositorySinkJobState,
  RepositorySinkService,
  SinkDestinationSchema,
} from '@/gen/ai/protomolt/proto/repo/sink/v1/repository_sink_pb'
import { routerClients } from '@/repository/repositoryTestKit'
import { createSaveAsJobStore, SAVE_AS_JOBS } from '@/repository/saveAsJobs'
import { vuetifyForTests } from '@/componentTestKit'
import SaveAsJobBanner from './SaveAsJobBanner.vue'

describe('Save-as job banner', () => {
  it('lets a retained job be retried or cancelled from the banner after the dialog is gone', async () => {
    const published: string[] = []
    const cancelled: string[] = []
    let lost = true
    const client = routerClients((r) =>
      r.service(RepositorySinkService, {
        publishRepositorySink(req) {
          published.push(req.operationId)
          if (lost) throw new ConnectError('lost', Code.Unavailable)
          return create(PublishRepositorySinkResponseSchema, {
            job: create(RepositorySinkJobSchema, {
              operationId: req.operationId,
              accountId: 'acct-a',
              state: RepositorySinkJobState.RUNNING,
            }),
          })
        },
        cancelRepositorySinkJob(req) {
          cancelled.push(req.operationId)
          return create(CancelRepositorySinkJobResponseSchema, {
            job: create(RepositorySinkJobSchema, {
              operationId: req.operationId,
              accountId: 'acct-a',
              state: RepositorySinkJobState.CANCELLED,
            }),
          })
        },
      }),
    ).repositorySink
    const jobs = createSaveAsJobStore()
    await jobs.publish(
      client,
      create(PublishRepositorySinkRequestSchema, {
        operationId: 'op-retained',
        destination: create(SinkDestinationSchema, {
          address: create(EntryAddressSchema, { accountId: 'acct-a', archive: 'datasets', entryId: 'e-1' }),
        }),
      }),
      false,
    )
    const banner = mount(SaveAsJobBanner, {
      global: { plugins: [vuetifyForTests()], provide: { [SAVE_AS_JOBS as symbol]: jobs } },
      attachTo: document.body,
    })
    // The publish reply was lost: the same request can be sent again from here.
    expect(banner.find('[data-testid="save-as-job-banner-cancel"]').exists()).toBe(false)
    lost = false
    await banner.get('[data-testid="save-as-job-banner-retry"]').trigger('click')
    await flushPromises()
    expect(published).toEqual(['op-retained', 'op-retained'])
    expect(banner.get('[data-testid="save-as-job-banner"]').attributes('data-job-phase')).toBe('running')

    // The job is running: it can be cancelled from here, and retry is no longer offered.
    expect(banner.find('[data-testid="save-as-job-banner-retry"]').exists()).toBe(false)
    await banner.get('[data-testid="save-as-job-banner-cancel"]').trigger('click')
    await flushPromises()
    expect(cancelled).toEqual(['op-retained'])
    expect(banner.get('[data-testid="save-as-job-banner"]').attributes('data-job-phase')).toBe('cancelled')
    banner.unmount()
    jobs.dispose()
  })

  it('shows a failed cancel or status call beside the last known job state', async () => {
    const client = routerClients((r) =>
      r.service(RepositorySinkService, {
        publishRepositorySink(req) {
          return create(PublishRepositorySinkResponseSchema, {
            job: create(RepositorySinkJobSchema, {
              operationId: req.operationId,
              accountId: 'acct-a',
              state: RepositorySinkJobState.RUNNING,
            }),
          })
        },
        cancelRepositorySinkJob() {
          throw new ConnectError('not allowed to cancel', Code.PermissionDenied)
        },
      }),
    ).repositorySink
    const jobs = createSaveAsJobStore()
    await jobs.publish(
      client,
      create(PublishRepositorySinkRequestSchema, {
        operationId: 'op-denied',
        destination: create(SinkDestinationSchema, {
          address: create(EntryAddressSchema, { accountId: 'acct-a', archive: 'datasets', entryId: 'e-1' }),
        }),
      }),
      false,
    )
    const banner = mount(SaveAsJobBanner, {
      global: { plugins: [vuetifyForTests()], provide: { [SAVE_AS_JOBS as symbol]: jobs } },
      attachTo: document.body,
    })
    await banner.get('[data-testid="save-as-job-banner-cancel"]').trigger('click')
    await flushPromises()
    const shown = banner.get('[data-testid="save-as-job-banner"]')
    expect(shown.attributes('data-job-phase')).toBe('running')
    const failure = banner.get('[data-testid="save-as-job-banner-error"]')
    expect(failure.text()).toContain('PermissionDenied')
    expect(failure.text()).toContain('not allowed to cancel')
    banner.unmount()
    jobs.dispose()
  })
})
