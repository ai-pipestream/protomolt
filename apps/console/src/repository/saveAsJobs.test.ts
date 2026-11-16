import { create } from '@bufbuild/protobuf'
import { Code, ConnectError } from '@connectrpc/connect'
import { describe, expect, it } from 'vitest'
import { EntryAddressSchema, RenditionDescriptorSchema } from '@/gen/ai/protomolt/proto/repo/archive/v1/entry_pb'
import {
  PublishRepositorySinkRequestSchema,
  PublishRepositorySinkResponseSchema,
  RepositorySinkJobSchema,
  RepositorySinkJobState,
  RepositorySinkService,
  SinkDestinationSchema,
} from '@/gen/ai/protomolt/proto/repo/sink/v1/repository_sink_pb'
import { routerClients } from './repositoryTestKit'
import { createSaveAsJobStore } from './saveAsJobs'

const request = (operationId: string) =>
  create(PublishRepositorySinkRequestSchema, {
    operationId,
    destination: create(SinkDestinationSchema, {
      address: create(EntryAddressSchema, { accountId: 'acct-a', archive: 'datasets', entryId: 'e-1' }),
      rendition: create(RenditionDescriptorSchema, { name: 'parquet' }),
    }),
  })

describe('Save-as job store', () => {
  it('ignores the abort of its own dispose and records no failure', async () => {
    let release = () => {}
    const gate = new Promise<void>((resolve) => {
      release = resolve
    })
    const client = routerClients((r) =>
      r.service(RepositorySinkService, {
        async publishRepositorySink(_req, context) {
          await Promise.race([
            gate,
            new Promise((_, reject) =>
              context.signal.addEventListener('abort', () => reject(new ConnectError('aborted', Code.Canceled))),
            ),
          ])
          return create(PublishRepositorySinkResponseSchema)
        },
      }),
    ).repositorySink
    const jobs = createSaveAsJobStore()
    const pending = jobs.publish(client, request('op-1'), true)
    jobs.dispose()
    release()
    await pending
    const entry = jobs.find('op-1')!
    expect(entry.error).toBeNull()
    expect(entry.summary).toBeNull()
    expect(entry.submitting).toBe(false)
  })

  it('re-publishes the frozen request under the same operation ID and keeps one entry', async () => {
    const seen: string[] = []
    let fail = true
    const client = routerClients((r) =>
      r.service(RepositorySinkService, {
        publishRepositorySink(req) {
          seen.push(req.operationId)
          if (fail) throw new ConnectError('lost', Code.Unavailable)
          return create(PublishRepositorySinkResponseSchema, {
            job: create(RepositorySinkJobSchema, {
              operationId: req.operationId,
              accountId: 'acct-a',
              state: RepositorySinkJobState.FAILED,
            }),
          })
        },
      }),
    ).repositorySink
    const jobs = createSaveAsJobStore()
    await jobs.publish(client, request('op-2'), true)
    expect(jobs.find('op-2')!.error?.kind).toBe('unsettled')
    fail = false
    await jobs.publish(client, jobs.find('op-2')!.request, true)
    expect(seen).toEqual(['op-2', 'op-2'])
    expect(jobs.items).toHaveLength(1)
    expect(jobs.find('op-2')!.error).toBeNull()
    expect(jobs.find('op-2')!.summary?.phase).toBe('failed')
    jobs.dispose()
  })
})
