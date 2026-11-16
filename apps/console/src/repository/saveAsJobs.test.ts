import { create } from '@bufbuild/protobuf'
import { Code, ConnectError } from '@connectrpc/connect'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { EntryAddressSchema, RenditionDescriptorSchema } from '@/gen/ai/protomolt/proto/repo/archive/v1/entry_pb'
import {
  GetRepositorySinkJobResponseSchema,
  PublishRepositorySinkRequestSchema,
  PublishRepositorySinkResponseSchema,
  RepositorySinkJobSchema,
  RepositorySinkJobState,
  RepositorySinkService,
  SinkDestinationSchema,
} from '@/gen/ai/protomolt/proto/repo/sink/v1/repository_sink_pb'
import { routerClients } from './repositoryTestKit'
import { createSaveAsJobStore, SaveAsJobCapacityError } from './saveAsJobs'

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

  const job = (operationId: string, state: RepositorySinkJobState) =>
    create(RepositorySinkJobSchema, { operationId, accountId: 'acct-a', state })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('ignores a status response that started before a newer call and finishes after it', async () => {
    let releaseGet = () => {}
    const getGate = new Promise<void>((resolve) => {
      releaseGet = resolve
    })
    let publishes = 0
    const client = routerClients((r) =>
      r.service(RepositorySinkService, {
        publishRepositorySink(req) {
          publishes += 1
          if (publishes === 1) throw new ConnectError('lost', Code.Unavailable)
          return create(PublishRepositorySinkResponseSchema, {
            job: job(req.operationId, RepositorySinkJobState.SUCCEEDED),
          })
        },
        async getRepositorySinkJob(req) {
          await getGate
          return create(GetRepositorySinkJobResponseSchema, {
            job: job(req.operationId, RepositorySinkJobState.UNRESOLVED),
          })
        },
      }),
    ).repositorySink
    const jobs = createSaveAsJobStore()
    await jobs.publish(client, request('op-race'), true)
    const staleRefresh = jobs.refresh('op-race')
    await jobs.publish(client, jobs.find('op-race')!.request, true)
    expect(jobs.find('op-race')!.summary?.phase).toBe('succeeded')
    releaseGet()
    await staleRefresh
    expect(jobs.find('op-race')!.summary?.phase).toBe('succeeded')
    expect(jobs.find('op-race')!.updatedBy).toBe('publish')
    expect(jobs.find('op-race')!.error).toBeNull()
    jobs.dispose()
  })

  it('keeps polling an active job after a status call fails', async () => {
    vi.useFakeTimers()
    let gets = 0
    const client = routerClients((r) =>
      r.service(RepositorySinkService, {
        publishRepositorySink(req) {
          return create(PublishRepositorySinkResponseSchema, {
            job: job(req.operationId, RepositorySinkJobState.RUNNING),
          })
        },
        getRepositorySinkJob(req) {
          gets += 1
          if (gets === 1) throw new ConnectError('blip', Code.Unavailable)
          return create(GetRepositorySinkJobResponseSchema, {
            job: job(req.operationId, RepositorySinkJobState.SUCCEEDED),
          })
        },
      }),
    ).repositorySink
    const jobs = createSaveAsJobStore()
    await jobs.publish(client, request('op-poll'), true)
    expect(jobs.find('op-poll')!.summary?.phase).toBe('running')
    await vi.advanceTimersByTimeAsync(600)
    expect(gets).toBe(1)
    expect(jobs.find('op-poll')!.error?.kind).toBe('unsettled')
    await vi.advanceTimersByTimeAsync(60_000)
    expect(gets).toBe(2)
    expect(jobs.find('op-poll')!.summary?.phase).toBe('succeeded')
    expect(jobs.find('op-poll')!.error).toBeNull()
    jobs.dispose()
  })

  it.each([
    ['failed', RepositorySinkJobState.FAILED],
    ['cancelled', RepositorySinkJobState.CANCELLED],
  ] as const)('clears a lost-response error once status confirms the job %s', async (phase, state) => {
    const client = routerClients((r) =>
      r.service(RepositorySinkService, {
        publishRepositorySink() {
          throw new ConnectError('lost', Code.Unavailable)
        },
        getRepositorySinkJob(req) {
          return create(GetRepositorySinkJobResponseSchema, { job: job(req.operationId, state) })
        },
      }),
    ).repositorySink
    const jobs = createSaveAsJobStore()
    await jobs.publish(client, request('op-lost'), true)
    expect(jobs.find('op-lost')!.error?.kind).toBe('unsettled')
    await jobs.refresh('op-lost')
    expect(jobs.find('op-lost')!.summary?.phase).toBe(phase)
    expect(jobs.find('op-lost')!.error).toBeNull()
    jobs.dispose()
  })

  it('evicts only a confirmed terminal job and refuses a new one when none can go', async () => {
    let outcome: 'lost' | 'failed' = 'lost'
    const client = routerClients((r) =>
      r.service(RepositorySinkService, {
        publishRepositorySink(req) {
          if (outcome === 'lost') throw new ConnectError('lost', Code.Unavailable)
          return create(PublishRepositorySinkResponseSchema, {
            job: job(req.operationId, RepositorySinkJobState.FAILED),
          })
        },
      }),
    ).repositorySink
    const jobs = createSaveAsJobStore()
    // Twenty operations whose outcome is unknown: none may be forgotten.
    for (let i = 0; i < 20; i++) {
      await jobs.publish(client, request(`op-${i}`), false)
    }
    await expect(jobs.publish(client, request('op-extra'), false)).rejects.toBeInstanceOf(
      SaveAsJobCapacityError,
    )
    expect(jobs.items).toHaveLength(20)
    expect(jobs.find('op-extra')).toBeUndefined()
    expect(jobs.find('op-0')).toBeDefined()

    // Once one of them is confirmed terminal it makes room.
    outcome = 'failed'
    await jobs.publish(client, jobs.find('op-7')!.request, false)
    expect(jobs.find('op-7')!.summary?.phase).toBe('failed')
    await jobs.publish(client, request('op-extra'), false)
    expect(jobs.find('op-7')).toBeUndefined()
    expect(jobs.find('op-extra')).toBeDefined()
    expect(jobs.items).toHaveLength(20)
    jobs.dispose()
  })

  it('keeps a publication uncertain when the status lookup that follows cannot find the job', async () => {
    const client = routerClients((r) =>
      r.service(RepositorySinkService, {
        publishRepositorySink() {
          throw new ConnectError('lost', Code.Unavailable)
        },
        getRepositorySinkJob() {
          // Admission may still be pending, so "not found" settles nothing.
          throw new ConnectError('no such job', Code.NotFound)
        },
      }),
    ).repositorySink
    const jobs = createSaveAsJobStore()
    await jobs.publish(client, request('op-pending'), true)
    await jobs.refresh('op-pending')
    const entry = jobs.find('op-pending')!
    expect(entry.summary).toBeNull()
    expect(entry.error?.kind).toBe('unsettled')
    expect(entry.error?.canReconcile).toBe(true)
    jobs.dispose()
  })

  it('applies a publish response that arrives after a later status lookup failed', async () => {
    vi.useFakeTimers()
    let releasePublish = () => {}
    const publishGate = new Promise<void>((resolve) => {
      releasePublish = resolve
    })
    let gets = 0
    const client = routerClients((r) =>
      r.service(RepositorySinkService, {
        async publishRepositorySink(req) {
          await publishGate
          return create(PublishRepositorySinkResponseSchema, {
            job: job(req.operationId, RepositorySinkJobState.QUEUED),
          })
        },
        getRepositorySinkJob(req) {
          gets += 1
          if (gets === 1) throw new ConnectError('no such job', Code.NotFound)
          return create(GetRepositorySinkJobResponseSchema, {
            job: job(req.operationId, RepositorySinkJobState.SUCCEEDED),
          })
        },
      }),
    ).repositorySink
    const jobs = createSaveAsJobStore()
    const publishing = jobs.publish(client, request('op-early'), false)
    await jobs.refresh('op-early')
    expect(jobs.find('op-early')!.error).toBeNull()
    releasePublish()
    await publishing
    expect(jobs.find('op-early')!.summary?.phase).toBe('queued')
    // The accepted job is polled to its end.
    await vi.advanceTimersByTimeAsync(6000)
    expect(jobs.find('op-early')!.summary?.phase).toBe('succeeded')
    jobs.dispose()
  })

  it('accepts a terminal outcome from an older call over a newer call that saw the job running', async () => {
    let releasePublish = () => {}
    const publishGate = new Promise<void>((resolve) => {
      releasePublish = resolve
    })
    const client = routerClients((r) =>
      r.service(RepositorySinkService, {
        async publishRepositorySink(req) {
          await publishGate
          return create(PublishRepositorySinkResponseSchema, {
            job: job(req.operationId, RepositorySinkJobState.SUCCEEDED),
          })
        },
        getRepositorySinkJob(req) {
          return create(GetRepositorySinkJobResponseSchema, {
            job: job(req.operationId, RepositorySinkJobState.UNRESOLVED),
          })
        },
      }),
    ).repositorySink
    const jobs = createSaveAsJobStore()
    const publishing = jobs.publish(client, request('op-sync'), true)
    await jobs.refresh('op-sync')
    expect(jobs.find('op-sync')!.summary?.phase).toBe('unresolved')
    releasePublish()
    await publishing
    expect(jobs.find('op-sync')!.summary?.phase).toBe('succeeded')
    jobs.dispose()
  })

  it('re-sends the retained request under the same operation ID without the dialog', async () => {
    const seen: string[] = []
    let lost = true
    const client = routerClients((r) =>
      r.service(RepositorySinkService, {
        publishRepositorySink(req) {
          seen.push(req.operationId)
          if (lost) throw new ConnectError('lost', Code.Unavailable)
          return create(PublishRepositorySinkResponseSchema, {
            job: job(req.operationId, RepositorySinkJobState.SUCCEEDED),
          })
        },
      }),
    ).repositorySink
    const jobs = createSaveAsJobStore()
    await jobs.publish(client, request('op-banner'), false)
    lost = false
    await jobs.retry('op-banner')
    expect(seen).toEqual(['op-banner', 'op-banner'])
    expect(jobs.find('op-banner')!.summary?.phase).toBe('succeeded')
    expect(jobs.find('op-banner')!.dialogOpen).toBe(false)
    jobs.dispose()
  })

  it('drops a status answer that describes the attempt before a retry', async () => {
    let releaseGet = () => {}
    const getGate = new Promise<void>((resolve) => {
      releaseGet = resolve
    })
    let publishes = 0
    const client = routerClients((r) =>
      r.service(RepositorySinkService, {
        publishRepositorySink(req) {
          publishes += 1
          if (publishes === 1) throw new ConnectError('lost', Code.Unavailable)
          // The server restarts a failed job when the same operation is retried.
          return create(PublishRepositorySinkResponseSchema, {
            job: job(req.operationId, RepositorySinkJobState.RUNNING),
          })
        },
        async getRepositorySinkJob(req) {
          await getGate
          return create(GetRepositorySinkJobResponseSchema, {
            job: job(req.operationId, RepositorySinkJobState.FAILED),
          })
        },
      }),
    ).repositorySink
    const jobs = createSaveAsJobStore()
    await jobs.publish(client, request('op-requeue'), true)
    const beforeRetry = jobs.refresh('op-requeue')
    await jobs.retry('op-requeue')
    expect(jobs.find('op-requeue')!.summary?.phase).toBe('running')
    releaseGet()
    await beforeRetry
    // The FAILED answer is about the attempt the retry replaced.
    expect(jobs.find('op-requeue')!.summary?.phase).toBe('running')
    jobs.dispose()
  })
})
