import { create } from '@bufbuild/protobuf'
import { Code, ConnectError } from '@connectrpc/connect'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { EntryAddressSchema, RenditionDescriptorSchema } from '@/gen/ai/protomolt/proto/repo/archive/v1/entry_pb'
import {
  CancelRepositorySinkJobResponseSchema,
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

  it('lets an in-flight publish answer for itself and then polls the job it started', async () => {
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
          return create(GetRepositorySinkJobResponseSchema, {
            job: job(req.operationId, RepositorySinkJobState.SUCCEEDED),
          })
        },
      }),
    ).repositorySink
    const jobs = createSaveAsJobStore()
    const publishing = jobs.publish(client, request('op-early'), false)
    // Admission may still be pending: a lookup now could only say "not found".
    await jobs.refresh('op-early')
    expect(gets).toBe(0)
    expect(jobs.find('op-early')!.error).toBeNull()
    releasePublish()
    await publishing
    expect(jobs.find('op-early')!.summary?.phase).toBe('queued')
    // The accepted job is polled to its end.
    await vi.advanceTimersByTimeAsync(6000)
    expect(gets).toBe(1)
    expect(jobs.find('op-early')!.summary?.phase).toBe('succeeded')
    jobs.dispose()
  })

  it('accepts a terminal outcome from an older call over a newer call that saw the job running', async () => {
    let releaseCancel = () => {}
    const cancelGate = new Promise<void>((resolve) => {
      releaseCancel = resolve
    })
    const client = routerClients((r) =>
      r.service(RepositorySinkService, {
        publishRepositorySink(req) {
          return create(PublishRepositorySinkResponseSchema, {
            job: job(req.operationId, RepositorySinkJobState.RUNNING),
          })
        },
        async cancelRepositorySinkJob(req) {
          await cancelGate
          return create(CancelRepositorySinkJobResponseSchema, {
            job: job(req.operationId, RepositorySinkJobState.CANCELLED),
          })
        },
        getRepositorySinkJob(req) {
          return create(GetRepositorySinkJobResponseSchema, {
            job: job(req.operationId, RepositorySinkJobState.RUNNING),
          })
        },
      }),
    ).repositorySink
    const jobs = createSaveAsJobStore()
    await jobs.publish(client, request('op-sync'), true)
    const cancelling = jobs.cancel('op-sync', 'stop')
    await jobs.refresh('op-sync')
    expect(jobs.find('op-sync')!.summary?.phase).toBe('running')
    releaseCancel()
    await cancelling
    // The cancel began first, but a job that has ended does not go back to running.
    expect(jobs.find('op-sync')!.summary?.phase).toBe('cancelled')
    jobs.dispose()
  })

  it('does not look a job up or cancel it while its publish is still in flight', async () => {
    let releasePublish = () => {}
    const publishGate = new Promise<void>((resolve) => {
      releasePublish = resolve
    })
    let publishes = 0
    let lookups = 0
    let cancels = 0
    const client = routerClients((r) =>
      r.service(RepositorySinkService, {
        async publishRepositorySink(req) {
          publishes += 1
          if (publishes === 1) throw new ConnectError('lost', Code.Unavailable)
          await publishGate
          return create(PublishRepositorySinkResponseSchema, {
            job: job(req.operationId, RepositorySinkJobState.RUNNING),
          })
        },
        getRepositorySinkJob(req) {
          lookups += 1
          // What the server held before the retry restarted the job.
          return create(GetRepositorySinkJobResponseSchema, {
            job: job(req.operationId, RepositorySinkJobState.FAILED),
          })
        },
        cancelRepositorySinkJob(req) {
          cancels += 1
          return create(CancelRepositorySinkJobResponseSchema, {
            job: job(req.operationId, RepositorySinkJobState.CANCELLED),
          })
        },
      }),
    ).repositorySink
    const jobs = createSaveAsJobStore()
    await jobs.publish(client, request('op-serial'), true)
    const retrying = jobs.retry('op-serial')
    // Either answer could describe the job from before the retry, in an order nobody can know.
    await jobs.refresh('op-serial')
    await jobs.cancel('op-serial', 'stop')
    expect(lookups).toBe(0)
    expect(cancels).toBe(0)
    releasePublish()
    await retrying
    expect(jobs.find('op-serial')!.summary?.phase).toBe('running')
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

  it('stops treating a failed job as settled once it is retried and the reply is lost', async () => {
    let call = 0
    const client = routerClients((r) =>
      r.service(RepositorySinkService, {
        publishRepositorySink(req) {
          call += 1
          if (call === 1) {
            return create(PublishRepositorySinkResponseSchema, {
              job: job(req.operationId, RepositorySinkJobState.FAILED),
            })
          }
          // The server restarted the job; its answer never arrived.
          throw new ConnectError('lost', Code.Unavailable)
        },
      }),
    ).repositorySink
    const jobs = createSaveAsJobStore()
    await jobs.publish(client, request('op-again'), false)
    expect(jobs.find('op-again')!.summary?.phase).toBe('failed')
    await jobs.retry('op-again')
    const entry = jobs.find('op-again')!
    // What the job is doing now is unknown: the old "failed" no longer describes it.
    expect(entry.summary).toBeNull()
    expect(entry.error?.kind).toBe('unsettled')

    // So it is not evicted to make room, and its request can still be retried.
    for (let i = 0; i < 19; i++) {
      await jobs.publish(client, request(`op-fill-${i}`), false)
    }
    await expect(jobs.publish(client, request('op-one-more'), false)).rejects.toBeInstanceOf(
      SaveAsJobCapacityError,
    )
    expect(jobs.find('op-again')).toBeDefined()
    jobs.dispose()
  })

  it('does not put a job that has ended back to running when an older sample arrives late', async () => {
    let releaseGet = () => {}
    const getGate = new Promise<void>((resolve) => {
      releaseGet = resolve
    })
    const client = routerClients((r) =>
      r.service(RepositorySinkService, {
        publishRepositorySink(req) {
          return create(PublishRepositorySinkResponseSchema, {
            job: job(req.operationId, RepositorySinkJobState.RUNNING),
          })
        },
        cancelRepositorySinkJob(req) {
          return create(CancelRepositorySinkJobResponseSchema, {
            job: job(req.operationId, RepositorySinkJobState.CANCELLED),
          })
        },
        async getRepositorySinkJob(req) {
          await getGate
          // Sampled before the cancel took effect.
          return create(GetRepositorySinkJobResponseSchema, {
            job: job(req.operationId, RepositorySinkJobState.RUNNING),
          })
        },
      }),
    ).repositorySink
    const jobs = createSaveAsJobStore()
    await jobs.publish(client, request('op-ended'), true)
    const cancelling = jobs.cancel('op-ended', 'stop')
    const looking = jobs.refresh('op-ended')
    await cancelling
    expect(jobs.find('op-ended')!.summary?.phase).toBe('cancelled')
    releaseGet()
    await looking
    expect(jobs.find('op-ended')!.summary?.phase).toBe('cancelled')
    jobs.dispose()
  })

  it('gives up waiting on a publish that never answers and leaves the job reconcilable', async () => {
    vi.useFakeTimers()
    const client = routerClients((r) =>
      r.service(RepositorySinkService, {
        publishRepositorySink(_req, context) {
          // The server may have committed; the reply never comes.
          return new Promise((_, reject) => {
            context.signal.addEventListener('abort', () =>
              reject(new ConnectError('deadline exceeded', Code.DeadlineExceeded)),
            )
          })
        },
      }),
    ).repositorySink
    const jobs = createSaveAsJobStore()
    const publishing = jobs.publish(client, request('op-silent'), true)
    await vi.advanceTimersByTimeAsync(10 * 60_000)
    await publishing
    const entry = jobs.find('op-silent')!
    expect(entry.submitting).toBe(false)
    expect(entry.error?.kind).toBe('unsettled')
    expect(entry.error?.canReconcile).toBe(true)
    // The request and its operation ID are still there to refresh or retry with.
    expect(entry.request.operationId).toBe('op-silent')
    jobs.dispose()
  })
})
