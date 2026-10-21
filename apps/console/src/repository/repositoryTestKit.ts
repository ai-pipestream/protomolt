/**
 * Test plumbing for the repository screens: generated service clients over
 * Connect-ES's in-memory router, which runs the real Connect protocol with
 * binary protobuf. Handlers answer with messages built from the generated
 * types; there are no JSON fixtures and no fetch mocks.
 */
import { create, toBinary, type DescFile } from '@bufbuild/protobuf'
import { FileDescriptorSetSchema, timestampFromDate } from '@bufbuild/protobuf/wkt'
import { createRouterTransport, type ConnectRouter } from '@connectrpc/connect'
import { NodeAddressSchema, type NodeAddress } from '@/gen/ai/protomolt/proto/repo/v1/address_pb'
import { repositoryClients, type RepositoryClients } from './clients'

/** Clients whose calls the given routes answer in memory. */
export function routerClients(routes: (router: ConnectRouter) => void): RepositoryClients {
  return repositoryClients(createRouterTransport(routes))
}

/** A fixed call number for tests. */
export function testAddress(overrides: Partial<NodeAddress> = {}): NodeAddress {
  return create(NodeAddressSchema, {
    accountId: 'acct-a',
    graphId: 'intake:acct-a',
    graphAddressId: 'ds-files',
    docId: 'doc-1',
    ...overrides,
  })
}

/** A Timestamp for a fixed instant. */
export function at(iso: string) {
  return timestampFromDate(new Date(iso))
}

/**
 * The serialized FileDescriptorSet of a generated file and its whole import
 * closure, as the repository retains one at save time.
 */
export function descriptorSetOf(file: DescFile): Uint8Array {
  const seen = new Map<string, DescFile>()
  const visit = (f: DescFile) => {
    if (seen.has(f.name)) return
    for (const dep of f.dependencies) visit(dep)
    seen.set(f.name, f)
  }
  visit(file)
  return toBinary(FileDescriptorSetSchema, create(FileDescriptorSetSchema, { file: [...seen.values()].map((f) => f.proto) }))
}

/**
 * An in-memory stand-in for the upload route's XMLHttpRequest. `answer`
 * decides the response from the request; progress is reported in 64 KiB
 * steps of the body actually given to send().
 */
export class FakeUploadRequest {
  static sent: FakeUploadRequest[] = []
  method = ''
  url = ''
  headers: Record<string, string> = {}
  body: Blob | null = null
  responseType: XMLHttpRequestResponseType = ''
  withCredentials = false
  status = 0
  response: unknown = null
  responseHeaders: Record<string, string> = {}
  upload: { onprogress: ((event: ProgressEvent) => void) | null } = { onprogress: null }
  onload: (() => void) | null = null
  onerror: (() => void) | null = null
  onabort: (() => void) | null = null
  aborted = false

  constructor(private readonly answer: (request: FakeUploadRequest) => FakeAnswer | 'network-error' | 'hang') {}

  open(method: string, url: string): void {
    this.method = method
    this.url = url
  }
  setRequestHeader(name: string, value: string): void {
    this.headers[name.toLowerCase()] = value
  }
  getResponseHeader(name: string): string | null {
    return this.responseHeaders[name.toLowerCase()] ?? null
  }
  abort(): void {
    this.aborted = true
    queueMicrotask(() => this.onabort?.())
  }
  send(body: Blob): void {
    this.body = body
    FakeUploadRequest.sent.push(this)
    const outcome = this.answer(this)
    if (outcome === 'hang') return
    void (async () => {
      for (let loaded = Math.min(65536, body.size); ; loaded = Math.min(body.size, loaded + 65536)) {
        this.upload.onprogress?.({ loaded } as ProgressEvent)
        await Promise.resolve()
        if (loaded >= body.size) break
      }
      if (outcome === 'network-error') {
        this.onerror?.()
        return
      }
      this.status = outcome.status
      this.responseHeaders = Object.fromEntries(Object.entries(outcome.headers ?? {}).map(([k, v]) => [k.toLowerCase(), v]))
      this.response = outcome.body?.buffer.slice(outcome.body.byteOffset, outcome.body.byteOffset + outcome.body.byteLength) ?? new ArrayBuffer(0)
      this.onload?.()
    })()
  }

  /** The query parameters the request was sent with. */
  params(): URLSearchParams {
    return new URLSearchParams(this.url.slice(this.url.indexOf('?') + 1))
  }
}

export interface FakeAnswer {
  status: number
  headers?: Record<string, string>
  body?: Uint8Array
}
