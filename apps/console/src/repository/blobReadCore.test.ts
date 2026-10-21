import { createHash } from 'node:crypto'
import { create } from '@bufbuild/protobuf'
import { Code, ConnectError } from '@connectrpc/connect'
import { describe, expect, it } from 'vitest'
import {
  BlobHeaderSchema,
  DocumentService,
  ReadBlobResponseSchema,
  type ReadBlobRequest,
  type ReadBlobResponse,
} from '@/gen/ai/protomolt/proto/repo/v2/document_service_pb'
import { BlobStreamError, collector, parseRange, readBlob } from './blobReadCore'
import { routerClients, testAddress } from './repositoryTestKit'

const FILE = new Uint8Array(Array.from({ length: 200_000 }, (_, i) => (i * 31 + 7) & 0xff))
const FILE_SHA = createHash('sha256').update(FILE).digest('hex')

function header(totalLength: bigint, sha256 = FILE_SHA): ReadBlobResponse {
  return create(ReadBlobResponseSchema, {
    frame: {
      case: 'header',
      value: create(BlobHeaderSchema, { revision: 2n, totalLength, sha256, mimeType: 'application/pdf', filename: 'a.pdf' }),
    },
  })
}

function data(bytes: Uint8Array): ReadBlobResponse {
  return create(ReadBlobResponseSchema, { frame: { case: 'data', value: bytes } })
}

/** A ReadBlob server that honours offset and length over FILE, in 16 KiB frames. */
function honestServer(seen: ReadBlobRequest[] = []) {
  return routerClients((router) =>
    router.service(DocumentService, {
      async *readBlob(request) {
        seen.push(request)
        // The server's own range rule (DocumentStreams.prepareBlobRead).
        if (request.offset > BigInt(FILE.length) || request.length > BigInt(FILE.length) - request.offset) {
          throw new ConnectError(`Requested byte range [${request.offset}, +${request.length}) is outside blob length ${FILE.length}`, Code.InvalidArgument)
        }
        yield header(BigInt(FILE.length))
        const start = Number(request.offset)
        const end = request.length === 0n ? FILE.length : Math.min(FILE.length, start + Number(request.length))
        for (let i = start; i < end; i += 16384) yield data(FILE.subarray(i, Math.min(end, i + 16384)))
      },
    }),
  )
}

function spec(offset = 0n, length = 0n) {
  return { address: testAddress(), revision: null, blobId: 'blob-1', range: { offset, length } }
}

describe('readBlob', () => {
  it('streams a full read to the sink and verifies the SHA-256 incrementally', async () => {
    const seen: ReadBlobRequest[] = []
    const out = collector()
    const progress: bigint[] = []
    const result = await readBlob(honestServer(seen).documents, spec(), out.sink, new AbortController().signal, (r) =>
      progress.push(r),
    )
    expect(result.verification).toEqual({ kind: 'verified', sha256: FILE_SHA })
    expect(result.receivedBytes).toBe(BigInt(FILE.length))
    expect(out.bytes()).toEqual(FILE)
    expect(progress.length).toBe(Math.ceil(FILE.length / 16384))
    expect(seen[0].revision?.selector).toEqual({ case: 'current', value: true })
    expect(seen[0].blobId).toBe('blob-1')
  })

  it('reads a byte range and says it is not verified against the whole-object digest', async () => {
    const out = collector()
    const result = await readBlob(honestServer().documents, spec(1000n, 5000n), out.sink, new AbortController().signal)
    expect(out.bytes()).toEqual(FILE.subarray(1000, 6000))
    expect(result.verification).toEqual({ kind: 'range-unverified', wholeObjectSha256: FILE_SHA })
  })

  it('reads an exact revision when one is named', async () => {
    const seen: ReadBlobRequest[] = []
    await readBlob(honestServer(seen).documents, { ...spec(), revision: 2n }, () => {}, new AbortController().signal)
    expect(seen[0].revision?.selector).toEqual({ case: 'revision', value: 2n })
  })

  it('fails a full read whose bytes do not match the header digest, naming both', async () => {
    const wrong = 'f'.repeat(64)
    const clients = routerClients((router) =>
      router.service(DocumentService, {
        async *readBlob() {
          yield header(BigInt(FILE.length), wrong)
          yield data(FILE)
        },
      }),
    )
    await expect(readBlob(clients.documents, spec(), () => {}, new AbortController().signal)).rejects.toThrow(
      `hash to ${FILE_SHA}, but the header declares ${wrong}`,
    )
  })

  it.each([
    ['ends short of the declared length', [header(10n), data(new Uint8Array(4))], 'ended after 4 of 10 bytes'],
    ['sends data before the header', [data(new Uint8Array(1))], 'data before its header'],
    ['sends two headers', [header(0n), header(0n)], 'second header'],
    ['sends more than the range holds', [header(2n), data(new Uint8Array(3))], 'sent 3 bytes; the requested range holds 2'],
    ['sends nothing', [], 'without a header'],
  ])('refuses a stream that %s', async (_name, frames, message) => {
    const clients = routerClients((router) =>
      router.service(DocumentService, {
        async *readBlob() {
          yield* frames as ReadBlobResponse[]
        },
      }),
    )
    const result = readBlob(clients.documents, spec(), () => {}, new AbortController().signal)
    await expect(result).rejects.toThrow(BlobStreamError)
    await expect(readBlob(clients.documents, spec(), () => {}, new AbortController().signal)).rejects.toThrow(message)
  })

  it('passes the server refusal of an offset past the end of the file', async () => {
    const error = await readBlob(honestServer().documents, spec(BigInt(FILE.length + 1)), () => {}, new AbortController().signal).catch((e) => e)
    expect(ConnectError.from(error).code).toBe(Code.InvalidArgument)
  })

  it('keeps only stopAfter bytes of a longer range and ends the stream itself', async () => {
    let serverSignal: AbortSignal | null = null
    const clients = routerClients((router) =>
      router.service(DocumentService, {
        async *readBlob(_request, context) {
          serverSignal = context.signal
          yield header(BigInt(FILE.length))
          for (let i = 0; i < FILE.length && !context.signal.aborted; i += 1000) yield data(FILE.subarray(i, i + 1000))
        },
      }),
    )
    const out = collector()
    const result = await readBlob(clients.documents, spec(), out.sink, new AbortController().signal, undefined, 2500n)
    expect(out.bytes()).toEqual(FILE.subarray(0, 2500))
    expect(result.verification.kind).toBe('range-unverified')
    expect(serverSignal!.aborted).toBe(true)
  })

  it('reads a file shorter than stopAfter whole, without asking for bytes past its end', async () => {
    const seen: ReadBlobRequest[] = []
    const out = collector()
    const result = await readBlob(honestServer(seen).documents, spec(BigInt(FILE.length - 10)), out.sink, new AbortController().signal, undefined, 65536n)
    expect(seen[0].length).toBe(0n)
    expect(out.bytes()).toEqual(FILE.subarray(FILE.length - 10))
    expect(result.receivedBytes).toBe(10n)
  })

  it('ends the server stream when it refuses a frame', async () => {
    let serverSignal: AbortSignal | null = null
    const clients = routerClients((router) =>
      router.service(DocumentService, {
        async *readBlob(_request, context) {
          serverSignal = context.signal
          yield header(10n)
          yield header(10n)
          await new Promise<void>((resolve) => context.signal.addEventListener('abort', () => resolve(), { once: true }))
        },
      }),
    )
    await expect(readBlob(clients.documents, spec(), () => {}, new AbortController().signal)).rejects.toThrow('second header')
    expect(serverSignal!.aborted).toBe(true)
  })

  it('refuses a header for another revision than the one asked for', async () => {
    await expect(readBlob(honestServer().documents, { ...spec(), revision: 7n }, () => {}, new AbortController().signal)).rejects.toThrow(
      'answered for revision 2; revision 7 was asked for',
    )
  })

  it('passes the server status through unchanged, DATA_LOSS included', async () => {
    const clients = routerClients((router) =>
      router.service(DocumentService, {
        // eslint-disable-next-line require-yield
        async *readBlob() {
          throw new ConnectError('stored object failed its SHA-256 check', Code.DataLoss)
        },
      }),
    )
    const error = await readBlob(clients.documents, spec(), () => {}, new AbortController().signal).catch((e) => e)
    expect(ConnectError.from(error).code).toBe(Code.DataLoss)
    expect(ConnectError.from(error).rawMessage).toBe('stored object failed its SHA-256 check')
  })

  it('ends the server stream when the signal aborts', async () => {
    let serverSignal: AbortSignal | null = null
    let framesSent = 0
    const controller = new AbortController()
    const clients = routerClients((router) =>
      router.service(DocumentService, {
        async *readBlob(_request, context) {
          serverSignal = context.signal
          yield header(BigInt(FILE.length))
          for (let i = 0; i < FILE.length && !context.signal.aborted; i += 10) {
            framesSent += 1
            yield data(FILE.subarray(i, i + 10))
          }
        },
      }),
    )
    const reading = readBlob(clients.documents, spec(), () => controller.abort(), controller.signal)
    const error = await reading.catch((e) => e)
    expect(ConnectError.from(error).code).toBe(Code.Canceled)
    await new Promise((r) => setTimeout(r, 10))
    // The handler's signal is the request's: the gateway sees the abort.
    expect(serverSignal!.aborted).toBe(true)
    expect(framesSent).toBeLessThan(FILE.length / 10)
  })
})

describe('parseRange', () => {
  it('reads blanks as the start and "to the end", and refuses negative or fractional input', () => {
    expect(parseRange('', '')).toEqual({ offset: 0n, length: 0n })
    expect(parseRange('10', '20')).toEqual({ offset: 10n, length: 20n })
    expect(() => parseRange('-1', '')).toThrow('Offset must not be negative')
    expect(() => parseRange('', '1.5')).toThrow('Length "1.5" is not a whole number')
  })
})
