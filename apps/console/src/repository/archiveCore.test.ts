import { createHash } from 'node:crypto'
import { create, toBinary } from '@bufbuild/protobuf'
import { Code, ConnectError } from '@connectrpc/connect'
import { describe, expect, it } from 'vitest'
import { ClassificationSchema, ClassificationState } from '@/gen/ai/protomolt/proto/asset/v1/classification_pb'
import { ContentClass, ContentProfileSchema, QualityDimensionSchema, QualityScoreSchema } from '@/gen/ai/protomolt/proto/asset/v1/content_pb'
import { FormatFactSchema, PdfDocumentSchema } from '@/gen/ai/protomolt/proto/asset/v1/format_pb'
import {
  ArchiveService,
  ReadRenditionResponseSchema,
  RenditionReadHeaderSchema,
  UploadRenditionResponseSchema,
  type ReadRenditionRequest,
  type ReadRenditionResponse,
} from '@/gen/ai/protomolt/proto/repo/archive/v1/archive_service_pb'
import {
  EntryAddressSchema,
  EntryInfoSchema,
  RenditionDescriptorSchema,
  RenditionManifestEntrySchema,
  RenditionState,
  VersionManifestSchema,
  WriteAttributionSchema,
} from '@/gen/ai/protomolt/proto/repo/archive/v1/entry_pb'
import { collector } from './blobReadCore'
import {
  RenditionStreamError,
  classificationLabel,
  contentProfileLabel,
  entryAddress,
  entryRow,
  parseVersion,
  planRenditionUpload,
  readRendition,
  renditionDiff,
  renditionRows,
  sendRenditionUpload,
  versionRow,
} from './archiveCore'
import { at, routerClients } from './repositoryTestKit'

const BYTES = new Uint8Array(200_000).map((_, i) => (i * 11) & 0xff)
const SHA = createHash('sha256').update(BYTES).digest('hex')
const ADDRESS = create(EntryAddressSchema, { accountId: 'acct-a', archive: 'papers', entryId: 'e-1' })
const rendition = create(RenditionDescriptorSchema, { name: 'original', mediaType: 'application/pdf' })

/** A server that serves BYTES the way ReadRendition does, refusing ranges past the end. */
function server(seen: ReadRenditionRequest[] = [], sha = SHA) {
  return routerClients((router) =>
    router.service(ArchiveService, {
      async *readRendition(request) {
        seen.push(request)
        const size = BigInt(BYTES.length)
        if (request.offset > size || request.length > size - request.offset) {
          throw new ConnectError(`range [${request.offset}, +${request.length}) is outside ${size} bytes`, Code.InvalidArgument)
        }
        const length = request.length === 0n ? size - request.offset : request.length
        yield create(ReadRenditionResponseSchema, {
          frame: {
            case: 'header',
            value: create(RenditionReadHeaderSchema, {
              version: request.version === 0n ? 3n : request.version,
              rendition,
              sizeBytes: size,
              sha256: sha,
              offset: request.offset,
              length,
              wholeObject: request.offset === 0n && length === size,
            }),
          },
        })
        const end = Number(request.offset + length)
        for (let i = Number(request.offset); i < end; i += 65536) {
          yield create(ReadRenditionResponseSchema, { frame: { case: 'data', value: BYTES.subarray(i, Math.min(end, i + 65536)) } })
        }
      },
    }),
  ).archives
}

const spec = (offset = 0n, length = 0n, version: bigint | null = null) => ({ address: ADDRESS, version, name: 'original', subKey: '', range: { offset, length } })
const signal = () => new AbortController().signal

describe('readRendition', () => {
  it('streams a whole rendition and checks it against the attested SHA-256', async () => {
    const seen: ReadRenditionRequest[] = []
    const out = collector()
    const result = await readRendition(server(seen), spec(), out.sink, signal())
    expect(out.bytes()).toEqual(BYTES)
    expect(result.verification).toEqual({ kind: 'verified', sha256: SHA })
    expect(seen[0]).toMatchObject({ version: 0n, rendition: 'original', subKey: '', offset: 0n, length: 0n })
  })

  it('reads a range of an exact version and reports it unverified', async () => {
    const seen: ReadRenditionRequest[] = []
    const out = collector()
    const result = await readRendition(server(seen), spec(100n, 1000n, 2n), out.sink, signal())
    expect(out.bytes()).toEqual(BYTES.subarray(100, 1100))
    expect(result.verification).toEqual({ kind: 'range-unverified', wholeObjectSha256: SHA })
    expect(seen[0].version).toBe(2n)
  })

  it('fails a whole read whose bytes do not match the attested digest', async () => {
    await expect(readRendition(server([], 'e'.repeat(64)), spec(), () => {}, signal())).rejects.toThrow(`hash to ${SHA}`)
  })

  it('keeps stopAfter bytes of a long rendition and reads a short one to its end', async () => {
    const out = collector()
    const result = await readRendition(server(), spec(), out.sink, signal(), undefined, 1000n)
    expect(out.bytes()).toEqual(BYTES.subarray(0, 1000))
    expect(result.verification.kind).toBe('range-unverified')
    const tail = collector()
    await readRendition(server(), spec(BigInt(BYTES.length - 5)), tail.sink, signal(), undefined, 65536n)
    expect(tail.bytes()).toEqual(BYTES.subarray(BYTES.length - 5))
  })

  function frames(...items: ReadRenditionResponse[]) {
    return routerClients((router) =>
      router.service(ArchiveService, {
        async *readRendition() {
          yield* items
        },
      }),
    ).archives
  }
  const header = (fields: Record<string, unknown>) =>
    create(ReadRenditionResponseSchema, {
      frame: { case: 'header', value: create(RenditionReadHeaderSchema, { version: 3n, rendition, sizeBytes: 10n, sha256: SHA, offset: 0n, length: 10n, wholeObject: true, ...fields }) },
    })
  const data = (n: number) => create(ReadRenditionResponseSchema, { frame: { case: 'data', value: new Uint8Array(n) } })

  it.each([
    ['answers for another version', [header({ version: 4n }), data(10)], spec(0n, 0n, 3n), 'answered for version 4; version 3 was requested'],
    ['serves another offset', [header({ offset: 5n, length: 5n })], spec(0n), 'serves bytes from 5'],
    ['serves another length', [header({ length: 4n })], spec(0n, 10n), 'serves 4 bytes; the request names 10'],
    ['ends short', [header({}), data(3)], spec(), 'ended after 3 of 10 bytes'],
    ['sends more than declared', [header({}), data(11)], spec(), 'sent 11 bytes'],
    ['sends data first', [data(1)], spec(), 'before its header'],
    ['sends nothing', [], spec(), 'without a header'],
  ])('refuses a stream that %s', async (_name, items, s, message) => {
    const result = readRendition(frames(...(items as ReadRenditionResponse[])), s, () => {}, signal())
    await expect(result).rejects.toThrow(RenditionStreamError)
    await expect(readRendition(frames(...(items as ReadRenditionResponse[])), s, () => {}, signal())).rejects.toThrow(message)
  })
})

describe('rows and labels', () => {
  it('lists renditions with state and provenance, readable only when PRESENT', () => {
    const rows = renditionRows(
      create(VersionManifestSchema, {
        address: ADDRESS,
        version: 3n,
        renditions: [
          create(RenditionManifestEntrySchema, { rendition, state: RenditionState.PRESENT, sizeBytes: 10n, sha256: SHA, writtenAt: at('2026-10-01T00:00:00Z'), writtenBy: create(WriteAttributionSchema, { module: 'intake', actor: 'ada' }) }),
          create(RenditionManifestEntrySchema, { rendition: create(RenditionDescriptorSchema, { name: 'ocr', subKey: 'page-1' }), state: RenditionState.DELETED, deletedReason: 'RTBF' }),
        ],
      }),
    )
    expect(rows[0]).toMatchObject({ label: 'original', state: 'PRESENT', readable: true, writtenBy: 'intake by ada', writtenAt: '2026-10-01T00:00:00.000Z' })
    expect(rows[1]).toMatchObject({ label: 'ocr / page-1', state: 'DELETED', readable: false, deletedReason: 'RTBF' })
  })

  it('labels classification by state and the formats it records', () => {
    expect(classificationLabel(undefined)).toBe('not recorded')
    expect(
      classificationLabel(
        create(ClassificationSchema, {
          state: ClassificationState.IDENTIFIED,
          identified: create(FormatFactSchema, { format: { case: 'pdf', value: create(PdfDocumentSchema) } }),
        }),
      ),
    ).toBe('IDENTIFIED, identified pdf')
    const row = entryRow(create(EntryInfoSchema, { address: ADDRESS, currentVersion: 3n, title: 'T' }))
    expect(row).toMatchObject({ entryId: 'e-1', currentVersion: '3', classification: 'not recorded' })
    expect(() => entryRow(create(EntryInfoSchema))).toThrow('without an address')
    expect(versionRow(create(VersionManifestSchema, { version: 2n, totalBytes: 9n })).version).toBe(2n)
  })

  it('needs every part of an entry address and a positive version', () => {
    expect(() => entryAddress('acct-a', '', 'e-1')).toThrow('Archive is blank')
    expect(parseVersion('')).toBeNull()
    expect(parseVersion('4')).toBe(4n)
    expect(() => parseVersion('0')).toThrow('greater than zero')
  })

  it('formats content profiles and diffs two version manifests by rendition key', () => {
    const profile = create(ContentProfileSchema, {
      contentClass: ContentClass.OCR_TEXT,
      quality: create(QualityScoreSchema, {
        score: 0.94,
        dimensions: [create(QualityDimensionSchema, { name: 'character-confidence', score: 0.98, weight: 1.0 })],
      }),
    })
    expect(contentProfileLabel(profile)).toBe('OCR_TEXT · quality 0.94 (character-confidence: 0.98)')

    const v1 = create(VersionManifestSchema, {
      address: ADDRESS,
      version: 1n,
      renditions: [
        create(RenditionManifestEntrySchema, {
          rendition,
          state: RenditionState.PRESENT,
          sizeBytes: 100n,
          sha256: 'a'.repeat(64),
        }),
        create(RenditionManifestEntrySchema, {
          rendition: create(RenditionDescriptorSchema, { name: 'legacy' }),
          state: RenditionState.PRESENT,
          sizeBytes: 50n,
          sha256: 'b'.repeat(64),
        }),
      ],
    })
    const v2 = create(VersionManifestSchema, {
      address: ADDRESS,
      version: 2n,
      renditions: [
        create(RenditionManifestEntrySchema, {
          rendition,
          state: RenditionState.PRESENT,
          sizeBytes: 100n,
          sha256: 'a'.repeat(64),
        }),
        create(RenditionManifestEntrySchema, {
          rendition: create(RenditionDescriptorSchema, { name: 'ocr', subKey: 'p1', mediaType: 'text/plain' }),
          state: RenditionState.PRESENT,
          sizeBytes: 240n,
          sha256: 'c'.repeat(64),
          contentProfile: profile,
        }),
      ],
    })
    const diff = renditionDiff(v1, v2)
    expect(diff.map((d) => ({ label: d.label, change: d.change }))).toEqual([
      { label: 'original', change: 'unchanged' },
      { label: 'ocr / p1', change: 'added' },
      { label: 'legacy', change: 'removed' },
    ])
  })

  it('plans and sends a streaming rendition upload to /v1/archive:upload', async () => {
    const body = new TextEncoder().encode('rendition payload')
    const sha = createHash('sha256').update(body).digest('hex')
    const file = new File([body], 'ocr-p1.txt', { type: 'text/plain' })
    const plan = planRenditionUpload(
      {
        accountId: 'acct-a',
        archive: 'papers',
        entryId: 'e-1',
        rendition: 'ocr',
        subKey: 'page-1',
        contentType: 'text/plain',
        module: 'grparse',
        expectedVersion: '3',
      },
      file,
      '018f4063-7d4b-7000-8000-000000000099',
      sha,
    )
    expect(plan.url).toContain('/v1/archive:upload?account_id=acct-a&archive=papers&entry_id=e-1&rendition=ocr')
    expect(plan.url).toContain('sub_key=page-1')
    expect(plan.url).toContain('expected_version=3')
    expect(plan.url).toContain(`expected_sha256=${sha}`)
    expect(plan.url).toContain('operation_id=018f4063-7d4b-7000-8000-000000000099')

    const wire = toBinary(
      UploadRenditionResponseSchema,
      create(UploadRenditionResponseSchema, {
        operationId: '018f4063-7d4b-7000-8000-000000000099',
        entryUuid: '00000000-0000-5000-8000-000000000001',
        version: 4n,
        sha256: sha,
        replayed: false,
      }),
    )
    const outcome = await sendRenditionUpload(
      plan,
      () => {
        const req = {
          upload: { onprogress: null as ((e: ProgressEvent) => void) | null },
          onload: null as ((this: XMLHttpRequest, ev: ProgressEvent) => unknown) | null,
          onerror: null as ((this: XMLHttpRequest, ev: ProgressEvent) => unknown) | null,
          onabort: null as ((this: XMLHttpRequest, ev: ProgressEvent) => unknown) | null,
          withCredentials: false,
          status: 200,
          responseType: 'arraybuffer' as XMLHttpRequestResponseType,
          response: wire.buffer.slice(wire.byteOffset, wire.byteOffset + wire.byteLength),
          open: () => {},
          setRequestHeader: () => {},
          getResponseHeader: () => 'application/protobuf',
          send: () => {
            req.onload?.call(req as unknown as XMLHttpRequest, {} as ProgressEvent)
          },
          abort: () => {},
        }
        return req
      },
      () => {},
      signal(),
    )
    expect(outcome.kind).toBe('saved')
    if (outcome.kind === 'saved') {
      expect(outcome.response.version).toBe(4n)
      expect(outcome.response.operationId).toBe('018f4063-7d4b-7000-8000-000000000099')
    }
  })
})
