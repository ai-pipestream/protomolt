import { create, fromBinary } from '@bufbuild/protobuf'
import { Code, ConnectError } from '@connectrpc/connect'
import { describe, expect, it } from 'vitest'
import { IntakeWriterScopeSchema } from '@/gen/ai/protomolt/proto/authz/v1/account_bindings_pb'
import { ConsoleSessionStatusSchema } from '@/gen/ai/protomolt/proto/authz/v1/console_session_pb'
import {
  ParquetMaskStrategy,
  ParquetRendererOptionsSchema,
} from '@/gen/ai/protomolt/proto/emit/parquet/v1/parquet_renderer_pb'
import { RendererCapabilitySchema } from '@/gen/ai/protomolt/proto/emit/v1/renderer_pb'
import {
  EntryAddressSchema,
  RenditionDescriptorSchema,
  RenditionManifestEntrySchema,
  RenditionState,
  VersionManifestSchema,
} from '@/gen/ai/protomolt/proto/repo/archive/v1/entry_pb'
import {
  ListRenditionProvenanceResponseSchema,
  ListSaveAsCapabilitiesResponseSchema,
  RenditionProvenanceEntrySchema,
  RenditionProvenanceSchema,
  RepositorySinkFailureSchema,
  RepositorySinkJobSchema,
  RepositorySinkJobState,
  SaveAsCapabilitySchema,
  SaveAsCompatibilityStatus,
} from '@/gen/ai/protomolt/proto/repo/sink/v1/repository_sink_pb'
import { DocumentSchema } from '@/gen/ai/protomolt/proto/repo/v1/document_pb'
import {
  DocumentPart,
  PartSelectorSchema,
  RevisionValidationSchema,
} from '@/gen/ai/protomolt/proto/repo/v2/manifest_pb'
import { renditionRows } from './archiveCore'
import { at, testAddress } from './repositoryTestKit'
import {
  buildArchiveRenditionSinkSource,
  buildDocumentPartSinkSource,
  buildPublishSaveAsRequest,
  classifySinkRpcError,
  defaultRenditionSlotName,
  describeSaveAsCapabilities,
  describeSinkJob,
  encodeParquetRendererOptions,
  evaluateSaveAsSessionAccess,
  listPartOccurrenceOptions,
  mergeRenditionsWithProvenance,
  PARQUET_OPTIONS_TYPE,
  SaveAsInputError,
  ZERO_PROVIDERS_MESSAGE,
} from './saveAsCore'

const parquetCapability = create(RendererCapabilitySchema, {
  providerId: 'parquet-v1',
  providerVersion: '1.0.0',
  sourceProfile: 'tabular-scalar-v1',
  mediaType: 'application/vnd.apache.parquet',
  extension: 'parquet',
  optionsType: PARQUET_OPTIONS_TYPE,
  optionsFingerprint: 'fp-opts-1',
  lossDescription: 'Projects scalar top-level columns only; nested fields are omitted.',
  maxInputBytes: 4_194_304,
  maxOutputBytes: 1_048_576,
})

describe('saveAsCore', () => {
  it('evaluates session write access across unknown, unauthenticated, read-only, and writer sessions', () => {
    expect(evaluateSaveAsSessionAccess(null, 'acct-a')).toEqual({
      readOnly: false,
      reason: null,
      principal: null,
    })

    const unauth = create(ConsoleSessionStatusSchema, { authenticated: false })
    expect(evaluateSaveAsSessionAccess(unauth, 'acct-a').readOnly).toBe(true)

    const readOnly = create(ConsoleSessionStatusSchema, {
      authenticated: true,
      principal: 'viewer@example.com',
      accountIds: ['acct-a'],
      intakeWriters: [
        create(IntakeWriterScopeSchema, { accountId: 'acct-other', datasourceId: 'ds-1' }),
      ],
    })
    const roVerdict = evaluateSaveAsSessionAccess(readOnly, 'acct-a')
    expect(roVerdict.readOnly).toBe(true)
    expect(roVerdict.reason).toContain('viewer@example.com')
    expect(roVerdict.reason).toContain('acct-a')

    const writer = create(ConsoleSessionStatusSchema, {
      authenticated: true,
      principal: 'writer@example.com',
      accountIds: ['acct-a'],
      intakeWriters: [
        create(IntakeWriterScopeSchema, { accountId: 'acct-a', datasourceId: 'ds-1' }),
      ],
    })
    expect(evaluateSaveAsSessionAccess(writer, 'acct-a')).toEqual({
      readOnly: false,
      reason: null,
      principal: 'writer@example.com',
    })
  })

  it('builds pinned SinkSource envelopes for archive renditions and document parts', () => {
    const entryAddr = create(EntryAddressSchema, { accountId: 'acct-a', archive: 'papers', entryId: 'e-1' })
    expect(() =>
      buildArchiveRenditionSinkSource({
        address: entryAddr,
        rendition: { name: 'parsed' },
        revision: 0n,
      }),
    ).toThrow(SaveAsInputError)

    const archiveSrc = buildArchiveRenditionSinkSource({
      address: entryAddr,
      rendition: { name: 'parsed', mediaType: 'application/x-protobuf' },
      revision: 4n,
      expectedSchemaFingerprint: 'fp-src',
    })
    expect(archiveSrc.kind.case).toBe('repositoryRevision')
    if (archiveSrc.kind.case === 'repositoryRevision' && archiveSrc.kind.value.target.case === 'archiveRendition') {
      expect(archiveSrc.kind.value.target.value.revision?.selector).toEqual({ case: 'revision', value: 4n })
      expect(archiveSrc.kind.value.target.value.rendition?.name).toBe('parsed')
      expect(archiveSrc.kind.value.target.value.expectedSchemaFingerprint).toBe('fp-src')
    }

    const coreSelector = create(PartSelectorSchema, { selector: { case: 'part', value: DocumentPart.CORE } })
    const docSrc = buildDocumentPartSinkSource({
      address: testAddress(),
      selector: coreSelector,
      revision: 7n,
      occurrenceFieldPath: 'structured_data',
    })
    if (docSrc.kind.case === 'repositoryRevision' && docSrc.kind.value.target.case === 'documentPart') {
      expect(docSrc.kind.value.target.value.revision?.selector).toEqual({ case: 'revision', value: 7n })
      expect(docSrc.kind.value.target.value.occurrenceFieldPath).toBe('structured_data')
    }
  })

  it('describes capabilities without inventing unconfigured formats and filters out non-rendition transport schemes', () => {
    const empty = describeSaveAsCapabilities(create(ListSaveAsCapabilitiesResponseSchema))
    expect(empty.hasAnyProviders).toBe(false)
    expect(empty.compatible).toEqual([])
    expect(empty.incompatible).toEqual([])
    expect(empty.emptyStateMessage).toBe(ZERO_PROVIDERS_MESSAGE)

    const mixed = describeSaveAsCapabilities(
      create(ListSaveAsCapabilitiesResponseSchema, {
        resolvedSourceRevision: 5n,
        sourceSchemaType: 'ai.protomolt.CustomerRow',
        sourceSchemaFingerprint: 'fp-row',
        sourceSizeBytes: 256n,
        capabilities: [
          create(SaveAsCapabilitySchema, {
            capability: parquetCapability,
            status: SaveAsCompatibilityStatus.COMPATIBLE,
          }),
          create(SaveAsCapabilitySchema, {
            capability: create(RendererCapabilitySchema, {
              providerId: 'avro-v1',
              providerVersion: '1.2.0',
              sourceProfile: 'strict-record',
              mediaType: 'application/vnd.apache.avro',
              extension: 'avro',
            }),
            status: SaveAsCompatibilityStatus.INCOMPATIBLE,
            incompatibilityReason: 'Source contains unsupported map<int32, bytes> field.',
          }),
          create(SaveAsCapabilitySchema, {
            capability: create(RendererCapabilitySchema, {
              providerId: 'kafka-sink',
              providerVersion: '1.0.0',
              sourceProfile: 'events',
              mediaType: 'kafka://events-topic',
            }),
            status: SaveAsCompatibilityStatus.COMPATIBLE,
          }),
        ],
      }),
    )

    expect(mixed.hasAnyProviders).toBe(true)
    expect(mixed.sourceRevision).toBe(5n)
    expect(mixed.compatible).toHaveLength(1)
    expect(mixed.compatible[0]!.providerId).toBe('parquet-v1')
    expect(mixed.compatible[0]!.optionsKind).toBe('parquet')
    expect(mixed.compatible[0]!.defaultRenditionName).toBe('parquet')
    expect(mixed.incompatible).toHaveLength(1)
    expect(mixed.incompatible[0]!.providerId).toBe('avro-v1')
    expect(mixed.incompatible[0]!.incompatibilityReason).toContain('unsupported map<int32, bytes>')
  })

  it('validates and binary-encodes ParquetRendererOptions and PublishRepositorySinkRequest', () => {
    const encoded = encodeParquetRendererOptions(
      {
        columnsText: 'doc_id, title\ncreated_epoch',
        maskedSensitivityClassesText: 'PII, SECRET, PII',
        maskStrategy: ParquetMaskStrategy.REDACT,
        maxOutputBytesText: '524288',
        mappingId: 'map-finance',
        policyId: 'pol-redacted',
      },
      1_048_576n,
    )
    const decoded = fromBinary(ParquetRendererOptionsSchema, encoded.bytes)
    expect(decoded.columns).toEqual(['doc_id', 'title', 'created_epoch'])
    expect(decoded.maskedSensitivityClasses).toEqual(['PII', 'SECRET'])
    expect(decoded.maskStrategy).toBe(ParquetMaskStrategy.REDACT)
    expect(decoded.maxOutputBytes).toBe(524288n)
    expect(decoded.mappingId).toBe('map-finance')
    expect(decoded.policyId).toBe('pol-redacted')

    expect(() =>
      encodeParquetRendererOptions({
        columnsText: 'nested.field',
        maskedSensitivityClassesText: '',
        maskStrategy: ParquetMaskStrategy.UNSPECIFIED,
        maxOutputBytesText: '',
        mappingId: '',
        policyId: '',
      }),
    ).toThrow('without dots')

    expect(() =>
      encodeParquetRendererOptions({
        columnsText: 'dup, dup',
        maskedSensitivityClassesText: '',
        maskStrategy: ParquetMaskStrategy.UNSPECIFIED,
        maxOutputBytesText: '',
        mappingId: '',
        policyId: '',
      }),
    ).toThrow('duplicate column')

    expect(() =>
      encodeParquetRendererOptions({
        columnsText: '',
        maskedSensitivityClassesText: 'PII',
        maskStrategy: ParquetMaskStrategy.UNSPECIFIED,
        maxOutputBytesText: '',
        mappingId: '',
        policyId: '',
      }),
    ).toThrow('choose REDACT or REMOVE')

    expect(() =>
      encodeParquetRendererOptions(
        {
          columnsText: '',
          maskedSensitivityClassesText: '',
          maskStrategy: ParquetMaskStrategy.UNSPECIFIED,
          maxOutputBytesText: '2000000',
          mappingId: '',
          policyId: '',
        },
        1_048_576n,
      ),
    ).toThrow('exceeds provider ceiling')

    const capView = describeSaveAsCapabilities(
      create(ListSaveAsCapabilitiesResponseSchema, {
        capabilities: [
          create(SaveAsCapabilitySchema, {
            capability: parquetCapability,
            status: SaveAsCompatibilityStatus.COMPATIBLE,
          }),
        ],
      }),
    )
    const source = buildArchiveRenditionSinkSource({
      address: create(EntryAddressSchema, { accountId: 'acct-a', archive: 'papers', entryId: 'e-1' }),
      rendition: { name: 'parsed' },
      revision: 3n,
    })

    // Forbid overwriting 'parsed' with non-protobuf output
    expect(() =>
      buildPublishSaveAsRequest({
        operationId: 'op-1',
        source,
        capability: capView.compatible[0]!,
        destination: {
          accountId: 'acct-a',
          archive: 'papers',
          entryId: 'e-1',
          renditionName: 'parsed',
          preconditionMode: 'expected-version',
          expectedDestinationVersionText: '3',
        },
      }),
    ).toThrow("cannot overwrite the 'parsed' protobuf slot")

    const req = buildPublishSaveAsRequest({
      operationId: 'op-1',
      source,
      capability: capView.compatible[0]!,
      destination: {
        accountId: 'acct-a',
        archive: 'papers',
        entryId: 'e-1',
        renditionName: 'parquet',
        preconditionMode: 'expected-version',
        expectedDestinationVersionText: '3',
      },
      parquetOptions: {
        columnsText: 'doc_id',
        maskedSensitivityClassesText: '',
        maskStrategy: ParquetMaskStrategy.UNSPECIFIED,
        maxOutputBytesText: '',
        mappingId: 'm1',
        policyId: 'p1',
      },
    })
    expect(req.operationId).toBe('op-1')
    expect(req.destination?.expectedDestinationVersion).toBe(3n)
    expect(req.destination?.createOnly).toBe(false)
    expect(req.destination?.rendition?.name).toBe('parquet')
    expect(req.destination?.rendition?.mediaType).toBe('application/vnd.apache.parquet')
    expect(req.conversion?.mappingId).toBe('m1')
    expect(req.conversion?.policyId).toBe('p1')
    expect(defaultRenditionSlotName('parsed', 'p')).toBe('parsed-derived')
  })

  it('models sink job lifecycle states and classifies lost responses vs version conflicts', () => {
    const running = describeSinkJob(
      create(RepositorySinkJobSchema, {
        operationId: 'op-run',
        state: RepositorySinkJobState.RUNNING,
        frozenSourceRevision: 2n,
      }),
    )
    expect(running.canCancel).toBe(true)
    expect(running.canReconcile).toBe(true)
    expect(running.terminal).toBe(false)

    const unresolved = describeSinkJob(
      create(RepositorySinkJobSchema, {
        operationId: 'op-unres',
        state: RepositorySinkJobState.UNRESOLVED,
      }),
    )
    expect(unresolved.phase).toBe('unresolved')
    expect(unresolved.terminal).toBe(false)
    expect(unresolved.canReconcile).toBe(true)
    expect(unresolved.canRetrySameOperation).toBe(true)

    const conflictFailed = describeSinkJob(
      create(RepositorySinkJobSchema, {
        operationId: 'op-conflict',
        state: RepositorySinkJobState.FAILED,
        failure: create(RepositorySinkFailureSchema, {
          grpcCode: Code.Aborted,
          conflict: true,
          message: 'Destination version advanced to 9',
        }),
      }),
    )
    expect(conflictFailed.isConflict).toBe(true)
    expect(conflictFailed.canRetrySameOperation).toBe(false)

    expect(classifySinkRpcError(new ConnectError('connection reset', Code.Unavailable))).toMatchObject({
      kind: 'unsettled',
      canReconcile: true,
    })
    expect(classifySinkRpcError(new ConnectError('version mismatch', Code.Aborted))).toMatchObject({
      kind: 'conflict',
      canReconcile: false,
    })
  })

  it('enriches renditions with provenance, detects stale derived renditions, and guards external URLs', () => {
    const entryAddr = create(EntryAddressSchema, { accountId: 'acct-a', archive: 'papers', entryId: 'e-1' })
    const manifest = create(VersionManifestSchema, {
      address: entryAddr,
      version: 4n,
      renditions: [
        create(RenditionManifestEntrySchema, {
          rendition: create(RenditionDescriptorSchema, { name: 'parsed', mediaType: 'application/x-protobuf' }),
          state: RenditionState.PRESENT,
          sizeBytes: 128n,
        }),
        create(RenditionManifestEntrySchema, {
          rendition: create(RenditionDescriptorSchema, { name: 'parquet', mediaType: 'application/vnd.apache.parquet' }),
          state: RenditionState.PRESENT,
          sizeBytes: 1024n,
        }),
        create(RenditionManifestEntrySchema, {
          rendition: create(RenditionDescriptorSchema, { name: 'original', mediaType: 'application/pdf' }),
          state: RenditionState.PRESENT,
          sizeBytes: 2048n,
        }),
      ],
    })

    const provResp = create(ListRenditionProvenanceResponseSchema, {
      entries: [
        create(RenditionProvenanceEntrySchema, {
          rendition: create(RenditionDescriptorSchema, { name: 'parsed', mediaType: 'application/x-protobuf' }),
          provenance: create(RenditionProvenanceSchema, {
            authoritativeProtobuf: true,
            sourceRevision: 4n,
            committedAt: at('2026-11-14T00:00:00Z'),
          }),
          currentSourceRevision: 4n,
          sourceStale: false,
        }),
        create(RenditionProvenanceEntrySchema, {
          rendition: create(RenditionDescriptorSchema, { name: 'parquet', mediaType: 'application/vnd.apache.parquet' }),
          provenance: create(RenditionProvenanceSchema, {
            authoritativeProtobuf: false,
            providerId: 'parquet-v1',
            providerVersion: '1.0.0',
            sourceProfile: 'tabular-scalar-v1',
            sourceRevision: 2n,
            sourceEntry: entryAddr,
            destinationAddress: entryAddr,
            mappingId: 'map-1',
            policyId: 'pol-1',
            lossDescription: 'Scalar columns only',
          }),
          currentSourceRevision: 4n,
          sourceStale: true,
        }),
      ],
    })

    const enriched = mergeRenditionsWithProvenance(renditionRows(manifest), provResp, {
      viewedEntryVersion: 4n,
      originalFilename: 'paper.pdf',
      sourceUri: 'javascript:alert(1)',
    })

    const parsed = enriched.find((r) => r.name === 'parsed')!
    expect(parsed.authoritativeProtobuf).toBe(true)
    expect(parsed.isDerived).toBe(false)
    expect(parsed.isStale).toBe(false)
    expect(parsed.canSaveAsSource).toBe(true)

    const parquet = enriched.find((r) => r.name === 'parquet')!
    expect(parquet.isDerived).toBe(true)
    expect(parquet.isStale).toBe(true)
    expect(parquet.staleExplanation).toContain('r2')
    expect(parquet.staleExplanation).toContain('r4')
    expect(parquet.canSaveAsSource).toBe(false)
    expect(parquet.providerId).toBe('parquet-v1')
    expect(parquet.lossDescription).toBe('Scalar columns only')

    const original = enriched.find((r) => r.name === 'original')!
    expect(original.safeExternalUrl).toBeNull()
    expect(original.canSaveAsSource).toBe(false)
  })

  it('reconstructs occurrence_field_path options from RevisionValidation for decoded parts', () => {
    const core = create(PartSelectorSchema, { selector: { case: 'part', value: DocumentPart.CORE } })
    const validation = create(RevisionValidationSchema, {
      resolvedOccurrences: [
        {
          occurrence: {
            part: core,
            steps: [{ step: { case: 'fieldNumber', value: 4 } }],
          },
          schema: {
            typeUrl: 'type.googleapis.com/google.protobuf.StringValue',
            descriptorSha256: 'a'.repeat(64),
          },
          payloadLength: 24n,
        },
      ],
    })
    const options = listPartOccurrenceOptions(validation, core, DocumentSchema)
    expect(options).toEqual([
      {
        fieldPath: 'structured_data',
        typeName: 'google.protobuf.StringValue',
        typeUrl: 'type.googleapis.com/google.protobuf.StringValue',
        payloadSizeBytes: 24n,
      },
    ])
  })
})
