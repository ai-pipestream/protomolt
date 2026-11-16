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
  defaultRenditionSlotName,
  describeSaveAsCapabilities,
  encodeParquetRendererOptions,
  evaluateSaveAsSessionAccess,
  PARQUET_COLUMNS_HINT,
  PARQUET_MASK_STRATEGY_CHOICES,
  PARQUET_OPTIONS_TYPE,
  SaveAsInputError,
  ZERO_PROVIDERS_MESSAGE,
} from './saveAsCore'
import {
  classifySinkRpcError,
  commitFollowUp,
  describeSinkJob,
  enteredSucceeded,
} from './saveAsJobCore'
import {
  buildListRenditionProvenanceRequest,
  listPartOccurrenceOptions,
  mergeRenditionsWithProvenance,
} from './saveAsProvenanceCore'

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

  it('sends provider identity exactly as advertised, however long, and shortens it only for display', () => {
    const providerId = `provider-${'p'.repeat(140)}`
    const providerVersion = `1.0.0+${'v'.repeat(80)}`
    const sourceProfile = `profile-${'s'.repeat(140)}`
    const extension = `.${'x'.repeat(40)}`
    const view = describeSaveAsCapabilities(
      create(ListSaveAsCapabilitiesResponseSchema, {
        capabilities: [
          create(SaveAsCapabilitySchema, {
            status: SaveAsCompatibilityStatus.COMPATIBLE,
            capability: create(RendererCapabilitySchema, {
              providerId,
              providerVersion,
              sourceProfile,
              mediaType: 'text/csv',
              extension,
              maxInputBytes: 1024,
              maxOutputBytes: 1024,
            }),
          }),
        ],
      }),
    )
    const capability = view.compatible[0]
    // The labels are bounded for the page.
    expect(capability.providerId.length).toBeLessThan(providerId.length)
    expect(capability.providerVersion.length).toBeLessThan(providerVersion.length)

    const published = buildPublishSaveAsRequest({
      operationId: 'op-long',
      source: buildArchiveRenditionSinkSource({
        address: create(EntryAddressSchema, { accountId: 'acct-a', archive: 'datasets', entryId: 'e-1' }),
        rendition: { name: 'original' },
        revision: 1n,
      }),
      capability,
      destination: {
        accountId: 'acct-a',
        archive: 'datasets',
        entryId: 'e-1',
        renditionName: 'csv',
        preconditionMode: 'unconditional',
      },
    })
    expect(published.conversion?.providerId).toBe(providerId)
    expect(published.conversion?.providerVersion).toBe(providerVersion)
    expect(published.conversion?.sourceProfile).toBe(sourceProfile)
    expect(published.conversion?.extension).toBe(extension)
  })

  it('resolves a nested Any by where it occurs, not by its type URL alone', () => {
    const core = create(PartSelectorSchema, { selector: { case: 'part', value: DocumentPart.CORE } })
    const typeUrl = 'type.googleapis.com/ai.protomolt.proto.repo.v1.Document'
    const boundary = {
      step: {
        case: 'anyBoundary' as const,
        value: { typeUrl, valueSha256: 'b'.repeat(64), valueSizeBytes: 8n },
      },
    }
    const validation = create(RevisionValidationSchema, {
      resolvedOccurrences: [
        {
          occurrence: {
            part: core,
            steps: [
              { step: { case: 'fieldNumber', value: 4 } },
              boundary,
              { step: { case: 'fieldNumber', value: 4 } },
            ],
          },
          schema: { typeUrl: 'type.googleapis.com/google.protobuf.StringValue', descriptorSha256: 'a'.repeat(64) },
          payloadLength: 5n,
        },
      ],
    })
    const asked: string[][] = []
    const options = listPartOccurrenceOptions(validation, core, DocumentSchema, (part, stepsBefore) => {
      expect(part).toBe(core)
      asked.push(stepsBefore.map((step) => `${step.step.case}:${String(step.step.value)}`))
      return DocumentSchema
    })
    // The lookup is told which occurrence it is resolving: the Any at field 4.
    expect(asked).toEqual([['fieldNumber:4']])
    expect(options.map((option) => option.fieldPath)).toEqual(['structured_data.structured_data'])
  })

  it('addresses the selected source and destination exactly, including surrounding spaces', () => {
    // " foo " and "foo" are different entries; trimming would act on one the operator did not pick.
    const spaced = create(EntryAddressSchema, { accountId: 'acct-a', archive: 'datasets', entryId: ' foo ' })
    const source = buildArchiveRenditionSinkSource({ address: spaced, rendition: { name: 'original' }, revision: 1n })
    const target = source.kind.case === 'repositoryRevision' ? source.kind.value.target : undefined
    expect(target?.case === 'archiveRendition' ? target.value.address?.entryId : undefined).toBe(' foo ')
    expect(buildListRenditionProvenanceRequest(spaced, 1n).address?.entryId).toBe(' foo ')

    const view = describeSaveAsCapabilities(
      create(ListSaveAsCapabilitiesResponseSchema, {
        capabilities: [
          create(SaveAsCapabilitySchema, {
            status: SaveAsCompatibilityStatus.COMPATIBLE,
            capability: create(RendererCapabilitySchema, {
              providerId: 'csv', providerVersion: '1', sourceProfile: 'row', mediaType: 'text/csv',
            }),
          }),
        ],
      }),
    )
    const destination = {
      accountId: 'acct-a',
      archive: 'datasets',
      entryId: ' foo ',
      renditionName: 'csv',
      preconditionMode: 'unconditional' as const,
    }
    const published = buildPublishSaveAsRequest({
      operationId: 'op-spaced', source, capability: view.compatible[0], destination,
    })
    expect(published.destination?.address?.entryId).toBe(' foo ')
    expect(() =>
      buildPublishSaveAsRequest({
        operationId: 'op-blank', source, capability: view.compatible[0],
        destination: { ...destination, entryId: '   ' },
      }),
    ).toThrow(SaveAsInputError)

    // An entry ID is free-form, up to 500 characters: a URI is an ordinary one.
    for (const entryId of ['https://example.org/report', 'a\\b', '..', `urn:x:${'k'.repeat(400)}`]) {
      const sent = buildPublishSaveAsRequest({
        operationId: 'op-free', source, capability: view.compatible[0],
        destination: { ...destination, entryId },
      })
      expect(sent.destination?.address?.entryId).toBe(entryId)
    }
    expect(() =>
      buildPublishSaveAsRequest({
        operationId: 'op-long', source, capability: view.compatible[0],
        destination: { ...destination, entryId: 'e'.repeat(501) },
      }),
    ).toThrow(SaveAsInputError)
    // A rendition name is a path segment and stays path-safe.
    expect(() =>
      buildPublishSaveAsRequest({
        operationId: 'op-slash', source, capability: view.compatible[0],
        destination: { ...destination, renditionName: 'a/b' },
      }),
    ).toThrow(SaveAsInputError)
  })

  it('treats an Internal error and an unrecognized job state as outcomes still unknown', () => {
    // The sink reports INTERNAL when a commit may or may not have happened.
    const internal = classifySinkRpcError(new ConnectError('commit interrupted', Code.Internal))
    expect(internal.kind).toBe('unsettled')
    expect(internal.canReconcile).toBe(true)

    for (const state of [RepositorySinkJobState.UNSPECIFIED, 99 as RepositorySinkJobState]) {
      const unknown = describeSinkJob(create(RepositorySinkJobSchema, { operationId: 'op-odd', state }))
      expect(unknown.phase).toBe('unresolved')
      expect(unknown.terminal).toBe(false)
      expect(unknown.canReconcile).toBe(true)
      expect(unknown.stateLabel).toContain('not recognized')
    }
  })

  it('counts every arrival at succeeded as a commit to announce, whichever call reported it', () => {
    expect(enteredSucceeded('running', 'succeeded')).toBe(true)
    expect(enteredSucceeded(undefined, 'succeeded')).toBe(true)
    expect(enteredSucceeded('unresolved', 'succeeded')).toBe(true)
    expect(enteredSucceeded('succeeded', 'succeeded')).toBe(false)
    expect(enteredSucceeded('running', 'cancelled')).toBe(false)
  })

  it('describes each mask strategy as the Parquet renderer applies it', () => {
    const titles = new Map(PARQUET_MASK_STRATEGY_CHOICES.map((choice) => [choice.value, choice.title]))
    // REDACT writes "***" into strings and clears everything else; nothing is nulled out.
    expect(titles.get(ParquetMaskStrategy.REDACT)).toContain('"***"')
    expect(titles.get(ParquetMaskStrategy.REDACT)).not.toContain('null')
    // REMOVE clears values; the column stays unless it is left out of the column list.
    expect(titles.get(ParquetMaskStrategy.REMOVE)).toContain('column stays')
    expect(titles.get(ParquetMaskStrategy.REMOVE)).not.toContain('omit')
  })

  it('says that a blank column list exports every field, not only scalars', () => {
    // The renderer expands an empty column list to all fields: nested messages, maps and
    // repeated fields included.
    expect(PARQUET_COLUMNS_HINT).not.toContain('scalar')
    expect(PARQUET_COLUMNS_HINT).toContain('every field')
    expect(PARQUET_COLUMNS_HINT).toContain('nested')
  })

  it('follows a commit into the page only from the open dialog that made it', () => {
    const base = { dialogOpen: true, sameEntry: true, viewedVersion: 2n as bigint | null, committedVersion: 5n }
    // The operator is still in the dialog on the entry it wrote to.
    expect(commitFollowUp(base)).toBe('open-version')
    expect(commitFollowUp({ ...base, viewedVersion: null })).toBe('reload')
    expect(commitFollowUp({ ...base, viewedVersion: 5n })).toBe('reload')
    // The dialog was closed and the operator moved on: the banner reports the job, and the
    // page they are reading is left alone.
    expect(commitFollowUp({ ...base, dialogOpen: false })).toBe('none')
    expect(commitFollowUp({ ...base, dialogOpen: false, viewedVersion: null })).toBe('none')
    expect(commitFollowUp({ ...base, sameEntry: false })).toBe('none')
  })
})
