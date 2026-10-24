package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.authz.AuthenticatedCaller;
import ai.protomolt.proto.config.ConceptSchemeMounts;
import ai.protomolt.proto.repo.accession.AccessionGrpcService;
import ai.protomolt.proto.repo.accession.DocumentDeletes;
import ai.protomolt.proto.repo.accession.DocumentLists;
import ai.protomolt.proto.repo.accession.DocumentPurges;
import ai.protomolt.proto.repo.accession.DocumentReads;
import ai.protomolt.proto.repo.accession.DocumentSaves;
import ai.protomolt.proto.repo.accession.DocumentUploads;
import ai.protomolt.proto.repo.accession.OutboxRelay;
import ai.protomolt.proto.repo.admission.BuiltinDocumentSchema;
import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.admission.DocumentSchemas;
import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.catalog.CatalogConfig;
import ai.protomolt.proto.repo.catalog.CatalogException;
import ai.protomolt.proto.repo.catalog.PostgresCatalog;
import ai.protomolt.proto.repo.codec.RepositoryNamespaces;
import ai.protomolt.proto.repo.container.ledger.DriveLedger;
import ai.protomolt.proto.repo.container.ledger.DriveRecord;
import ai.protomolt.proto.repo.spi.PurgeAuthorization;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.stack.BlobStoreStack;
import ai.protomolt.proto.repo.stack.Stack;
import ai.protomolt.proto.repo.v2.StackRecord;
import ai.protomolt.proto.validate.spi.ConceptSchemeCatalog;
import java.time.Clock;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Builds the parts-based v2 document stack: the catalog in its own schema on
 * the ledger database, the dynamic per-account drive resolution onto this
 * host's blob store, the save/read/delete/list/purge operations, the
 * accession service, the internal step credential, and the Kafka outbox
 * relay when eventing is configured. A builder only: the catalog and the
 * publisher register with the caller's {@link OwnedResources} in the same
 * order as before, and the caller owns start/stop and the close order.
 */
final class RepoV2Wiring {

    private RepoV2Wiring() {
    }

    /**
     * What the v2 wiring produced; {@code relay} is null without Kafka eventing.
     * The drives, resolver and limits are the ones the saves run with, for a
     * host that writes through its own {@link DocumentSaves} over the same store.
     */
    record Wired(PostgresCatalog catalog, DocumentUploads uploads, OutboxRelay relay,
            AccessionGrpcService accession, DocumentSaves.Drives drives,
            DocumentSchemaAdmission.Resolver resolver, DocumentSchemaAdmission.Limits limits) {
    }

    /**
     * Wires the v2 document service.
     *
     * @param catalogSchema the schema the catalog migrates into on the ledger database
     * @param admittedSchemas the module-contributed schema definitions, read per admission
     * @param bindCaller the composition's v2 caller binding
     * @param readBudget the host read byte budget verified reads reserve on
     * @param conceptSchemes the host's mounted concept schemes, read per save and per
     *        concept-filtered page; {@link ConceptSchemeMounts#empty()} when none are configured
     */
    static Wired wire(RepoServiceConfig config, OwnedResources owned, DriveLedger driveLedger,
            BlobStore blobStore, String selectedDriveProvider, String catalogSchema,
            Supplier<Map<String, DocumentSchemaAdmission.Definition>> admittedSchemas,
            Function<AuthenticatedCaller, RepositoryCaller> bindCaller, ReadByteBudget readBudget,
            Supplier<ConceptSchemeMounts> conceptSchemes) {
        // The parts-based v2 document service. The catalog shares the ledger's
        // database in its own schema; the ledger's Flyway history occupies the
        // default one.
        final PostgresCatalog catalog = owned.add(new PostgresCatalog(
                new CatalogConfig(config.ledger().jdbcUrl(),
                        config.ledger().username(), config.ledger().password(), config.ledger().maxPoolSize()),
                catalogSchema));
        var v2Stacks = new ConcurrentHashMap<String, Stack>();
        BlobStore stackStore = new StackBlobStore(blobStore);
        Function<DriveRecord, Stack> stackForDrive = driveStacks(catalog::registerStack,
                (id, record, prefix) -> new BlobStoreStack(id, stackStore, record.bucket, prefix),
                selectedDriveProvider, config.s3Endpoint(), v2Stacks);
        var v2Drives = DocumentSaves.Drives.dynamic("intake",
                (accountId, driveName) -> driveLedger.findByName(accountId, driveName)
                        .or(() -> "default".equals(driveName) ? driveLedger.findByName(accountId, "intake")
                                : Optional.<DriveRecord>empty())
                        .map(stackForDrive),
                stackId -> {
                    var cached = v2Stacks.get(stackId);
                    if (cached != null) return Optional.of(cached);
                    // A peer host may have written the revision; resolve the drive directly by primary key.
                    if (!stackId.startsWith("stack-")) return Optional.empty();
                    try {
                        var driveId = UUID.fromString(stackId.substring("stack-".length()));
                        return driveLedger.findById(driveId).map(stackForDrive);
                    } catch (IllegalArgumentException malformed) {
                        return Optional.empty();
                    }
                });
        var limits = DocumentSchemaAdmission.Limits.SERVICE_DEFAULTS;
        DocumentSchemaAdmission.Resolver builtinSchemas =
                selection -> {
                    if (selection.typeUrl().equals("type.googleapis.com/ai.protomolt.proto.repo.v1.SearchMetadata"))
                        return BuiltinDocumentSchema.definition(
                                ai.protomolt.proto.repo.v1.SearchMetadata.getDescriptor());
                    // Module-contributed schemas (the fleet document model).
                    return admittedSchemas.get().get(selection.typeUrl());
                };
        // Bound concept tags are decided against the mounts the host has at
        // the save (requirements §17); with none mounted they refuse
        // fail-closed. Listings, and ResolveLink through them, expand concept
        // filters against the same mounts.
        Supplier<ConceptSchemeCatalog> mountedConcepts = conceptSchemes::get;
        // The save budget, sized like the read budget but tracked in its own
        // pool (up to 2x DOCUMENT_PLATFORM_READ_BUDGET_BYTES across concurrent
        // reads and saves): a unary save reserves its split fragments and
        // extracted inline raw-blob bytes from admission until the revision
        // commits, and a streamed upload reserves per received chunk. An
        // over-budget save is RESOURCE_EXHAUSTED ("Save buffer budget exhausted")
        // before any stack write.
        var v2Saves = new DocumentSaves(catalog, v2Drives, builtinSchemas,
                limits, ReadByteBudget.forSaves(config.readBudgetBytes()), Clock.systemUTC(),
                DocumentSchemas.compiled(), false, mountedConcepts);
        var v2Uploads = new DocumentUploads(v2Saves);
        var accession = new AccessionGrpcService(v2Saves, v2Uploads,
                new DocumentReads(catalog, v2Drives, limits,
                        // Verified reads buffer typed parts, so they draw on a real host
                        // byte budget; a read past the budget is RESOURCE_EXHAUSTED
                        // before anything is allocated.
                        readBudget, Clock.systemUTC()),
                new DocumentDeletes(catalog, Clock.systemUTC()),
                new DocumentLists(catalog, conceptSchemes),
                new DocumentPurges(catalog, v2Drives,
                        (identity, tenant) -> new PurgeAuthorization.Decision(false, false),
                        Clock.systemUTC()),
                bindCaller);
        // Co-mounted step writers (the parse coordinator) save with process
        // authority; the step-save interceptor rebinds those calls to this
        // credential. Idempotent across nodes sharing this catalog.
        if (catalog.credential("repo-service", RepoStepSaveRebinding.INTERNAL_STEP_CREDENTIAL_ID).isEmpty()) {
            try {
                catalog.registerCredential("repo-service",
                        RepoStepSaveRebinding.INTERNAL_STEP_CREDENTIAL_ID, "internal-step-writer");
            } catch (CatalogException concurrent) {
                if (concurrent.unavailable()
                        || catalog.credential("repo-service",
                                RepoStepSaveRebinding.INTERNAL_STEP_CREDENTIAL_ID).isEmpty()) {
                    throw concurrent;
                }
            }
        }
        OutboxRelay relay = null;
        if (config.kafkaEnabled()) {
            var publisher = owned.add(new KafkaOutboxPublisher(config.kafkaBootstrapServers(),
                    config.kafkaTopic(), config.schemaRegistryUrl()));
            relay = new OutboxRelay(catalog, publisher);
        }
        return new Wired(catalog, v2Uploads, relay, accession, v2Drives, builtinSchemas, limits);
    }

    /** Opens one stack for a resolved namespace; the composition supplies the blob store. */
    interface StackOpener {
        Stack open(String stackId, DriveRecord record, String prefix);
    }

    /**
     * Resolves a drive to its stack with a per-drive cache in front of the
     * catalog: only a drive's first resolve runs {@code registerStack} (the
     * catalog's INSERT ON CONFLICT plus SELECT write transaction); repeated
     * by-name resolves of a cached drive make zero catalog calls. The catalog
     * stays the namespace authority: the returned stack id (which can differ
     * from {@code stack-<driveId>} when two drives name one namespace) keys
     * the shared by-id cache exactly as before. A {@code registerStack}
     * failure propagates unchanged and caches nothing in either map.
     */
    static Function<DriveRecord, Stack> driveStacks(Function<StackRecord, String> registerStack,
            StackOpener openStack, String selectedDriveProvider, String s3Endpoint,
            ConcurrentHashMap<String, Stack> stacksById) {
        var stacksByDrive = new ConcurrentHashMap<UUID, Stack>();
        return record -> stacksByDrive.computeIfAbsent(record.driveId, driveId -> {
            String prefix = RepositoryNamespaces.under(
                    record.prefix == null ? "" : stripLeadingSlashes(record.prefix), "documents");
            String stackId = registerStack.apply(StackRecord.newBuilder()
                    .setStackId("stack-" + record.driveId)
                    .setProvider(selectedDriveProvider)
                    .setEndpoint(s3Endpoint == null ? "" : s3Endpoint)
                    .setNamespace(record.bucket + "/" + prefix)
                    .build());
            return stacksById.computeIfAbsent(stackId,
                    id -> openStack.open(id, record, prefix));
        });
    }

    /**
     * Strips leading {@code '/'} characters without regex, matching
     * {@code replaceFirst("^/+", "")} exactly: an empty or all-slash input
     * produces the empty string, and an input with no leading slash comes
     * back unchanged.
     */
    static String stripLeadingSlashes(String value) {
        int start = 0;
        while (start < value.length() && value.charAt(start) == '/') {
            start++;
        }
        return value.substring(start);
    }

    /**
     * Adapts a {@link BlobStore} for {@link BlobStoreStack}, providing range stream and
     * object length fallbacks over {@link BlobStore#get(String, String, String)} when the
     * backing store (such as {@code RedisBlobStore} or a dynamic proxy) does not override
     * {@link BlobStore#openStream(String, String, long, long)} or
     * {@link BlobStore#objectLength(String, String)}.
     */
    private static final class StackBlobStore implements BlobStore {
        private final BlobStore delegate;

        StackBlobStore(BlobStore delegate) {
            this.delegate = java.util.Objects.requireNonNull(delegate, "delegate");
        }

        @Override
        public PutResult put(PutSpec spec, byte[] body) {
            if (java.lang.reflect.Proxy.isProxyClass(delegate.getClass())
                    && !DocumentArchiveRepository.IN_PUT_ENTRY.get()) {
                try {
                    return delegate.conditionalPut(spec, body, WriteCondition.absent());
                } catch (UnsupportedOperationException unsupported) {
                    return delegate.put(spec, body);
                }
            }
            return delegate.put(spec, body);
        }

        @Override
        public PutResult put(PutSpec spec, java.io.InputStream body, long contentLength) {
            return delegate.put(spec, body, contentLength);
        }

        @Override
        public PutResult putMultipart(PutSpec spec, java.io.InputStream body, int partSize) {
            return delegate.putMultipart(spec, body, partSize);
        }

        @Override
        public PutResult conditionalPut(PutSpec spec, byte[] body, WriteCondition condition) {
            return delegate.conditionalPut(spec, body, condition);
        }

        @Override
        public GetResult get(String bucket, String key, String versionId) {
            return delegate.get(bucket, key, versionId);
        }

        @Override
        public GetResult getBounded(String bucket, String key, String versionId, int maxBytes) {
            return delegate.getBounded(bucket, key, versionId, maxBytes);
        }

        @Override
        public GetResult getForUpdate(String bucket, String key) {
            return delegate.getForUpdate(bucket, key);
        }

        @Override
        public java.io.InputStream openStream(String bucket, String key, long offset, long length) {
            try {
                return delegate.openStream(bucket, key, offset, length);
            } catch (UnsupportedOperationException unsupported) {
                byte[] data = delegate.get(bucket, key, null).data();
                if (offset < 0 || offset > data.length) {
                    throw new IllegalArgumentException(
                            "offset " + offset + " is out of bounds for length " + data.length);
                }
                int start = (int) offset;
                int count = length < 0
                        ? data.length - start
                        : (int) Math.min(length, data.length - start);
                return new java.io.ByteArrayInputStream(data, start, count);
            }
        }

        @Override
        public java.io.InputStream openStream(
                String bucket, String key, String versionId, long offset, long length) {
            if (versionId == null) {
                return openStream(bucket, key, offset, length);
            }
            return delegate.openStream(bucket, key, versionId, offset, length);
        }

        @Override
        public long objectLength(String bucket, String key) {
            try {
                return delegate.objectLength(bucket, key);
            } catch (UnsupportedOperationException unsupported) {
                return delegate.get(bucket, key, null).data().length;
            }
        }

        @Override
        public void copy(String srcBucket, String srcKey, String dstBucket, String dstKey) {
            delegate.copy(srcBucket, srcKey, dstBucket, dstKey);
        }

        @Override
        public boolean delete(String bucket, String key) {
            return delegate.delete(bucket, key);
        }

        @Override
        public BatchDeleteResult deleteAll(String bucket, java.util.List<String> keys) {
            return delegate.deleteAll(bucket, keys);
        }

        @Override
        public java.util.List<ListedObject> list(String bucket, String prefix) {
            return delegate.list(bucket, prefix);
        }

        @Override
        public void headBucket(String bucket) {
            delegate.headBucket(bucket);
        }

        @Override
        public void headObject(String bucket, String key) {
            delegate.headObject(bucket, key);
        }
    }
}
