package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.authz.AuthenticatedCaller;
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
import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.catalog.CatalogConfig;
import ai.protomolt.proto.repo.catalog.PostgresCatalog;
import ai.protomolt.proto.repo.codec.RepositoryNamespaces;
import ai.protomolt.proto.repo.container.ledger.DriveLedger;
import ai.protomolt.proto.repo.container.ledger.DriveRecord;
import ai.protomolt.proto.repo.spi.PurgeAuthorization;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.stack.BlobStoreStack;
import ai.protomolt.proto.repo.stack.Stack;
import ai.protomolt.proto.repo.v2.StackRecord;
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
     */
    static Wired wire(RepoServiceConfig config, OwnedResources owned, DriveLedger driveLedger,
            BlobStore blobStore, String selectedDriveProvider, String catalogSchema,
            Supplier<Map<String, DocumentSchemaAdmission.Definition>> admittedSchemas,
            Function<AuthenticatedCaller, RepositoryCaller> bindCaller, ReadByteBudget readBudget) {
        // The parts-based v2 document service. The catalog shares the ledger's
        // database in its own schema; the ledger's Flyway history occupies the
        // default one.
        final PostgresCatalog catalog = owned.add(new PostgresCatalog(
                new CatalogConfig(config.ledger().jdbcUrl(),
                        config.ledger().username(), config.ledger().password(), config.ledger().maxPoolSize()),
                catalogSchema));
        var v2Stacks = new ConcurrentHashMap<String, Stack>();
        Function<DriveRecord, Stack> stackForDrive = record -> {
            String prefix = RepositoryNamespaces.under(
                    record.prefix == null ? "" : stripLeadingSlashes(record.prefix), "documents");
            String stackId = catalog.registerStack(StackRecord.newBuilder()
                    .setStackId("stack-" + record.driveId)
                    .setProvider(selectedDriveProvider)
                    .setEndpoint(config.s3Endpoint() == null ? "" : config.s3Endpoint())
                    .setNamespace(record.bucket + "/" + prefix)
                    .build());
            return v2Stacks.computeIfAbsent(stackId,
                    id -> new BlobStoreStack(id, blobStore, record.bucket, prefix));
        };
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
        var v2Saves = new DocumentSaves(catalog, v2Drives, builtinSchemas,
                limits, bytes -> () -> {}, Clock.systemUTC());
        var v2Uploads = new DocumentUploads(v2Saves);
        var accession = new AccessionGrpcService(v2Saves, v2Uploads,
                new DocumentReads(catalog, v2Drives, limits,
                        // Verified reads buffer typed parts, so they draw on a real host
                        // byte budget; a read past the budget is RESOURCE_EXHAUSTED
                        // before anything is allocated.
                        readBudget, Clock.systemUTC()),
                new DocumentDeletes(catalog, Clock.systemUTC()),
                new DocumentLists(catalog),
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
            } catch (RuntimeException concurrent) {
                if (catalog.credential("repo-service",
                        RepoStepSaveRebinding.INTERNAL_STEP_CREDENTIAL_ID).isEmpty()) throw concurrent;
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
}
