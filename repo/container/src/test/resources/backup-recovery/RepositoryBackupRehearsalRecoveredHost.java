package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.asset.bridge.BridgeEngine;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.service.RepoServiceConfig;
import ai.protomolt.proto.repo.service.RepoServices;
import ai.protomolt.proto.repo.spi.HistoricalMaterializationRepository;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import ai.protomolt.proto.repo.v1.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

/**
 * Fresh production-JAR host over restored resources. No schema registry exists; a live
 * resolution attempt during historical verification fails the run. READY is written only
 * after every verification passed. Negative modes observe the refusal the restored content
 * must produce and never write READY. Adapted from GitHub PR #413 and extended with the
 * pending-operation, missing-descriptor, wrong-credentials and inconsistent-snapshot modes.
 */
public final class RepositoryBackupRehearsalRecoveredHost {
    private static final RepositoryReadControl NONE = RepositoryReadControl.NONE;

    public static void main(String[] args) throws Exception {
        RepositoryBackupRehearsalFixture.requireNoTestFramework();
        String mode = RepositoryBackupRehearsalFixture.env("PROTOMOLT_REHEARSAL_MODE");
        var identities = Path.of(RepositoryBackupRehearsalFixture.env("PROTOMOLT_REHEARSAL_IDENTITIES"));
        var out = Path.of(RepositoryBackupRehearsalFixture.env("PROTOMOLT_REHEARSAL_OUT"));
        Files.createDirectories(out);
        var checks = new RepositoryBackupRehearsalChecks("recovered-" + mode, out.resolve("markers.log"));
        var record = RepositoryBackupRehearsalJson.read(identities.resolve("identities.json"));
        var manifest = RepositoryBackupRehearsalJson.read(Path.of(RepositoryBackupRehearsalFixture.env("PROTOMOLT_REHEARSAL_MANIFEST")));
        String endpoint = RepositoryBackupRehearsalFixture.env("PROTOMOLT_REHEARSAL_S3_ENDPOINT");
        String generation = RepositoryBackupRehearsalFixture.env("PROTOMOLT_REHEARSAL_GENERATION");
        String realm = RepositoryBackupRehearsalFixture.env("PROTOMOLT_REHEARSAL_REALM");
        var bundle = Path.of(RepositoryBackupRehearsalFixture.env("PROTOMOLT_REHEARSAL_BUNDLE"));
        var config = RepositoryBackupRehearsalFixture.config(endpoint, generation, realm);
        var offline = new RepositoryBackupRehearsalFixture.SwitchableSchemas();
        var ready = out.resolve("READY");
        try {
            switch (mode) {
                case "verify" -> verify(checks, config, offline, bundle, identities, record, manifest, endpoint, generation, ready);
                case "wrong-endpoint" -> wrongEndpoint(checks, config, offline, bundle);
                case "new-generation" -> newGeneration(checks, config, offline, bundle, identities, record, generation);
                case "missing-version" -> missingVersion(checks, config, offline, bundle, identities, record, endpoint);
                case "corrupt-descriptor", "missing-descriptor" -> damagedDescriptor(mode, checks, config, offline, bundle, identities, record);
                case "wrong-credentials" -> wrongCredentials(checks, endpoint, generation, realm, offline, bundle, identities, record);
                case "inconsistent-snapshot" -> inconsistentSnapshot(checks, config, offline, bundle, identities, record, endpoint);
                default -> throw new RepositoryBackupRehearsalFailure("Unknown recovered host mode " + mode);
            }
            // verify mode asserts zero attempts before its fresh write and then deliberately provokes one refusal.
            if (!mode.equals("verify")) checks.require(offline.offlineAttempts.get() == 0, mode + ".schema.no_live_lookup",
                    "live schema resolution attempts=" + offline.offlineAttempts.get());
            checks.require(!Files.exists(ready) || mode.equals("verify"), mode + ".not_ready", "READY is only written by a fully verified host");
        } finally {
            checks.writeXml(out.resolve("results.xml"));
        }
        System.out.println("REPOSITORY_BACKUP_REHEARSAL_RECOVERED_" + mode.toUpperCase().replace('-', '_') + "_OK");
    }

    private static RepoServices open(RepoServiceConfig config, ai.protomolt.proto.repo.service.ManagedSchemaAccess schemas, Path bundle) {
        return RepoServices.build(config, BridgeEngine.standard(), RepositoryBackupRehearsalFixture.historicalAccess(), schemas,
                RepositoryBackupRehearsalFixture.publicationOptions(bundle));
    }

    private static Map<String, Long> toLongs(Map<String, Object> values) {
        var result = new TreeMap<String, Long>();
        values.forEach((key, value) -> result.put(key, value instanceof Number n ? n.longValue() : Long.parseLong(value.toString())));
        return result;
    }

    private static void verify(RepositoryBackupRehearsalChecks checks, RepoServiceConfig config, RepositoryBackupRehearsalFixture.SwitchableSchemas offline,
            Path bundle, Path identities, Map<String, Object> record, Map<String, Object> manifest, String endpoint, String generation, Path ready) throws Exception {
        long restoredActive;
        try (var ledger = RepositoryBackupRehearsalLedger.fromEnvironment()) {
            var counts = ledger.rowCounts();
            checks.require(counts.equals(toLongs(RepositoryBackupRehearsalJson.object(record, "rowCountsAfterClose"))), "restore.row_counts",
                    "every base table's row count equals the seed record after close: tables=" + counts.size());
            checks.require(counts.equals(toLongs(RepositoryBackupRehearsalJson.object(manifest, "rowCounts"))), "restore.row_counts_manifest",
                    "row counts equal the sealed manifest");
            var incarnations = RepositoryBackupRehearsalJson.object(record, "readerIncarnations");
            for (String state : List.of("ACTIVE", "FENCED", "UNKNOWN", "QUIESCED"))
                checks.require(ledger.readerIncarnations(state) == RepositoryBackupRehearsalJson.number(incarnations, state), "restore.reader_incarnations." + state.toLowerCase(),
                        "restored " + state + " incarnations=" + ledger.readerIncarnations(state) + " (restored verbatim, none revived)");
            restoredActive = ledger.readerIncarnations("ACTIVE");
            checks.require(ledger.backendProfile(generation).equals(RepositoryBackupRehearsalJson.string(record, "backendProfile")), "restore.backend_profile",
                    "managed_backend_profiles row equals recorded identity for generation " + generation);
            var sequences = ledger.sequences();
            checks.require(sequences.equals(toLongs(RepositoryBackupRehearsalJson.object(manifest, "sequences"))), "restore.sequences", "sequence values=" + sequences);
            var catalog = ledger.schemaCatalog();
            checks.require(catalog.equals(toLongs(RepositoryBackupRehearsalJson.object(record, "schemaCatalog"))), "restore.schema_catalog", "catalog digests=" + catalog.size());
            checks.require(ledger.migrationLevel() == RepositoryBackupRehearsalJson.number(RepositoryBackupRehearsalJson.object(manifest, "database"), "migrationLevel")
                    && ledger.migrationLevel() == 119, "restore.migration_level", "flyway level V" + ledger.migrationLevel());
            checks.require(ledger.currentXid() > ledger.maxStoredXid(), "restore.xid_past_stored", "current=" + ledger.currentXid() + " maxStored=" + ledger.maxStoredXid());
        }
        try (var database = new LedgerDatabase(config.ledger()); var host = open(config, offline, bundle)) {
            var tx = new Tx(database.entityManagerFactory());
            try (var ledger = RepositoryBackupRehearsalLedger.fromEnvironment()) {
                long active = ledger.readerIncarnations("ACTIVE");
                checks.require(active > restoredActive, "restore.fresh_incarnation", "recovered host registered " + (active - restoredActive)
                        + " new ACTIVE incarnation(s); restored states unchanged");
            }
            var content = new RepositoryBackupRehearsalContentChecks(host, checks, identities, record);
            content.verifyHistory("restore");
            content.verifyMaterialization("restore");
            RepositoryBackupRehearsalContentChecks.verifyProviderObjects(checks, "restore", endpoint, RepositoryBackupRehearsalJson.list(record, "documentObjects"));
            content.verifyReplay("restore");
            var matrix = content.verifyAccess("restore");
            checks.require(RepositoryBackupRehearsalJson.canonical(new LinkedHashMap<>(matrix)).equals(RepositoryBackupRehearsalJson.canonical(RepositoryBackupRehearsalJson.object(record, "accessMatrix"))),
                    "restore.access_matrix_unchanged", "matrix=" + matrix);
            content.verifyTransport("restore");
            var pendingIntent = DocumentPublicationIntent.parseFrom(RepositoryBackupRehearsalContentChecks.read(identities, "pending-intent.pb"));
            RepositoryBackupRehearsalPendingOperation.verifyRestored(tx, checks, RepositoryBackupRehearsalFixture.operator(),
                    RepositoryBackupRehearsalJson.object(record, "pending"), pendingIntent, "restore");
            checks.require(offline.offlineAttempts.get() == 0, "restore.schema.no_live_lookup", "live schema resolution attempts during historical verification=" + offline.offlineAttempts.get());
            freshWrites(checks, host, offline, identities, record, manifest);
            content.verifyHistory("after-write");
            content.verifyMaterialization("after-write");
            Files.writeString(ready, "READY " + java.time.Instant.now() + "\n");
            checks.pass("restore.ready", "recovered host advertised ready after verification");
        }
    }

    /** New document revision through the ordinary path; the registry is rebuilt only from retained catalog bytes. */
    private static void freshWrites(RepositoryBackupRehearsalChecks checks, RepoServices host, RepositoryBackupRehearsalFixture.SwitchableSchemas offline,
            Path identities, Map<String, Object> record, Map<String, Object> manifest) throws Exception {
        var revisions = RepositoryBackupRehearsalJson.list(record, "revisions");
        var last = revisions.stream().filter(revision -> RepositoryBackupRehearsalJson.string(revision, "tag").equals("alpha-v2")).findFirst().orElseThrow();
        String account = RepositoryBackupRehearsalJson.string(last, "account");
        String drive = RepositoryBackupRehearsalJson.string(RepositoryBackupRehearsalJson.object(RepositoryBackupRehearsalJson.object(record, "drives"), account), "id");
        long restoredDocumentSequence = RepositoryBackupRehearsalJson.number(RepositoryBackupRehearsalJson.object(manifest, "sequences"), "document_mutation_revision_seq");
        var types = RepositoryBackupRehearsalFixture.types();
        var unregistered = RepositoryBackupRehearsalFixture.fixture(account, drive, RepositoryBackupRehearsalFixture.payload(types, "alpha-unregistered", 1_700_000_150L,
                "attempt without any registry", "attachment-none", new byte[] {1}), RepositoryBackupRehearsalFixture.memberOnlySecurity(account),
                RepositoryBackupRehearsalJson.number(last, "mutationRevision"));
        String refused = RepositoryBackupRehearsalContentChecks.outcome(() -> host.publicationRepository().publishDocument(RepositoryBackupRehearsalFixture.operator(),
                unregistered.request(), NONE));
        checks.require(!refused.equals("OK") && offline.offlineAttempts.get() >= 1, "after-write.typed_without_registry_refused",
                "typed publication with no registry -> " + refused);
        // Rebuild a registry from the retained catalog bytes delivered by materialized reads, never from current linked descriptors.
        var retained = new LinkedHashMap<String, ai.protomolt.proto.repo.admission.DocumentSchemaAdmission.Definition>();
        var member = RepositoryBackupRehearsalFixture.member(account);
        for (var selection : RepositoryBackupRehearsalJson.list(last, "selections")) {
            var selector = new HistoricalMaterializationRepository.Selection((int) RepositoryBackupRehearsalJson.number(selection, "revisionOrdinal"),
                    RepositoryBackupRehearsalJson.string(selection, "rootSha256"), RepositoryBackupRehearsalJson.string(selection, "pathSha256"));
            try (var result = host.historicalMaterializationRepository().readMaterialized(member, RepositoryBackupRehearsalFixture.address(account),
                    UUID.fromString(RepositoryBackupRehearsalJson.string(last, "revisionId")), selector, RepositoryBackupRehearsalFixture.LIMITS, NONE)) {
                var definition = result.view(NONE).definition();
                retained.put(definition.metadata().getTypeUrl(), new ai.protomolt.proto.repo.admission.DocumentSchemaAdmission.Definition(
                        definition.metadata(), definition.descriptorArtifact(), java.util.Optional.empty()));
            }
        }
        var artifacts = RepositoryBackupRehearsalJson.object(record, "artifacts");
        for (var entry : retained.entrySet())
            checks.require(entry.getValue().metadata().getArtifactSha256().equals(artifacts.get(entry.getKey()))
                    && DocumentPartCodec.sha256Hex(entry.getValue().descriptors().toByteArray()).equals(artifacts.get(entry.getKey())),
                    "after-write.registry_rebuilt_from_retained_bytes." + entry.getKey().substring(entry.getKey().lastIndexOf('.') + 1),
                    "artifact sha256=" + entry.getValue().metadata().getArtifactSha256());
        checks.require(retained.size() == 2, "after-write.registry_definitions", "two retained definitions rebuilt: " + retained.keySet());
        var registry = Files.createTempDirectory("rehearsal-rebuilt-registry-");
        var live = RepositoryBackupRehearsalFixture.liveSchemas(registry, retained);
        offline.arm(live);
        PublishDocumentResponse receipt;
        var fresh = RepositoryBackupRehearsalFixture.fixture(account, drive, RepositoryBackupRehearsalFixture.payload(types, "alpha-v3", 1_700_000_400L,
                "revision committed after restore", "attachment-three", "alpha attachment bytes three".getBytes()),
                RepositoryBackupRehearsalFixture.memberOnlySecurity(account), RepositoryBackupRehearsalJson.number(last, "mutationRevision"));
        try {
            receipt = host.publicationRepository().publishDocument(RepositoryBackupRehearsalFixture.operator(), fresh.request(), NONE);
        } finally {
            offline.disarm();
            live.access().close();
            live.store().close();
            try (var paths = Files.walk(registry)) { for (var path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) Files.delete(path); }
        }
        var committed = receipt.getCommitted().getMembers(0);
        boolean distinct = revisions.stream().noneMatch(revision -> RepositoryBackupRehearsalJson.string(revision, "revisionId").equals(committed.getRevisionId()));
        checks.require(receipt.hasCommitted() && distinct && committed.getMutationRevision() > RepositoryBackupRehearsalJson.number(last, "mutationRevision")
                && committed.getMutationRevision() > restoredDocumentSequence, "after-write.new_revision",
                "revision=" + committed.getRevisionId() + " mutation=" + committed.getMutationRevision() + " > restored sequence " + restoredDocumentSequence);
        checks.require(live.opens().get() >= 1, "after-write.rebuilt_registry_used", "resolutions=" + live.opens().get());
        try (var raw = host.historicalRepository().readRaw(member, RepositoryBackupRehearsalFixture.address(account), UUID.fromString(committed.getRevisionId()), NONE)) {
            checks.require(raw.publicationRevision() == committed.getMutationRevision(), "after-write.readable", "new revision readable");
        }
        try (var ledger = RepositoryBackupRehearsalLedger.fromEnvironment()) {
            checks.require(ledger.sequences().get("document_mutation_revision_seq") > restoredDocumentSequence, "after-write.sequences_advanced",
                    "document sequence " + ledger.sequences().get("document_mutation_revision_seq") + " > restored " + restoredDocumentSequence);
            var recorded = RepositoryBackupRehearsalJson.list(record, "documentObjects");
            var ids = new ArrayList<UUID>();
            for (var revision : revisions) ids.add(UUID.fromString(RepositoryBackupRehearsalJson.string(revision, "revisionId")));
            var objects = ledger.documentObjects(ids);
            checks.require(RepositoryBackupRehearsalJson.canonical(Map.of("o", objects)).equals(RepositoryBackupRehearsalJson.canonical(Map.of("o", recorded))),
                    "after-write.old_bindings_intact", "restored object bindings unchanged by the new revision");
        }
    }

    private static void wrongEndpoint(RepositoryBackupRehearsalChecks checks, RepoServiceConfig config, ai.protomolt.proto.repo.service.ManagedSchemaAccess offline, Path bundle) {
        try (var host = open(config, offline, bundle)) {
            checks.require(false, "wrong-endpoint.refused", "host started under the recorded generation with a different endpoint: " + host);
        } catch (RuntimeException refusal) {
            boolean bindingRefusal = false;
            for (Throwable cause = refusal; cause != null; cause = cause.getCause())
                if (cause.getMessage() != null && cause.getMessage().contains("already bound to another physical profile")) bindingRefusal = true;
            checks.require(bindingRefusal, "wrong-endpoint.refused", refusal.getClass().getName() + ": " + refusal.getMessage());
        }
    }

    private static void newGeneration(RepositoryBackupRehearsalChecks checks, RepoServiceConfig config, ai.protomolt.proto.repo.service.ManagedSchemaAccess offline,
            Path bundle, Path identities, Map<String, Object> record, String generation) throws Exception {
        checks.require(!generation.equals(RepositoryBackupRehearsalJson.string(record, "generation")), "new-generation.distinct", "generation=" + generation);
        try (var host = open(config, offline, bundle)) {
            checks.pass("new-generation.host_started", "a new generation binds freely at the recorded endpoint");
            var content = new RepositoryBackupRehearsalContentChecks(host, checks, identities, record);
            for (var revision : content.revisions()) {
                String account = RepositoryBackupRehearsalJson.string(revision, "account");
                var id = UUID.fromString(RepositoryBackupRehearsalJson.string(revision, "revisionId"));
                String raw = RepositoryBackupRehearsalContentChecks.outcome(() -> host.historicalRepository().readRaw(RepositoryBackupRehearsalFixture.member(account),
                        RepositoryBackupRehearsalFixture.address(account), id, NONE).close());
                checks.require(raw.equals("FAILED_PRECONDITION"), "new-generation.history_refused." + RepositoryBackupRehearsalJson.string(revision, "tag"),
                        "readRaw -> " + raw + " (original generation not configured on this host)");
            }
        }
    }

    private static void missingVersion(RepositoryBackupRehearsalChecks checks, RepoServiceConfig config, ai.protomolt.proto.repo.service.ManagedSchemaAccess offline,
            Path bundle, Path identities, Map<String, Object> record, String endpoint) throws Exception {
        var damaged = RepositoryBackupRehearsalJson.object(record, "injectedMissingVersion");
        try (var client = RepositoryBackupRehearsalFixture.s3(endpoint)) {
            String refused = RepositoryBackupRehearsalContentChecks.outcome(() -> client.getObjectAsBytes(builder -> builder.bucket(RepositoryBackupRehearsalJson.string(damaged, "namespace"))
                    .key(RepositoryBackupRehearsalJson.string(damaged, "key")).versionId(RepositoryBackupRehearsalJson.string(damaged, "providerVersion"))));
            checks.require(refused.startsWith("NoSuchKeyException") || refused.startsWith("NoSuchVersionException"), "missing-version.provider_version_absent",
                    "GET recorded version -> " + refused);
            var latest = client.getObjectAsBytes(builder -> builder.bucket(RepositoryBackupRehearsalJson.string(damaged, "namespace")).key(RepositoryBackupRehearsalJson.string(damaged, "key")));
            checks.require(!latest.response().versionId().equals(RepositoryBackupRehearsalJson.string(damaged, "providerVersion"))
                    && !DocumentPartCodec.sha256Hex(latest.asByteArray()).equals(RepositoryBackupRehearsalJson.string(damaged, "sha256")),
                    "missing-version.latest_differs", "a newer latest version exists under the same key: " + latest.response().versionId());
        }
        try (var host = open(config, offline, bundle)) {
            var content = new RepositoryBackupRehearsalContentChecks(host, checks, identities, record);
            for (var revision : content.revisions()) {
                String tag = RepositoryBackupRehearsalJson.string(revision, "tag");
                String account = RepositoryBackupRehearsalJson.string(revision, "account");
                var member = RepositoryBackupRehearsalFixture.member(account);
                var address = RepositoryBackupRehearsalFixture.address(account);
                var id = UUID.fromString(RepositoryBackupRehearsalJson.string(revision, "revisionId"));
                boolean affected = RepositoryBackupRehearsalJson.string(damaged, "revisionId").equals(id.toString());
                String raw = RepositoryBackupRehearsalContentChecks.outcome(() -> host.historicalRepository().readRaw(member, address, id, NONE).close());
                String validated = RepositoryBackupRehearsalContentChecks.outcome(() -> host.historicalRepository().readValidated(member, address, id, NONE).close());
                if (affected) {
                    checks.require(raw.equals("DATA_LOSS"), "missing-version.raw_refused", "readRaw(" + tag + ") -> " + raw);
                    checks.require(validated.equals("DATA_LOSS"), "missing-version.validated_refused", "readValidated(" + tag + ") -> " + validated);
                    for (var selection : RepositoryBackupRehearsalJson.list(revision, "selections")) {
                        var selector = new HistoricalMaterializationRepository.Selection((int) RepositoryBackupRehearsalJson.number(selection, "revisionOrdinal"),
                                RepositoryBackupRehearsalJson.string(selection, "rootSha256"), RepositoryBackupRehearsalJson.string(selection, "pathSha256"));
                        String materialized = RepositoryBackupRehearsalContentChecks.outcome(() -> { try (var result = host.historicalMaterializationRepository()
                                .readMaterialized(member, address, id, selector, RepositoryBackupRehearsalFixture.LIMITS, NONE)) { result.view(NONE); } });
                        checks.require(materialized.equals("DATA_LOSS"), "missing-version.materialized_refused." + RepositoryBackupRehearsalJson.number(selection, "steps"),
                                "readMaterialized(" + tag + ") -> " + materialized + "; the newer version was not substituted");
                    }
                } else {
                    var expected = Document.parseFrom(RepositoryBackupRehearsalContentChecks.read(identities, "document-" + tag + ".pb"));
                    try (var read = host.historicalRepository().readValidated(member, address, id, NONE)) {
                        checks.require(read.document().equals(expected), "missing-version.unaffected." + tag, "unaffected revision still reads byte-equal");
                    }
                }
            }
        }
    }

    /** The retained artifact is damaged or absent; a live registry holding the correct definition must never be consulted. */
    private static void damagedDescriptor(String mode, RepositoryBackupRehearsalChecks checks, RepoServiceConfig config,
            RepositoryBackupRehearsalFixture.SwitchableSchemas offline, Path bundle, Path identities, Map<String, Object> record) throws Exception {
        var registry = Files.createTempDirectory("rehearsal-live-registry-");
        var live = RepositoryBackupRehearsalFixture.liveSchemas(registry, RepositoryBackupRehearsalFixture.definitions(RepositoryBackupRehearsalFixture.types()));
        offline.arm(live);
        String damagedArtifact = RepositoryBackupRehearsalJson.string(record, "injectedArtifact");
        try (var host = open(config, offline, bundle)) {
            var content = new RepositoryBackupRehearsalContentChecks(host, checks, identities, record);
            for (var revision : content.revisions()) {
                String tag = RepositoryBackupRehearsalJson.string(revision, "tag");
                String account = RepositoryBackupRehearsalJson.string(revision, "account");
                var member = RepositoryBackupRehearsalFixture.member(account);
                var address = RepositoryBackupRehearsalFixture.address(account);
                var id = UUID.fromString(RepositoryBackupRehearsalJson.string(revision, "revisionId"));
                var fragments = new ArrayList<String>();
                try (var raw = host.historicalRepository().readRaw(member, address, id, NONE)) {
                    for (var fragment : raw.fragments()) { var bytes = new byte[fragment.bytes().remaining()]; fragment.bytes().get(bytes); fragments.add(DocumentPartCodec.sha256Hex(bytes)); }
                }
                var recorded = RepositoryBackupRehearsalJson.list(revision, "rawFragments").stream().map(f -> RepositoryBackupRehearsalJson.string(f, "sha256")).toList();
                checks.require(fragments.equals(recorded), mode + ".raw_explicit_opaque." + tag, "raw fragments still byte-equal: explicit opaque access, no validity claim");
                String validated = RepositoryBackupRehearsalContentChecks.outcome(() -> host.historicalRepository().readValidated(member, address, id, NONE).close());
                checks.require(validated.equals("DATA_LOSS"), mode + ".validated_refused." + tag, "readValidated -> " + validated);
                // An occurrence whose artifact is damaged refuses. An occurrence whose own artifact is intact behaves
                // by damage kind: a missing row makes the revision's retained snapshot incomplete and refuses every
                // typed read; corrupt bytes are detected only where they are used, so the intact root still decodes
                // (and must decode the recorded value). Both are explicit; neither substitutes the live registry.
                for (var selection : RepositoryBackupRehearsalJson.list(revision, "selections")) {
                    var selector = new HistoricalMaterializationRepository.Selection((int) RepositoryBackupRehearsalJson.number(selection, "revisionOrdinal"),
                            RepositoryBackupRehearsalJson.string(selection, "rootSha256"), RepositoryBackupRehearsalJson.string(selection, "pathSha256"));
                    boolean affected = RepositoryBackupRehearsalJson.string(selection, "artifactSha256").equals(damagedArtifact);
                    var decoded = new String[1];
                    String materialized = RepositoryBackupRehearsalContentChecks.outcome(() -> { try (var result = host.historicalMaterializationRepository()
                            .readMaterialized(member, address, id, selector, RepositoryBackupRehearsalFixture.LIMITS, NONE)) {
                        var view = result.view(NONE);
                        decoded[0] = view.schema().getArtifactSha256() + "/" + view.value().getDescriptorForType().getFullName();
                    } });
                    String name = mode + ".materialized." + tag + "." + (RepositoryBackupRehearsalJson.number(selection, "steps") == 1 ? "root" : "nested");
                    if (affected) checks.require(materialized.equals("DATA_LOSS"), name, "readMaterialized -> " + materialized + "; damaged artifact, no typed value invented");
                    else if (mode.equals("missing-descriptor")) checks.require(materialized.equals("DATA_LOSS"), name,
                            "readMaterialized -> " + materialized + "; intact artifact, but the revision's retained snapshot is incomplete");
                    else checks.require(materialized.equals("OK") && decoded[0].equals(RepositoryBackupRehearsalJson.string(selection, "artifactSha256") + "/" + RepositoryBackupRehearsalFixture.RECORD_TYPE),
                            name, "readMaterialized -> " + materialized + "; intact root artifact decodes its own retained definition " + decoded[0]);
                }
            }
            checks.require(live.opens().get() == 0, mode + ".live_registry_never_consulted", "armed live registry with the correct definition: resolutions=" + live.opens().get());
        } finally {
            offline.disarm();
            live.access().close(); live.store().close();
            try (var paths = Files.walk(registry)) { for (var path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) Files.delete(path); }
        }
    }

    /** Wrong provider credentials refuse the affected operations explicitly; nothing falls back or caches. */
    private static void wrongCredentials(RepositoryBackupRehearsalChecks checks, String endpoint, String generation, String realm,
            RepositoryBackupRehearsalFixture.SwitchableSchemas offline, Path bundle, Path identities, Map<String, Object> record) throws Exception {
        String wrongSecret = "wrong-" + UUID.randomUUID();
        var config = RepositoryBackupRehearsalFixture.config(endpoint, generation, realm, RepositoryBackupRehearsalFixture.env("PROTOMOLT_REHEARSAL_S3_ACCESS"), wrongSecret);
        try (var client = RepositoryBackupRehearsalFixture.s3(endpoint, RepositoryBackupRehearsalFixture.env("PROTOMOLT_REHEARSAL_S3_ACCESS"), wrongSecret)) {
            var object = RepositoryBackupRehearsalJson.list(record, "documentObjects").getFirst();
            String direct = RepositoryBackupRehearsalContentChecks.outcome(() -> client.getObjectAsBytes(builder -> builder.bucket(RepositoryBackupRehearsalJson.string(object, "namespace"))
                    .key(RepositoryBackupRehearsalJson.string(object, "key")).versionId(RepositoryBackupRehearsalJson.string(object, "providerVersion"))));
            checks.require(!direct.equals("OK") && (direct.contains("403") || direct.toLowerCase().contains("signature") || direct.toLowerCase().contains("access")),
                    "wrong-credentials.provider_refuses", "direct GET with the wrong secret -> " + direct);
        }
        // Composition binds the backend identity from the ledger without a provider call, so the host starts;
        // the first provider read is where the credential is refused. Both facts are asserted, not just recorded.
        try (var host = open(config, offline, bundle)) {
            checks.pass("wrong-credentials.host", "host composition with the wrong provider secret succeeds (no provider call at composition)");
            var content = new RepositoryBackupRehearsalContentChecks(host, checks, identities, record);
            for (var revision : content.revisions()) {
                String tag = RepositoryBackupRehearsalJson.string(revision, "tag");
                String account = RepositoryBackupRehearsalJson.string(revision, "account");
                var id = UUID.fromString(RepositoryBackupRehearsalJson.string(revision, "revisionId"));
                String raw = RepositoryBackupRehearsalContentChecks.outcome(() -> host.historicalRepository().readRaw(RepositoryBackupRehearsalFixture.member(account),
                        RepositoryBackupRehearsalFixture.address(account), id, NONE).close());
                checks.require(raw.startsWith("BlobStoreException") && raw.contains("rejected the read"), "wrong-credentials.history_refused." + tag, "readRaw -> " + raw);
            }
        }
    }

    /** The catalog is one capture and the provider volume another: every recorded version is absent and reads refuse. */
    private static void inconsistentSnapshot(RepositoryBackupRehearsalChecks checks, RepoServiceConfig config, ai.protomolt.proto.repo.service.ManagedSchemaAccess offline,
            Path bundle, Path identities, Map<String, Object> record, String endpoint) throws Exception {
        var objects = RepositoryBackupRehearsalJson.list(record, "documentObjects");
        try (var client = RepositoryBackupRehearsalFixture.s3(endpoint)) {
            int absent = 0;
            for (var object : objects) {
                String outcome = RepositoryBackupRehearsalContentChecks.outcome(() -> client.getObjectAsBytes(builder -> builder.bucket(RepositoryBackupRehearsalJson.string(object, "namespace"))
                        .key(RepositoryBackupRehearsalJson.string(object, "key")).versionId(RepositoryBackupRehearsalJson.string(object, "providerVersion"))));
                if (outcome.startsWith("NoSuchKeyException") || outcome.startsWith("NoSuchVersionException") || outcome.startsWith("NoSuchBucketException")) absent++;
                else throw new RepositoryBackupRehearsalFailure("Unexpected provider outcome for a recorded version: " + outcome);
            }
            checks.require(absent == objects.size(), "inconsistent-snapshot.provider_versions_absent", absent + "/" + objects.size() + " recorded versions absent from the other capture's provider");
        }
        try (var database = new LedgerDatabase(config.ledger()); var host = open(config, offline, bundle)) {
            var content = new RepositoryBackupRehearsalContentChecks(host, checks, identities, record);
            for (var revision : content.revisions()) {
                String tag = RepositoryBackupRehearsalJson.string(revision, "tag");
                String account = RepositoryBackupRehearsalJson.string(revision, "account");
                var id = UUID.fromString(RepositoryBackupRehearsalJson.string(revision, "revisionId"));
                String raw = RepositoryBackupRehearsalContentChecks.outcome(() -> host.historicalRepository().readRaw(RepositoryBackupRehearsalFixture.member(account),
                        RepositoryBackupRehearsalFixture.address(account), id, NONE).close());
                String validated = RepositoryBackupRehearsalContentChecks.outcome(() -> host.historicalRepository().readValidated(RepositoryBackupRehearsalFixture.member(account),
                        RepositoryBackupRehearsalFixture.address(account), id, NONE).close());
                checks.require(raw.startsWith("BlobStoreException") && raw.contains("rejected the read") && validated.startsWith("BlobStoreException"),
                        "inconsistent-snapshot.history_refused." + tag, "readRaw -> " + raw + "; readValidated -> " + validated);
            }
            // SQL-only evidence is still the catalog's own: receipts replay from the ledger, the pending chain is intact.
            content.verifyReplay("inconsistent-snapshot");
            var tx = new Tx(database.entityManagerFactory());
            var pending = RepositoryBackupRehearsalJson.object(record, "pending");
            var state = RepositoryBackupRehearsalPendingOperation.coverage(tx, UUID.fromString(RepositoryBackupRehearsalJson.string(pending, "operationId")));
            checks.require(RepositoryBackupRehearsalJson.canonical(state).equals(RepositoryBackupRehearsalJson.canonical(RepositoryBackupRehearsalJson.object(pending, "coverage"))),
                    "inconsistent-snapshot.catalog_intact", "catalog evidence is the recorded capture's; only provider content is from another capture");
        }
    }
}
