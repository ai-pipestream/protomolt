package ai.protomolt.proto.repo.recovery;

import ai.protomolt.proto.asset.bridge.BridgeEngine;
import ai.protomolt.proto.repo.archive.v1.*;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.service.RepoServices;
import ai.protomolt.proto.repo.spi.HistoricalMaterializationRepository;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.ByteString;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Fresh production-JAR host over restored resources. No schema registry exists; a live
 * resolution attempt fails the run. READY is written only after every verification passed.
 * Negative modes observe the refusal the restored content must produce and never write READY.
 */
public final class RecoveredHost {
    public static void main(String[] args) throws Exception {
        RehearsalFixture.requireNoTestFramework();
        String mode = RehearsalFixture.env("PROTOMOLT_RECOVERY_MODE");
        var identities = Path.of(RehearsalFixture.env("PROTOMOLT_RECOVERY_IDENTITIES"));
        var out = Path.of(RehearsalFixture.env("PROTOMOLT_RECOVERY_OUT"));
        Files.createDirectories(out);
        var checks = new Checks("recovered-" + mode, out.resolve("markers.log"));
        var record = Json.read(identities.resolve("identities.json"));
        // Quiescent-capture values (sequences, counts) come from the sealed manifest, not from the live seed record.
        var manifest = Json.read(Path.of(RehearsalFixture.env("PROTOMOLT_RECOVERY_MANIFEST")));
        String endpoint = RehearsalFixture.env("PROTOMOLT_RECOVERY_S3_ENDPOINT");
        String generation = RehearsalFixture.env("PROTOMOLT_RECOVERY_GENERATION");
        String realm = RehearsalFixture.env("PROTOMOLT_RECOVERY_REALM");
        var bundle = Path.of(RehearsalFixture.env("PROTOMOLT_RECOVERY_BUNDLE"));
        var config = RehearsalFixture.config(endpoint, generation, realm);
        var offline = new RehearsalFixture.SwitchableSchemas();
        var attempts = offline.offlineAttempts;
        var ready = out.resolve("READY");
        try {
            switch (mode) {
                case "verify" -> verify(checks, config, offline, bundle, identities, record, manifest, endpoint, generation, ready);
                case "wrong-endpoint" -> wrongEndpoint(checks, config, offline, bundle, record);
                case "new-generation" -> newGeneration(checks, config, offline, bundle, identities, record, generation);
                case "missing-version" -> missingVersion(checks, config, offline, bundle, identities, record);
                case "corrupt-descriptor" -> corruptDescriptor(checks, config, offline, bundle, identities, record);
                default -> throw new RehearsalFailure("Unknown recovered host mode " + mode);
            }
            // verify mode asserts zero attempts before its fresh write and then deliberately provokes one refusal.
            if (!mode.equals("verify")) checks.require(attempts.get() == 0, mode + ".schema.no_live_lookup", "live schema resolution attempts=" + attempts.get());
        } finally {
            checks.writeXml(out.resolve("results.xml"));
        }
        System.out.println("RECOVERED_HOST_" + mode.toUpperCase().replace('-', '_') + "_OK");
    }

    private static RepoServices open(ai.protomolt.proto.repo.service.RepoServiceConfig config, ai.protomolt.proto.repo.service.ManagedSchemaAccess schemas, Path bundle) {
        return RepoServices.build(config, BridgeEngine.standard(), RehearsalFixture.historicalAccess(), schemas, RehearsalFixture.publicationOptions(bundle));
    }

    private static Ledger ledger() {
        return new Ledger(RehearsalFixture.env("PROTOMOLT_RECOVERY_JDBC"), RehearsalFixture.env("PROTOMOLT_RECOVERY_DB_USER"), RehearsalFixture.env("PROTOMOLT_RECOVERY_DB_PASSWORD"));
    }

    private static void verify(Checks checks, ai.protomolt.proto.repo.service.RepoServiceConfig config, RehearsalFixture.SwitchableSchemas offline,
            Path bundle, Path identities, Map<String, Object> record, Map<String, Object> manifest, String endpoint, String generation, Path ready) throws Exception {
        long restoredIncarnations;
        try (var ledger = ledger()) {
            var counts = ledger.rowCounts();
            restoredIncarnations = counts.get("repository_reader_incarnations");
            checks.require(counts.equals(toLongs(Json.object(manifest, "rowCounts"))), "restore.row_counts", "identity table counts equal the sealed manifest: " + counts);
            checks.require(restoredIncarnations == Json.number(record, "readerIncarnations"), "restore.reader_incarnations_restored",
                    "restored reader incarnations=" + restoredIncarnations + " (the seed host's registration, restored verbatim, never reused)");
            checks.require(ledger.backendProfile(generation).equals(Json.object(record, "backendProfile")), "restore.backend_profile",
                    "managed_backend_profiles row equals recorded identity for generation " + generation);
            var sequences = ledger.sequences();
            checks.require(sequences.equals(toLongs(Json.object(manifest, "sequences"))), "restore.sequences", "sequence values=" + sequences);
            var catalog = ledger.schemaCatalog();
            checks.require(catalog.equals(toLongs(Json.object(record, "schemaCatalog"))), "restore.schema_catalog", "catalog digests=" + catalog.size());
            checks.require(ledger.migrationLevel() == Json.number(Json.object(manifest, "database"), "migrationLevel"), "restore.migration_level", "flyway level V" + ledger.migrationLevel());
            checks.require(ledger.currentXid() > ledger.maxStoredXid(), "restore.xid_past_stored", "current=" + ledger.currentXid() + " maxStored=" + ledger.maxStoredXid());
        }
        try (var host = open(config, offline, bundle)) {
            try (var ledger = ledger()) {
                long total = ledger.rowCounts().get("repository_reader_incarnations"), active = ledger.readerIncarnations("ACTIVE");
                checks.require(total > restoredIncarnations && active == total - restoredIncarnations && ledger.readerIncarnations("QUIESCED") == restoredIncarnations,
                        "restore.fresh_incarnation", "restored incarnations=" + restoredIncarnations + " all QUIESCED; recovered host registered " + (total - restoredIncarnations)
                        + " new ACTIVE incarnation(s); none restored revived");
            }
            var content = new ContentChecks(host, checks, identities, record);
            content.verifyHistory("restore");
            content.verifyMaterialization("restore");
            content.verifyArchive("restore");
            ContentChecks.verifyProviderObjects(checks, "restore", endpoint, SeedHost.allObjects(record));
            content.verifyReplay("restore");
            var matrix = content.verifyAccess("restore");
            checks.require(matrix.equals(Json.object(record, "accessMatrix")), "restore.access_matrix_unchanged", "matrix=" + matrix);
            content.verifyTransport("restore");
            checks.require(offline.offlineAttempts.get() == 0, "restore.schema.no_live_lookup", "live schema resolution attempts during historical verification=" + offline.offlineAttempts.get());
            freshWrites(checks, host, offline, identities, record, manifest);
            content.verifyHistory("after-write");
            content.verifyMaterialization("after-write");
            content.verifyArchive("after-write");
            Files.writeString(ready, "READY " + java.time.Instant.now() + "\n");
            checks.pass("restore.ready", "recovered host advertised ready after verification");
        }
    }

    /** New document revision and archive version through the ordinary paths; sequences continue past restored values. */
    @SuppressWarnings("unchecked")
    private static void freshWrites(Checks checks, RepoServices host, RehearsalFixture.SwitchableSchemas offline, Path identities, Map<String, Object> record, Map<String, Object> manifest) throws Exception {
        String account = Json.string(record, "account");
        var revisions = (List<Map<String, Object>>) record.get("revisions");
        var last = revisions.getLast();
        long restoredDocumentSequence = Json.number(Json.object(manifest, "sequences"), "document_mutation_revision_seq");
        long restoredArchiveSequence = Json.number(Json.object(manifest, "sequences"), "archive_mutation_revision_seq");
        var type = RehearsalFixture.recordType();
        var v3 = RehearsalFixture.fixture(account, Json.string(Json.object(record, "drive"), "id"),
                RehearsalFixture.payload(type, "rehearsal-v3", 1_700_000_200L, "revision committed after restore"),
                RehearsalFixture.memberOnlySecurity(), Json.number(last, "mutationRevision"));
        // Typed admission needs a schema definition. The original registry is gone, so a fresh one is
        // rebuilt from the retained catalog bytes delivered by a materialized read, never from current
        // linked descriptors. A write without any registry is refused first, as it must be.
        var unregistered = RehearsalFixture.fixture(account, Json.string(Json.object(record, "drive"), "id"),
                RehearsalFixture.payload(type, "rehearsal-unregistered", 1_700_000_150L, "attempt without any registry"),
                RehearsalFixture.memberOnlySecurity(), Json.number(last, "mutationRevision"));
        String refused = outcome(() -> host.publicationRepository().publishDocument(RehearsalFixture.operator(), unregistered.request(), RepositoryReadControl.NONE));
        checks.require(!refused.equals("OK") && offline.offlineAttempts.get() >= 1, "after-write.document.typed_without_registry_refused",
                "typed publication with no registry -> " + refused);
        var selection = Json.object(record, "materialization");
        var selector = new HistoricalMaterializationRepository.Selection((int) Json.number(selection, "revisionOrdinal"), Json.string(selection, "rootSha256"), Json.string(selection, "pathSha256"));
        ai.protomolt.proto.repo.admission.DocumentSchemaAdmission.Definition retained;
        try (var result = host.historicalMaterializationRepository().readMaterialized(RehearsalFixture.member(account), RehearsalFixture.address(account),
                UUID.fromString(Json.string(revisions.getFirst(), "revisionId")), selector,
                new HistoricalMaterializationRepository.Limits(4_000_000, 4_000_000, 16_000_000, 64, 8_000_000, 64), RepositoryReadControl.NONE)) {
            var definition = result.view(RepositoryReadControl.NONE).definition();
            retained = new ai.protomolt.proto.repo.admission.DocumentSchemaAdmission.Definition(definition.metadata(), definition.descriptorArtifact(), java.util.Optional.empty());
        }
        checks.require(retained.metadata().getArtifactSha256().equals(Json.string(record, "schemaArtifactSha256"))
                && DocumentPartCodec.sha256Hex(retained.descriptors().toByteArray()).equals(Json.string(record, "schemaArtifactSha256")),
                "after-write.registry_rebuilt_from_retained_bytes", "artifact sha256=" + retained.metadata().getArtifactSha256());
        var registry = Files.createTempDirectory("recovery-rebuilt-registry-");
        var live = RehearsalFixture.liveSchemas(registry, retained);
        offline.arm(live);
        PublishDocumentResponse receipt3;
        try {
            receipt3 = host.publicationRepository().publishDocument(RehearsalFixture.operator(), v3.request(), RepositoryReadControl.NONE);
        } finally {
            offline.disarm();
            live.access().close();
            live.store().close();
            try (var paths = Files.walk(registry)) { for (var path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) Files.delete(path); }
        }
        var member3 = receipt3.getCommitted().getMembers(0);
        boolean fresh = revisions.stream().noneMatch(revision -> Json.string(revision, "revisionId").equals(member3.getRevisionId()));
        checks.require(receipt3.hasCommitted() && fresh && member3.getMutationRevision() > Json.number(last, "mutationRevision")
                && member3.getMutationRevision() > restoredDocumentSequence, "after-write.document.new_revision",
                "revision=" + member3.getRevisionId() + " mutation=" + member3.getMutationRevision() + " > restored sequence " + restoredDocumentSequence);
        checks.require(live.opens().get() >= 1, "after-write.document.rebuilt_registry_used", "resolutions=" + live.opens().get());
        try (var raw = host.historicalRepository().readRaw(RehearsalFixture.member(account), RehearsalFixture.address(account),
                UUID.fromString(member3.getRevisionId()), RepositoryReadControl.NONE)) {
            checks.require(raw.publicationRevision() == member3.getMutationRevision(), "after-write.document.readable", "new revision readable");
        }
        var archive = Json.object(record, "archive");
        var entry = EntryAddress.newBuilder().setAccountId(account).setArchive(Json.string(archive, "name")).setEntryId(Json.string(archive, "entryId")).build();
        var put3 = host.archiveRepository().putEntry(RehearsalFixture.operator(), PutEntryRequest.newBuilder().setAddress(entry).setTitle("Restored report")
                .addRenditions(RenditionContent.newBuilder().setRendition(RenditionDescriptor.newBuilder().setName("markdown").setMediaType("text/markdown"))
                        .setData(ByteString.copyFromUtf8("# Report, restored"))).build());
        checks.require(put3.getVersion() == 3 && !put3.getDeduplicated() && put3.getManifest().getMetadataSnapshot().getCurrentVersion() == 3,
                "after-write.archive.new_version", "archive version 3 committed after restore");
        try (var ledger = ledger()) {
            var sequences = ledger.sequences();
            checks.require(sequences.get("archive_mutation_revision_seq") > restoredArchiveSequence
                    && sequences.get("document_mutation_revision_seq") >= restoredDocumentSequence, "after-write.sequences_advanced",
                    "sequences=" + sequences + " restored=" + restoredDocumentSequence + "/" + restoredArchiveSequence);
            var objects = ledger.archiveObjects(UUID.fromString(Json.string(archive, "entryUuid")));
            var recorded = (List<Map<String, Object>>) record.get("archiveObjects");
            checks.require(objects.size() == recorded.size() + 2 && Json.normalize(objects.subList(0, recorded.size())).equals(Json.normalize(recorded)), "after-write.archive.old_bindings_intact",
                    "restored bindings unchanged, new version bound two objects (shared original, new markdown)");
        }
    }

    private static void wrongEndpoint(Checks checks, ai.protomolt.proto.repo.service.RepoServiceConfig config, ai.protomolt.proto.repo.service.ManagedSchemaAccess offline, Path bundle, Map<String, Object> record) {
        try (var host = open(config, offline, bundle)) {
            checks.require(false, "wrong-endpoint.refused", "host started under the recorded generation with a different endpoint: " + host);
        } catch (RuntimeException refusal) {
            boolean bindingRefusal = false;
            for (Throwable cause = refusal; cause != null; cause = cause.getCause())
                if (cause.getMessage() != null && cause.getMessage().contains("already bound to another physical profile")) bindingRefusal = true;
            checks.require(bindingRefusal, "wrong-endpoint.refused", refusal.getClass().getName() + ": " + refusal.getMessage());
        }
    }

    private static void newGeneration(Checks checks, ai.protomolt.proto.repo.service.RepoServiceConfig config, ai.protomolt.proto.repo.service.ManagedSchemaAccess offline,
            Path bundle, Path identities, Map<String, Object> record, String generation) throws Exception {
        checks.require(!generation.equals(Json.string(record, "generation")), "new-generation.distinct", "generation=" + generation);
        try (var host = open(config, offline, bundle)) {
            checks.pass("new-generation.host_started", "a new generation binds freely at the recorded endpoint");
            var content = new ContentChecks(host, checks, identities, record);
            String account = Json.string(record, "account");
            var first = UUID.fromString(Json.string(content.revisions().getFirst(), "revisionId"));
            String raw = outcome(() -> host.historicalRepository().readRaw(RehearsalFixture.member(account), RehearsalFixture.address(account), first, RepositoryReadControl.NONE).close());
            checks.require(!raw.equals("OK"), "new-generation.history_refused", "readRaw -> " + raw);
            var archive = Json.object(record, "archive");
            var entry = EntryAddress.newBuilder().setAccountId(account).setArchive(Json.string(archive, "name")).setEntryId(Json.string(archive, "entryId")).build();
            String archiveRead = outcome(() -> host.archiveRepository().getEntry(RehearsalFixture.operator(), GetEntryRequest.newBuilder().setAddress(entry).setVersion(1).build()));
            checks.require(!archiveRead.equals("OK"), "new-generation.archive_refused", "getEntry -> " + archiveRead);
        }
    }

    private static void missingVersion(Checks checks, ai.protomolt.proto.repo.service.RepoServiceConfig config, ai.protomolt.proto.repo.service.ManagedSchemaAccess offline,
            Path bundle, Path identities, Map<String, Object> record) throws Exception {
        try (var host = open(config, offline, bundle)) {
            var content = new ContentChecks(host, checks, identities, record);
            String account = Json.string(record, "account");
            var first = UUID.fromString(Json.string(content.revisions().get(0), "revisionId"));
            var second = UUID.fromString(Json.string(content.revisions().get(1), "revisionId"));
            var member = RehearsalFixture.member(account);
            String raw = outcome(() -> host.historicalRepository().readRaw(member, RehearsalFixture.address(account), first, RepositoryReadControl.NONE).close());
            String validated = outcome(() -> host.historicalRepository().readValidated(member, RehearsalFixture.address(account), first, RepositoryReadControl.NONE).close());
            checks.require(!raw.equals("OK"), "missing-version.raw_refused", "readRaw(v1) -> " + raw);
            checks.require(!validated.equals("OK"), "missing-version.validated_refused", "readValidated(v1) -> " + validated);
            var selection = Json.object(record, "materialization");
            var selector = new HistoricalMaterializationRepository.Selection((int) Json.number(selection, "revisionOrdinal"), Json.string(selection, "rootSha256"), Json.string(selection, "pathSha256"));
            String materialized = outcome(() -> { try (var result = host.historicalMaterializationRepository().readMaterialized(member, RehearsalFixture.address(account), first, selector,
                    new HistoricalMaterializationRepository.Limits(4_000_000, 4_000_000, 16_000_000, 64, 8_000_000, 64), RepositoryReadControl.NONE)) { result.view(RepositoryReadControl.NONE); } });
            checks.require(!materialized.equals("OK"), "missing-version.materialized_refused", "readMaterialized(v1) -> " + materialized);
            try (var second2 = host.historicalRepository().readValidated(member, RehearsalFixture.address(account), second, RepositoryReadControl.NONE)) {
                var expected = Document.parseFrom(ContentChecks.read(identities, "document-v2.pb"));
                checks.require(second2.document().equals(expected), "missing-version.latest_still_reads", "revision 2 unaffected; revision 1 was not served from the newer version");
            }
        }
    }

    private static void corruptDescriptor(Checks checks, ai.protomolt.proto.repo.service.RepoServiceConfig config, ai.protomolt.proto.repo.service.ManagedSchemaAccess offline,
            Path bundle, Path identities, Map<String, Object> record) throws Exception {
        try (var host = open(config, offline, bundle)) {
            var content = new ContentChecks(host, checks, identities, record);
            String account = Json.string(record, "account");
            var first = UUID.fromString(Json.string(content.revisions().getFirst(), "revisionId"));
            var member = RehearsalFixture.member(account);
            var fragments = new java.util.ArrayList<String>();
            try (var raw = host.historicalRepository().readRaw(member, RehearsalFixture.address(account), first, RepositoryReadControl.NONE)) {
                for (var fragment : raw.fragments()) { var bytes = new byte[fragment.bytes().remaining()]; fragment.bytes().get(bytes); fragments.add(DocumentPartCodec.sha256Hex(bytes)); }
            }
            @SuppressWarnings("unchecked") var recorded = ((List<Map<String, Object>>) content.revisions().getFirst().get("rawFragments")).stream().map(f -> Json.string(f, "sha256")).toList();
            checks.require(fragments.equals(recorded), "corrupt-descriptor.raw_explicit_opaque", "raw fragments still byte-equal: explicit opaque access, no validity claim");
            String validated = outcome(() -> host.historicalRepository().readValidated(member, RehearsalFixture.address(account), first, RepositoryReadControl.NONE).close());
            checks.require(validated.equals("DATA_LOSS"), "corrupt-descriptor.validated_refused", "readValidated(v1) -> " + validated);
            var selection = Json.object(record, "materialization");
            var selector = new HistoricalMaterializationRepository.Selection((int) Json.number(selection, "revisionOrdinal"), Json.string(selection, "rootSha256"), Json.string(selection, "pathSha256"));
            String materialized = outcome(() -> { try (var result = host.historicalMaterializationRepository().readMaterialized(member, RehearsalFixture.address(account), first, selector,
                    new HistoricalMaterializationRepository.Limits(4_000_000, 4_000_000, 16_000_000, 64, 8_000_000, 64), RepositoryReadControl.NONE)) { result.view(RepositoryReadControl.NONE); } });
            checks.require(materialized.equals("DATA_LOSS"), "corrupt-descriptor.materialized_refused", "readMaterialized(v1) -> " + materialized + "; no typed value invented");
        }
    }

    interface Action { void run() throws Exception; }

    /** The actual failure class: a RepositoryException code, or the exception type for anything else. */
    static String outcome(Action action) {
        try { action.run(); return "OK"; }
        catch (RepositoryException failure) { return failure.code().name(); }
        catch (Exception failure) { return failure.getClass().getSimpleName() + ": " + failure.getMessage(); }
    }

    private static Map<String, Long> toLongs(Map<String, Object> values) {
        var result = new java.util.TreeMap<String, Long>();
        values.forEach((key, value) -> result.put(key, value instanceof Number n ? n.longValue() : Long.parseLong(value.toString())));
        return result;
    }

}
