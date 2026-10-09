package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.archive.v1.ArchiveMutationReceipt;
import ai.protomolt.proto.repo.archive.v1.ArchiveMutationRequest;
import ai.protomolt.proto.repo.archive.v1.ArchiveMutationState;
import ai.protomolt.proto.repo.archive.v1.DeleteRenditionRequest;
import ai.protomolt.proto.repo.archive.v1.GetArchiveStatsRequest;
import ai.protomolt.proto.repo.archive.v1.GetEntryManifestRequest;
import ai.protomolt.proto.repo.archive.v1.GetEntryRequest;
import ai.protomolt.proto.repo.archive.v1.ListVersionsRequest;
import ai.protomolt.proto.repo.archive.v1.PutEntryRequest;
import ai.protomolt.proto.repo.archive.v1.RenditionState;
import ai.protomolt.proto.repo.archive.v1.VersionManifest;
import ai.protomolt.proto.repo.archive.v1.WriteAttribution;
import ai.protomolt.proto.repo.container.archive.ArchiveManifests;
import ai.protomolt.proto.repo.container.archive.ArchiveMutationObservations;
import ai.protomolt.proto.repo.service.RepoServiceConfig;
import ai.protomolt.proto.repo.service.RepoServices;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

/**
 * Fresh production-JAR host over restored resources. Provider identities and the pending
 * mutation's observation are verified before the host opens, because the host's mutation
 * recovery lane runs within a second of its first archive call. READY is written only after
 * every verification, the fresh write and the authorized lifecycle operation passed. Negative
 * modes observe the exact refusal the restored content must produce and never write READY.
 */
public final class ArchiveBackupQualificationRecoveredHost {
    static final Duration CLEANUP_DEADLINE = Duration.ofSeconds(90);
    static final String BACKEND_NOT_CONFIGURED = "Original archive backend is not configured on this host";

    public static void main(String[] args) throws Exception {
        ArchiveBackupQualificationFixture.requireNoTestFramework();
        String mode = ArchiveBackupQualificationFixture.env("PROTOMOLT_REHEARSAL_MODE");
        var identities = Path.of(ArchiveBackupQualificationFixture.env("PROTOMOLT_REHEARSAL_IDENTITIES"));
        var out = Path.of(ArchiveBackupQualificationFixture.env("PROTOMOLT_REHEARSAL_OUT"));
        Files.createDirectories(out);
        var checks = new RepositoryBackupRehearsalChecks("recovered-" + mode, out.resolve("markers.log"));
        var record = RepositoryBackupRehearsalJson.read(identities.resolve("identities.json"));
        var manifest = RepositoryBackupRehearsalJson.read(Path.of(ArchiveBackupQualificationFixture.env("PROTOMOLT_REHEARSAL_MANIFEST")));
        String endpoint = ArchiveBackupQualificationFixture.env("PROTOMOLT_REHEARSAL_S3_ENDPOINT");
        String generation = ArchiveBackupQualificationFixture.env("PROTOMOLT_REHEARSAL_GENERATION");
        String realm = ArchiveBackupQualificationFixture.env("PROTOMOLT_REHEARSAL_REALM");
        var config = ArchiveBackupQualificationFixture.recoveredConfig(endpoint, generation, realm);
        var ready = out.resolve("READY");
        try {
            switch (mode) {
                case "verify" -> verify(checks, config, identities, record, manifest, endpoint, generation, ready);
                case "missing-version" -> missingVersion(checks, config, identities, record, endpoint);
                case "corrupt-payload" -> corruptPayload(checks, config, identities, record, endpoint);
                case "inconsistent-snapshot" -> inconsistentSnapshot(checks, config, identities, record, endpoint);
                case "wrong-endpoint" -> wrongEndpoint(checks, config);
                case "new-generation" -> newGeneration(checks, config, identities, record, endpoint, generation);
                case "wrong-credentials" -> wrongCredentials(checks, endpoint, generation, realm, identities, record);
                default -> throw new RepositoryBackupRehearsalFailure("Unknown recovered host mode " + mode);
            }
            checks.require(!Files.exists(ready) || mode.equals("verify"), mode + ".not_ready", "READY is only written by a fully verified host");
        } finally {
            checks.writeXml(out.resolve("results.xml"));
        }
        System.out.println("ARCHIVE_BACKUP_QUALIFICATION_RECOVERED_" + mode.toUpperCase().replace('-', '_') + "_OK");
    }

    private static Map<String, Long> toLongs(Map<String, Object> values) {
        var result = new TreeMap<String, Long>();
        values.forEach((key, value) -> result.put(key, value instanceof Number n ? n.longValue() : Long.parseLong(value.toString())));
        return result;
    }

    private static Map<String, Object> pending(Map<String, Object> record) { return RepositoryBackupRehearsalJson.object(record, "pending"); }

    private static ArchiveMutationReceipt recordedReceipt(Path identities, Map<String, Object> record) throws Exception {
        return ArchiveMutationReceipt.parseFrom(ArchiveBackupQualificationContentChecks.read(identities, "mutation-receipt-" + RepositoryBackupRehearsalJson.string(pending(record), "tag") + ".pb"));
    }

    /** Restored rows equal the seed record, including every binding, provider version id, reference and retention fence. */
    private static long verifyCatalog(RepositoryBackupRehearsalChecks checks, Path identities, Map<String, Object> record, Map<String, Object> manifest,
            String generation, String phase) throws Exception {
        long restoredActive;
        try (var catalog = RepositoryBackupRehearsalLedger.fromEnvironment(); var ledger = ArchiveBackupQualificationLedger.fromEnvironment()) {
            var counts = catalog.rowCounts();
            checks.require(counts.equals(toLongs(RepositoryBackupRehearsalJson.object(record, "rowCountsAfterClose"))), phase + ".row_counts",
                    "every base table's row count equals the seed record after close: tables=" + counts.size());
            checks.require(counts.equals(toLongs(RepositoryBackupRehearsalJson.object(manifest, "rowCounts"))), phase + ".row_counts_manifest", "row counts equal the sealed manifest");
            var incarnations = RepositoryBackupRehearsalJson.object(record, "readerIncarnations");
            for (String state : List.of("ACTIVE", "FENCED", "UNKNOWN", "QUIESCED"))
                checks.require(catalog.readerIncarnations(state) == RepositoryBackupRehearsalJson.number(incarnations, state), phase + ".reader_incarnations." + state.toLowerCase(),
                        "restored " + state + " incarnations=" + catalog.readerIncarnations(state) + " (restored verbatim, none revived)");
            restoredActive = catalog.readerIncarnations("ACTIVE");
            checks.require(catalog.backendProfile(generation).equals(RepositoryBackupRehearsalJson.string(record, "backendProfile")), phase + ".backend_profile",
                    "managed_backend_profiles row equals the recorded identity for generation " + generation);
            checks.require(catalog.sequences().equals(toLongs(RepositoryBackupRehearsalJson.object(manifest, "sequences"))), phase + ".sequences", "sequence values=" + catalog.sequences());
            checks.require(catalog.migrationLevel() == RepositoryBackupRehearsalJson.number(RepositoryBackupRehearsalJson.object(manifest, "database"), "migrationLevel")
                    && catalog.migrationLevel() == 119, phase + ".migration_level", "flyway level V" + catalog.migrationLevel());
            checks.require(catalog.currentXid() > catalog.maxStoredXid(), phase + ".xid_past_stored", "current=" + catalog.currentXid() + " maxStored=" + catalog.maxStoredXid());
            var bindings = RepositoryBackupRehearsalJson.object(record, "bindings");
            var entryRows = RepositoryBackupRehearsalJson.object(record, "entryRows");
            var versionRows = RepositoryBackupRehearsalJson.object(record, "versionRows");
            for (var entry : RepositoryBackupRehearsalJson.list(record, "entries")) {
                String tag = RepositoryBackupRehearsalJson.string(entry, "tag");
                var id = UUID.fromString(RepositoryBackupRehearsalJson.string(entry, "entryUuid"));
                var rows = ledger.bindings(id);
                checks.require(RepositoryBackupRehearsalJson.canonical(Map.of("rows", rows)).equals(RepositoryBackupRehearsalJson.canonical(Map.of("rows", bindings.get(tag)))),
                        phase + ".bindings." + tag, rows.size() + " bindings equal the seed record: generation, realm, bucket, key, provider version, etag, digest, size, references, retention fence");
                checks.require(RepositoryBackupRehearsalJson.canonical(ledger.entry(id)).equals(RepositoryBackupRehearsalJson.canonical(RepositoryBackupRehearsalJson.object(entryRows, tag)))
                        && RepositoryBackupRehearsalJson.canonical(Map.of("rows", ledger.versions(id))).equals(RepositoryBackupRehearsalJson.canonical(Map.of("rows", versionRows.get(tag)))),
                        phase + ".rows." + tag, "entry cursor, mutation revisions, root checksums and byte totals equal the seed record");
            }
            var pending = pending(record);
            var admission = ledger.mutation(RepositoryBackupRehearsalJson.string(pending, "account"), RepositoryBackupRehearsalJson.string(pending, "principal"),
                    UUID.fromString(RepositoryBackupRehearsalJson.string(pending, "operationId")));
            checks.require(RepositoryBackupRehearsalJson.canonical(admission).equals(RepositoryBackupRehearsalJson.canonical(RepositoryBackupRehearsalJson.object(pending, "admission"))),
                    phase + ".pending.admission_rows", "immutable admission, command bytes, receipt bytes, targets and observation revision equal the seed record: " + admission);
            checks.require(ledger.archiveReadPins() == 0 && ledger.uploadStates().equals(toLongs(RepositoryBackupRehearsalJson.object(record, "uploadStatesAfterClose"))),
                    phase + ".cleanup_state", "no read pin; upload states equal the seed record: " + ledger.uploadStates());
        }
        return restoredActive;
    }

    /** The production observation of the pending mutation, computed without opening a host. */
    private static ArchiveMutationReceipt observe(RepoServiceConfig config, Map<String, Object> record) {
        var pending = pending(record);
        try (var database = new LedgerDatabase(config.ledger())) {
            var tx = new Tx(database.entityManagerFactory());
            return new ArchiveMutationObservations(tx).observe(RepositoryBackupRehearsalJson.string(pending, "principal"),
                    RepositoryBackupRehearsalJson.string(pending, "account"), UUID.fromString(RepositoryBackupRehearsalJson.string(pending, "operationId")))
                    .orElseThrow(() -> new RepositoryBackupRehearsalFailure("Pending mutation admission is missing after restore"));
        }
    }

    private static void verify(RepositoryBackupRehearsalChecks checks, RepoServiceConfig config, Path identities, Map<String, Object> record,
            Map<String, Object> manifest, String endpoint, String generation, Path ready) throws Exception {
        long restoredActive = verifyCatalog(checks, identities, record, manifest, generation, "restore");
        // Provider identities first: every recorded LIVE object, including the two pending cleanup targets, by exact version id.
        ArchiveBackupQualificationContentChecks.verifyProviderObjects(checks, "restore", endpoint, ArchiveBackupQualificationSeedHost.allBindings(record));
        var recorded = recordedReceipt(identities, record);
        var observed = observe(config, record);
        checks.require(observed.equals(recorded) && observed.getState() == ArchiveMutationState.ARCHIVE_MUTATION_STATE_ADMITTED && observed.getObjectsPending() == 2
                && observed.getStatusRevision() == 1, "restore.pending.observation", "production observation before any host: still ADMITTED with "
                + observed.getObjectsPending() + " pending targets at status revision " + observed.getStatusRevision() + ", equal to the recorded receipt");
        var pending = pending(record);
        String pendingAccount = RepositoryBackupRehearsalJson.string(pending, "account"), operation = RepositoryBackupRehearsalJson.string(pending, "operationId");
        var operator = ArchiveBackupQualificationFixture.operator();
        try (var host = RepoServices.build(config)) {
            var content = new ArchiveBackupQualificationContentChecks(host, checks, identities, record);
            content.verifyEntries("restore", true);
            try (var catalog = RepositoryBackupRehearsalLedger.fromEnvironment()) {
                long active = catalog.readerIncarnations("ACTIVE");
                checks.require(active > restoredActive, "restore.fresh_incarnation", "recovered host registered " + (active - restoredActive) + " new ACTIVE incarnation(s); restored states unchanged");
            }
            content.verifyVersions("restore", true);
            content.verifyStats("restore");
            var matrix = content.verifyAccess("restore");
            checks.require(RepositoryBackupRehearsalJson.canonical(new LinkedHashMap<>(matrix)).equals(RepositoryBackupRehearsalJson.canonical(RepositoryBackupRehearsalJson.object(record, "accessMatrix"))),
                    "restore.access_matrix_unchanged", "matrix=" + matrix);
            content.verifyTransport("restore");

            // The pending operation completes through the production recovery lane at the exact original backend.
            var completed = content.awaitObservation(host.archiveMutationRepository(), pendingAccount, operation,
                    receipt -> receipt.getState() == ArchiveMutationState.ARCHIVE_MUTATION_STATE_COMPLETED, CLEANUP_DEADLINE,
                    "restore.pending.completed", "mutation recovery lane reclaimed every pending target after restore");
            checks.require(completed.getObjectsConfirmedAbsent() == 2 && completed.getObjectsPending() == 0 && !completed.hasErrorCode() && completed.getStatusRevision() > 1
                    && ArchiveBackupQualificationFixture.logical(completed).equals(ArchiveBackupQualificationFixture.logical(recorded)),
                    "restore.pending.completed_receipt", "confirmedAbsent=" + completed.getObjectsConfirmedAbsent() + " statusRevision=" + completed.getStatusRevision() + "; logical admission unchanged");
            content.verifyReplay("restore");
            var subject = content.entry(RepositoryBackupRehearsalJson.string(pending, "tag"));
            try (var ledger = ArchiveBackupQualificationLedger.fromEnvironment()) {
                var rows = ledger.bindings(UUID.fromString(RepositoryBackupRehearsalJson.string(subject, "entryUuid")));
                var targets = rows.stream().filter(row -> Boolean.TRUE.equals(row.get("mutationTarget"))).toList();
                checks.require(targets.size() == 2 && targets.stream().allMatch(row -> "DELETED".equals(row.get("state")) && Boolean.TRUE.equals(row.get("reclaiming"))
                        && ((Number) row.get("cleanupAttempts")).longValue() == 1 && row.get("cleanupError") == null)
                        && rows.stream().filter(row -> !Boolean.TRUE.equals(row.get("mutationTarget"))).allMatch(row -> "LIVE".equals(row.get("state"))),
                        "restore.pending.targets_reclaimed", "both targets DELETED after one cleanup attempt each; the shared original stays LIVE");
                ArchiveBackupQualificationContentChecks.verifyProviderObjects(checks, "restore.after-cleanup", endpoint, rows);
            }
            content.verifyEntries("after-cleanup", false);

            // A legitimate subsequent write, then an authorized lifecycle operation that keeps every retained sibling.
            var report = content.entry("alpha-report");
            var address = ArchiveBackupQualificationContentChecks.address(report);
            long restoredArchiveSequence = RepositoryBackupRehearsalJson.number(RepositoryBackupRehearsalJson.object(manifest, "sequences"), "archive_mutation_revision_seq");
            var recordedManifest3 = content.recorded("alpha-report", 3).getManifest();
            var original = ArchiveBackupQualificationContentChecks.rendition(recordedManifest3, "original");
            var attachment = ArchiveBackupQualificationContentChecks.rendition(recordedManifest3, "attachment.bin");
            byte[] markdown4 = "# Report, restored\n\nsynthetic qualification fixture, revision four after restore\n".getBytes(StandardCharsets.UTF_8);
            var attribution = WriteAttribution.newBuilder().setModule("archive-backup-qualification-recovered").setActor(operator.principalName()).build();
            var put4 = host.archiveRepository().putEntry(operator, PutEntryRequest.newBuilder().setAddress(address).setTitle("Restored report").setWrittenBy(attribution)
                    .addRenditions(ArchiveBackupQualificationFixture.rendition("markdown", "text/markdown", markdown4)).build());
            var original4 = ArchiveBackupQualificationContentChecks.rendition(put4.getManifest(), "original");
            var attachment4 = ArchiveBackupQualificationContentChecks.rendition(put4.getManifest(), "attachment.bin");
            var markdown4Entry = ArchiveBackupQualificationContentChecks.rendition(put4.getManifest(), "markdown");
            checks.require(put4.getVersion() == 4 && !put4.getDeduplicated() && put4.getManifest().getMetadataSnapshot().getCurrentVersion() == 4
                    && original4.getStorageObjectId().equals(original.getStorageObjectId()) && original4.getObjectKey().equals(original.getObjectKey())
                    && attachment4.getStorageObjectId().equals(attachment.getStorageObjectId())
                    && !markdown4Entry.getStorageObjectId().equals(ArchiveBackupQualificationContentChecks.rendition(recordedManifest3, "markdown").getStorageObjectId()),
                    "after-write.new_version", "version 4 committed after restore; original and attachment re-referenced by the recorded bindings, new markdown object bound");
            try (var catalog = RepositoryBackupRehearsalLedger.fromEnvironment(); var ledger = ArchiveBackupQualificationLedger.fromEnvironment()) {
                long sequence = catalog.sequences().get("archive_mutation_revision_seq");
                var row = ledger.entry(UUID.fromString(RepositoryBackupRehearsalJson.string(report, "entryUuid")));
                long recordedRevision = RepositoryBackupRehearsalJson.number(RepositoryBackupRehearsalJson.object(RepositoryBackupRehearsalJson.object(record, "entryRows"), "alpha-report"), "mutationRevision");
                checks.require(sequence > restoredArchiveSequence && ((Number) row.get("mutationRevision")).longValue() > recordedRevision
                        && ((Number) row.get("currentVersion")).longValue() == 4, "after-write.sequence_advanced",
                        "archive_mutation_revision_seq " + sequence + " > restored " + restoredArchiveSequence + "; entry revision " + row.get("mutationRevision") + " > " + recordedRevision);
            }
            var read4 = host.archiveRepository().getEntry(operator, GetEntryRequest.newBuilder().setAddress(address).setVersion(4).build());
            checks.require(read4.getManifest().equals(put4.getManifest()) && read4.getRenditionsCount() == 3
                    && read4.getRenditionsList().stream().anyMatch(item -> item.getRendition().getName().equals("markdown") && item.getData().toByteArray().length == markdown4.length
                            && ArchiveManifests.sha256Hex(item.getData().toByteArray()).equals(markdown4Entry.getSha256())),
                    "after-write.readable", "version 4 reads with the shared original, the shared attachment and the new markdown");
            var listing = host.archiveRepository().listVersions(operator, ListVersionsRequest.newBuilder().setAddress(address).build()).getVersionsList().stream().map(VersionManifest::getVersion).toList();
            checks.require(listing.equals(List.of(4L, 3L, 2L, 1L)), "after-write.listing", "versions=" + listing);
            content.verifyEntries("after-write", false);
            content.verifyVersions("after-write", false);

            var retention = ArchiveMutationRequest.newBuilder().setOperationId(UUID.randomUUID().toString())
                    .setDeleteRendition(DeleteRenditionRequest.newBuilder().setAddress(address).setRendition("markdown").setReason("RETENTION")).build();
            var admitted = host.archiveMutationRepository().mutateArchive(operator, retention);
            checks.require(admitted.getVersionsTombstoned() == 4 && admitted.getObjectsTargeted() == 3 && !admitted.getEntryDeleted() && admitted.getVersionsRemoved() == 0,
                    "after-write.lifecycle.admitted", "markdown deleted from four retained versions: three distinct objects targeted, state=" + admitted.getState());
            var done = content.awaitObservation(host.archiveMutationRepository(), RepositoryBackupRehearsalJson.string(report, "account"), retention.getOperationId(),
                    receipt -> receipt.getState() == ArchiveMutationState.ARCHIVE_MUTATION_STATE_COMPLETED, CLEANUP_DEADLINE,
                    "after-write.lifecycle.completed", "retention cleanup completed at the original backend");
            checks.require(done.getObjectsConfirmedAbsent() == 3, "after-write.lifecycle.confirmed", "confirmedAbsent=" + done.getObjectsConfirmedAbsent());
            var expectedManifests = new LinkedHashMap<Long, VersionManifest>();
            for (long version : List.of(1L, 2L, 3L)) expectedManifests.put(version, tombstoned(content.recorded("alpha-report", version).getManifest(), "markdown", "RETENTION"));
            expectedManifests.put(4L, tombstoned(put4.getManifest(), "markdown", "RETENTION"));
            for (var expected : expectedManifests.entrySet()) {
                long version = expected.getKey();
                var actual = host.archiveRepository().getEntry(operator, GetEntryRequest.newBuilder().setAddress(address).setVersion(version).build());
                var expectedRenditions = version == 4 ? read4.getRenditionsList() : content.recorded("alpha-report", version).getRenditionsList();
                var survivors = expectedRenditions.stream().filter(item -> !item.getRendition().getName().equals("markdown")).toList();
                checks.require(actual.getManifest().equals(expected.getValue()) && actual.getRenditionsList().equals(survivors),
                        "after-write.lifecycle.v" + version, "markdown tombstoned with reason and provenance; " + survivors.size() + " retained sibling rendition(s) byte-equal");
            }
            try (var ledger = ArchiveBackupQualificationLedger.fromEnvironment()) {
                var rows = ledger.bindings(UUID.fromString(RepositoryBackupRehearsalJson.string(report, "entryUuid")));
                long live = rows.stream().filter(row -> "LIVE".equals(row.get("state"))).count();
                long deleted = rows.stream().filter(row -> "DELETED".equals(row.get("state"))).count();
                boolean siblings = rows.stream().filter(row -> "LIVE".equals(row.get("state"))).allMatch(row -> row.get("key").equals(original.getObjectKey()) && "1,2,3,4".equals(row.get("referencedVersions"))
                        || row.get("key").equals(attachment.getObjectKey()) && "3,4".equals(row.get("referencedVersions")));
                checks.require(live == 2 && deleted == 3 && siblings, "after-write.lifecycle.bindings", "the shared original (versions 1,2,3,4) and attachment (versions 3,4) stay LIVE; three markdown objects DELETED");
                ArchiveBackupQualificationContentChecks.verifyProviderObjects(checks, "after-write.lifecycle", endpoint, rows);
            }
            var stats = host.archiveRepository().stats(operator, GetArchiveStatsRequest.newBuilder().setAccountId(RepositoryBackupRehearsalJson.string(report, "account"))
                    .setArchive(RepositoryBackupRehearsalJson.string(report, "archive")).build()).getStats();
            long expectedRetained = original.getSizeBytes() + attachment.getSizeBytes() + notesBytes(content);
            checks.require(stats.getVersions() == 5 && stats.getEntries() == 2 && stats.getRetainedBytes() == expectedRetained && stats.getCurrentBytes() == expectedRetained,
                    "after-write.lifecycle.stats", "entries=" + stats.getEntries() + " versions=" + stats.getVersions() + " retainedBytes=" + stats.getRetainedBytes() + " currentBytes=" + stats.getCurrentBytes());
            content.verifyEntries("after-lifecycle", false, Set.of("alpha-report"));
            Files.writeString(ready, "READY " + Instant.now() + "\n");
            checks.pass("restore.ready", "recovered host advertised ready after verification, fresh write and lifecycle operation");
        }
    }

    /** The alpha "notes" entry's PRESENT bytes, the other contributor to the alpha archive's counters. */
    private static long notesBytes(ArchiveBackupQualificationContentChecks content) {
        return ArchiveManifests.totalBytes(content.recorded("alpha-notes", 1).getManifest().getRenditionsList());
    }

    /** The manifest a rendition deletion must leave behind, computed with the production manifest mechanics. */
    static VersionManifest tombstoned(VersionManifest manifest, String rendition, String reason) {
        var updated = manifest.toBuilder();
        for (int i = 0; i < manifest.getRenditionsCount(); i++) {
            var item = manifest.getRenditions(i);
            if (item.getState() == RenditionState.RENDITION_STATE_PRESENT && item.getRendition().getName().equals(rendition))
                updated.setRenditions(i, item.toBuilder().setState(RenditionState.RENDITION_STATE_DELETED).setDeletedReason(reason));
        }
        return updated.setRootChecksum(ArchiveManifests.rootChecksum(updated.getRenditionsList())).setTotalBytes(ArchiveManifests.totalBytes(updated.getRenditionsList())).build();
    }

    /** Pending cleanup under a wrong or absent original backend records a retryable failure and never deletes bytes. */
    private static void pendingRetryRequired(ArchiveBackupQualificationContentChecks content, RepositoryBackupRehearsalChecks checks, RepoServices host,
            Map<String, Object> record, String endpoint, String mode) throws Exception {
        var pending = pending(record);
        var observed = content.awaitObservation(host.archiveMutationRepository(), RepositoryBackupRehearsalJson.string(pending, "account"),
                RepositoryBackupRehearsalJson.string(pending, "operationId"), receipt -> receipt.getState() == ArchiveMutationState.ARCHIVE_MUTATION_STATE_RETRY_REQUIRED,
                CLEANUP_DEADLINE, mode + ".pending.retry_required", "recovery lane recorded a retryable cleanup failure instead of completion");
        checks.require(observed.getObjectsPending() == 2 && observed.getObjectsConfirmedAbsent() == 0 && "BACKEND_RECLAMATION_FAILED".equals(observed.getErrorCode()),
                mode + ".pending.error_code", "pending=" + observed.getObjectsPending() + " error=" + observed.getErrorCode());
        var subject = content.entry(RepositoryBackupRehearsalJson.string(pending, "tag"));
        // The lane re-claims every second and clears the error for the length of one failing attempt, so the rows are polled.
        try (var ledger = ArchiveBackupQualificationLedger.fromEnvironment()) {
            var id = UUID.fromString(RepositoryBackupRehearsalJson.string(subject, "entryUuid"));
            long until = System.nanoTime() + CLEANUP_DEADLINE.toNanos();
            List<Map<String, Object>> targets;
            boolean recorded;
            do {
                targets = ledger.bindings(id).stream().filter(row -> Boolean.TRUE.equals(row.get("mutationTarget"))).toList();
                recorded = targets.size() == 2 && targets.stream().allMatch(row -> "DELETING".equals(row.get("state")) && "BACKEND_RECLAMATION_FAILED".equals(row.get("cleanupError"))
                        && ((Number) row.get("cleanupAttempts")).longValue() >= 1);
                if (!recorded) Thread.sleep(200);
            } while (!recorded && System.nanoTime() < until);
            checks.require(recorded, mode + ".pending.targets_retained", "targets stay DELETING with the failure recorded and no byte deleted; attempts="
                    + targets.stream().map(row -> row.get("cleanupAttempts")).toList() + " states=" + targets.stream().map(row -> row.get("state") + "/" + row.get("cleanupError")).toList());
        }
    }

    /** The recorded version of one rendition was deleted on the disposable provider; a newer version exists under the key. */
    private static void missingVersion(RepositoryBackupRehearsalChecks checks, RepoServiceConfig config, Path identities, Map<String, Object> record, String endpoint) throws Exception {
        var damaged = RepositoryBackupRehearsalJson.object(record, "injectedMissingVersion");
        String tag = RepositoryBackupRehearsalJson.string(damaged, "tag"), rendition = RepositoryBackupRehearsalJson.string(damaged, "rendition");
        long version = RepositoryBackupRehearsalJson.number(damaged, "version");
        try (var client = ArchiveBackupQualificationFixture.s3(endpoint)) {
            String refused = ArchiveBackupQualificationContentChecks.outcome(() -> client.getObjectAsBytes(builder -> builder.bucket(RepositoryBackupRehearsalJson.string(damaged, "namespace"))
                    .key(RepositoryBackupRehearsalJson.string(damaged, "key")).versionId(RepositoryBackupRehearsalJson.string(damaged, "providerVersion"))));
            checks.require(refused.startsWith("NoSuchKeyException") || refused.startsWith("NoSuchVersionException"), "missing-version.provider_version_absent", "GET recorded version -> " + refused);
            var latest = client.getObjectAsBytes(builder -> builder.bucket(RepositoryBackupRehearsalJson.string(damaged, "namespace")).key(RepositoryBackupRehearsalJson.string(damaged, "key")));
            checks.require(!latest.response().versionId().equals(RepositoryBackupRehearsalJson.string(damaged, "providerVersion"))
                    && !ArchiveManifests.sha256Hex(latest.asByteArray()).equals(RepositoryBackupRehearsalJson.string(damaged, "sha256")),
                    "missing-version.latest_differs", "a newer latest version exists under the same key: " + latest.response().versionId());
        }
        var operator = ArchiveBackupQualificationFixture.operator();
        try (var host = RepoServices.build(config)) {
            var content = new ArchiveBackupQualificationContentChecks(host, checks, identities, record);
            var entry = content.entry(tag);
            var address = ArchiveBackupQualificationContentChecks.address(entry);
            String whole = ArchiveBackupQualificationContentChecks.outcome(() -> host.archiveRepository().getEntry(operator, GetEntryRequest.newBuilder().setAddress(address).setVersion(version).build()));
            String causes = ArchiveBackupQualificationContentChecks.causes(() -> host.archiveRepository().getEntry(operator, GetEntryRequest.newBuilder().setAddress(address).setVersion(version).build()));
            checks.require(whole.equals("DATA_LOSS"), "missing-version.read_refused", "getEntry(" + tag + " v" + version + ") -> " + whole + " [" + causes + "]; the newer version was not substituted");
            var manifest = host.archiveRepository().getManifest(operator, GetEntryManifestRequest.newBuilder().setAddress(address).setVersion(version).build()).getManifest();
            checks.require(manifest.equals(content.recorded(tag, version).getManifest()), "missing-version.manifest_intact", "the catalog still describes the version exactly");
            var survivors = content.recorded(tag, version).getManifest().getRenditionsList().stream()
                    .filter(item -> item.getState() == RenditionState.RENDITION_STATE_PRESENT && !item.getRendition().getName().equals(rendition))
                    .map(item -> item.getRendition().getName()).toList();
            var partial = host.archiveRepository().getEntry(operator, GetEntryRequest.newBuilder().setAddress(address).setVersion(version).addAllRenditions(survivors).build());
            var expectedSurvivors = content.recorded(tag, version).getRenditionsList().stream().filter(item -> survivors.contains(item.getRendition().getName())).toList();
            checks.require(partial.getRenditionsList().equals(expectedSurvivors), "missing-version.siblings_readable", "sibling renditions of the same version still read byte-equal: " + survivors);
            content.verifyEntries("missing-version", true, Set.of(tag));
            for (long other : ArchiveBackupQualificationContentChecks.versions(entry)) {
                if (other == version) continue;
                var actual = host.archiveRepository().getEntry(operator, GetEntryRequest.newBuilder().setAddress(address).setVersion(other).build());
                checks.require(actual.getRenditionsList().equals(content.recorded(tag, other).getRenditionsList()) && actual.getManifest().equals(content.recorded(tag, other).getManifest()),
                        "missing-version.unaffected." + tag + ".v" + other, "other versions of the same entry read byte-equal");
            }
        }
    }

    /** The bytes behind one recorded version were damaged on the disposable restored volume. */
    private static void corruptPayload(RepositoryBackupRehearsalChecks checks, RepoServiceConfig config, Path identities, Map<String, Object> record, String endpoint) throws Exception {
        var damaged = RepositoryBackupRehearsalJson.object(record, "injectedCorruption");
        String tag = RepositoryBackupRehearsalJson.string(damaged, "tag"), rendition = RepositoryBackupRehearsalJson.string(damaged, "rendition");
        long version = RepositoryBackupRehearsalJson.number(damaged, "version");
        String expectedCode = RepositoryBackupRehearsalJson.string(damaged, "expectedCode");
        try (var client = ArchiveBackupQualificationFixture.s3(endpoint)) {
            String direct = ArchiveBackupQualificationContentChecks.outcome(() -> {
                var response = client.getObjectAsBytes(builder -> builder.bucket(RepositoryBackupRehearsalJson.string(damaged, "namespace"))
                        .key(RepositoryBackupRehearsalJson.string(damaged, "key")).versionId(RepositoryBackupRehearsalJson.string(damaged, "providerVersion")));
                if (ArchiveManifests.sha256Hex(response.asByteArray()).equals(RepositoryBackupRehearsalJson.string(damaged, "sha256")))
                    throw new RepositoryBackupRehearsalFailure("Damaged object still digests to the recorded value");
            });
            checks.pass("corrupt-payload.provider_observation", "direct GET of the damaged version by its recorded version id -> " + direct);
            checks.require(!direct.startsWith("RepositoryBackupRehearsalFailure"), "corrupt-payload.provider_bytes_changed", "the provider no longer serves the recorded digest for the recorded version");
        }
        var operator = ArchiveBackupQualificationFixture.operator();
        try (var host = RepoServices.build(config)) {
            var content = new ArchiveBackupQualificationContentChecks(host, checks, identities, record);
            var entry = content.entry(tag);
            var address = ArchiveBackupQualificationContentChecks.address(entry);
            String whole = ArchiveBackupQualificationContentChecks.outcome(() -> host.archiveRepository().getEntry(operator, GetEntryRequest.newBuilder().setAddress(address).setVersion(version).build()));
            String causes = ArchiveBackupQualificationContentChecks.causes(() -> host.archiveRepository().getEntry(operator, GetEntryRequest.newBuilder().setAddress(address).setVersion(version).build()));
            // RustFS detects the damaged part and aborts the response; the SDK gives up and the host reports the provider refusal.
            checks.require(whole.equals(expectedCode) && causes.contains("BlobStoreException(" + expectedCode + ")"), "corrupt-payload.read_refused",
                    "getEntry(" + tag + " v" + version + ") -> " + whole + " [" + causes + "]; the provider aborted the read of the damaged part, no bytes served");
            String single = ArchiveBackupQualificationContentChecks.outcome(() -> host.archiveRepository().getEntry(operator, GetEntryRequest.newBuilder().setAddress(address).setVersion(version).addRenditions(rendition).build()));
            checks.require(single.equals(expectedCode), "corrupt-payload.rendition_refused", "getEntry(" + rendition + " only) -> " + single);
            var manifest = host.archiveRepository().getManifest(operator, GetEntryManifestRequest.newBuilder().setAddress(address).setVersion(version).build()).getManifest();
            checks.require(manifest.equals(content.recorded(tag, version).getManifest()), "corrupt-payload.manifest_intact", "the catalog still attests the recorded digest");
            var survivors = content.recorded(tag, version).getManifest().getRenditionsList().stream()
                    .filter(item -> item.getState() == RenditionState.RENDITION_STATE_PRESENT && !item.getRendition().getName().equals(rendition))
                    .map(item -> item.getRendition().getName()).toList();
            var partial = host.archiveRepository().getEntry(operator, GetEntryRequest.newBuilder().setAddress(address).setVersion(version).addAllRenditions(survivors).build());
            var expectedSurvivors = content.recorded(tag, version).getRenditionsList().stream().filter(item -> survivors.contains(item.getRendition().getName())).toList();
            checks.require(partial.getRenditionsList().equals(expectedSurvivors), "corrupt-payload.siblings_readable", "sibling renditions still read byte-equal: " + survivors);
            content.verifyEntries("corrupt-payload", true, Set.of(tag));
            for (long other : ArchiveBackupQualificationContentChecks.versions(entry)) {
                if (other == version) continue;
                var actual = host.archiveRepository().getEntry(operator, GetEntryRequest.newBuilder().setAddress(address).setVersion(other).build());
                checks.require(actual.getRenditionsList().equals(content.recorded(tag, other).getRenditionsList()), "corrupt-payload.unaffected." + tag + ".v" + other,
                        "versions that do not reference the damaged object read byte-equal");
            }
        }
    }

    /** The catalog is one capture and the provider volume another: every recorded identity is absent and nothing is substituted. */
    private static void inconsistentSnapshot(RepositoryBackupRehearsalChecks checks, RepoServiceConfig config, Path identities, Map<String, Object> record, String endpoint) throws Exception {
        var rows = ArchiveBackupQualificationSeedHost.allBindings(record);
        try (var client = ArchiveBackupQualificationFixture.s3(endpoint)) {
            int absent = 0;
            for (var row : rows) {
                String outcome = ArchiveBackupQualificationContentChecks.outcome(() -> client.getObjectAsBytes(builder -> builder.bucket(RepositoryBackupRehearsalJson.string(row, "namespace"))
                        .key(RepositoryBackupRehearsalJson.string(row, "key")).versionId(RepositoryBackupRehearsalJson.string(row, "providerVersion"))));
                if (outcome.startsWith("NoSuchKeyException") || outcome.startsWith("NoSuchVersionException") || outcome.startsWith("NoSuchBucketException")) absent++;
                else throw new RepositoryBackupRehearsalFailure("Unexpected provider outcome for a recorded version: " + outcome);
            }
            checks.require(absent == rows.size(), "inconsistent-snapshot.provider_versions_absent", absent + "/" + rows.size() + " recorded versions absent from the other capture's provider");
        }
        var operator = ArchiveBackupQualificationFixture.operator();
        try (var host = RepoServices.build(config)) {
            var content = new ArchiveBackupQualificationContentChecks(host, checks, identities, record);
            for (var entry : content.entries()) {
                String tag = RepositoryBackupRehearsalJson.string(entry, "tag");
                var address = ArchiveBackupQualificationContentChecks.address(entry);
                for (long version : ArchiveBackupQualificationContentChecks.versions(entry)) {
                    String read = ArchiveBackupQualificationContentChecks.outcome(() -> host.archiveRepository().getEntry(operator, GetEntryRequest.newBuilder().setAddress(address).setVersion(version).build()));
                    String causes = ArchiveBackupQualificationContentChecks.causes(() -> host.archiveRepository().getEntry(operator, GetEntryRequest.newBuilder().setAddress(address).setVersion(version).build()));
                    checks.require(read.equals("FAILED_PRECONDITION"), "inconsistent-snapshot.read_refused." + tag + ".v" + version, "getEntry -> " + read + " [" + causes + "]");
                }
            }
            // SQL-only evidence is still the catalog's own: manifests, listings and counters read; the admission replays.
            content.verifyVersions("inconsistent-snapshot", true);
            content.verifyStats("inconsistent-snapshot");
            var recorded = recordedReceipt(identities, record);
            var pending = pending(record);
            var replayed = host.archiveMutationRepository().mutateArchive(operator, ArchiveMutationRequest.parseFrom(
                    ArchiveBackupQualificationContentChecks.read(identities, "mutation-request-" + RepositoryBackupRehearsalJson.string(pending, "tag") + ".pb")));
            checks.require(ArchiveBackupQualificationFixture.logical(replayed).equals(ArchiveBackupQualificationFixture.logical(recorded)), "inconsistent-snapshot.replay",
                    "mutation replay returns the recorded logical admission; state=" + replayed.getState());
            pendingRetryRequired(content, checks, host, record, endpoint, "inconsistent-snapshot");
        }
    }

    private static void wrongEndpoint(RepositoryBackupRehearsalChecks checks, RepoServiceConfig config) {
        try (var host = RepoServices.build(config)) {
            checks.require(false, "wrong-endpoint.refused", "host started under the recorded generation with a different endpoint: " + host);
        } catch (RuntimeException refusal) {
            boolean bindingRefusal = false;
            for (Throwable cause = refusal; cause != null; cause = cause.getCause())
                if (cause.getMessage() != null && cause.getMessage().contains("already bound to another physical profile")) bindingRefusal = true;
            checks.require(bindingRefusal, "wrong-endpoint.refused", refusal.getClass().getName() + ": " + refusal.getMessage());
        }
    }

    /** A new generation at the recorded endpoint composes, but no bound archive object resolves and cleanup cannot reclaim. */
    private static void newGeneration(RepositoryBackupRehearsalChecks checks, RepoServiceConfig config, Path identities, Map<String, Object> record,
            String endpoint, String generation) throws Exception {
        checks.require(!generation.equals(RepositoryBackupRehearsalJson.string(record, "generation")), "new-generation.distinct", "generation=" + generation);
        var operator = ArchiveBackupQualificationFixture.operator();
        try (var host = RepoServices.build(config)) {
            checks.pass("new-generation.host_started", "a new generation binds freely at the recorded endpoint");
            var content = new ArchiveBackupQualificationContentChecks(host, checks, identities, record);
            for (var entry : content.entries()) {
                String tag = RepositoryBackupRehearsalJson.string(entry, "tag");
                var address = ArchiveBackupQualificationContentChecks.address(entry);
                long version = ArchiveBackupQualificationContentChecks.versions(entry).getLast();
                String read = ArchiveBackupQualificationContentChecks.outcome(() -> host.archiveRepository().getEntry(operator, GetEntryRequest.newBuilder().setAddress(address).setVersion(version).build()));
                checks.require(read.equals("IllegalStateException: " + BACKEND_NOT_CONFIGURED), "new-generation.read_refused." + tag,
                        "getEntry -> " + read + " (baseline: the resolver's IllegalStateException is not translated to a RepositoryException code)");
                var manifest = host.archiveRepository().getManifest(operator, GetEntryManifestRequest.newBuilder().setAddress(address).setVersion(version).build()).getManifest();
                checks.require(manifest.equals(content.recorded(tag, version).getManifest()), "new-generation.manifest_intact." + tag, "catalog-only reads still serve the recorded manifest");
            }
            pendingRetryRequired(content, checks, host, record, endpoint, "new-generation");
            ArchiveBackupQualificationContentChecks.verifyProviderObjects(checks, "new-generation.bytes_intact", endpoint, ArchiveBackupQualificationSeedHost.allBindings(record));
        }
    }

    /** Wrong provider credentials: composition succeeds without a provider call; every bound read and every reclaim attempt is refused by the provider. */
    private static void wrongCredentials(RepositoryBackupRehearsalChecks checks, String endpoint, String generation, String realm, Path identities, Map<String, Object> record) throws Exception {
        String wrongSecret = "wrong-" + UUID.randomUUID();
        var config = ArchiveBackupQualificationFixture.config(endpoint, generation, realm, ArchiveBackupQualificationFixture.env("PROTOMOLT_REHEARSAL_S3_ACCESS"), wrongSecret,
                ArchiveBackupQualificationFixture.FAST_MUTATION_RECOVERY_MS);
        var rows = ArchiveBackupQualificationSeedHost.allBindings(record);
        try (var client = ArchiveBackupQualificationFixture.s3(endpoint, ArchiveBackupQualificationFixture.env("PROTOMOLT_REHEARSAL_S3_ACCESS"), wrongSecret)) {
            var row = rows.getFirst();
            String direct = ArchiveBackupQualificationContentChecks.outcome(() -> client.getObjectAsBytes(builder -> builder.bucket(RepositoryBackupRehearsalJson.string(row, "namespace"))
                    .key(RepositoryBackupRehearsalJson.string(row, "key")).versionId(RepositoryBackupRehearsalJson.string(row, "providerVersion"))));
            checks.require(direct.contains(": 403 "), "wrong-credentials.provider_refuses", "direct GET with the wrong secret -> " + direct);
        }
        var operator = ArchiveBackupQualificationFixture.operator();
        try (var host = RepoServices.build(config)) {
            checks.pass("wrong-credentials.host", "host composition with the wrong provider secret succeeds (no provider call at composition)");
            var content = new ArchiveBackupQualificationContentChecks(host, checks, identities, record);
            for (var entry : content.entries()) {
                String tag = RepositoryBackupRehearsalJson.string(entry, "tag");
                var address = ArchiveBackupQualificationContentChecks.address(entry);
                long version = ArchiveBackupQualificationContentChecks.versions(entry).getLast();
                String read = ArchiveBackupQualificationContentChecks.outcome(() -> host.archiveRepository().getEntry(operator, GetEntryRequest.newBuilder().setAddress(address).setVersion(version).build()));
                String causes = ArchiveBackupQualificationContentChecks.causes(() -> host.archiveRepository().getEntry(operator, GetEntryRequest.newBuilder().setAddress(address).setVersion(version).build()));
                checks.require(read.equals("PERMISSION_DENIED") && causes.contains("BlobStoreException(PERMISSION_DENIED)"), "wrong-credentials.read_refused." + tag,
                        "getEntry -> " + read + " [" + causes + "]");
            }
            pendingRetryRequired(content, checks, host, record, endpoint, "wrong-credentials");
            ArchiveBackupQualificationContentChecks.verifyProviderObjects(checks, "wrong-credentials.bytes_intact", endpoint, rows);
        }
    }
}
