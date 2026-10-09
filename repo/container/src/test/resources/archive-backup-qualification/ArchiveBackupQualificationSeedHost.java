package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.archive.v1.Archive;
import ai.protomolt.proto.repo.archive.v1.ArchiveMutationRequest;
import ai.protomolt.proto.repo.archive.v1.ArchiveMutationState;
import ai.protomolt.proto.repo.archive.v1.CreateArchiveRequest;
import ai.protomolt.proto.repo.archive.v1.DeleteRenditionRequest;
import ai.protomolt.proto.repo.archive.v1.GetArchiveStatsRequest;
import ai.protomolt.proto.repo.archive.v1.GetEntryManifestRequest;
import ai.protomolt.proto.repo.archive.v1.GetEntryRequest;
import ai.protomolt.proto.repo.archive.v1.ListVersionsRequest;
import ai.protomolt.proto.repo.archive.v1.PutEntryRequest;
import ai.protomolt.proto.repo.archive.v1.RenditionDescriptor;
import ai.protomolt.proto.repo.archive.v1.RenditionState;
import ai.protomolt.proto.repo.archive.v1.VersionManifest;
import ai.protomolt.proto.repo.archive.v1.VersioningPolicy;
import ai.protomolt.proto.repo.archive.v1.WriteAttribution;
import ai.protomolt.proto.repo.container.archive.ArchiveManifests;
import ai.protomolt.proto.repo.service.RepoServices;
import ai.protomolt.proto.repo.v1.CreateDriveRequest;
import ai.protomolt.proto.repo.v1.DriveType;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;

/**
 * Seeds archive entries through the production host over real SQL and pinned RustFS: two
 * accounts with host-provisioned versioned drives, a retained archive in each, one entry with
 * three versions (unary saves, a streamed upload, a deduplicated save) sharing unchanged
 * rendition objects, an entry with an EMPTY rendition, an entry whose RTBF rendition deletion
 * is admitted but whose physical cleanup is left pending, and an unversioned archive whose
 * superseded state is gone. Records every identity the responses and the ledger report,
 * verifies them once while live, then drains and exits. Every payload is a labeled synthetic
 * fixture.
 */
public final class ArchiveBackupQualificationSeedHost {
    public static void main(String[] args) throws Exception {
        ArchiveBackupQualificationFixture.requireNoTestFramework();
        var out = Path.of(ArchiveBackupQualificationFixture.env("PROTOMOLT_REHEARSAL_IDENTITIES"));
        Files.createDirectories(out);
        var checks = new RepositoryBackupRehearsalChecks("seed", out.resolve("markers.log"));
        String endpoint = ArchiveBackupQualificationFixture.env("PROTOMOLT_REHEARSAL_S3_ENDPOINT");
        String generation = ArchiveBackupQualificationFixture.env("PROTOMOLT_REHEARSAL_GENERATION");
        String realm = ArchiveBackupQualificationFixture.env("PROTOMOLT_REHEARSAL_REALM");
        var config = ArchiveBackupQualificationFixture.seedConfig(endpoint, generation, realm);
        var record = new LinkedHashMap<String, Object>();
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        String alpha = "abq-alpha-" + suffix, beta = "abq-beta-" + suffix;
        record.put("fixture", ArchiveBackupQualificationFixture.FIXTURE_LABEL);
        record.put("accounts", List.of(alpha, beta));
        var accountsDetail = new ArrayList<Map<String, Object>>();
        for (String account : List.of(alpha, beta)) accountsDetail.add(Map.of("account", account));
        record.put("accountsDetail", accountsDetail);
        record.put("generation", generation);
        record.put("realm", realm);
        record.put("endpoint", endpoint);
        record.put("principal", ArchiveBackupQualificationFixture.operator().principalName());
        var operator = ArchiveBackupQualificationFixture.operator();
        var attribution = WriteAttribution.newBuilder().setModule(ArchiveBackupQualificationFixture.SEED_MODULE).setActor(operator.principalName()).build();
        try (var host = RepoServices.build(config)) {
            var archives = host.archiveRepository();
            var mutations = host.archiveMutationRepository();
            var drives = new LinkedHashMap<String, Object>();
            for (String account : List.of(alpha, beta)) {
                // Host provisioning: the production provisioner creates the bucket with versioning enabled and
                // verifies it, which is what makes every committed object carry a provider version id.
                var drive = host.driveRepository().createDrive(operator, CreateDriveRequest.newBuilder().setName(ArchiveBackupQualificationFixture.DRIVE)
                        .setAccountId(account).setDriveType(DriveType.DRIVE_TYPE_CUSTOM).build()).getDrive();
                checks.require("s3".equals(drive.getProvider()) && !drive.getBucket().isBlank(), "seed.drive." + account, "drive=" + drive.getDriveId() + " bucket=" + drive.getBucket());
                checks.require(versioningEnabled(endpoint, drive.getBucket()), "seed.drive.provisioned_versioned." + account,
                        "host-provisioned bucket reports versioning ENABLED through the plain S3 API: " + drive.getBucket());
                drives.put(account, Map.of("id", drive.getDriveId(), "bucket", drive.getBucket(), "prefix", drive.getPrefix(), "name", drive.getName()));
            }
            record.put("drives", drives);
            var archiveRecords = new ArrayList<Map<String, Object>>();
            for (String account : List.of(alpha, beta)) {
                archives.createArchive(operator, CreateArchiveRequest.newBuilder().setArchive(Archive.newBuilder().setName(ArchiveBackupQualificationFixture.ARCHIVE)
                        .setAccountId(account).setDriveName(ArchiveBackupQualificationFixture.DRIVE).setVersioning(VersioningPolicy.VERSIONING_POLICY_RETAINED)
                        .setDescription(ArchiveBackupQualificationFixture.FIXTURE_LABEL).putMetadata("fixture", ArchiveBackupQualificationFixture.FIXTURE_LABEL)).build());
                archiveRecords.add(Map.of("account", account, "name", ArchiveBackupQualificationFixture.ARCHIVE, "versioning", "RETAINED"));
            }
            archives.createArchive(operator, CreateArchiveRequest.newBuilder().setArchive(Archive.newBuilder().setName(ArchiveBackupQualificationFixture.SCRATCH_ARCHIVE)
                    .setAccountId(beta).setDriveName(ArchiveBackupQualificationFixture.DRIVE).setVersioning(VersioningPolicy.VERSIONING_POLICY_NONE)
                    .setDescription(ArchiveBackupQualificationFixture.FIXTURE_LABEL)).build());
            archiveRecords.add(Map.of("account", beta, "name", ArchiveBackupQualificationFixture.SCRATCH_ARCHIVE, "versioning", "NONE"));
            record.put("archives", archiveRecords);

            // Alpha "report": v1 unary, v2 changes one rendition (the other is re-referenced, not copied),
            // v3 streams a large rendition in (both siblings re-referenced), then an identical save deduplicates.
            var report = ArchiveBackupQualificationFixture.address(alpha, ArchiveBackupQualificationFixture.ARCHIVE, "report.pdf#2026");
            byte[] original = ArchiveBackupQualificationFixture.bytes("alpha-original", 2048);
            byte[] markdown1 = "# Report\n\nsynthetic qualification fixture, revision one\n".getBytes(StandardCharsets.UTF_8);
            byte[] markdown2 = "# Report, revised\n\nsynthetic qualification fixture, revision two\n".getBytes(StandardCharsets.UTF_8);
            var put1 = archives.putEntry(operator, PutEntryRequest.newBuilder().setAddress(report).setTitle("Quarterly report").setFilename("report.pdf")
                    .setContentType("application/pdf").setSourceUri("synthetic://qualification/report.pdf").putMetadata("department", "finance")
                    .putMetadata("fixture", ArchiveBackupQualificationFixture.FIXTURE_LABEL).setWrittenBy(attribution)
                    .addRenditions(ArchiveBackupQualificationFixture.rendition("original", "application/pdf", original))
                    .addRenditions(ArchiveBackupQualificationFixture.rendition("markdown", "text/markdown", markdown1)).build());
            checks.require(put1.getVersion() == 1 && !put1.getDeduplicated() && put1.getManifest().getRenditionsCount() == 2
                    && put1.getManifest().getRenditionsList().stream().allMatch(item -> !item.getStorageObjectId().isBlank())
                    && put1.getManifest().getMetadataSnapshot().getCurrentVersion() == 1, "seed.report.v1", "version 1 with two bound renditions, snapshot title=" + put1.getManifest().getMetadataSnapshot().getTitle());
            var put2 = archives.putEntry(operator, PutEntryRequest.newBuilder().setAddress(report).setTitle("Revised report").setWrittenBy(attribution)
                    .addRenditions(ArchiveBackupQualificationFixture.rendition("original", "application/pdf", original))
                    .addRenditions(ArchiveBackupQualificationFixture.rendition("markdown", "text/markdown", markdown2)).build());
            var original1 = ArchiveBackupQualificationContentChecks.rendition(put1.getManifest(), "original");
            var original2 = ArchiveBackupQualificationContentChecks.rendition(put2.getManifest(), "original");
            checks.require(put2.getVersion() == 2 && !put2.getDeduplicated() && original1.getObjectKey().equals(original2.getObjectKey())
                    && original1.getStorageObjectId().equals(original2.getStorageObjectId())
                    && !ArchiveBackupQualificationContentChecks.rendition(put1.getManifest(), "markdown").getObjectKey()
                            .equals(ArchiveBackupQualificationContentChecks.rendition(put2.getManifest(), "markdown").getObjectKey()),
                    "seed.report.v2.shared_object", "version 2 re-references the unchanged original (object " + original2.getStorageObjectId() + ") and lands a new markdown object");
            byte[] attachment = ArchiveBackupQualificationFixture.bytes("alpha-attachment", 1_048_576);
            String attachmentSha = ArchiveManifests.sha256Hex(attachment);
            var streamed = archives.uploadStream(operator, report, RenditionDescriptor.newBuilder().setName("attachment.bin").setMediaType("application/octet-stream").build(),
                    attachment.length, attachmentSha, attribution, null, null, null, new ByteArrayInputStream(attachment));
            checks.require(streamed.version() == 3 && !streamed.deduplicated() && streamed.sha256().equals(attachmentSha) && streamed.sizeBytes() == attachment.length,
                    "seed.report.v3.streamed", "streamed 1 MiB rendition landed version 3 at key " + streamed.objectKey());
            var manifest3 = archives.getManifest(operator, GetEntryManifestRequest.newBuilder().setAddress(report).setVersion(3).build()).getManifest();
            checks.require(manifest3.getRenditionsCount() == 3
                    && ArchiveBackupQualificationContentChecks.rendition(manifest3, "original").getStorageObjectId().equals(original1.getStorageObjectId())
                    && ArchiveBackupQualificationContentChecks.rendition(manifest3, "markdown").getStorageObjectId()
                            .equals(ArchiveBackupQualificationContentChecks.rendition(put2.getManifest(), "markdown").getStorageObjectId())
                    && !ArchiveBackupQualificationContentChecks.rendition(manifest3, "attachment.bin").getStorageObjectId().isBlank(),
                    "seed.report.v3.re_referenced", "version 3 re-references both current siblings and binds the streamed object");
            var replayRequest = PutEntryRequest.newBuilder().setAddress(report).setTitle("Current label").setWrittenBy(attribution)
                    .addRenditions(ArchiveBackupQualificationFixture.rendition("markdown", "text/markdown", markdown2)).build();
            var deduplicated = archives.putEntry(operator, replayRequest);
            checks.require(deduplicated.getDeduplicated() && deduplicated.getVersion() == 3 && deduplicated.getManifest().equals(manifest3),
                    "seed.report.dedup", "identical content elided: version stays 3, retained manifest returned");
            Files.write(out.resolve("put-replay-alpha-report.pb"), replayRequest.toByteArray());
            record.put("replay", Map.of("tag", "alpha-report", "version", 3L));

            // Alpha "notes": one version carrying an EMPTY rendition (no object) beside a PRESENT one.
            var notes = ArchiveBackupQualificationFixture.address(alpha, ArchiveBackupQualificationFixture.ARCHIVE, "notes#1");
            var notesPut = archives.putEntry(operator, PutEntryRequest.newBuilder().setAddress(notes).setTitle("Notes").setWrittenBy(attribution)
                    .addRenditions(ArchiveBackupQualificationFixture.rendition("original", "text/plain", "synthetic notes body\n".getBytes(StandardCharsets.UTF_8)))
                    .addRenditions(ArchiveBackupQualificationFixture.rendition("empty", "text/plain", new byte[0])).build());
            checks.require(notesPut.getVersion() == 1 && ArchiveBackupQualificationContentChecks.rendition(notesPut.getManifest(), "empty").getState() == RenditionState.RENDITION_STATE_EMPTY
                    && ArchiveBackupQualificationContentChecks.rendition(notesPut.getManifest(), "original").getState() == RenditionState.RENDITION_STATE_PRESENT,
                    "seed.notes.v1", "PRESENT original and EMPTY rendition in one manifest");

            // Beta "subject": two versions sharing the original; the scratch rendition is then deleted (RTBF)
            // through the durable mutation protocol. Physical cleanup is left pending for the cut.
            var subject = ArchiveBackupQualificationFixture.address(beta, ArchiveBackupQualificationFixture.ARCHIVE, "subject-record");
            var subject1 = archives.putEntry(operator, PutEntryRequest.newBuilder().setAddress(subject).setTitle("Subject record").setWrittenBy(attribution)
                    .addRenditions(ArchiveBackupQualificationFixture.rendition("original", "text/plain", "synthetic subject source\n".getBytes(StandardCharsets.UTF_8)))
                    .addRenditions(ArchiveBackupQualificationFixture.rendition("scratch", "text/plain", "synthetic scratch one\n".getBytes(StandardCharsets.UTF_8))).build());
            var subject2 = archives.putEntry(operator, PutEntryRequest.newBuilder().setAddress(subject).setWrittenBy(attribution)
                    .addRenditions(ArchiveBackupQualificationFixture.rendition("scratch", "text/plain", "synthetic scratch two\n".getBytes(StandardCharsets.UTF_8))).build());
            checks.require(subject1.getVersion() == 1 && subject2.getVersion() == 2
                    && ArchiveBackupQualificationContentChecks.rendition(subject2.getManifest(), "original").getStorageObjectId()
                            .equals(ArchiveBackupQualificationContentChecks.rendition(subject1.getManifest(), "original").getStorageObjectId()),
                    "seed.subject.v2.shared_object", "version 2 re-references the original it did not carry");
            var deletion = ArchiveMutationRequest.newBuilder().setOperationId(UUID.randomUUID().toString())
                    .setDeleteRendition(DeleteRenditionRequest.newBuilder().setAddress(subject).setRendition("scratch").setReason("RTBF")).build();
            var admitted = mutations.mutateArchive(operator, deletion);
            checks.require(admitted.getState() == ArchiveMutationState.ARCHIVE_MUTATION_STATE_ADMITTED && admitted.getObjectsTargeted() == 2
                    && admitted.getObjectsPending() == 2 && admitted.getObjectsConfirmedAbsent() == 0 && admitted.getVersionsTombstoned() == 2
                    && !admitted.getEntryDeleted() && admitted.getStatusRevision() == 1, "seed.pending.admitted",
                    "RTBF rendition deletion admitted: two tombstoned versions, two physical targets pending, no cleanup yet");
            var replayedWhileLive = mutations.mutateArchive(operator, deletion);
            checks.require(ArchiveBackupQualificationFixture.logical(replayedWhileLive).equals(ArchiveBackupQualificationFixture.logical(admitted))
                    && replayedWhileLive.getState() == ArchiveMutationState.ARCHIVE_MUTATION_STATE_ADMITTED, "seed.pending.replay_live",
                    "replay while live returns the same logical admission, still pending");
            for (long version : List.of(1L, 2L)) {
                var manifest = archives.getManifest(operator, GetEntryManifestRequest.newBuilder().setAddress(subject).setVersion(version).build()).getManifest();
                var tombstone = ArchiveBackupQualificationContentChecks.rendition(manifest, "scratch");
                checks.require(tombstone.getState() == RenditionState.RENDITION_STATE_DELETED && tombstone.getDeletedReason().equals("RTBF")
                        && !tombstone.getSha256().isBlank() && tombstone.getSizeBytes() > 0 && !tombstone.getStorageObjectId().isBlank()
                        && manifest.getRootChecksum().equals(ArchiveManifests.rootChecksum(manifest.getRenditionsList())),
                        "seed.pending.tombstone.v" + version, "tombstone keeps size, digest, binding and reason as provenance");
            }
            Files.write(out.resolve("mutation-request-beta-subject.pb"), deletion.toByteArray());
            Files.write(out.resolve("mutation-receipt-beta-subject.pb"), admitted.toByteArray());

            // Beta unversioned scratchpad: the superseded state is genuinely gone, the counter still advances.
            var draft = ArchiveBackupQualificationFixture.address(beta, ArchiveBackupQualificationFixture.SCRATCH_ARCHIVE, "draft");
            var draft1 = archives.putEntry(operator, PutEntryRequest.newBuilder().setAddress(draft).setTitle("Draft").setWrittenBy(attribution)
                    .addRenditions(ArchiveBackupQualificationFixture.rendition("original", "text/plain", "synthetic draft, first state\n".getBytes(StandardCharsets.UTF_8))).build());
            var draft2 = archives.putEntry(operator, PutEntryRequest.newBuilder().setAddress(draft).setWrittenBy(attribution)
                    .addRenditions(ArchiveBackupQualificationFixture.rendition("original", "text/plain", "synthetic draft, second state\n".getBytes(StandardCharsets.UTF_8))).build());
            String superseded = ArchiveBackupQualificationContentChecks.outcome(() -> archives.getEntry(operator, GetEntryRequest.newBuilder().setAddress(draft).setVersion(1).build()));
            checks.require(draft1.getVersion() == 1 && draft2.getVersion() == 2 && superseded.equals("NOT_FOUND")
                    && archives.listVersions(operator, ListVersionsRequest.newBuilder().setAddress(draft).build()).getVersionsList().stream()
                            .map(VersionManifest::getVersion).toList().equals(List.of(2L)),
                    "seed.draft.unversioned", "NONE policy: version 1 is not retained (" + superseded + "), only version 2 lists");

            var entries = List.of(
                    entry("alpha-report", alpha, ArchiveBackupQualificationFixture.ARCHIVE, "report.pdf#2026", put1.getEntryUuid(), List.of(1L, 2L, 3L), 3L),
                    entry("alpha-notes", alpha, ArchiveBackupQualificationFixture.ARCHIVE, "notes#1", notesPut.getEntryUuid(), List.of(1L), 1L),
                    entry("beta-subject", beta, ArchiveBackupQualificationFixture.ARCHIVE, "subject-record", subject1.getEntryUuid(), List.of(1L, 2L), 2L),
                    entry("beta-draft", beta, ArchiveBackupQualificationFixture.SCRATCH_ARCHIVE, "draft", draft1.getEntryUuid(), List.of(2L), 2L));
            record.put("entries", entries);

            // Ledger-reported identities: physical bindings, provider versions, references, cleanup and retention state.
            var bindings = new LinkedHashMap<String, Object>();
            var entryRows = new LinkedHashMap<String, Object>();
            var versionRows = new LinkedHashMap<String, Object>();
            try (var ledger = ArchiveBackupQualificationLedger.fromEnvironment(); var catalog = RepositoryBackupRehearsalLedger.fromEnvironment()) {
                for (var entry : entries) {
                    String tag = RepositoryBackupRehearsalJson.string(entry, "tag");
                    var id = UUID.fromString(RepositoryBackupRehearsalJson.string(entry, "entryUuid"));
                    var rows = ledger.bindings(id);
                    checks.require(rows.stream().allMatch(row -> "LIVE".equals(row.get("state")) && row.get("providerVersion") != null
                            && !((String) row.get("providerVersion")).isBlank() && generation.equals(row.get("generation")) && realm.equals(row.get("realm"))
                            && ((Number) row.get("references")).longValue() == ((Number) row.get("sharedReferences")).longValue()),
                            "seed.ledger.bindings." + tag, rows.size() + " LIVE bindings under generation " + generation + ", each with a provider version id and mirrored references");
                    bindings.put(tag, rows);
                    entryRows.put(tag, ledger.entry(id));
                    versionRows.put(tag, ledger.versions(id));
                }
                var reportRows = ledger.bindings(UUID.fromString(put1.getEntryUuid()));
                checks.require(reportRows.size() == 4 && reportRows.stream().mapToLong(row -> ((Number) row.get("references")).longValue()).sum() == 7
                        && reportRows.stream().anyMatch(row -> "1,2,3".equals(row.get("referencedVersions"))),
                        "seed.ledger.report_sharing", "four physical objects across seven version references; the original is referenced by versions 1,2,3");
                var subjectRows = ledger.bindings(UUID.fromString(subject1.getEntryUuid()));
                var targets = subjectRows.stream().filter(row -> Boolean.TRUE.equals(row.get("mutationTarget"))).toList();
                checks.require(subjectRows.size() == 3 && targets.size() == 2 && targets.stream().allMatch(row -> ((Number) row.get("references")).longValue() == 0
                        && Boolean.TRUE.equals(row.get("retiring")) && Boolean.FALSE.equals(row.get("reclaiming")) && ((Number) row.get("cleanupAttempts")).longValue() == 0)
                        && subjectRows.stream().filter(row -> !Boolean.TRUE.equals(row.get("mutationTarget"))).allMatch(row -> "1,2".equals(row.get("referencedVersions"))),
                        "seed.ledger.pending_targets", "two unreferenced LIVE targets fenced retiring without a cleanup attempt; the shared original keeps both references");
                var draftRows = ledger.bindings(UUID.fromString(draft1.getEntryUuid()));
                checks.require(draftRows.size() == 2 && draftRows.stream().filter(row -> ((Number) row.get("references")).longValue() == 0).count() == 1
                        && draftRows.stream().noneMatch(row -> Boolean.TRUE.equals(row.get("retiring"))),
                        "seed.ledger.draft_superseded", "the superseded NONE-policy object stays LIVE and unreferenced for aged reconciliation");
                record.put("bindings", bindings);
                record.put("entryRows", entryRows);
                record.put("versionRows", versionRows);
                var pending = new LinkedHashMap<String, Object>();
                pending.put("tag", "beta-subject");
                pending.put("operationId", deletion.getOperationId());
                pending.put("principal", operator.principalName());
                pending.put("account", beta);
                pending.put("entryUuid", subject1.getEntryUuid());
                pending.put("commandSha256", admitted.getCommandSha256());
                pending.put("admission", ledger.mutation(beta, operator.principalName(), UUID.fromString(deletion.getOperationId())));
                pending.put("targets", targets.stream().map(row -> row.get("objectId")).toList());
                record.put("pending", pending);
                var catalogRecord = new LinkedHashMap<String, Object>();
                catalog.schemaCatalog().forEach(catalogRecord::put);
                record.put("schemaCatalog", catalogRecord);
                record.put("sequences", new LinkedHashMap<String, Object>(catalog.sequences()));
                record.put("rowCounts", new LinkedHashMap<String, Object>(catalog.rowCounts()));
                record.put("migrationLevel", catalog.migrationLevel());
                record.put("backendProfile", catalog.backendProfile(generation));
            }
            for (var entry : entries) {
                String tag = RepositoryBackupRehearsalJson.string(entry, "tag");
                var address = ArchiveBackupQualificationContentChecks.address(entry);
                Files.write(out.resolve("versions-" + tag + ".pb"), archives.listVersions(operator, ListVersionsRequest.newBuilder().setAddress(address).build()).toByteArray());
            }
            for (var archive : archiveRecords) {
                String account = RepositoryBackupRehearsalJson.string(archive, "account"), name = RepositoryBackupRehearsalJson.string(archive, "name");
                Files.write(out.resolve("stats-" + account + "-" + name + ".pb"),
                        archives.stats(operator, GetArchiveStatsRequest.newBuilder().setAccountId(account).setArchive(name).build()).toByteArray());
            }
            RepositoryBackupRehearsalJson.write(out.resolve("identities.json"), record);

            // Replay first (an elided save still touches current metadata), then record the final reads.
            var content = new ArchiveBackupQualificationContentChecks(host, checks, out, record);
            content.verifyReplay("seed");
            // An elided save still revises the entry row, so the ledger rows are recorded after the replay check.
            try (var ledger = ArchiveBackupQualificationLedger.fromEnvironment()) {
                for (var entry : entries) {
                    String tag = RepositoryBackupRehearsalJson.string(entry, "tag");
                    var id = UUID.fromString(RepositoryBackupRehearsalJson.string(entry, "entryUuid"));
                    bindings.put(tag, ledger.bindings(id));
                    entryRows.put(tag, ledger.entry(id));
                    versionRows.put(tag, ledger.versions(id));
                }
            }
            for (var entry : entries) {
                String tag = RepositoryBackupRehearsalJson.string(entry, "tag");
                var address = ArchiveBackupQualificationContentChecks.address(entry);
                for (long version : ArchiveBackupQualificationContentChecks.versions(entry))
                    Files.write(out.resolve("entry-" + tag + "-v" + version + ".pb"),
                            archives.getEntry(operator, GetEntryRequest.newBuilder().setAddress(address).setVersion(version).build()).toByteArray());
            }
            content.verifyEntries("seed", true);
            content.verifyVersions("seed", true);
            content.verifyStats("seed");
            record.put("accessMatrix", content.verifyAccess("seed"));
            content.verifyTransport("seed");
            ArchiveBackupQualificationContentChecks.verifyProviderObjects(checks, "seed", endpoint, allBindings(record));
            RepositoryBackupRehearsalJson.write(out.resolve("identities.json"), record);
        }
        // The host is closed: record the state it left behind for the recovered host to compare.
        try (var ledger = ArchiveBackupQualificationLedger.fromEnvironment(); var catalog = RepositoryBackupRehearsalLedger.fromEnvironment()) {
            var incarnations = new LinkedHashMap<String, Object>();
            for (String state : List.of("ACTIVE", "FENCED", "UNKNOWN", "QUIESCED")) incarnations.put(state, catalog.readerIncarnations(state));
            record.put("readerIncarnations", incarnations);
            record.put("rowCountsAfterClose", new LinkedHashMap<String, Object>(catalog.rowCounts()));
            var uploadStates = ledger.uploadStates();
            record.put("uploadStatesAfterClose", new LinkedHashMap<String, Object>(uploadStates));
            checks.require(catalog.readerIncarnations("ACTIVE") == 0, "seed.host.incarnations_closed", "no ACTIVE reader incarnation after close: " + incarnations);
            checks.require(ledger.archiveReadPins() == 0, "seed.host.pins_released", "no archive read pin after close");
            checks.require(uploadStates.keySet().equals(Set.of("LIVE")), "seed.host.uploads_settled", "every upload row is LIVE (no staging, verification or cleanup in flight): " + uploadStates);
        }
        RepositoryBackupRehearsalJson.write(out.resolve("identities.json"), record);
        checks.writeXml(out.resolve("seed-results.xml"));
        System.out.println("ARCHIVE_BACKUP_QUALIFICATION_SEED_OK");
    }

    static Map<String, Object> entry(String tag, String account, String archive, String entryId, String entryUuid, List<Long> versions, long current) {
        var entry = new LinkedHashMap<String, Object>();
        entry.put("tag", tag);
        entry.put("account", account);
        entry.put("archive", archive);
        entry.put("entryId", entryId);
        entry.put("entryUuid", entryUuid);
        entry.put("versions", versions);
        entry.put("currentVersion", current);
        return entry;
    }

    /** Every recorded binding of every entry, in record order. */
    @SuppressWarnings("unchecked")
    static List<Map<String, Object>> allBindings(Map<String, Object> record) {
        var all = new ArrayList<Map<String, Object>>();
        for (var rows : RepositoryBackupRehearsalJson.object(record, "bindings").values()) all.addAll((List<Map<String, Object>>) rows);
        return all;
    }

    /** Reads the provisioned bucket's versioning status through the plain S3 API, independent of the host. */
    private static boolean versioningEnabled(String endpoint, String bucket) {
        try (var client = ArchiveBackupQualificationFixture.s3(endpoint)) {
            return client.getBucketVersioning(request -> request.bucket(bucket)).status() == BucketVersioningStatus.ENABLED;
        }
    }
}
