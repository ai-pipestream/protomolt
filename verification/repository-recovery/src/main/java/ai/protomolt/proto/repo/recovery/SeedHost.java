package ai.protomolt.proto.repo.recovery;

import ai.protomolt.proto.asset.bridge.BridgeEngine;
import ai.protomolt.proto.repo.archive.v1.*;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.container.ledger.LedgerDatabase;
import ai.protomolt.proto.repo.container.ledger.Tx;
import ai.protomolt.proto.repo.service.RepoServices;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.ByteString;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Seeds one repository through production paths (drives, typed publication with a retained
 * imported descriptor, archive versions) over real SQL and RustFS, records every identity the
 * responses and the ledger report, verifies them once while live, then drains and exits.
 */
public final class SeedHost {
    public static void main(String[] args) throws Exception {
        RehearsalFixture.requireNoTestFramework();
        var out = Path.of(RehearsalFixture.env("PROTOMOLT_RECOVERY_IDENTITIES"));
        Files.createDirectories(out);
        var checks = new Checks("seed", out.resolve("markers.log"));
        String endpoint = RehearsalFixture.env("PROTOMOLT_RECOVERY_S3_ENDPOINT");
        String generation = RehearsalFixture.env("PROTOMOLT_RECOVERY_GENERATION");
        String realm = RehearsalFixture.env("PROTOMOLT_RECOVERY_REALM");
        var bundle = Path.of(RehearsalFixture.env("PROTOMOLT_RECOVERY_BUNDLE"));
        var config = RehearsalFixture.config(endpoint, generation, realm);
        var type = RehearsalFixture.recordType();
        var definition = RehearsalFixture.definition(type);
        var registry = Files.createTempDirectory("recovery-seed-registry-");
        var record = new LinkedHashMap<String, Object>();
        String account = "recovery-account-" + UUID.randomUUID();
        record.put("account", account);
        record.put("generation", generation);
        record.put("realm", realm);
        record.put("endpoint", endpoint);
        record.put("typeUrl", definition.metadata().getTypeUrl());
        record.put("schemaArtifactSha256", definition.metadata().getArtifactSha256());
        record.put("toolIdentity", Map.of("name", definition.metadata().getCompilation().getAdmissionRuntime().getName(),
                "version", definition.metadata().getCompilation().getAdmissionRuntime().getVersion()));
        var live = RehearsalFixture.liveSchemas(registry, definition);
        try (var database = new LedgerDatabase(config.ledger());
             var host = RepoServices.build(config, BridgeEngine.standard(), RehearsalFixture.historicalAccess(), live.access(),
                     RehearsalFixture.publicationOptions(bundle))) {
            var tx = new Tx(database.entityManagerFactory());
            var operator = RehearsalFixture.operator();
            // Qualified buckets are versioned. The provisioner creates unversioned buckets, under which
            // S3 returns no version id and the ledger records none; this rehearsal requires exact versions.
            String suffix = account.substring(account.length() - 8);
            String pipelineBucket = versionedBucket(endpoint, "recovery-pipeline-" + suffix);
            String archiveBucket = versionedBucket(endpoint, "recovery-archive-" + suffix);
            var drive = host.driveRepository().createDrive(operator, CreateDriveRequest.newBuilder().setName("recovery-pipeline")
                    .setAccountId(account).setDriveType(DriveType.DRIVE_TYPE_PIPELINE).setBucket(pipelineBucket).build()).getDrive();
            var archiveDrive = host.driveRepository().createDrive(operator, CreateDriveRequest.newBuilder().setName("recovery-archive")
                    .setAccountId(account).setDriveType(DriveType.DRIVE_TYPE_CUSTOM).setBucket(archiveBucket).build()).getDrive();
            checks.require("s3".equals(drive.getProvider()) && !drive.getBucket().isBlank(), "seed.drive", "drive=" + drive.getDriveId() + " bucket=" + drive.getBucket());
            record.put("drive", Map.of("id", drive.getDriveId(), "bucket", drive.getBucket(), "prefix", drive.getPrefix()));
            record.put("archiveDrive", Map.of("id", archiveDrive.getDriveId(), "bucket", archiveDrive.getBucket(), "prefix", archiveDrive.getPrefix()));
            var policy = RehearsalFixture.installTypedPolicy(tx, account);
            record.put("policySha256", policy.sha256());

            // Revision 1: open security; revision 2: different data, public grant revoked.
            var v1 = RehearsalFixture.fixture(account, drive.getDriveId(),
                    RehearsalFixture.payload(type, "rehearsal-v1", 1_700_000_000L, "first committed revision"), RehearsalFixture.openSecurity(), -1);
            var receipt1 = host.publicationRepository().publishDocument(operator, v1.request(), RepositoryReadControl.NONE);
            checks.require(receipt1.hasCommitted() && receipt1.getCommitted().getMembersCount() == 1, "seed.publish.v1", "committed revision " + receipt1.getCommitted().getMembers(0).getRevisionId());
            var member1 = receipt1.getCommitted().getMembers(0);
            checks.require(host.publicationRepository().publishDocument(operator, v1.request(), RepositoryReadControl.NONE).equals(receipt1), "seed.replay.v1.live", "receipt replay while live");
            var v2 = RehearsalFixture.fixture(account, drive.getDriveId(),
                    RehearsalFixture.payload(type, "rehearsal-v2", 1_700_000_100L, "second committed revision with revoked public grant"),
                    RehearsalFixture.memberOnlySecurity(), member1.getMutationRevision());
            var receipt2 = host.publicationRepository().publishDocument(operator, v2.request(), RepositoryReadControl.NONE);
            checks.require(receipt2.hasCommitted(), "seed.publish.v2", "committed revision " + receipt2.getCommitted().getMembers(0).getRevisionId());
            var member2 = receipt2.getCommitted().getMembers(0);
            checks.require(member2.getAddress().equals(member1.getAddress()) && !member2.getRevisionId().equals(member1.getRevisionId())
                    && member2.getMutationRevision() > member1.getMutationRevision(), "seed.publish.v2.identity",
                    "same address, new revision, mutation " + member1.getMutationRevision() + " -> " + member2.getMutationRevision());
            checks.require(!v1.document().equals(v2.document()), "seed.publish.distinct_data", "revision payloads differ");
            checks.require(live.opens().get() >= 2, "seed.schema.live_resolution_used", "registry resolutions=" + live.opens().get());
            Files.write(out.resolve("request-v1.pb"), v1.request().toByteArray());
            Files.write(out.resolve("receipt-v1.pb"), receipt1.toByteArray());
            Files.write(out.resolve("document-v1.pb"), v1.document().toByteArray());
            Files.write(out.resolve("request-v2.pb"), v2.request().toByteArray());
            Files.write(out.resolve("receipt-v2.pb"), receipt2.toByteArray());
            Files.write(out.resolve("document-v2.pb"), v2.document().toByteArray());

            var revisions = new ArrayList<Map<String, Object>>();
            var member = RehearsalFixture.member(account);
            for (var entry : List.of(Map.entry("v1", member1), Map.entry("v2", member2))) {
                var revision = new LinkedHashMap<String, Object>();
                var id = UUID.fromString(entry.getValue().getRevisionId());
                revision.put("tag", entry.getKey());
                revision.put("revisionId", entry.getValue().getRevisionId());
                revision.put("mutationRevision", entry.getValue().getMutationRevision());
                revision.put("operationId", entry.getKey().equals("v1") ? v1.request().getIntent().getOperationId() : v2.request().getIntent().getOperationId());
                var fragments = new ArrayList<Map<String, Object>>();
                try (var raw = host.historicalRepository().readRaw(member, RehearsalFixture.address(account), id, RepositoryReadControl.NONE)) {
                    for (var fragment : raw.fragments()) {
                        var bytes = new byte[fragment.bytes().remaining()]; fragment.bytes().get(bytes);
                        fragments.add(Map.of("revisionOrdinal", (long) fragment.revisionOrdinal(), "sha256", DocumentPartCodec.sha256Hex(bytes), "size", (long) bytes.length));
                    }
                }
                revision.put("rawFragments", fragments);
                try (var validated = host.historicalRepository().readValidated(member, RehearsalFixture.address(account), id, RepositoryReadControl.NONE)) {
                    revision.put("commandSha256Sha256", DocumentPartCodec.sha256Hex(validated.commandSha256().toByteArray()));
                    revision.put("policySha256", validated.policySha256());
                    revision.put("validationProfile", validated.validationProfile());
                    revision.put("metadataSha256", DocumentPartCodec.sha256Hex(validated.metadata().toByteArray()));
                    revision.put("manifestSha256", DocumentPartCodec.sha256Hex(validated.manifest().toByteArray()));
                }
                revisions.add(revision);
            }
            record.put("revisions", revisions);

            // Archive: two versions sharing the unchanged rendition, then a deduplicated save.
            host.archiveRepository().createArchive(operator, CreateArchiveRequest.newBuilder().setArchive(Archive.newBuilder()
                    .setName("recovery-docs").setAccountId(account).setDriveName("recovery-archive")
                    .setVersioning(VersioningPolicy.VERSIONING_POLICY_RETAINED)).build());
            var entryAddress = EntryAddress.newBuilder().setAccountId(account).setArchive("recovery-docs").setEntryId("report.pdf#2026").build();
            String original = "raw-pdf-bytes-" + account, markdown1 = "# Report", markdown2 = "# Report, revised";
            var put1 = host.archiveRepository().putEntry(operator, PutEntryRequest.newBuilder().setAddress(entryAddress).setTitle("Quarterly report")
                    .setFilename("report.pdf").setContentType("application/pdf")
                    .addRenditions(rendition("original", "application/pdf", original))
                    .addRenditions(rendition("markdown", "text/markdown", markdown1)).build());
            var put2 = host.archiveRepository().putEntry(operator, PutEntryRequest.newBuilder().setAddress(entryAddress).setTitle("Revised report")
                    .addRenditions(rendition("original", "application/pdf", original))
                    .addRenditions(rendition("markdown", "text/markdown", markdown2)).build());
            checks.require(put1.getVersion() == 1 && put2.getVersion() == 2 && !put2.getDeduplicated(), "seed.archive.versions", "versions 1 and 2 committed");
            var shared = ContentChecks.rendition(put1.getManifest(), "original").getObjectKey();
            checks.require(shared.equals(ContentChecks.rendition(put2.getManifest(), "original").getObjectKey()), "seed.archive.shared_object", "shared key=" + shared);
            var dedup = host.archiveRepository().putEntry(operator, PutEntryRequest.newBuilder().setAddress(entryAddress).setTitle("Current label")
                    .addRenditions(rendition("markdown", "text/markdown", markdown2)).build());
            checks.require(dedup.getDeduplicated() && dedup.getVersion() == 2, "seed.archive.dedup", "identical content did not create a version");
            Files.write(out.resolve("archive-manifest-v1.pb"), put1.getManifest().toByteArray());
            Files.write(out.resolve("archive-manifest-v2.pb"), put2.getManifest().toByteArray());
            for (long version : List.of(1L, 2L)) {
                var entry = host.archiveRepository().getEntry(operator, GetEntryRequest.newBuilder().setAddress(entryAddress).setVersion(version).build());
                Files.write(out.resolve("archive-entry-v" + version + ".pb"), entry.toByteArray());
            }
            var stats = host.archiveRepository().stats(operator, GetArchiveStatsRequest.newBuilder().setAccountId(account).setArchive("recovery-docs").build()).getStats();
            var archiveRecord = new LinkedHashMap<String, Object>();
            archiveRecord.put("name", "recovery-docs");
            archiveRecord.put("entryId", "report.pdf#2026");
            archiveRecord.put("entryUuid", put1.getManifest().getMetadataSnapshot().getEntryUuid());
            archiveRecord.put("sharedObjectKey", shared);
            archiveRecord.put("markdownV2", markdown2);
            archiveRecord.put("retainedBytes", stats.getRetainedBytes());
            archiveRecord.put("v3Bytes", (long) "# Report, restored".length());
            record.put("archive", archiveRecord);

            // Ledger-reported identities: provider versions, catalog digests, sequences, counts.
            try (var ledger = new Ledger(RehearsalFixture.env("PROTOMOLT_RECOVERY_JDBC"), RehearsalFixture.env("PROTOMOLT_RECOVERY_DB_USER"), RehearsalFixture.env("PROTOMOLT_RECOVERY_DB_PASSWORD"))) {
                var documentObjects = ledger.documentObjects(List.of(UUID.fromString(member1.getRevisionId()), UUID.fromString(member2.getRevisionId())));
                var archiveObjects = ledger.archiveObjects(UUID.fromString(put1.getManifest().getMetadataSnapshot().getEntryUuid()));
                checks.require(documentObjects.stream().allMatch(row -> row.get("providerVersion") != null && !((String) row.get("providerVersion")).isBlank())
                        && archiveObjects.stream().allMatch(row -> row.get("providerVersion") != null && !((String) row.get("providerVersion")).isBlank()
                                && ("VERIFIED".equals(row.get("state")) || "LIVE".equals(row.get("state")))),
                        "seed.ledger.provider_versions", "document objects=" + documentObjects.size() + " archive objects=" + archiveObjects.size());
                checks.require(archiveObjects.stream().map(row -> row.get("objectId")).distinct().count() == 3 && archiveObjects.size() == 4,
                        "seed.ledger.archive_sharing", "three physical objects across four version references");
                record.put("documentObjects", documentObjects);
                record.put("archiveObjects", archiveObjects);
                var catalog = ledger.schemaCatalog();
                checks.require(catalog.keySet().stream().anyMatch(key -> key.endsWith("/" + definition.metadata().getArtifactSha256())), "seed.ledger.catalog",
                        "catalog holds the retained descriptor artifact; entries=" + catalog.size());
                var catalogRecord = new LinkedHashMap<String, Object>();
                catalog.forEach(catalogRecord::put);
                record.put("schemaCatalog", catalogRecord);
                record.put("sequences", new LinkedHashMap<String, Object>(ledger.sequences()));
                record.put("rowCounts", new LinkedHashMap<String, Object>(ledger.rowCounts()));
                record.put("migrationLevel", ledger.migrationLevel());
                record.put("backendProfile", ledger.backendProfile(generation));
                var selection = ledger.materializationSelection(UUID.fromString(member1.getRevisionId()));
                selection.put("label", "rehearsal-v1");
                checks.require(definition.metadata().getArtifactSha256().equals(selection.get("artifactSha256")), "seed.ledger.evidence_artifact", "evidence references the retained artifact");
                record.put("materialization", selection);
                record.put("readerIncarnations", ledger.rowCounts().get("repository_reader_incarnations"));
            }
            record.put("registryOpensBeforeHistoricalReads", live.opens().get());
            Json.write(out.resolve("identities.json"), record);

            // Observe the matrix once while live, with the registry closed: historical reads are retained-only.
            live.access().close();
            checks.require(live.access().awaitIdle(Duration.ofSeconds(10)), "seed.schema.resolver_drained", "live resolver closed before historical verification");
            int opensBefore = live.opens().get();
            var content = new ContentChecks(host, checks, out, record);
            content.verifyHistory("seed");
            content.verifyMaterialization("seed");
            content.verifyArchive("seed");
            content.verifyReplay("seed");
            record.put("accessMatrix", content.verifyAccess("seed"));
            content.verifyTransport("seed");
            ContentChecks.verifyProviderObjects(checks, "seed", endpoint, allObjects(record));
            checks.require(live.opens().get() == opensBefore, "seed.schema.no_live_lookup_for_history", "registry resolutions unchanged at " + opensBefore);
            Json.write(out.resolve("identities.json"), record);
        } finally {
            live.store().close();
            try (var paths = Files.walk(registry)) { for (var path : paths.sorted(Comparator.reverseOrder()).toList()) Files.delete(path); }
            checks.pass("seed.registry_deleted", "git schema registry directory removed: " + registry);
        }
        checks.writeXml(out.resolve("seed-results.xml"));
        System.out.println("RECOVERY_SEED_OK");
    }

    @SuppressWarnings("unchecked")
    static List<Map<String, Object>> allObjects(Map<String, Object> record) {
        var all = new ArrayList<Map<String, Object>>((List<Map<String, Object>>) record.get("documentObjects"));
        all.addAll((List<Map<String, Object>>) record.get("archiveObjects"));
        return all;
    }

    /** Operator-qualified bucket: created with versioning enabled before the drive binds to it. */
    private static String versionedBucket(String endpoint, String name) {
        try (var client = software.amazon.awssdk.services.s3.S3Client.builder().endpointOverride(java.net.URI.create(endpoint))
                .region(software.amazon.awssdk.regions.Region.of(RehearsalFixture.env("PROTOMOLT_RECOVERY_S3_REGION"))).forcePathStyle(true)
                .httpClientBuilder(software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient.builder())
                .credentialsProvider(software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
                        software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create(
                                RehearsalFixture.env("PROTOMOLT_RECOVERY_S3_ACCESS"), RehearsalFixture.env("PROTOMOLT_RECOVERY_S3_SECRET")))).build()) {
            client.createBucket(request -> request.bucket(name));
            client.putBucketVersioning(request -> request.bucket(name).versioningConfiguration(
                    configuration -> configuration.status(software.amazon.awssdk.services.s3.model.BucketVersioningStatus.ENABLED)));
            var status = client.getBucketVersioning(request -> request.bucket(name)).status();
            if (status != software.amazon.awssdk.services.s3.model.BucketVersioningStatus.ENABLED)
                throw new RehearsalFailure("Bucket versioning is not enabled on " + name + ": " + status);
            return name;
        }
    }

    private static RenditionContent rendition(String name, String mediaType, String body) {
        return RenditionContent.newBuilder().setRendition(RenditionDescriptor.newBuilder().setName(name).setMediaType(mediaType))
                .setData(ByteString.copyFromUtf8(body)).build();
    }
}
