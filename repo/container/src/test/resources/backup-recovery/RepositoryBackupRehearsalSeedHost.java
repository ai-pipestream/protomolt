package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.asset.bridge.BridgeEngine;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.service.RepoServices;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import ai.protomolt.proto.repo.v1.*;
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
 * Seeds one repository through production paths over real SQL and pinned RustFS: two accounts,
 * typed revisions with a nested Any attachment, a current and a historical revision with a
 * revoked public grant, and a journaled publication left pending with V118/V119 coverage
 * states. Records every identity the responses and the ledger report, verifies them once
 * while live with the registry closed, then drains and exits. Adapted from GitHub PR #413.
 */
public final class RepositoryBackupRehearsalSeedHost {
    public static void main(String[] args) throws Exception {
        RepositoryBackupRehearsalFixture.requireNoTestFramework();
        var out = Path.of(RepositoryBackupRehearsalFixture.env("PROTOMOLT_REHEARSAL_IDENTITIES"));
        Files.createDirectories(out);
        var checks = new RepositoryBackupRehearsalChecks("seed", out.resolve("markers.log"));
        String endpoint = RepositoryBackupRehearsalFixture.env("PROTOMOLT_REHEARSAL_S3_ENDPOINT");
        String generation = RepositoryBackupRehearsalFixture.env("PROTOMOLT_REHEARSAL_GENERATION");
        String realm = RepositoryBackupRehearsalFixture.env("PROTOMOLT_REHEARSAL_REALM");
        var bundle = Path.of(RepositoryBackupRehearsalFixture.env("PROTOMOLT_REHEARSAL_BUNDLE"));
        var config = RepositoryBackupRehearsalFixture.config(endpoint, generation, realm);
        var types = RepositoryBackupRehearsalFixture.types();
        var definitions = RepositoryBackupRehearsalFixture.definitions(types);
        var registry = Files.createTempDirectory("rehearsal-seed-registry-");
        var record = new LinkedHashMap<String, Object>();
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        String alpha = "rehearsal-alpha-" + suffix, beta = "rehearsal-beta-" + suffix;
        record.put("accounts", List.of(alpha, beta));
        record.put("generation", generation);
        record.put("realm", realm);
        record.put("endpoint", endpoint);
        var artifacts = new LinkedHashMap<String, Object>();
        definitions.forEach((url, definition) -> artifacts.put(url, definition.metadata().getArtifactSha256()));
        record.put("artifacts", artifacts);
        var live = RepositoryBackupRehearsalFixture.liveSchemas(registry, definitions);
        try (var database = new LedgerDatabase(config.ledger());
             var host = RepoServices.build(config, BridgeEngine.standard(), RepositoryBackupRehearsalFixture.historicalAccess(), live.access(),
                     RepositoryBackupRehearsalFixture.publicationOptions(bundle))) {
            var tx = new Tx(database.entityManagerFactory());
            var operator = RepositoryBackupRehearsalFixture.operator();
            var drives = new LinkedHashMap<String, Object>();
            var policies = new LinkedHashMap<String, Object>();
            for (String account : List.of(alpha, beta)) {
                // Qualified buckets are versioned. The provisioner would create an unversioned bucket, under
                // which S3 returns no version id and the ledger records none; the rehearsal requires exact versions.
                String bucket = versionedBucket(endpoint, account);
                var drive = host.driveRepository().createDrive(operator, CreateDriveRequest.newBuilder().setName("rehearsal-pipeline")
                        .setAccountId(account).setDriveType(DriveType.DRIVE_TYPE_PIPELINE).setBucket(bucket).build()).getDrive();
                checks.require("s3".equals(drive.getProvider()) && drive.getBucket().equals(bucket), "seed.drive." + account, "drive=" + drive.getDriveId() + " bucket=" + bucket);
                drives.put(account, Map.of("id", drive.getDriveId(), "bucket", drive.getBucket(), "prefix", drive.getPrefix()));
                policies.put(account, RepositoryBackupRehearsalFixture.installTypedPolicy(tx, account).sha256());
            }
            record.put("drives", drives);
            record.put("policies", policies);
            String alphaDrive = (String) ((Map<?, ?>) drives.get(alpha)).get("id"), betaDrive = (String) ((Map<?, ?>) drives.get(beta)).get("id");

            // Alpha: revision 1 open to the public, revision 2 revokes the grant with different data and attachment.
            var a1 = RepositoryBackupRehearsalFixture.fixture(alpha, alphaDrive, RepositoryBackupRehearsalFixture.payload(types, "alpha-v1", 1_700_000_000L,
                    "first committed revision", "attachment-one", "alpha attachment bytes one".getBytes()), RepositoryBackupRehearsalFixture.openSecurity(alpha), -1);
            var receiptA1 = host.publicationRepository().publishDocument(operator, a1.request(), RepositoryReadControl.NONE);
            checks.require(receiptA1.hasCommitted() && receiptA1.getCommitted().getMembersCount() == 1, "seed.publish.alpha-v1", "committed revision " + receiptA1.getCommitted().getMembers(0).getRevisionId());
            var memberA1 = receiptA1.getCommitted().getMembers(0);
            checks.require(host.publicationRepository().publishDocument(operator, a1.request(), RepositoryReadControl.NONE).equals(receiptA1), "seed.replay.alpha-v1.live", "receipt replay while live");
            var a2 = RepositoryBackupRehearsalFixture.fixture(alpha, alphaDrive, RepositoryBackupRehearsalFixture.payload(types, "alpha-v2", 1_700_000_100L,
                    "second committed revision with revoked public grant", "attachment-two", "alpha attachment bytes two".getBytes()),
                    RepositoryBackupRehearsalFixture.memberOnlySecurity(alpha), memberA1.getMutationRevision());
            var receiptA2 = host.publicationRepository().publishDocument(operator, a2.request(), RepositoryReadControl.NONE);
            checks.require(receiptA2.hasCommitted(), "seed.publish.alpha-v2", "committed revision " + receiptA2.getCommitted().getMembers(0).getRevisionId());
            var memberA2 = receiptA2.getCommitted().getMembers(0);
            checks.require(memberA2.getAddress().equals(memberA1.getAddress()) && !memberA2.getRevisionId().equals(memberA1.getRevisionId())
                    && memberA2.getMutationRevision() > memberA1.getMutationRevision() && !a1.document().equals(a2.document()), "seed.publish.alpha-v2.identity",
                    "same address, new revision, distinct data, mutation " + memberA1.getMutationRevision() + " -> " + memberA2.getMutationRevision());
            // Beta: one member-only revision in a second account.
            var b1 = RepositoryBackupRehearsalFixture.fixture(beta, betaDrive, RepositoryBackupRehearsalFixture.payload(types, "beta-v1", 1_700_000_200L,
                    "second account revision", "attachment-beta", "beta attachment bytes".getBytes()), RepositoryBackupRehearsalFixture.memberOnlySecurity(beta), -1);
            var receiptB1 = host.publicationRepository().publishDocument(operator, b1.request(), RepositoryReadControl.NONE);
            checks.require(receiptB1.hasCommitted(), "seed.publish.beta-v1", "committed revision " + receiptB1.getCommitted().getMembers(0).getRevisionId());
            var memberB1 = receiptB1.getCommitted().getMembers(0);
            checks.require(live.opens().get() >= 3, "seed.schema.live_resolution_used", "registry resolutions=" + live.opens().get());
            var published = List.of(
                    new Published("alpha-v1", alpha, a1, receiptA1, memberA1, "attachment-one"),
                    new Published("alpha-v2", alpha, a2, receiptA2, memberA2, "attachment-two"),
                    new Published("beta-v1", beta, b1, receiptB1, memberB1, "attachment-beta"));
            var revisions = new ArrayList<Map<String, Object>>();
            try (var ledger = RepositoryBackupRehearsalLedger.fromEnvironment()) {
                for (var entry : published) {
                    Files.write(out.resolve("request-" + entry.tag() + ".pb"), entry.fixture().request().toByteArray());
                    Files.write(out.resolve("receipt-" + entry.tag() + ".pb"), entry.receipt().toByteArray());
                    Files.write(out.resolve("document-" + entry.tag() + ".pb"), entry.fixture().document().toByteArray());
                    var revision = new LinkedHashMap<String, Object>();
                    var id = UUID.fromString(entry.member().getRevisionId());
                    var member = RepositoryBackupRehearsalFixture.member(entry.account());
                    var address = RepositoryBackupRehearsalFixture.address(entry.account());
                    revision.put("tag", entry.tag());
                    revision.put("account", entry.account());
                    revision.put("revisionId", entry.member().getRevisionId());
                    revision.put("mutationRevision", entry.member().getMutationRevision());
                    revision.put("operationId", entry.fixture().request().getIntent().getOperationId());
                    var fragments = new ArrayList<Map<String, Object>>();
                    try (var raw = host.historicalRepository().readRaw(member, address, id, RepositoryReadControl.NONE)) {
                        for (var fragment : raw.fragments()) {
                            var bytes = new byte[fragment.bytes().remaining()]; fragment.bytes().get(bytes);
                            fragments.add(Map.of("revisionOrdinal", (long) fragment.revisionOrdinal(), "sha256", DocumentPartCodec.sha256Hex(bytes), "size", (long) bytes.length));
                        }
                    }
                    revision.put("rawFragments", fragments);
                    try (var validated = host.historicalRepository().readValidated(member, address, id, RepositoryReadControl.NONE)) {
                        revision.put("commandSha256Sha256", DocumentPartCodec.sha256Hex(validated.commandSha256().toByteArray()));
                        revision.put("policySha256", validated.policySha256());
                        revision.put("validationProfile", validated.validationProfile());
                        revision.put("metadataSha256", DocumentPartCodec.sha256Hex(validated.metadata().toByteArray()));
                        revision.put("manifestSha256", DocumentPartCodec.sha256Hex(validated.manifest().toByteArray()));
                    }
                    var selections = ledger.materializationSelections(id);
                    boolean nested = false, root = false;
                    for (var selection : selections) {
                        String url = RepositoryBackupRehearsalJson.string(selection, "typeUrl");
                        if (url.endsWith(RepositoryBackupRehearsalFixture.ATTACHMENT_TYPE)) { nested = true; selection.put("expectedField", entry.attachment()); }
                        else if (url.endsWith(RepositoryBackupRehearsalFixture.RECORD_TYPE)) { root = true; selection.put("expectedField", entry.tag()); }
                        else throw new RepositoryBackupRehearsalFailure("Unexpected occurrence type " + url);
                        checks.require(artifacts.get(url).equals(selection.get("artifactSha256")), "seed.ledger.evidence_artifact." + entry.tag() + "." + url.substring(url.lastIndexOf('.') + 1),
                                "evidence references the retained artifact " + selection.get("artifactSha256") + " for " + url);
                    }
                    checks.require(root && nested && selections.size() == 2, "seed.ledger.occurrences." + entry.tag(), "root and nested Any occurrences retained: " + selections.size());
                    revision.put("selections", selections);
                    revisions.add(revision);
                }
                record.put("revisions", revisions);

                // A journaled publication that never completed, with its coverage states (V118/V119).
                var pendingIntent = RepositoryBackupRehearsalFixture.fixture(alpha, alphaDrive, RepositoryBackupRehearsalFixture.payload(types, "alpha-pending", 1_700_000_300L,
                        "pending publication never executed", "attachment-pending", "pending bytes".getBytes()),
                        RepositoryBackupRehearsalFixture.memberOnlySecurity(alpha), memberA2.getMutationRevision()).request().getIntent();
                Files.write(out.resolve("pending-intent.pb"), pendingIntent.toByteArray());
                record.put("pending", RepositoryBackupRehearsalPendingOperation.seed(tx, checks, operator, generation, pendingIntent));

                // Ledger-reported identities: provider versions, catalog digests, sequences, counts.
                var documentObjects = ledger.documentObjects(published.stream().map(entry -> UUID.fromString(entry.member().getRevisionId())).toList());
                checks.require(documentObjects.size() >= published.size() && documentObjects.stream().allMatch(row -> row.get("providerVersion") != null
                        && !((String) row.get("providerVersion")).isBlank() && Boolean.TRUE.equals(row.get("verified"))),
                        "seed.ledger.provider_versions", "document objects=" + documentObjects.size() + ", every one verified with a provider version id");
                record.put("documentObjects", documentObjects);
                var catalog = ledger.schemaCatalog();
                for (var entry : artifacts.entrySet())
                    checks.require(catalog.keySet().stream().anyMatch(key -> key.endsWith("/" + entry.getValue())), "seed.ledger.catalog." + entry.getKey().substring(entry.getKey().lastIndexOf('.') + 1),
                            "catalog holds the retained descriptor artifact " + entry.getValue() + "; entries=" + catalog.size());
                var catalogRecord = new LinkedHashMap<String, Object>();
                catalog.forEach(catalogRecord::put);
                record.put("schemaCatalog", catalogRecord);
                record.put("sequences", new LinkedHashMap<String, Object>(ledger.sequences()));
                record.put("rowCounts", new LinkedHashMap<String, Object>(ledger.rowCounts()));
                record.put("migrationLevel", ledger.migrationLevel());
                record.put("backendProfile", ledger.backendProfile(generation));
            }
            record.put("registryOpensBeforeHistoricalReads", live.opens().get());
            RepositoryBackupRehearsalJson.write(out.resolve("identities.json"), record);

            // Observe everything once while live, with the registry closed: historical reads are retained-only.
            live.access().close();
            checks.require(live.access().awaitIdle(Duration.ofSeconds(10)), "seed.schema.resolver_drained", "live resolver closed before historical verification");
            int opensBefore = live.opens().get();
            var content = new RepositoryBackupRehearsalContentChecks(host, checks, out, record);
            content.verifyHistory("seed");
            content.verifyMaterialization("seed");
            content.verifyReplay("seed");
            record.put("accessMatrix", content.verifyAccess("seed"));
            content.verifyTransport("seed");
            RepositoryBackupRehearsalContentChecks.verifyProviderObjects(checks, "seed", endpoint, RepositoryBackupRehearsalJson.list(record, "documentObjects"));
            checks.require(live.opens().get() == opensBefore, "seed.schema.no_live_lookup_for_history", "registry resolutions unchanged at " + opensBefore);
            RepositoryBackupRehearsalJson.write(out.resolve("identities.json"), record);
        } finally {
            live.store().close();
            try (var paths = Files.walk(registry)) { for (var path : paths.sorted(Comparator.reverseOrder()).toList()) Files.delete(path); }
            checks.pass("seed.registry_deleted", "git schema registry directory removed: " + registry);
        }
        // The host is closed: record the incarnation states it left behind for the recovered host to compare.
        try (var ledger = RepositoryBackupRehearsalLedger.fromEnvironment()) {
            var incarnations = new LinkedHashMap<String, Object>();
            for (String state : List.of("ACTIVE", "FENCED", "UNKNOWN", "QUIESCED")) incarnations.put(state, ledger.readerIncarnations(state));
            record.put("readerIncarnations", incarnations);
            record.put("rowCountsAfterClose", new LinkedHashMap<String, Object>(ledger.rowCounts()));
            checks.require(ledger.readerIncarnations("ACTIVE") == 0, "seed.host.incarnations_closed", "no ACTIVE reader incarnation after close: " + incarnations);
        }
        RepositoryBackupRehearsalJson.write(out.resolve("identities.json"), record);
        checks.writeXml(out.resolve("seed-results.xml"));
        System.out.println("REPOSITORY_BACKUP_REHEARSAL_SEED_OK");
    }

    record Published(String tag, String account, RepositoryBackupRehearsalFixture.Fixture fixture, PublishDocumentResponse receipt,
            DocumentPublishedRevision member, String attachment) {}

    /** Operator-qualified bucket: created with versioning enabled before the drive binds to it. */
    private static String versionedBucket(String endpoint, String name) {
        try (var client = RepositoryBackupRehearsalFixture.s3(endpoint)) {
            client.createBucket(request -> request.bucket(name));
            client.putBucketVersioning(request -> request.bucket(name).versioningConfiguration(
                    configuration -> configuration.status(software.amazon.awssdk.services.s3.model.BucketVersioningStatus.ENABLED)));
            var status = client.getBucketVersioning(request -> request.bucket(name)).status();
            if (status != software.amazon.awssdk.services.s3.model.BucketVersioningStatus.ENABLED)
                throw new RepositoryBackupRehearsalFailure("Bucket versioning is not enabled on " + name + ": " + status);
            return name;
        }
    }
}
