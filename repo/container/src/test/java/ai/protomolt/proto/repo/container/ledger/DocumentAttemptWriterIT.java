package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.*;
import ai.protomolt.proto.repo.blob.s3.S3BackendIdentity;
import ai.protomolt.proto.repo.codec.*;
import ai.protomolt.proto.repo.container.blob.DocumentIds;
import ai.protomolt.proto.repo.container.lifecycle.DocumentEventFactory;
import ai.protomolt.proto.repo.container.lifecycle.JdbcEventOutbox;
import ai.protomolt.proto.repo.v1.*;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
class DocumentAttemptWriterIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    @Container static final LocalStackContainer S3 = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8")).withServices("s3");
    static LedgerDatabase database;
    static Tx tx;
    static OpenedBlobStore opened;
    static BackendIdentity identity;
    static final String GENERATION = "writer-original";
    static final String NAMESPACE = "writer-original";

    @BeforeAll static void open() {
        database = new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
        tx = new Tx(database.entityManagerFactory());
        opened = BlobStores.discover().open("s3", Map.of("endpoint", S3.getEndpoint().toString(), "region", S3.getRegion(),
                "access-key", S3.getAccessKey(), "secret-key", S3.getSecretKey(), "path-style", "true", "conditional-writes", "false"));
        opened.ensureNamespace(NAMESPACE);
        identity = S3BackendIdentity.of(S3.getEndpoint().toString(), S3.getRegion(), true);
        new ManagedBackendLedger(tx).bind(GENERATION, new ManagedBackendLedger.Profile(identity, "writer-realm"));
    }
    @AfterAll static void close() throws Exception {
        if (opened != null) opened.close();
        if (database != null) database.close();
    }

    private record Input(NodeAddress address, DriveRecord drive, DocumentPartAttemptLedger.Plan plan, List<PartObject> parts) {}
    private static Input input() {
        var address = NodeAddress.newBuilder().setAccountId("account").setGraphId("intake:account")
                .setGraphAddressId("source").setDocId(UUID.randomUUID().toString()).build();
        UUID node = DocumentIds.nodeId(address), attempt = UUID.randomUUID();
        var drive = new DriveRecord(); drive.driveId = UUID.randomUUID(); drive.name = "drive-" + drive.driveId;
        drive.accountId = "account"; drive.provider = "s3"; drive.driveType = "CUSTOM";
        drive.bucket = NAMESPACE; drive.prefix = "selected/";
        new DriveLedger(tx).insert(drive);
        var parts = DocumentPartCodec.split(Document.newBuilder().setDocId(address.getDocId()).build(), PartLayouts.document());
        String prefix = "selected/documents/account/" + node + "/attempts/" + attempt + "/";
        var objects = parts.stream().map(p -> new DocumentPartAttemptLedger.PlannedObject(p.part(), p.subKey(),
                DocumentPartCodec.objectKey(prefix, p.part(), p.subKey()), p.bytes().length, p.sha256(), "application/protobuf")).toList();
        return new Input(address, drive, new DocumentPartAttemptLedger.Plan(attempt,
                new DocumentPartAttemptLedger.Location(node, "account", GENERATION, NAMESPACE), 0, Map.of(), objects), parts);
    }
    private static DocumentRecord candidate(Input f, List<DocumentPublicationLedger.Part> parts) {
        var row = new DocumentRecord(); row.nodeId = f.plan.location().nodeId(); row.accountId = f.address.getAccountId();
        row.docId = f.address.getDocId(); row.graphId = f.address.getGraphId(); row.graphAddressId = f.address.getGraphAddressId();
        row.driveName = f.drive.name; row.rowKind = DocumentRowKind.INTAKE; row.datasourceId = "source";
        row.createdAt = Instant.now(); row.updatedAt = row.createdAt;
        var core = parts.stream().filter(p -> p.part() == DocumentPart.DOCUMENT_PART_CORE).findFirst().orElseThrow();
        row.objectKey = core.key().substring(0, core.key().lastIndexOf('/') + 1);
        row.versionId = core.providerVersion(); row.etag = core.etag(); row.sizeBytes = parts.stream().mapToLong(DocumentPublicationLedger.Part::size).sum();
        var manifest = DocumentManifest.newBuilder().setDocVersion(1).setAddress(f.address);
        for (var p : parts) manifest.addParts(PartManifestEntry.newBuilder().setPart(p.part()).setSubKey(p.subKey())
                .setObjectKey(p.key()).setSizeBytes(p.size()).setSha256(p.sha256()).setState(PartState.PART_STATE_PRESENT));
        row.writeManifest(manifest.build()); row.checksum = DocumentPartCodec.rootChecksumFromManifest(manifest.build());
        return row;
    }
    private static DocumentAttemptWriter writer() { return new DocumentAttemptWriter(tx, new DriveLedger(tx), GENERATION, identity, opened); }
    private static long events(Input f) {
        return tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM document_events_outbox WHERE kafka_key=:id")
                .setParameter("id", f.address.getDocId()).getSingleResult()).longValue());
    }
    private static void assertUnpublished(Input f) {
        assertThat(new DocumentLedger(tx).findByNodeId(f.plan.location().nodeId())).isEmpty();
        assertThat(new DocumentLedger(tx).hasPartPublication(f.plan.location().nodeId())).isFalse();
    }

    @Test void realBytesRowPublicationAndOutboxCommitTogether() throws Exception {
        var f = input(); var writer = writer();
        try {
            var saved = writer.write(f.plan, f.address, f.drive, f.parts, Duration.ofMinutes(1), Map.of(),
                    parts -> candidate(f, parts), () -> {},
                    (em, row) -> new JdbcEventOutbox(tx).enqueue(em, DocumentEventFactory.saved(row, Instant.now())));
            var publication = new DocumentPublicationLedger(tx).findForRead(saved).orElseThrow();
            assertThat(publication.attemptId()).isEqualTo(f.plan.attemptId());
            assertThat(events(f)).isEqualTo(1);
            for (int i = 0; i < publication.parts().size(); i++) {
                var p = publication.parts().get(i);
                assertThat(opened.store().get(NAMESPACE, p.key(), p.providerVersion()).data()).isEqualTo(f.parts.get(i).bytes());
            }
        } finally { writer.close(); assertThat(writer.awaitIdle(Duration.ofSeconds(5))).isTrue(); }
    }

    @ParameterizedTest @ValueSource(strings = {"prefix", "namespace", "account", "node", "generation", "changed-drive"})
    void invalidDestinationCannotAdmitAnAttempt(String defect) throws Exception {
        var f = input(); var location = f.plan.location(); var address = f.address; var objects = f.plan.objects();
        if (defect.equals("prefix")) objects = objects.stream().map(p -> new DocumentPartAttemptLedger.PlannedObject(
                p.part(), p.subKey(), "elsewhere/" + p.objectKey(), p.size(), p.sha256(), p.contentType())).toList();
        if (defect.equals("namespace")) location = new DocumentPartAttemptLedger.Location(location.nodeId(), location.accountId(), GENERATION, "elsewhere");
        if (defect.equals("generation")) location = new DocumentPartAttemptLedger.Location(location.nodeId(), location.accountId(), "elsewhere", NAMESPACE);
        if (defect.equals("account")) address = address.toBuilder().setAccountId("another").build();
        if (defect.equals("node")) address = address.toBuilder().setDocId("another").build();
        if (defect.equals("changed-drive")) tx.inTransaction(em -> {
            em.createNativeQuery("UPDATE drives SET prefix='changed' WHERE drive_id=:id").setParameter("id", f.drive.driveId).executeUpdate();
        });
        var plan = new DocumentPartAttemptLedger.Plan(f.plan.attemptId(), location, 0, Map.of(), objects);
        var selectedAddress = address;
        var writer = writer();
        try {
            assertThatThrownBy(() -> writer.write(plan, selectedAddress, f.drive, f.parts, Duration.ofMinutes(1), Map.of(),
                    parts -> { throw new AssertionError("Must reject before staging"); }, () -> {}, (em, row) -> {}))
                    .isInstanceOfAny(IllegalArgumentException.class, DocumentPartAttemptLedger.FenceException.class);
            assertThat(new DocumentPartAttemptLedger(tx).find(plan.attemptId())).isEmpty();
            assertUnpublished(f);
        } finally { writer.close(); assertThat(writer.awaitIdle(Duration.ofSeconds(5))).isTrue(); }
    }

    @Test void callbackFailureRollsBackPublicationAndRetainsAttempt() throws Exception {
        var f = input(); var writer = writer(); var signal = new IllegalStateException("Injected callback failure");
        try {
            assertThatThrownBy(() -> writer.write(f.plan, f.address, f.drive, f.parts, Duration.ofMinutes(1), Map.of(),
                    parts -> candidate(f, parts), () -> {}, (em, row) -> {
                        new JdbcEventOutbox(tx).enqueue(em, DocumentEventFactory.saved(row, Instant.now()));
                        em.flush(); throw signal;
                    })).isInstanceOfSatisfying(DocumentAttemptWriter.WriteFailure.class, failure -> {
                        assertThat(failure.attemptId()).isEqualTo(f.plan.attemptId());
                        assertThat(failure.phase()).isEqualTo("publication"); assertThat(failure.getCause()).isSameAs(signal);
                    });
            assertUnpublished(f); assertThat(events(f)).isZero();
            assertThat(new DocumentPartAttemptLedger(tx).find(f.plan.attemptId()).orElseThrow().state()).isEqualTo("VERIFIED");
        } finally { writer.close(); assertThat(writer.awaitIdle(Duration.ofSeconds(5))).isTrue(); }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void shutdownWaitsThroughCandidateAndPublication(boolean inPublication) throws Exception {
        var f = input(); var writer = writer(); var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        Runnable pause = () -> {
            entered.countDown();
            try { if (!release.await(10, TimeUnit.SECONDS)) throw new AssertionError("Release timed out"); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new CancellationException(); }
        };
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var result = executor.submit(() -> writer.write(f.plan, f.address, f.drive, f.parts, Duration.ofMinutes(1), Map.of(),
                    parts -> { if (!inPublication) pause.run(); return candidate(f, parts); }, () -> {},
                    (em, row) -> { if (inPublication) pause.run(); }));
            try {
                assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
                writer.close(); assertThat(writer.awaitIdle(Duration.ofMillis(30))).isFalse();
                release.countDown();
                assertThatThrownBy(() -> result.get(10, TimeUnit.SECONDS)).hasRootCauseInstanceOf(CancellationException.class);
                assertThat(writer.awaitIdle(Duration.ofSeconds(5))).isTrue();
                assertUnpublished(f);
                // The writer borrows the provider; shutdown must leave it usable for recovery.
                var p = f.plan.objects().getFirst();
                assertThat(opened.store().get(NAMESPACE, p.objectKey(), null).data()).isEqualTo(f.parts.getFirst().bytes());
            } finally { release.countDown(); writer.close(); }
        }
    }
}
