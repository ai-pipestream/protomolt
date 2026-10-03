package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.BackendIdentity;
import ai.protomolt.proto.repo.v1.*;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
class DocumentKeyReservationIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static LedgerDatabase database;
    private static Tx tx;
    @BeforeAll static void open() {
        database = new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(),POSTGRES.getUsername(),POSTGRES.getPassword()));
        tx = new Tx(database.entityManagerFactory());
    }
    @AfterAll static void close() { if (database != null) database.close(); }

    @Test void existingLegacyManifestPreventsAdmissionOfItsKey() {
        var plan = plan();
        var row = legacyRow(plan);
        new DocumentLedger(tx).save(row);
        assertThatThrownBy(() -> new DocumentPartAttemptLedger(tx).begin(plan,Duration.ofMinutes(5)))
                .hasStackTraceContaining("reserved for legacy");
        assertThat(new DocumentPartAttemptLedger(tx).find(plan.attemptId())).isEmpty();
        assertThat(new DocumentLedger(tx).findByNodeId(row.nodeId)).isPresent();
    }

    @Test void preIoReservationSurvivesAnUnpublishedOrDeletedLegacyWrite() {
        var plan = plan();
        var documents = new DocumentLedger(tx);
        documents.reserveLegacyPartKeys(keys(plan));
        assertThatThrownBy(() -> new DocumentPartAttemptLedger(tx).begin(plan,Duration.ofMinutes(5)))
                .hasStackTraceContaining("reserved for legacy");
        var row = documents.save(legacyRow(plan));
        documents.deleteByNodeId(row.nodeId);
        assertThatThrownBy(() -> new DocumentPartAttemptLedger(tx).begin(plan,Duration.ofMinutes(5)))
                .hasStackTraceContaining("reserved for legacy");
    }

    @Test void admittedKeysCannotBeReservedForLegacyIo() {
        var plan = plan();
        var attempt = new DocumentPartAttemptLedger(tx).begin(plan,Duration.ofMinutes(5));
        assertThatThrownBy(() -> new DocumentLedger(tx).reserveLegacyPartKeys(keys(plan)))
                .hasStackTraceContaining("reserved for a managed attempt");
        assertThat(new DocumentPartAttemptLedger(tx).find(attempt.id())).contains(attempt);
    }

    @Test void competingReservationAndAdmissionChooseExactlyOneOwner() throws Exception {
        var plan = plan();
        var start = new java.util.concurrent.CountDownLatch(1);
        try (var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var legacy = executor.submit(() -> {
                assertThat(start.await(10,java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                new DocumentLedger(tx).reserveLegacyPartKeys(keys(plan));
                return "legacy";
            });
            var managed = executor.submit(() -> {
                assertThat(start.await(10,java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                new DocumentPartAttemptLedger(tx).begin(plan,Duration.ofMinutes(5));
                return "managed";
            });
            start.countDown();
            int success = 0, rejected = 0;
            for (var result : List.of(legacy,managed)) {
                try { result.get(10,java.util.concurrent.TimeUnit.SECONDS); success++; }
                catch (java.util.concurrent.ExecutionException failure) {
                    assertThat(failure.getCause()).hasStackTraceContaining("Document key is reserved for");
                    rejected++;
                }
            }
            assertThat(success).isEqualTo(1);
            assertThat(rejected).isEqualTo(1);
        }
    }

    @Test void reversedOverlappingLegacyBatchesCoalesceWithoutDeadlock() throws Exception {
        var first = plan().objects().getFirst().objectKey();
        var second = plan().objects().getFirst().objectKey();
        var start = new java.util.concurrent.CountDownLatch(1);
        try (var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var futures = new java.util.ArrayList<java.util.concurrent.Future<?>>();
            for (var keys : List.of(List.of(first,second),List.of(second,first))) futures.add(executor.submit(() -> {
                assertThat(start.await(10,java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                new DocumentLedger(tx).reserveLegacyPartKeys(keys);
                return null;
            }));
            start.countDown();
            for (var future : futures) future.get(10,java.util.concurrent.TimeUnit.SECONDS);
        }
        long count = tx.readOnly(em -> ((Number)em.createNativeQuery(
                "SELECT count(*) FROM document_part_key_reservations WHERE object_key IN (:keys)")
                .setParameter("keys",List.of(first,second)).getSingleResult()).longValue());
        assertThat(count).isEqualTo(2);
    }

    @Test void purgeSnapshotReservesDocumentKeysButNotRawBlobKeys() {
        var plan = plan();
        var command = new DocumentPurgeRecord();
        command.purgeId=UUID.randomUUID(); command.nodeId=plan.location().nodeId(); command.docId="purge-"+command.nodeId;
        command.accountId="account"; command.graphId="intake:account"; command.graphAddressId="source"; command.driveName="legacy";
        command.requestedAt=java.time.Instant.now();
        String raw="blobs/"+UUID.randomUUID();
        command.writeObjectKeys(List.of(plan.objects().getFirst().objectKey(),raw));
        tx.inTransaction(em -> { em.persist(command); });
        assertThatThrownBy(() -> new DocumentPartAttemptLedger(tx).begin(plan,Duration.ofMinutes(5)))
                .hasStackTraceContaining("reserved for legacy");
        long rawCount=tx.readOnly(em -> ((Number)em.createNativeQuery("SELECT count(*) FROM document_part_key_reservations WHERE object_key=:key")
                .setParameter("key",raw).getSingleResult()).longValue());
        assertThat(rawCount).isZero();
    }

    private static List<String> keys(DocumentPartAttemptLedger.Plan plan) {
        return plan.objects().stream().map(DocumentPartAttemptLedger.PlannedObject::objectKey).toList();
    }

    static DocumentPartAttemptLedger.Plan plan() {
        UUID node = UUID.randomUUID(), attempt = UUID.randomUUID();
        String generation = "reservation-" + UUID.randomUUID();
        new ManagedBackendLedger(tx).bind(generation,new ManagedBackendLedger.Profile(
                new BackendIdentity("test-location","test-location/v1",Map.of("endpoint","https://storage.example")),generation));
        var object = new DocumentPartAttemptLedger.PlannedObject(DocumentPart.DOCUMENT_PART_CORE,"",
                "documents/account/"+node+"/attempts/"+attempt+"/core",3,"ab".repeat(32),"application/protobuf");
        return new DocumentPartAttemptLedger.Plan(attempt,new DocumentPartAttemptLedger.Location(node,"account",generation,"container"),
                0,Map.of(),List.of(object));
    }
    private static DocumentRecord legacyRow(DocumentPartAttemptLedger.Plan plan) {
        var row = new DocumentRecord(); row.nodeId=plan.location().nodeId(); row.accountId="account";
        row.docId="doc-"+row.nodeId; row.graphId="intake:account"; row.graphAddressId="source";
        row.rowKind=DocumentRowKind.INTAKE; row.datasourceId="source"; row.driveName="legacy";
        row.objectKey=plan.objects().getFirst().objectKey(); row.checksum="legacy"; row.sizeBytes=3L; row.etag="legacy";
        row.writeManifest(DocumentManifest.newBuilder().setDocVersion(1).addParts(PartManifestEntry.newBuilder()
                .setPart(DocumentPart.DOCUMENT_PART_CORE).setState(PartState.PART_STATE_PRESENT)
                .setObjectKey(row.objectKey).setSizeBytes(3).setSha256("ab".repeat(32))).build());
        return row;
    }
}
