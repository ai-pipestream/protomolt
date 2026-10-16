package ai.protomolt.proto.repo.accession;

import static ai.protomolt.proto.repo.accession.AccessionFixture.*;
import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.repo.catalog.Catalog;
import ai.protomolt.proto.repo.spi.PurgeAuthorization;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v2.*;
import io.grpc.Status;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.DriverManager;
import java.time.Clock;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class DocumentPurgesIT {
    private static void assertCode(Status.Code code, org.assertj.core.api.ThrowableAssert.ThrowingCallable action) {
        assertThatThrownBy(action).isInstanceOfSatisfying(AccessionException.class, failure -> assertThat(failure.code()).isEqualTo(code));
    }
    private static DocumentPurges purges(AccessionFixture f) {
        return new DocumentPurges(f.catalog, f.drives, (caller, account) -> new PurgeAuthorization.Decision(true, true), Clock.systemUTC());
    }
    private static PurgeDocumentRequest request(String id, NodeAddress address) {
        return PurgeDocumentRequest.newBuilder().setPurgeId(id).setAddress(address).build();
    }
    private static ai.protomolt.proto.repo.spi.RepositoryCaller administrator() {
        return writer("a", "administrator", UUID.randomUUID(), 1);
    }
    static SaveDocumentRequest source(NodeAddress from, NodeAddress to) {
        return SaveDocumentRequest.newBuilder().setOperationId("source")
                .setTarget(DocumentTarget.newBuilder().setAddress(to)).setExpected(ExpectedRevision.newBuilder().setNone(true))
                .setOwner(intakeOwner()).setDocument(Document.newBuilder().setDocId(to.getDocId()).setOwnership(ownership("a")))
                .addWrites(put(DocumentPart.DOCUMENT_PART_CORE))
                .addSources(PartSource.newBuilder().setFromAddress(from).setFromRevision(1)
                        .setSelector(PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_PARSED)))
                .addSources(PartSource.newBuilder().setFromAddress(from).setFromRevision(1)
                        .setSelector(PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_BLOBS))).build();
    }

    @Test void sharedPartsAndRawBlobsSurviveUntilLastDocumentPurged() throws Exception {
        try (var f = new AccessionFixture()) {
            var a = address("a", "doc");
            var b = a.toBuilder().setGraphAddressId("other").build();
            f.saves.save(processAuthority(), create("save", a, document("a", "doc", "title")).build());
            var before = f.catalog.manifest(a, 1).orElseThrow();
            f.saves.save(processAuthority(), source(a, b));
            var derived = f.reads.getDocument(processAuthority(), GetDocumentRequest.newBuilder().setAddress(b).build()).getDocument();
            var result = purges(f).purge(administrator(), request("purge-a", a));
            assertThat(result.getObjectsRetainedShared()).isEqualTo(3); // PARSED, BLOBS fragment and raw blob
            assertThat(result.getObjectsToDelete()).isEqualTo(1); // original CORE
            assertThat(f.reads.getDocument(processAuthority(), GetDocumentRequest.newBuilder().setAddress(b).build()).getDocument()).isEqualTo(derived);
            assertThat(f.blobBytes(before, "b1")).isEqualTo(document("a", "doc", "title").getBlobBag().getBlob().getData().toByteArray());
            assertCode(Status.Code.NOT_FOUND, () -> f.reads.getDocument(processAuthority(), GetDocumentRequest.newBuilder().setAddress(a)
                    .setRevision(RevisionSelector.newBuilder().setRevision(1)).build()));
            assertThat(f.catalog.manifest(a, 1)).isEmpty();
            assertCode(Status.Code.NOT_FOUND, () -> f.reads.listRevisions(processAuthority(),
                    ListRevisionsRequest.newBuilder().setAddress(a).setPageSize(10).build()));
            assertThat(new DocumentLists(f.catalog).list(processAuthority(), ListDocumentsRequest.newBuilder()
                    .setAccountId("a").setPageSize(10).setIncludeTombstoned(true).build()).getDocumentsList())
                    .extracting(DocumentSummary::getAddress).containsExactly(b);
            assertThat(f.catalog.events(a, java.util.List.of(1L))).hasSize(1).allSatisfy(e -> assertThat(e.getRevision()).isEqualTo(1));
            assertThat(purges(f).purge(administrator(), request("purge-a", a))).isEqualTo(result);
            assertCode(Status.Code.ALREADY_EXISTS, () -> purges(f).purge(administrator(), request("purge-a", b)));
            var last = purges(f).purge(administrator(), request("purge-b", b));
            assertThat(last.getObjectsRetainedShared()).isZero();
            assertThat(last.getObjectsToDelete()).isEqualTo(4);
            assertThatThrownBy(() -> f.blobBytes(before, "b1")).isInstanceOf(java.io.IOException.class);
            assertThat(f.catalog.events(a)).hasSize(3);
            var events = f.catalog.events(a).stream().filter(e -> e.getRevision() == 0).toList();
            assertThat(events).hasSize(2).allSatisfy(e -> assertThat(DocumentSaves.VALIDATOR.validate(e).valid()).isTrue());
            assertThat(events.get(0).getEventId()).isNotEqualTo(events.get(1).getEventId());
            purges(f).purge(administrator(), request("second-purge-a", a));
            assertThat(f.catalog.events(a).stream().filter(e -> e.getRevision() == 0)).hasSize(4);
        }
    }

    @Test void providerFailureAfterOneActualDeleteLeavesPlanRetryable() throws Exception {
        try (var f = new AccessionFixture()) {
            var a = address("a", "doc");
            f.saves.save(processAuthority(), create("save", a, document("a", "doc", "title")).build());
            var calls = new AtomicInteger();
            var outage = new java.io.IOException("Injected lost provider response after real delete");
            f.stack.afterDelete = () -> { calls.incrementAndGet(); throw outage; };
            assertThatThrownBy(() -> purges(f).purge(administrator(), request("purge", a)))
                    .isInstanceOfSatisfying(AccessionException.class, failure -> {
                        assertThat(failure.code()).isEqualTo(Status.Code.UNAVAILABLE);
                        assertThat(failure.getCause()).isSameAs(outage);
                    });
            assertThat(calls).hasValue(1);
            assertThat(f.catalog.events(a).stream().filter(DocumentEvent::hasPurgeCompleted)).isEmpty();
            try (var restarted = f.restarted()) {
                assertThat(purges(restarted).purge(administrator(), request("purge", a)).getObjectsToDelete()).isEqualTo(4);
                assertThat(restarted.catalog.events(a).stream().filter(DocumentEvent::hasPurgeCompleted)).hasSize(1);
            }
        }
    }

    @Test void completionFailureResumesExactPlanAfterRestart() throws Exception {
        try (var f = new AccessionFixture()) {
            var a = address("a", "doc");
            f.saves.save(processAuthority(), create("save", a, document("a", "doc", "title")).build());
            var interrupted = (Catalog) Proxy.newProxyInstance(Catalog.class.getClassLoader(), new Class<?>[]{Catalog.class}, (proxy, method, args) -> {
                if (method.getName().equals("completePurge")) throw new RepositoryException(RepositoryException.Code.UNAVAILABLE, "Injected completion outage");
                try { return method.invoke(f.catalog, args); } catch (InvocationTargetException failure) { throw failure.getCause(); }
            });
            var service = new DocumentPurges(interrupted, f.drives, (c, account) -> new PurgeAuthorization.Decision(true, true), Clock.systemUTC());
            assertCode(Status.Code.UNAVAILABLE, () -> service.purge(administrator(), request("purge", a)));
            assertThat(f.catalog.events(a).stream().filter(DocumentEvent::hasPurgeMarked)).hasSize(1);
            assertThat(f.catalog.events(a).stream().filter(DocumentEvent::hasPurgeCompleted)).isEmpty();
            try (var restarted = f.restarted()) {
                var result = purges(restarted).purge(administrator(), request("purge", a));
                assertThat(result.getObjectsToDelete()).isEqualTo(4);
                assertThat(purges(restarted).purge(administrator(), request("purge", a))).isEqualTo(result);
                assertThat(restarted.catalog.events(a).stream().filter(DocumentEvent::hasPurgeCompleted)).hasSize(1);
            }
        }
    }

    @Test void authorizationRefusesBeforeMarkAndNeverUsesWriterGrants() throws Exception {
        try (var f = new AccessionFixture()) {
            var a = address("a", "doc");
            f.saves.save(processAuthority(), create("save", a, document("a", "doc", "title")).build());
            var callbacks = new AtomicInteger();
            var denied = new DocumentPurges(f.catalog, f.drives, (c, account) -> {
                callbacks.incrementAndGet(); return new PurgeAuthorization.Decision(true, true);
            }, Clock.systemUTC());
            assertCode(Status.Code.PERMISSION_DENIED, () -> denied.purge(processAuthority(), request("purge", a)));
            assertCode(Status.Code.PERMISSION_DENIED, () -> denied.purge(writer("other", "administrator", UUID.randomUUID(), 1), request("purge", a)));
            assertThat(callbacks).hasValue(0);
            for (var decision : new PurgeAuthorization.Decision[]{new PurgeAuthorization.Decision(false, true), new PurgeAuthorization.Decision(true, false)}) {
                var service = new DocumentPurges(f.catalog, f.drives, (c, account) -> decision, Clock.systemUTC());
                assertCode(Status.Code.PERMISSION_DENIED, () -> service.purge(administrator(), request("purge", a)));
            }
            var unavailable = new DocumentPurges(f.catalog, f.drives, (c, account) -> {
                throw new RepositoryException(RepositoryException.Code.UNAVAILABLE, "Authority unavailable");
            }, Clock.systemUTC());
            assertCode(Status.Code.UNAVAILABLE, () -> unavailable.purge(administrator(), request("purge", a)));
            assertThat(f.catalog.events(a)).hasSize(1);
            assertThat(f.catalog.outcome("a", "purge")).isEmpty();
            try (var connection = DriverManager.getConnection(f.url, POSTGRES.getUsername(), POSTGRES.getPassword());
                 var statement = connection.createStatement(); var rows = statement.executeQuery("SELECT count(*) FROM catalog_purges")) {
                rows.next(); assertThat(rows.getLong(1)).isZero();
            }
            // No credential or grant provisioning was done: purge uses only the trusted host SPI.
            assertThat(denied.purge(administrator(), request("purge", a)).getObjectsToDelete()).isEqualTo(4);
        }
    }
    @Test @org.junit.jupiter.api.Timeout(60)
    void saveWinningObjectLockMakesPurgeRetainPartsAndRawBlob() throws Exception { race(true); }

    @Test @org.junit.jupiter.api.Timeout(60)
    void purgeWinningObjectLockRefusesSaveAndRecordsSourcePurged() throws Exception { race(false); }

    private void race(boolean saveFirst) throws Exception {
        try (var f = new AccessionFixture(); var threads = java.util.concurrent.Executors.newFixedThreadPool(2);
             var barrier = DriverManager.getConnection(f.url, POSTGRES.getUsername(), POSTGRES.getPassword())) {
            var a = address("a", "doc");
            var b = a.toBuilder().setGraphAddressId("derived").build();
            f.saves.save(processAuthority(), create("save", a, document("a", "doc", "title")).build());
            // The barrier runs INSIDE the winning transaction after it has locked the actual objects.
            // All catalog and provider behavior remains real; the SQL hook only controls ordering.
            try (var sql = barrier.createStatement()) {
                sql.execute("SELECT pg_advisory_lock(318)");
                sql.execute("CREATE FUNCTION qualification_barrier() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                        + (saveFirst ? "IF NEW.operation_id = 'source' THEN PERFORM pg_advisory_xact_lock(318); END IF; "
                                     : "PERFORM pg_advisory_xact_lock(318); ")
                        + "RETURN NEW; END $$");
                sql.execute("CREATE TRIGGER qualification_barrier BEFORE INSERT ON "
                        + (saveFirst ? "catalog_revisions" : "catalog_purges")
                        + " FOR EACH ROW EXECUTE FUNCTION qualification_barrier()");
            }
            java.util.concurrent.Future<SaveDocumentResponse> save;
            java.util.concurrent.Future<PurgeDocumentResponse> purge;
            if (saveFirst) {
                save = threads.submit(() -> f.saves.save(processAuthority(), source(a, b)));
                awaitSqlWait(barrier, "INSERT INTO catalog_revisions");
                purge = threads.submit(() -> purges(f).purge(administrator(), request("purge", a)));
                awaitSqlWait(barrier, "SELECT o.stack_id");
            } else {
                purge = threads.submit(() -> purges(f).purge(administrator(), request("purge", a)));
                awaitSqlWait(barrier, "INSERT INTO catalog_purges");
                save = threads.submit(() -> f.saves.save(processAuthority(), source(a, b)));
                awaitSqlWait(barrier, "SELECT purged, sha256");
            }
            try (var sql = barrier.createStatement()) { sql.execute("SELECT pg_advisory_unlock(318)"); }
            var result = purge.get(20, java.util.concurrent.TimeUnit.SECONDS);
            if (saveFirst) {
                assertThat(save.get(20, java.util.concurrent.TimeUnit.SECONDS).getAccession().getRevision()).isEqualTo(1);
                assertThat(result.getObjectsRetainedShared()).isEqualTo(3);
                assertThat(f.reads.getDocument(processAuthority(), GetDocumentRequest.newBuilder().setAddress(b).build()).hasDocument()).isTrue();
                assertThat(f.blobBytes(f.catalog.manifest(b, 1).orElseThrow(), "b1")).isNotEmpty();
            } else {
                assertThatThrownBy(() -> save.get(20, java.util.concurrent.TimeUnit.SECONDS))
                        .isInstanceOf(java.util.concurrent.ExecutionException.class)
                        .cause().isInstanceOfSatisfying(AccessionException.class, failure -> {
                            assertThat(failure.code()).isEqualTo(Status.Code.FAILED_PRECONDITION);
                            assertThat(((SaveRejection) failure.detail().orElseThrow()).getReason()).isEqualTo(RejectionReason.REJECTION_REASON_SOURCE_PURGED);
                        });
                assertThat(result.getObjectsRetainedShared()).isZero();
                assertThat(f.catalog.current(b)).isEmpty();
                assertThat(f.catalog.outcome("a", "source").orElseThrow().outcome().getRejection().getReason())
                        .isEqualTo(RejectionReason.REJECTION_REASON_SOURCE_PURGED);
            }
        }
    }

    private static void awaitSqlWait(java.sql.Connection connection, String queryPrefix) throws Exception {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(15);
        try (var query = connection.prepareStatement("SELECT EXISTS (SELECT 1 FROM pg_stat_activity "
                + "WHERE datname = current_database() AND pid <> pg_backend_pid() AND wait_event_type = 'Lock' AND ltrim(query) LIKE ? )")) {
            query.setString(1, queryPrefix + "%");
            do {
                try (var rows = query.executeQuery()) { rows.next(); if (rows.getBoolean(1)) return; }
                java.util.concurrent.locks.LockSupport.parkNanos(java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(10));
            } while (System.nanoTime() < deadline);
        }
        throw new AssertionError("Expected real PostgreSQL lock wait for " + queryPrefix);
    }

}
