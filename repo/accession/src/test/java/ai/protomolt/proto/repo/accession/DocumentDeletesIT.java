package ai.protomolt.proto.repo.accession;

import static ai.protomolt.proto.repo.accession.AccessionFixture.*;
import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.repo.catalog.Catalog;
import ai.protomolt.proto.repo.v2.*;
import io.grpc.Status;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.util.UUID;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;

class DocumentDeletesIT {
    @Test void tombstonePreservesHistoryReceiptsAndBytesAndReplays() throws Exception {
        try (var f = new AccessionFixture()) {
            var address = address("a", "doc");
            var saved = f.saves.save(processAuthority(), create("save", address, document("a", "doc", "title")).build());
            var original = f.catalog.manifest(address, 1).orElseThrow();
            var deletes = new DocumentDeletes(f.catalog, Clock.systemUTC());
            var request = DeleteDocumentRequest.newBuilder().setAddress(address).setOperationId("delete").setExpected(at(1)).setReason("removed").build();
            f.stack.reset();
            var response = deletes.delete(processAuthority(), request);
            assertThat(response.getAccession().getRevision()).isEqualTo(2);
            assertThat(response.getAccession().getValidation().getStatus()).isEqualTo(ValidationStatus.VALIDATION_STATUS_NOT_APPLICABLE);
            assertThat(response.getAccession().getValidation().hasProcessAuthority()).isTrue();
            assertThat(response.getAccession().getManifest().getPartsCount()).isZero();
            assertThat(DocumentSaves.VALIDATOR.validate(response).valid()).isTrue();
            assertThat(f.stack.puts.get()).isZero();
            assertThat(f.stack.opens.get()).isZero();
            assertCode(Status.Code.NOT_FOUND, () -> f.reads.getDocument(processAuthority(), GetDocumentRequest.newBuilder().setAddress(address).build()));
            assertThat(f.reads.getDocument(processAuthority(), GetDocumentRequest.newBuilder().setAddress(address)
                    .setRevision(RevisionSelector.newBuilder().setRevision(1)).build()).getDocument())
                    .isEqualTo(claimChecked(document("a", "doc", "title"), original));
            assertThat(f.blobBytes(original, "b1")).isEqualTo(document("a", "doc", "title").getBlobBag().getBlob().getData().toByteArray());
            assertThat(f.reads.listRevisions(processAuthority(), ListRevisionsRequest.newBuilder().setAddress(address).setPageSize(10).build()).getRevisionsCount()).isEqualTo(2);
            assertThat(f.reads.outcome(processAuthority(), GetOperationOutcomeRequest.newBuilder().setAccountId("a").setOperationId("delete").build()).getCommitted().getAccession()).isEqualTo(response.getAccession());
            assertThat(deletes.delete(processAuthority(), request).getReplayed()).isTrue();
            assertCode(Status.Code.ALREADY_EXISTS, () -> deletes.delete(processAuthority(), request.toBuilder().setReason("different").build()));
            assertCode(Status.Code.ALREADY_EXISTS, () -> deletes.delete(processAuthority(), request.toBuilder().setExpected(at(2)).build()));
            assertCode(Status.Code.ALREADY_EXISTS, () -> deletes.delete(processAuthority(), request.toBuilder().setOperationId("save").build()));
            assertThatThrownBy(() -> deletes.delete(processAuthority(), request.toBuilder()
                    .setOperationId("second-delete").setExpected(at(2)).build()))
                    .isInstanceOfSatisfying(AccessionException.class, failure -> {
                        assertThat(failure.code()).isEqualTo(Status.Code.ABORTED);
                        var conflict = (RevisionConflict) failure.detail().orElseThrow();
                        assertThat(conflict.getCurrentRevision()).isEqualTo(2);
                        assertThat(conflict.getCurrentIsTombstone()).isTrue();
                    });
            assertThat(f.catalog.outcome("a", "second-delete")).isEmpty();
            assertThat(f.catalog.events(address)).hasSize(2);
            assertThat(f.catalog.events(address).getLast().getTombstoned().getReason()).isEqualTo("removed");
            assertThat(DocumentSaves.VALIDATOR.validate(response.toBuilder().setAccession(response.getAccession().toBuilder()
                    .setValidation(response.getAccession().getValidation().toBuilder().setStatus(ValidationStatus.VALIDATION_STATUS_TYPED_VALID))).build()).valid()).isFalse();
            assertThat(DocumentSaves.VALIDATOR.validate(saved.getAccession().toBuilder().setValidation(response.getAccession().getValidation()).build()).valid()).isFalse();
        }
    }

    @Test void deleteNeedsIntakeGrantAndRecordsIt() throws Exception {
        try (var f = new AccessionFixture()) {
            var address = address("a", "doc");
            f.saves.save(processAuthority(), create("save", address, document("a", "doc", "title", "writer")).build());
            var credential = UUID.randomUUID();
            f.catalog.registerCredential(ISSUER, credential, "writer");
            var caller = writer("a", "writer", credential, 1);
            var request = DeleteDocumentRequest.newBuilder().setAddress(address).setOperationId("delete").setExpected(at(1)).build();
            var deletes = new DocumentDeletes(f.catalog, Clock.systemUTC());
            assertCode(Status.Code.PERMISSION_DENIED, () -> deletes.delete(caller, request));
            f.stepGrant("a", credential, "worker", false);
            assertCode(Status.Code.PERMISSION_DENIED, () -> deletes.delete(caller, request));
            f.intakeGrant("a", credential, "other-datasource", false);
            assertCode(Status.Code.PERMISSION_DENIED, () -> deletes.delete(caller, request));
            assertThat(f.catalog.outcome("a", "delete")).isEmpty();
            var grant = f.intakeGrant("a", credential, "ds-1", false);
            var response = deletes.delete(caller, request);
            assertThat(response.getAccession().getValidation().getWriterGrant().getGrantSnapshot()).isEqualTo(grant.toByteString());
            f.catalog.revokeGrant(grant.getGrantId());
            assertThat(deletes.delete(caller, request).getReplayed()).isTrue();
        }
    }

    @Test void revocationBetweenAuthorizationAndCommitStoresNothing() throws Exception {
        try (var f = new AccessionFixture()) {
            var address = address("a", "doc");
            f.saves.save(processAuthority(), create("save", address, document("a", "doc", "title", "writer")).build());
            var credential = UUID.randomUUID();
            f.catalog.registerCredential(ISSUER, credential, "writer");
            var grant = f.intakeGrant("a", credential, "ds-1", false);
            // Instrument only the handoff to the real PostgreSQL commit; no catalog behavior is mocked.
            var catalog = (Catalog) Proxy.newProxyInstance(Catalog.class.getClassLoader(), new Class<?>[]{Catalog.class}, (proxy, method, args) -> {
                if (method.getName().equals("commit")) f.catalog.revokeGrant(grant.getGrantId());
                try { return method.invoke(f.catalog, args); }
                catch (InvocationTargetException failure) { throw failure.getCause(); }
            });
            var deletes = new DocumentDeletes(catalog, Clock.systemUTC());
            assertCode(Status.Code.PERMISSION_DENIED, () -> deletes.delete(writer("a", "writer", credential, 1),
                    DeleteDocumentRequest.newBuilder().setAddress(address).setOperationId("delete").setExpected(at(1)).build()));
            assertThat(f.catalog.outcome("a", "delete")).isEmpty();
            assertThat(f.catalog.current(address).orElseThrow().revision()).isEqualTo(1);
            assertThat(f.catalog.events(address)).hasSize(1);
        }
    }

    @Test void concurrentSameIntentHasOneTombstoneAndOneReplay() throws Exception {
        try (var f = new AccessionFixture(); var threads = Executors.newFixedThreadPool(2)) {
            var address = address("a", "doc");
            f.saves.save(processAuthority(), create("save", address, document("a", "doc", "title")).build());
            var barrier = new CyclicBarrier(2);
            var catalog = (Catalog) Proxy.newProxyInstance(Catalog.class.getClassLoader(), new Class<?>[]{Catalog.class}, (proxy, method, args) -> {
                if (method.getName().equals("commit")) barrier.await(20, TimeUnit.SECONDS);
                try { return method.invoke(f.catalog, args); }
                catch (InvocationTargetException failure) { throw failure.getCause(); }
            });
            var deletes = new DocumentDeletes(catalog, Clock.systemUTC());
            var request = DeleteDocumentRequest.newBuilder().setAddress(address).setOperationId("delete").setExpected(at(1)).build();
            var first = threads.submit(() -> deletes.delete(processAuthority(), request));
            var second = threads.submit(() -> deletes.delete(processAuthority(), request));
            var a = first.get(30, TimeUnit.SECONDS); var b = second.get(30, TimeUnit.SECONDS);
            assertThat(a.getAccession()).isEqualTo(b.getAccession());
            assertThat(a.getReplayed()).isNotEqualTo(b.getReplayed());
            assertThat(f.catalog.events(address)).hasSize(2);
        }
    }

    static void assertCode(Status.Code code, org.assertj.core.api.ThrowableAssert.ThrowingCallable action) {
        var error = catchThrowableOfType(action, AccessionException.class);
        assertThat(error).isNotNull();
        assertThat(error.code()).isEqualTo(code);
    }
}
