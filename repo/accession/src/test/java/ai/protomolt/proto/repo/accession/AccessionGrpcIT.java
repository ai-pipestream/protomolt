package ai.protomolt.proto.repo.accession;

import static ai.protomolt.proto.repo.accession.AccessionFixture.*;
import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v2.*;
import ai.protomolt.proto.repo.v2.DocumentPart;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.protobuf.StatusProto;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** The v2 RPCs through an in-process gRPC server, with errors.proto details on refusals. */
class AccessionGrpcIT {
    @Test void purgeAuthorityOutageAndRecoveryCrossTheTransport() throws Exception {
        try (var f = new AccessionFixture()) {
            var address = address("a", "doc");
            f.saves.save(processAuthority(), create("save", address, document("a", "doc", "title")).build());
            var credential = UUID.randomUUID();
            var caller = ai.protomolt.proto.actions.Caller.scoped("administrator", java.util.Set.of());
            var authentication = new ai.protomolt.proto.authz.AuthenticatedCaller(caller, java.util.Optional.of(
                    new ai.protomolt.proto.authz.CredentialBinding(ISSUER, credential, 1)));
            var outage = new java.util.concurrent.atomic.AtomicBoolean(true);
            var purges = new DocumentPurges(f.catalog, f.drives, (bound, account) -> {
                if (outage.get()) throw new ai.protomolt.proto.repo.spi.RepositoryException(
                        ai.protomolt.proto.repo.spi.RepositoryException.Code.UNAVAILABLE, "Authority unavailable");
                return new ai.protomolt.proto.repo.spi.PurgeAuthorization.Decision(true, true);
            }, java.time.Clock.systemUTC());
            var service = new AccessionGrpcService(f.saves, f.reads, new DocumentDeletes(f.catalog, java.time.Clock.systemUTC()),
                    new DocumentLists(f.catalog), purges, trusted -> writer("a", "administrator", credential, 1));
            var name = "purge-" + UUID.randomUUID();
            var server = InProcessServerBuilder.forName(name).directExecutor().addService(io.grpc.ServerInterceptors.intercept(service,
                    new io.grpc.ServerInterceptor() {
                        @Override public <ReqT, RespT> io.grpc.ServerCall.Listener<ReqT> interceptCall(io.grpc.ServerCall<ReqT, RespT> call,
                                io.grpc.Metadata headers, io.grpc.ServerCallHandler<ReqT, RespT> next) {
                            return io.grpc.Contexts.interceptCall(io.grpc.Context.current()
                                    .withValue(ai.protomolt.proto.authz.grpc.CallerContexts.CALLER, caller)
                                    .withValue(ai.protomolt.proto.authz.grpc.CallerContexts.AUTHENTICATED_CALLER, authentication), call, headers, next);
                        }
                    })).build().start();
            var channel = InProcessChannelBuilder.forName(name).directExecutor().build();
            try {
                var stub = DocumentServiceGrpc.newBlockingStub(channel);
                var request = PurgeDocumentRequest.newBuilder().setPurgeId("purge").setAddress(address).build();
                assertThatThrownBy(() -> stub.purgeDocument(request)).isInstanceOfSatisfying(StatusRuntimeException.class,
                        failure -> assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE));
                assertThat(f.catalog.events(address)).hasSize(1);
                outage.set(false);
                var result = stub.purgeDocument(request);
                assertThat(result.getObjectsToDelete()).isEqualTo(4);
                assertThat(stub.purgeDocument(request)).isEqualTo(result);
                assertThat(DocumentSaves.VALIDATOR.validate(result).valid()).isTrue();
            } finally { channel.shutdownNow(); server.shutdownNow(); }
        }
    }

    @Test void deletesAndListingsCrossTheTransport() throws Exception {
        var name = "delete-list-" + UUID.randomUUID();
        try (var f = new AccessionFixture()) {
            var server = InProcessServerBuilder.forName(name).directExecutor()
                    .addService(new AccessionGrpcService(f.saves, f.reads,
                            new DocumentDeletes(f.catalog, java.time.Clock.systemUTC()), new DocumentLists(f.catalog), new DocumentPurges(f.catalog, f.drives, (caller, account) -> new ai.protomolt.proto.repo.spi.PurgeAuthorization.Decision(false, false), java.time.Clock.systemUTC()),
                            authentication -> new RepositoryCaller(authentication.caller().name(), authentication.caller().unrestricted())))
                    .build().start();
            var channel = InProcessChannelBuilder.forName(name).directExecutor().build();
            try {
                var stub = DocumentServiceGrpc.newBlockingStub(channel);
                var address = address("a", "doc");
                stub.saveDocument(create("save", address, document("a", "doc", "title")).build());
                var listing = ListDocumentsRequest.newBuilder().setAccountId("a").setPageSize(5).build();
                assertThat(stub.listDocuments(listing).getDocumentsCount()).isEqualTo(1);
                var request = DeleteDocumentRequest.newBuilder().setAddress(address).setExpected(at(1)).setOperationId("delete").build();
                assertThat(stub.deleteDocument(request).getAccession().getTombstone()).isTrue();
                assertThat(stub.deleteDocument(request).getReplayed()).isTrue();
                assertThat(stub.listDocuments(listing).getDocumentsCount()).isZero();
                assertThat(stub.listDocuments(listing.toBuilder().setIncludeTombstoned(true).build()).getDocuments(0).getTombstone()).isTrue();
                assertThatThrownBy(() -> stub.getDocument(GetDocumentRequest.newBuilder().setAddress(address).build()))
                        .isInstanceOfSatisfying(StatusRuntimeException.class, failure -> assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND));
                assertThat(stub.getDocument(GetDocumentRequest.newBuilder().setAddress(address)
                        .setRevision(RevisionSelector.newBuilder().setRevision(1)).build()).getDocument().getDocId()).isEqualTo("doc");
                assertThat(stub.getOperationOutcome(GetOperationOutcomeRequest.newBuilder().setAccountId("a").setOperationId("delete").build())
                        .getCommitted().getAccession().getTombstone()).isTrue();
                assertThat(stub.listRevisions(ListRevisionsRequest.newBuilder().setAddress(address).setPageSize(10).build()).getRevisionsCount()).isEqualTo(2);
                detail(() -> stub.deleteDocument(request.toBuilder().setReason("changed").build()), Status.Code.ALREADY_EXISTS, OperationReuse.class);
            } finally {
                channel.shutdownNow();
                server.shutdownNow();
            }
        }
    }

    @Test void savesReadsAndRefusalsCrossTheTransportWithTheirDetails() throws Exception {
        var name = "accession-" + UUID.randomUUID();
        try (var f = new AccessionFixture()) {
            var server = InProcessServerBuilder.forName(name).directExecutor()
                    .addService(new AccessionGrpcService(f.saves, f.reads, new DocumentDeletes(f.catalog, java.time.Clock.systemUTC()), new DocumentLists(f.catalog), new DocumentPurges(f.catalog, f.drives, (caller, account) -> new ai.protomolt.proto.repo.spi.PurgeAuthorization.Decision(false, false), java.time.Clock.systemUTC()), authentication -> new RepositoryCaller(
                            authentication.caller().name(), authentication.caller().unrestricted())))
                    .build().start();
            var channel = InProcessChannelBuilder.forName(name).directExecutor().build();
            try {
                var stub = DocumentServiceGrpc.newBlockingStub(channel);
                var account = "acct-" + UUID.randomUUID();
                var address = address(account, "doc-grpc");
                var document = document(account, "doc-grpc", "over the wire");
                var unknown = com.google.protobuf.UnknownFieldSet.newBuilder().addField(999,
                        com.google.protobuf.UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build();
                var command = create("op-unknown", address, document).build();
                for (var unsupported : java.util.List.of(
                        command.toBuilder().setOwner(command.getOwner().toBuilder().setIntake(
                                command.getOwner().getIntake().toBuilder().setUnknownFields(unknown))).build(),
                        command.toBuilder().setWrites(0, command.getWrites(0).toBuilder().setSelector(
                                command.getWrites(0).getSelector().toBuilder().setUnknownFields(unknown))).build())) {
                    assertThat(detail(() -> stub.saveDocument(unsupported), Status.Code.INVALID_ARGUMENT,
                            SaveRejection.class).getReason()).isEqualTo(RejectionReason.REJECTION_REASON_MALFORMED_REQUEST);
                    assertThat(f.catalog.outcome(account, "op-unknown")).isEmpty();
                    assertThat(f.catalog.current(address)).isEmpty();
                    assertThat(f.stack.puts.get()).isZero();
                }
                var saved = stub.saveDocument(create("op-1", address, document).build());
                assertThat(saved.getAccession().getRevision()).isEqualTo(1);
                var stored = f.catalog.manifest(address, 1).orElseThrow();
                assertThat(stub.getDocument(GetDocumentRequest.newBuilder().setAddress(address).build()).getDocument())
                        .isEqualTo(claimChecked(document, stored));
                assertThat(f.blobBytes(stored, "b1")).isEqualTo(document.getBlobBag().getBlob().getData().toByteArray());
                assertThat(stub.getManifest(GetManifestRequest.newBuilder().setAddress(address).build())
                        .getManifest().getPartsCount()).isEqualTo(3);
                assertThat(stub.listRevisions(ListRevisionsRequest.newBuilder().setAddress(address).setPageSize(5).build())
                        .getRevisionsCount()).isEqualTo(1);
                assertThat(stub.getOperationOutcome(GetOperationOutcomeRequest.newBuilder().setAccountId(account)
                        .setOperationId("op-1").build()).getCommitted().getAccession()).isEqualTo(saved.getAccession());

                var stale = create("op-2", address, document).build();
                assertThat(detail(() -> stub.saveDocument(stale), Status.Code.ABORTED, RevisionConflict.class)
                        .getCurrentRevision()).isEqualTo(1);
                var reuse = create("op-1", address, document(account, "doc-grpc", "changed")).build();
                assertThat(detail(() -> stub.saveDocument(reuse), Status.Code.ALREADY_EXISTS, OperationReuse.class)
                        .getRecordedIntentSha256()).isNotEqualTo(saved.getAccession().getValidation().getCandidateSha256());
                var malformed = create("op-3", address, document).clearOwner().build();
                assertThat(detail(() -> stub.saveDocument(malformed), Status.Code.INVALID_ARGUMENT, SaveRejection.class)
                        .getReason()).isEqualTo(RejectionReason.REJECTION_REASON_MALFORMED_REQUEST);
                var frames = new java.util.ArrayList<ReadDocumentResponse>();
                stub.readDocument(ReadDocumentRequest.newBuilder().setAddress(address).build())
                        .forEachRemaining(frames::add);
                assertThat(frames.getFirst().hasManifest()).isTrue();
                assertThat(frames.getLast().hasComplete()).isTrue();
                var partFrames = frames.subList(1, frames.size() - 1);
                assertThat(partFrames).allSatisfy(frame -> assertThat(frame.hasPartData()).isTrue());
                assertThat(partFrames.stream().map(frame -> frame.getPartData().getSelector().getPart()).toList())
                        .containsExactly(DocumentPart.DOCUMENT_PART_CORE, DocumentPart.DOCUMENT_PART_BLOBS,
                                DocumentPart.DOCUMENT_PART_PARSED);
                assertThat(partFrames.stream().map(frame -> frame.getPartData().getLast()).toList())
                        .containsExactly(true, true, true);
                assertThat(stub.getDocument(GetDocumentRequest.newBuilder().setAddress(address)
                        .setSelection(PartSelection.newBuilder().addSelectors(PartSelector.newBuilder()
                                .setPart(DocumentPart.DOCUMENT_PART_BLOBS))).build()).getDocument().hasBlobBag()).isTrue();
                // No semantic parts exist in this document. A wildcard for them must
                // not return unrelated CORE, BLOBS or PARSED content.
                for (var selection : java.util.List.of(
                        PartSelection.newBuilder().setAllSegmentations(true).build(),
                        PartSelection.newBuilder().setAllEmbeddingSets(true).build(),
                        PartSelection.newBuilder().setAllSegmentations(true).setAllEmbeddingSets(true).build())) {
                    int opensBefore = f.stack.opens.get();
                    var selected = stub.getDocument(GetDocumentRequest.newBuilder().setAddress(address)
                            .setSelection(selection).build());
                    assertThat(selected.getDocument()).isEqualTo(
                            ai.protomolt.proto.repo.v1.Document.getDefaultInstance());
                    assertThat(selected.getManifest().getPartsList()).allSatisfy(part ->
                            assertThat(part.getContentIncluded()).isFalse());
                    assertThat(f.stack.opens.get()).isEqualTo(opensBefore);
                }
            } finally {
                channel.shutdownNow();
                server.shutdownNow();
            }
        }
    }

    @Test void structuralRefusalsNameTheActualBoundOverGrpc() throws Exception {
        var name = "structure-" + UUID.randomUUID();
        try (var f = new AccessionFixture()) {
            var server = InProcessServerBuilder.forName(name).directExecutor()
                    .addService(new AccessionGrpcService(f.saves, f.reads, new DocumentDeletes(f.catalog, java.time.Clock.systemUTC()), new DocumentLists(f.catalog), new DocumentPurges(f.catalog, f.drives, (caller, account) -> new ai.protomolt.proto.repo.spi.PurgeAuthorization.Decision(false, false), java.time.Clock.systemUTC()), authentication -> new RepositoryCaller(
                            authentication.caller().name(), authentication.caller().unrestricted())))
                    .build().start();
            var channel = InProcessChannelBuilder.forName(name).directExecutor().build();
            try {
                var stub = DocumentServiceGrpc.newBlockingStub(channel);
                var account = "acct-" + UUID.randomUUID();
                var address = address(account, "structure");
                // Struct is an existing Document field, populated as protobuf, without JSON conversion.
                var deep = com.google.protobuf.Value.getDefaultInstance();
                for (int i = 0; i < 33; i++) deep = com.google.protobuf.Value.newBuilder()
                        .setListValue(com.google.protobuf.ListValue.newBuilder().addValues(deep)).build();
                var wide = com.google.protobuf.Value.newBuilder().setListValue(com.google.protobuf.ListValue.newBuilder()
                        .addAllValues(java.util.Collections.nCopies(1_000_001, com.google.protobuf.Value.getDefaultInstance()))).build();
                var unknownContent = document(account, "structure", "unknown").toBuilder()
                        .setUnknownFields(com.google.protobuf.UnknownFieldSet.newBuilder().addField(4000,
                                com.google.protobuf.UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build()).build();
                var unknownRequest = create("unknown-content", address, unknownContent).build();
                // Decision 35: a field no registered Document version knows is a recorded SCHEMA_MISSING.
                assertThat(detail(() -> stub.saveDocument(unknownRequest), Status.Code.FAILED_PRECONDITION,
                        SaveRejection.class).getMessage()).isEqualTo(
                        "DOCUMENT_PART_CORE carries a field the latest registered Document version does not know");
                assertThat(f.catalog.outcome(account, "unknown-content").orElseThrow().outcome().getRejection().getReason())
                        .isEqualTo(RejectionReason.REJECTION_REASON_SCHEMA_MISSING);
                for (var value : java.util.List.of(deep, wide)) {
                    var document = document(account, "structure", "shape").toBuilder();
                    document.getSearchMetadataBuilder().setCustomFields(com.google.protobuf.Struct.newBuilder().putFields("test", value));
                    var request = create("structure", address, document.build()).build();
                    var rejection = detail(() -> stub.saveDocument(request), Status.Code.INVALID_ARGUMENT, SaveRejection.class);
                    assertThat(rejection.getReason()).isEqualTo(RejectionReason.REJECTION_REASON_MALFORMED_REQUEST);
                    assertThat(rejection.toString()).contains(value == deep ? "nesting exceeds 64" : "exceeds 1,000,000 nodes");
                    assertThat(f.catalog.outcome(account, "structure")).isEmpty();
                    assertThat(f.catalog.current(address)).isEmpty();
                    assertThat(f.stack.puts.get()).isZero();
                }
            } finally {
                channel.shutdownNow();
                server.shutdownNow();
            }
        }
    }

    private static <T extends com.google.protobuf.Message> T detail(Runnable call, Status.Code code, Class<T> type) {
        try {
            call.run();
        } catch (StatusRuntimeException failure) {
            assertThat(failure.getStatus().getCode()).isEqualTo(code);
            var status = StatusProto.fromThrowable(failure);
            assertThat(status).isNotNull();
            try {
                return status.getDetails(0).unpack(type);
            } catch (com.google.protobuf.InvalidProtocolBufferException wrong) {
                throw new AssertionError(wrong);
            }
        }
        throw new AssertionError("call succeeded");
    }
}
