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
    @Test void savesReadsAndRefusalsCrossTheTransportWithTheirDetails() throws Exception {
        var name = "accession-" + UUID.randomUUID();
        try (var f = new AccessionFixture()) {
            var server = InProcessServerBuilder.forName(name).directExecutor()
                    .addService(new AccessionGrpcService(f.saves, f.reads, authentication -> new RepositoryCaller(
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
                assertThatThrownBy(() -> stub.readDocument(ReadDocumentRequest.newBuilder().setAddress(address).build())
                        .hasNext()).isInstanceOfSatisfying(StatusRuntimeException.class, failure ->
                        assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.UNIMPLEMENTED));
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
