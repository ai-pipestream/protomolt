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
                var saved = stub.saveDocument(create("op-1", address, document).build());
                assertThat(saved.getAccession().getRevision()).isEqualTo(1);
                assertThat(stub.getDocument(GetDocumentRequest.newBuilder().setAddress(address).build()).getDocument())
                        .isEqualTo(document);
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
