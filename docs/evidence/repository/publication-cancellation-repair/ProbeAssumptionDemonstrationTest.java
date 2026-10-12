package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.authz.grpc.CallerContexts;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v1.*;
import io.grpc.Context;
import io.grpc.stub.ServerCallStreamObserver;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

/**
 * Red reproducer for GitHub run 38121396523 (job 114416690422), preserved as
 * evidence under docs/evidence/repository/publication-cancellation-repair/.
 * It encodes the old BoundedPublicationRpcCancellationProbe assumption — a receipt
 * replay issued immediately after the committed response is observed, without
 * waiting for server drainage — under a forced legal ordering (the server terminal
 * callback lands after the client holds the response). It fails RED with
 * RESOURCE_EXHAUSTED: Publication transport is full, the exact CI signature.
 */
class ProbeAssumptionDemonstrationTest {
    @Test void replayImmediatelyAfterResponseReceipt() throws Exception {
        var service = new DocumentPublicationGrpcService((caller, request, control) -> {
            var command = new DocumentPublicationCommand(request.getIntent());
            var result = DocumentPublicationResult.newBuilder().setOperationId(command.operationId().toString())
                    .setAccountId(command.intent().getAccountId()).setCommandSha256(command.sha256())
                    .setPrincipal(caller.principalName()).setOwnerGeneration(1).setCommandEncodingVersion(1);
            for (var member : command.intent().getMembersList())
                result.addMembers(DocumentPublishedRevision.newBuilder().setMemberId(member.getMemberId())
                        .setAddress(member.getDestination().getAddress())
                        .setRevisionId("10000000-0000-4000-8000-000000000002").setMutationRevision(1));
            return PublishDocumentResponse.newBuilder().setCommitted(result).build();
        }, auth -> new RepositoryCaller(auth.caller().name(), auth.caller().unrestricted()),
                new PayloadBudget(64L * 1024 * 1024), 1);
        var member = DocumentPublicationMember.newBuilder().setMemberId("m")
                .setDriveId("10000000-0000-4000-8000-000000000001")
                .setRowKind(DocumentPublicationRowKind.DOCUMENT_PUBLICATION_ROW_KIND_PIPELINE)
                .setDestination(DocumentRevisionCondition.newBuilder().setAddress(NodeAddress.newBuilder()
                        .setAccountId("a").setDocId("d").setGraphId("g").setGraphAddressId("n")).setIfAbsent(true))
                .setOwnership(OwnershipContext.newBuilder().setAccountId("a").setDatasourceId("s")
                        .setSecurity(DocumentSecurity.getDefaultInstance()))
                .addParts(DocumentPublicationPart.newBuilder()
                        .setSlot(DocumentPublicationSlot.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE))
                        .setUpload(PublicationUpload.newBuilder().setSizeBytes(7).setSha256("a".repeat(64))
                                .setContentType("application/protobuf")))
                .build();
        var request = PublishDocumentRequest.newBuilder()
                .setIntent(DocumentPublicationIntent.newBuilder().setEncodingVersion(1).setAccountId("a")
                        .setOperationId(UUID.nameUUIDFromBytes(new byte[]{7}).toString()).addMembers(member))
                .addModes(DocumentPublicationMemberMode.newBuilder().setMemberId("m")
                        .setMode(DocumentPublicationMode.DOCUMENT_PUBLICATION_MODE_TYPED))
                .build();
        var committed = new Observer();
        Context.current().withValue(CallerContexts.CALLER, Caller.operator())
                .run(() -> service.publishDocument(request, committed));
        if (committed.error.get() != null) {
            var trace = new java.io.StringWriter();
            committed.error.get().printStackTrace(new java.io.PrintWriter(trace));
            throw new AssertionError("first publish failed:\n" + trace);
        }
        assertThat(committed.value.get()).isNotNull();
        // Probe line 61 equivalent: replay before the server terminal callback was delivered.
        var replay = new Observer();
        Context.current().withValue(CallerContexts.CALLER, Caller.operator())
                .run(() -> service.publishDocument(request, replay));
        if (replay.error.get() != null)
            throw new AssertionError("old fixture assumption fails under forced ordering: replay refused with "
                    + io.grpc.Status.fromThrowable(replay.error.get()).getCode()
                    + ": " + replay.error.get().getMessage());
    }

    private static final class Observer extends ServerCallStreamObserver<PublishDocumentResponse> {
        final CountDownLatch completed = new CountDownLatch(1);
        final AtomicReference<Throwable> error = new AtomicReference<>();
        final AtomicReference<PublishDocumentResponse> value = new AtomicReference<>();
        @Override public boolean isCancelled() { return false; }
        @Override public void setOnCancelHandler(Runnable handler) {}
        @Override public void setOnCloseHandler(Runnable handler) {} // captured, never delivered
        @Override public boolean isReady() { return true; }
        @Override public void setOnReadyHandler(Runnable handler) {}
        @Override public void setCompression(String compression) {}
        @Override public void request(int count) {}
        @Override public void disableAutoInboundFlowControl() {}
        @Override public void setMessageCompression(boolean enable) {}
        @Override public void onNext(PublishDocumentResponse response) { value.set(response); }
        @Override public void onCompleted() { completed.countDown(); }
        @Override public void onError(Throwable failure) { error.set(failure); }
    }
}
