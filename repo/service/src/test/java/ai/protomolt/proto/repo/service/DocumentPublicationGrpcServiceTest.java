package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.authz.grpc.CallerContexts;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.DocumentPublicationRepository;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v1.*;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

/**
 * Capacity lifecycle of the publication transport adapter with explicit control
 * over the server terminal callbacks. The transport holds one admitted call from
 * {@code admit()} until the producer finished AND gRPC delivered the call's
 * terminal (cancel/close) event; neither event alone releases capacity. Client
 * receipt of a response is not a capacity signal: the server-side close is
 * delivered asynchronously after the response is on the wire, so a replay issued
 * on response receipt alone is legitimately refused with RESOURCE_EXHAUSTED.
 * Callers synchronize on {@code awaitIdle}, the documented drainage event.
 */
class DocumentPublicationGrpcServiceTest {
    private static final String DRIVE = "10000000-0000-4000-8000-000000000001";
    private static final String REVISION = "10000000-0000-4000-8000-000000000002";
    private static final byte[] CONTENT = "fixture".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    private static final PublishDocumentRequest REQUEST = request();

    @Test void responseDeliveryDoesNotReturnTransportCapacityBeforeServerTermination() throws Exception {
        var f = new Fixture((caller, request, control) -> committed(request, caller));
        var first = new ControlledObserver();
        f.publish(REQUEST, first);
        assertThat(first.completed.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(first.error.get()).isNull();
        // The caller holds the complete response; the server terminal callback is still pending.
        assertThat(f.service.awaitIdle(Duration.ZERO)).isFalse();
        assertThat(f.budget.reservedBytes()).isPositive();
        var replay = new ControlledObserver();
        f.publish(REQUEST, replay);
        assertThat(replayedRefusal(replay)).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
        // The documented drainage event: the server terminal callback returns capacity once.
        first.close();
        assertThat(f.service.awaitIdle(Duration.ofSeconds(5))).isTrue();
        assertThat(f.budget.reservedBytes()).isZero();
        var drained = new ControlledObserver();
        f.publish(REQUEST, drained);
        assertThat(drained.error.get()).isNull();
        assertThat(drained.value.get().toByteArray()).isEqualTo(first.value.get().toByteArray());
        drained.close();
        assertThat(f.service.awaitIdle(Duration.ofSeconds(5))).isTrue();
    }

    @Test void cancellationRetainsCapacityUntilProducerActuallyExits() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var f = new Fixture(blocked(entered, release));
        var first = new ControlledObserver();
        var producer = Thread.ofVirtual().start(() -> f.publish(REQUEST, first));
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
        first.cancel();
        // Cancellation reached the server; the producer still holds its slot and bytes.
        assertThat(f.service.awaitIdle(Duration.ZERO)).isFalse();
        var refused = new ControlledObserver();
        f.publish(REQUEST, refused);
        assertThat(replayedRefusal(refused)).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
        release.countDown();
        assertThat(producer.join(Duration.ofSeconds(5))).isTrue();
        assertThat(Status.fromThrowable(first.error.get()).getCode()).isEqualTo(Status.Code.CANCELLED);
        // The cancel terminal signal already fired; producer exit alone completes the release.
        assertThat(f.service.awaitIdle(Duration.ofSeconds(5))).isTrue();
        assertThat(f.budget.reservedBytes()).isZero();
    }

    @Test void terminalSignalsReleaseCapacityExactlyOnce() throws Exception {
        var f = new Fixture((caller, request, control) -> committed(request, caller));
        var first = new ControlledObserver();
        f.publish(REQUEST, first);
        assertThat(first.completed.await(5, TimeUnit.SECONDS)).isTrue();
        first.cancel();
        first.close();
        first.close();
        assertThat(f.service.awaitIdle(Duration.ofSeconds(5))).isTrue();
        assertThat(f.budget.reservedBytes()).isZero();
        // Exactly-once proof: a second active call must still occupy the only slot.
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        f.use(blocked(entered, release));
        var active = new ControlledObserver();
        var producer = Thread.ofVirtual().start(() -> f.publish(REQUEST, active));
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
        var extra = new ControlledObserver();
        f.publish(REQUEST, extra);
        assertThat(replayedRefusal(extra)).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
        release.countDown();
        assertThat(producer.join(Duration.ofSeconds(5))).isTrue();
        assertThat(active.error.get()).isNull();
        active.close();
        assertThat(f.service.awaitIdle(Duration.ofSeconds(5))).isTrue();
        assertThat(f.budget.reservedBytes()).isZero();
    }

    @Test void closeRejectsNewCallsWhileAcceptedWorkRetainsCapacity() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var f = new Fixture(blocked(entered, release));
        var active = new ControlledObserver();
        var producer = Thread.ofVirtual().start(() -> f.publish(REQUEST, active));
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
        f.service.close();
        var late = new ControlledObserver();
        f.publish(REQUEST, late);
        assertThat(replayedRefusal(late)).isEqualTo(Status.Code.UNAVAILABLE);
        assertThat(f.service.awaitIdle(Duration.ZERO)).isFalse();
        release.countDown();
        assertThat(producer.join(Duration.ofSeconds(5))).isTrue();
        assertThat(active.error.get()).isNull();
        assertThat(active.value.get()).isNotNull();
        active.close();
        assertThat(f.service.awaitIdle(Duration.ofSeconds(5))).isTrue();
        assertThat(f.budget.reservedBytes()).isZero();
    }

    @Test void producerFailureHoldsCapacityUntilServerTermination() throws Exception {
        var f = new Fixture((caller, request, control) -> {
            throw new IllegalArgumentException("Invalid publication request");
        });
        var first = new ControlledObserver();
        f.publish(REQUEST, first);
        assertThat(replayedRefusal(first)).isEqualTo(Status.Code.INVALID_ARGUMENT);
        // The producer finished with an error; the terminal callback has not arrived.
        assertThat(f.service.awaitIdle(Duration.ZERO)).isFalse();
        assertThat(f.budget.reservedBytes()).isPositive();
        var refused = new ControlledObserver();
        f.publish(REQUEST, refused);
        assertThat(replayedRefusal(refused)).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
        first.close();
        assertThat(f.service.awaitIdle(Duration.ofSeconds(5))).isTrue();
        assertThat(f.budget.reservedBytes()).isZero();
    }

    private static Status.Code replayedRefusal(ControlledObserver observer) {
        var failure = observer.error.get();
        assertThat(failure).isNotNull();
        assertThat(observer.value.get()).isNull();
        return Status.fromThrowable(failure).getCode();
    }

    private static DocumentPublicationRepository blocked(CountDownLatch entered, CountDownLatch release) {
        return (caller, request, control) -> {
            entered.countDown();
            try {
                if (!release.await(10, TimeUnit.SECONDS)) throw new AssertionError("Producer release not signalled");
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(interrupted);
            }
            control.check();
            return committed(request, caller);
        };
    }

    private static PublishDocumentResponse committed(PublishDocumentRequest request, RepositoryCaller caller) {
        var command = new DocumentPublicationCommand(request.getIntent());
        var result = DocumentPublicationResult.newBuilder().setOperationId(command.operationId().toString())
                .setAccountId(command.intent().getAccountId()).setCommandSha256(command.sha256())
                .setPrincipal(caller.principalName()).setOwnerGeneration(1)
                .setCommandEncodingVersion(DocumentPublicationCommand.ENCODING_VERSION);
        for (var member : command.intent().getMembersList()) {
            result.addMembers(DocumentPublishedRevision.newBuilder().setMemberId(member.getMemberId())
                    .setAddress(member.getDestination().getAddress()).setRevisionId(REVISION).setMutationRevision(1));
        }
        return PublishDocumentResponse.newBuilder().setCommitted(result).build();
    }

    private static PublishDocumentRequest request() {
        String sha256;
        try {
            sha256 = java.util.HexFormat.of().formatHex(
                    java.security.MessageDigest.getInstance("SHA-256").digest(CONTENT));
        } catch (java.security.NoSuchAlgorithmException unavailable) {
            throw new IllegalStateException("SHA-256 is unavailable", unavailable);
        }
        var member = DocumentPublicationMember.newBuilder().setMemberId("m").setDriveId(DRIVE)
                .setRowKind(DocumentPublicationRowKind.DOCUMENT_PUBLICATION_ROW_KIND_PIPELINE)
                .setDestination(DocumentRevisionCondition.newBuilder()
                        .setAddress(NodeAddress.newBuilder().setAccountId("a").setDocId("d")
                                .setGraphId("g").setGraphAddressId("n"))
                        .setIfAbsent(true))
                .setOwnership(OwnershipContext.newBuilder().setAccountId("a").setDatasourceId("s")
                        .setSecurity(DocumentSecurity.getDefaultInstance()))
                .addParts(DocumentPublicationPart.newBuilder()
                        .setSlot(DocumentPublicationSlot.newBuilder()
                                .setPart(DocumentPart.DOCUMENT_PART_CORE).setSubKey(""))
                        .setUpload(PublicationUpload.newBuilder().setSizeBytes(CONTENT.length)
                                .setSha256(sha256).setContentType("application/protobuf")))
                .build();
        return PublishDocumentRequest.newBuilder()
                .setIntent(DocumentPublicationIntent.newBuilder().setEncodingVersion(1).setAccountId("a")
                        .setOperationId(UUID.nameUUIDFromBytes(CONTENT).toString()).addMembers(member))
                .addModes(DocumentPublicationMemberMode.newBuilder().setMemberId("m")
                        .setMode(DocumentPublicationMode.DOCUMENT_PUBLICATION_MODE_TYPED))
                .addPayloads(DocumentPublicationPayload.newBuilder().setMemberId("m")
                        .setRevisionOrdinal(0).setContent(com.google.protobuf.ByteString.copyFrom(CONTENT)))
                .build();
    }

    private static final class Fixture {
        final PayloadBudget budget = new PayloadBudget(64L * 1024 * 1024);
        final DocumentPublicationGrpcService service;
        private volatile DocumentPublicationRepository repository;

        Fixture(DocumentPublicationRepository repository) {
            this.repository = repository;
            this.service = new DocumentPublicationGrpcService((caller, request, control) ->
                    this.repository.publishDocument(caller, request, control),
                    auth -> new RepositoryCaller(auth.caller().name(), auth.caller().unrestricted()), budget, 1);
        }

        void use(DocumentPublicationRepository next) {
            repository = next;
        }

        void publish(PublishDocumentRequest request, ControlledObserver observer) {
            Context.current().withValue(CallerContexts.CALLER, Caller.operator())
                    .run(() -> service.publishDocument(request, observer));
        }
    }

    /** Server-side call handle whose terminal events fire only when the test delivers them. */
    private static final class ControlledObserver extends ServerCallStreamObserver<PublishDocumentResponse> {
        final CountDownLatch completed = new CountDownLatch(1);
        final AtomicReference<Throwable> error = new AtomicReference<>();
        final AtomicReference<PublishDocumentResponse> value = new AtomicReference<>();
        private final AtomicReference<Runnable> onCancel = new AtomicReference<>();
        private final AtomicReference<Runnable> onClose = new AtomicReference<>();
        private volatile boolean cancelled;

        @Override public boolean isCancelled() { return cancelled; }
        @Override public void setOnCancelHandler(Runnable handler) { onCancel.set(handler); }
        @Override public void setOnCloseHandler(Runnable handler) { onClose.set(handler); }
        @Override public boolean isReady() { return true; }
        @Override public void setOnReadyHandler(Runnable handler) {}
        @Override public void setCompression(String compression) {}
        @Override public void request(int count) {}
        @Override public void disableAutoInboundFlowControl() {}
        @Override public void setMessageCompression(boolean enable) {}
        @Override public void onNext(PublishDocumentResponse response) { value.set(response); }
        @Override public void onCompleted() { completed.countDown(); }
        @Override public void onError(Throwable failure) { error.set(failure); }

        void cancel() {
            cancelled = true;
            onCancel.get().run();
        }

        void close() {
            onClose.get().run();
        }
    }
}
