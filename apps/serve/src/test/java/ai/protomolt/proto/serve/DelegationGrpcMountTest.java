package ai.protomolt.proto.serve;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.actions.ProtoAction;
import ai.protomolt.proto.authz.CallerResolver;
import ai.protomolt.proto.delegation.DelegationActions;
import ai.protomolt.proto.delegation.DelegationBridge;
import ai.protomolt.proto.delegation.InProcessDelegationCoordinator;
import ai.protomolt.proto.delegation.v1.*;
import ai.protomolt.proto.grpc.invoke.DynamicGrpcCalls;
import ai.protomolt.proto.grpc.invoke.ReflectionClient;
import ai.protomolt.proto.grpc.service.ProtoMoltGrpcServer;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/** The contributed RPCs and JSON catalog must operate on the same coordinator state. */
class DelegationGrpcMountTest {
    private static final String SERVICE = "ai.protomolt.proto.delegation.v1.DelegationService";
    private static final String WORKER = "native-grpc-worker";
    private static final String CHECK = "protocol-check";
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Metadata.Key<String> TOKEN =
            Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER);

    @Test void customBinaryAnyAndReviewShareStateWithTheJsonCatalog() throws Exception {
        try (Fixture fixture = new Fixture()) {
            assertThat(fixture.call("RegisterWorker", RegisterWorkerRequest.newBuilder()
                    .setWorkerId(WORKER).setProvider("fixture").build(), "operator")
                    .getField(field("RegisterWorker", "admitted"))).isEqualTo(true);

            String task = UUID.randomUUID().toString();
            var contract = customContract();
            var spec = TaskSpec.newBuilder().setObjective("Return a caller-owned report")
                    .addAllowedScope("protocol/**")
                    .addRequiredChecks(AcceptanceCheck.newBuilder().setName(CHECK)
                            .setDescription("The harness inspected bytes"))
                    .setContract(contract).build();
            var offered = OfferTaskResponse.parseFrom(fixture.call("OfferTask",
                    OfferTaskRequest.newBuilder().setWorkerId(WORKER).setTaskId(task)
                            .setSpec(spec).setLeaseSeconds(300).build(), "operator").toByteString());
            assertThat(offered.getOk()).isTrue();
            fixture.call("AcceptTask", AcceptTaskRequest.newBuilder().setWorkerId(WORKER)
                    .setTaskId(task).setAttempt(1).build(), "operator");

            var descriptor = Descriptors.FileDescriptor.buildFrom(
                    DescriptorProtos.FileDescriptorSet.parseFrom(contract.getDescriptorSet())
                            .getFile(0), new Descriptors.FileDescriptor[0])
                    .findMessageTypeByName("Report");
            var report = DynamicMessage.newBuilder(descriptor)
                    .setField(descriptor.findFieldByName("title"), "real binary payload").build();
            var reference = ArtifactReference.newBuilder().setSha256("a".repeat(64))
                    .setMediaType("application/json").setSizeBytes(19).build();
            var candidate = CompletionCandidate.newBuilder().setAttempt(1).setRevision(1)
                    .setSummary("Validated a caller-owned binary Any")
                    .addEvidence(CheckEvidence.newBuilder().setCheckName(CHECK)
                            .setVerdict(CheckVerdict.CHECK_VERDICT_PASSED)
                            .setRanAt(Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()))
                            .addArtifacts(reference))
                    .addArtifacts(reference)
                    .setResult(Any.newBuilder().setTypeUrl("type.googleapis.com/native.v1.Report")
                            .setValue(report.toByteString())).build();
            fixture.call("SubmitCandidate", SubmitCandidateRequest.newBuilder()
                    .setWorkerId(WORKER).setTaskId(task).setCandidate(candidate).build(), "operator");
            long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
            long cursor = 0;
            while (fixture.coordinator.state().tasks().get(task).review().status()
                    != ai.protomolt.proto.delegation.DelegationReducer.ReviewStatus.DEFERRED) {
                assertThat(System.nanoTime()).isLessThan(deadline);
                var event = fixture.coordinator.waitForEvent(task, cursor, java.time.Duration.ofMillis(100));
                if (event.isPresent()) cursor = event.get().cursor();
            }
            int before = fixture.coordinator.transcript().getEntriesCount();
            var stale = catchThrowableOfType(() -> fixture.call("ReviewCandidate",
                    ReviewCandidateRequest.newBuilder().setTaskId(task).setAttempt(1)
                            .setRevision(2).setDecision(ReviewDecision.REVIEW_DECISION_ACCEPT)
                            .setVerdict("stale review").build(), "operator"),
                    StatusRuntimeException.class);
            assertThat(stale).isNotNull();
            assertThat(fixture.coordinator.transcript().getEntriesCount()).isEqualTo(before);
            fixture.call("ReviewCandidate", ReviewCandidateRequest.newBuilder().setTaskId(task)
                    .setAttempt(1).setRevision(1)
                    .setDecision(ReviewDecision.REVIEW_DECISION_ACCEPT)
                    .setVerdict("The harness inspected this candidate").build(), "operator");
            var transcript = fixture.catalog.execute("delegation-transcript",
                    JSON.createObjectNode().put("taskId", task).put("maxEntries", 100));
            assertThat(transcript.toString()).contains("accepted").contains("native.v1.Report");
            var nativeTranscript = ReadTranscriptResponse.parseFrom(fixture.call("ReadTranscript",
                    ReadTranscriptRequest.newBuilder().setTaskId(task).setMaxEntries(100).build(),
                    "operator").toByteString());
            var nativeWatch = WatchEventsResponse.parseFrom(fixture.call("WatchEvents",
                    WatchEventsRequest.newBuilder().setTaskId(task).setMaxEvents(100).build(),
                    "operator").toByteString());
            assertThat(nativeWatch.getEventsList()).isEqualTo(nativeTranscript.getEventsList());
            assertThat(nativeTranscript.getEventsList().stream()
                    .filter(event -> event.getEntry().getWorkerFrame().hasCompletion())
                    .map(event -> event.getEntry().getWorkerFrame().getCompletion().getResult()))
                    .containsExactly(candidate.getResult());
        }
    }

    @Test void nativeRejectRecordsTheAddressedOfferAndCannotRepeatIt() throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.call("RegisterWorker", RegisterWorkerRequest.newBuilder()
                    .setWorkerId(WORKER).setProvider("fixture").build(), "operator");
            String task = UUID.randomUUID().toString();
            fixture.call("OfferTask", OfferTaskRequest.newBuilder().setWorkerId(WORKER)
                    .setTaskId(task).setLeaseSeconds(300).setSpec(TaskSpec.newBuilder()
                            .setObjective("Decline unsupported work").addAllowedScope("protocol/**")
                            .addRequiredChecks(AcceptanceCheck.newBuilder().setName(CHECK)
                                    .setDescription("Inspect the report"))
                            .setContract(customContract())).build(), "operator");
            var request = RejectTaskRequest.newBuilder().setWorkerId(WORKER).setTaskId(task)
                    .setAttempt(1).setReason("Unsupported report contract").setRetryable(true).build();
            var reply = RejectTaskResponse.parseFrom(fixture.call("RejectTask", request,
                    "operator").toByteString());
            assertThat(reply.getOk()).isTrue();
            assertThat(reply.getTaskId()).isEqualTo(task);
            assertThat(reply.getAttempt()).isEqualTo(1);
            var transcript = ReadTranscriptResponse.parseFrom(fixture.call("ReadTranscript",
                    ReadTranscriptRequest.newBuilder().setTaskId(task).build(), "operator").toByteString());
            assertThat(transcript.getEventsList().stream()
                    .filter(event -> event.getEntry().getWorkerFrame().hasReject())
                    .map(event -> event.getEntry().getWorkerFrame().getReject().getReason()))
                    .containsExactly("Unsupported report contract");
            int before = fixture.coordinator.transcript().getEntriesCount();
            assertThat(catchThrowableOfType(() -> fixture.call("RejectTask", request, "operator"),
                    StatusRuntimeException.class)).isNotNull();
            assertThat(fixture.coordinator.transcript().getEntriesCount()).isEqualTo(before);
        }
    }

    @Test void invalidRequestAndMissingScopeCannotMutateAndReflectionNeedsAuthentication()
            throws Exception {
        try (Fixture fixture = new Fixture()) {
            var reflected = ReflectionClient.discover(fixture.authenticated("operator"), 5000);
            assertThat(reflected.services()).contains(SERVICE);
            var badReflection = catchThrowableOfType(
                    () -> ReflectionClient.discover(fixture.channel, 5000), Exception.class);
            assertThat(badReflection).isNotNull();

            int before = fixture.coordinator.transcript().getEntriesCount();
            var invalid = catchThrowableOfType(() -> fixture.call("OfferTask",
                    OfferTaskRequest.newBuilder().setWorkerId(WORKER).build(), "operator"),
                    StatusRuntimeException.class);
            assertThat(invalid.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
            var denied = catchThrowableOfType(() -> fixture.call("RegisterWorker",
                    RegisterWorkerRequest.newBuilder().setWorkerId(WORKER).build(), "reader"),
                    StatusRuntimeException.class);
            assertThat(denied.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
            var unknown = catchThrowableOfType(() -> fixture.call("RegisterWorker",
                    RegisterWorkerRequest.newBuilder().setWorkerId(WORKER).build(), "unknown"),
                    StatusRuntimeException.class);
            assertThat(unknown.getStatus().getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);
            assertThat(fixture.coordinator.transcript().getEntriesCount()).isEqualTo(before);
        }
    }

    @Test void invalidSuccessfulResponseIsReportedAsDataLoss() {
        try (Fixture fixture = new Fixture(true)) {
            var failure = catchThrowableOfType(() -> fixture.call("RejectTask",
                    RejectTaskRequest.newBuilder().setWorkerId(WORKER)
                            .setTaskId(UUID.randomUUID().toString()).setAttempt(1)
                            .setReason("Valid request to a deliberately broken test handler").build(),
                    "operator"), StatusRuntimeException.class);
            assertThat(failure).isNotNull();
            assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.DATA_LOSS);
            assertThat(failure.getStatus().getDescription()).contains("invalid-response");
        }
    }

    private static final class InvalidRejectAction implements ProtoAction {
        @Override public String name() { return "delegation-reject"; }
        @Override public String description() { return "Test handler with an invalid success envelope"; }
        @Override public String requiredScope() { return Scopes.WORKER_COORDINATE; }
        @Override public Descriptors.Descriptor requestType() { return RejectTaskRequest.getDescriptor(); }
        @Override public Descriptors.Descriptor responseType() { return RejectTaskResponse.getDescriptor(); }
        @Override public Message execute(Message input, ActionContext context) {
            return RejectTaskResponse.getDefaultInstance();
        }
    }

    private static Descriptors.FieldDescriptor field(String method, String name) {
        return ai.protomolt.proto.delegation.v1.DelegationActions.getDescriptor()
                .findServiceByName("DelegationService")
                .findMethodByName(method).getOutputType().findFieldByName(name);
    }

    private static DeliverableContract customContract() {
        var title = DescriptorProtos.FieldDescriptorProto.newBuilder().setName("title")
                .setNumber(1).setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
                .build();
        var file = DescriptorProtos.FileDescriptorProto.newBuilder().setName("native.proto")
                .setPackage("native.v1").setSyntax("proto3")
                .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("Report").addField(title)).build();
        return DeliverableContract.newBuilder().setTypeName("native.v1.Report")
                .setDescriptorSet(DescriptorProtos.FileDescriptorSet.newBuilder()
                        .addFile(file).build().toByteString()).build();
    }

    private static final class Fixture implements AutoCloseable {
        final InProcessDelegationCoordinator coordinator = new InProcessDelegationCoordinator();
        final DelegationBridge bridge = new DelegationBridge(coordinator);
        final ActionCatalog catalog = DelegationActions.register(
                ActionCatalog.defaults(ActionContext.create()), bridge);
        final ProtoMoltGrpcServer server;
        final ManagedChannel channel;

        Fixture() { this(false); }

        Fixture(boolean invalidResponse) {
            if (invalidResponse) catalog.replace(new InvalidRejectAction());
            CallerResolver resolver = credential -> "reader".equals(credential)
                    ? Optional.of(Caller.scoped("reader", Set.of(Scopes.SCHEMA_READ)))
                    : Optional.empty();
            server = ProtoMoltGrpcServer.start("127.0.0.1", 0, catalog, "operator", resolver,
                    List.of(ai.protomolt.proto.delegation.v1.DelegationActions.getDescriptor()
                            .findServiceByName("DelegationService")));
            channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.port())
                    .usePlaintext().build();
        }

        io.grpc.Channel authenticated(String credential) {
            Metadata headers = new Metadata();
            headers.put(TOKEN, credential);
            return io.grpc.ClientInterceptors.intercept(channel,
                    MetadataUtils.newAttachHeadersInterceptor(headers));
        }

        DynamicMessage call(String name, Message request, String credential) {
            var method = ai.protomolt.proto.delegation.v1.DelegationActions.getDescriptor()
                    .findServiceByName("DelegationService").findMethodByName(name);
            DynamicMessage dynamic;
            try {
                dynamic = DynamicMessage.parseFrom(method.getInputType(), request.toByteString());
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                throw new IllegalArgumentException(e);
            }
            Metadata headers = new Metadata();
            headers.put(TOKEN, credential);
            return DynamicGrpcCalls.call(channel, method, dynamic,
                    CallOptions.DEFAULT.withDeadlineAfter(5, TimeUnit.SECONDS), headers, 1).getFirst();
        }

        @Override public void close() {
            channel.shutdownNow();
            server.close();
            bridge.close();
            coordinator.close();
        }
    }
}
