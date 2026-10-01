package ai.protomolt.proto.samples;

import ai.protomolt.proto.delegation.v1.CheckEvidence;
import ai.protomolt.proto.delegation.v1.CheckVerdict;
import ai.protomolt.proto.delegation.v1.CompletionCandidate;
import ai.protomolt.proto.delegation.v1.RegisterWorkerRequest;
import ai.protomolt.proto.delegation.v1.AcceptTaskRequest;
import ai.protomolt.proto.delegation.v1.SubmitCandidateRequest;
import ai.protomolt.proto.delegation.v1.WorkerCapability;
import ai.protomolt.proto.grpc.invoke.DynamicGrpcCalls;
import ai.protomolt.proto.grpc.invoke.ReflectionClient;
import ai.protomolt.proto.samples.authoring.v1.AuthoringFixtureServiceGrpc;
import ai.protomolt.proto.samples.authoring.v1.NormalizeTextRequest;
import ai.protomolt.proto.samples.authoring.v1.NormalizeTextResponse;
import ai.protomolt.proto.samples.authoring.v1.WriteRecordRequest;
import ai.protomolt.proto.samples.authoring.v1.WriteRecordResponse;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringDeliverable;
import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowAuthorContextRequest;
import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowAuthorContextResponse;
import ai.protomolt.proto.workflow.authoring.v1.PrepareWorkflowCandidateRequest;
import ai.protomolt.proto.workflow.authoring.v1.PrepareWorkflowCandidateResponse;
import ai.protomolt.proto.workflow.authoring.v1.ReadWorkflowAuthorEventsRequest;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowAuthorTaskServiceGrpc;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowPreparationServiceGrpc;
import ai.protomolt.proto.validate.ProtoValidator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.Descriptors.FieldDescriptor;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** Separate-process demonstration author. All acceptance evidence comes from preparation. */
public final class AuthoringWorker {
    private static final long RPC_SECONDS = 10;
    private static final Duration TASK_WAIT = Duration.ofSeconds(90);
    private static final String FIXTURE_SERVICE = AuthoringFixtureServiceGrpc.SERVICE_NAME;
    private static final ObjectMapper JSON = new ObjectMapper();

    private AuthoringWorker() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            throw new IllegalArgumentException("usage: AuthoringWorker <coordinator-host:port> "
                    + "<fixture-host:port> <task-uuid> <attempt> <worker-id>");
        }
        String token = System.getenv("PROTOMOLT_AUTHOR_TOKEN");
        if (token == null || token.isBlank()) throw new IllegalArgumentException("author token is required");
        new AuthoringWorkerRun(args[0], args[1], args[2], Integer.parseInt(args[3]), args[4], token).run();
    }

    private record AuthoringWorkerRun(String coordinator, String fixture, String taskId,
            int attempt, String workerId, String token) {
        void run() throws Exception {
            ManagedChannel control = ManagedChannelBuilder.forTarget(coordinator)
                    .usePlaintext().maxInboundMessageSize(16 * 1024 * 1024).build();
            ManagedChannel external = ManagedChannelBuilder.forTarget(fixture)
                    .usePlaintext().maxInboundMessageSize(4 * 1024 * 1024).build();
            try {
                Metadata headers = new Metadata();
                headers.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER), token);
                var tasks = WorkflowAuthorTaskServiceGrpc.newBlockingStub(control)
                        .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
                var preparation = WorkflowPreparationServiceGrpc.newBlockingStub(control)
                        .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));

                var registration = tasks.withDeadlineAfter(RPC_SECONDS, TimeUnit.SECONDS)
                        .registerWorkflowAuthor(RegisterWorkerRequest.newBuilder()
                                .setWorkerId(workerId).setProvider("scripted")
                                .setModel("authoring-worker").setModelVersion("1")
                                .addCapabilities(WorkerCapability.newBuilder().setName("workflow-authoring"))
                                .build());
                validate(registration);
                require(registration.getOk() && registration.getAdmitted()
                        && registration.getWorkerId().equals(workerId), "author registration refused");
                System.out.println("AuthoringWorker ready");
                System.out.flush();

                var context = waitForOffer(tasks);
                require(context.getTaskId().equals(taskId) && context.getAttempt() == attempt,
                        "author context identity differs");
                require(context.getOfferEntry().getWorkerId().equals(workerId),
                        "offer belongs to another worker");
                validate(context);
                var accepted = tasks.withDeadlineAfter(RPC_SECONDS, TimeUnit.SECONDS)
                        .acceptWorkflowTask(AcceptTaskRequest.newBuilder().setWorkerId(workerId)
                                .setTaskId(taskId).setAttempt(attempt).build());
                validate(accepted);
                require(accepted.getOk() && accepted.getTaskId().equals(taskId)
                        && accepted.getAttempt() == attempt, "task acceptance differs");

                var reflected = ReflectionClient.discover(external, 10_000);
                var methods = methods(context, reflected.descriptorSet(), fixture);
                probe(external, methods);

                byte[] source = source(context, methods).getBytes(StandardCharsets.UTF_8);
                // A retry after a lost preparation response must name the same
                // reserved intent, even when this scripted process is restarted.
                String preparationId = UUID.nameUUIDFromBytes(("authoring-preparation-v1|"
                        + taskId + "|" + attempt + "|1|" + workerId)
                        .getBytes(StandardCharsets.UTF_8)).toString();
                var prepared = preparation.withDeadlineAfter(60, TimeUnit.SECONDS)
                        .prepareWorkflowCandidate(PrepareWorkflowCandidateRequest.newBuilder()
                                .setTaskId(taskId).setAttempt(attempt).setRevision(1)
                                .setPreparationId(preparationId)
                                .setExecutableSourceJson(ByteString.copyFrom(source)).build());
                verifyPrepared(prepared, context, preparationId, source);
                var authored = prepared.getAuthored();
                var deliverable = authored.getDeliverable();
                var candidate = CompletionCandidate.newBuilder().setAttempt(attempt).setRevision(1)
                        .setSummary("Scripted fixture probe and coordinator-observed preparation completed")
                        .addAllEvidence(deliverable.getChecksList())
                        .addArtifacts(deliverable.getWorkflowArtifact())
                        .addArtifacts(authored.getExecutableSource())
                        .addArtifacts(deliverable.getReceipt())
                        .setResult(Any.pack(authored)).build();
                var submitted = tasks.withDeadlineAfter(RPC_SECONDS, TimeUnit.SECONDS)
                        .submitWorkflowCandidate(SubmitCandidateRequest.newBuilder()
                                .setWorkerId(workerId).setTaskId(taskId).setCandidate(candidate).build());
                validate(submitted);
                require(submitted.getOk() && submitted.getTaskId().equals(taskId)
                        && submitted.getAttempt() == attempt && submitted.getRevision() == 1,
                        "candidate submission differs");
                waitForReview(tasks, candidate);
                System.out.println("AuthoringWorker accepted task=" + taskId + " attempt=" + attempt);
            } finally {
                external.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
                control.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            }
        }

        private GetWorkflowAuthorContextResponse waitForOffer(
                WorkflowAuthorTaskServiceGrpc.WorkflowAuthorTaskServiceBlockingStub tasks) throws Exception {
            long deadline = System.nanoTime() + TASK_WAIT.toNanos();
            var request = GetWorkflowAuthorContextRequest.newBuilder()
                    .setTaskId(taskId).setAttempt(attempt).build();
            while (System.nanoTime() < deadline) {
                try {
                    return tasks.withDeadlineAfter(RPC_SECONDS, TimeUnit.SECONDS)
                            .getWorkflowAuthorContext(request);
                } catch (StatusRuntimeException waiting) {
                    if (waiting.getStatus().getCode() != Status.Code.FAILED_PRECONDITION) throw waiting;
                    Thread.sleep(150);
                }
            }
            throw new IllegalStateException("timed out waiting for assigned offer");
        }

        private void waitForReview(
                WorkflowAuthorTaskServiceGrpc.WorkflowAuthorTaskServiceBlockingStub tasks,
                CompletionCandidate candidate) throws Exception {
            long cursor = 0;
            boolean seenSubmission = false;
            long deadline = System.nanoTime() + TASK_WAIT.toNanos();
            while (System.nanoTime() < deadline) {
                var response = tasks.withDeadlineAfter(RPC_SECONDS, TimeUnit.SECONDS)
                        .readWorkflowAuthorEvents(ReadWorkflowAuthorEventsRequest.newBuilder()
                                .setTaskId(taskId).setAttempt(attempt)
                                .setAfterCursor(cursor).setMaxEvents(32).build());
                validate(response);
                require(response.getTaskId().equals(taskId) && response.getAttempt() == attempt
                        && response.getAfterCursor() == cursor && response.getCursor() >= cursor,
                        "author event cursor differs");
                for (var observed : response.getEventsList()) {
                    var entry = observed.getEntry();
                    require(entry.getWorkerId().equals(workerId), "author event belongs to another worker");
                    if (entry.hasWorkerFrame()) {
                        var frame = entry.getWorkerFrame();
                        require(frame.getTaskId().equals(taskId), "author event belongs to another task");
                        if (frame.hasCompletion() && frame.getCompletion().getAttempt() == attempt
                                && frame.getCompletion().getRevision() == candidate.getRevision()) {
                            require(frame.getCompletion().equals(candidate),
                                    "recorded candidate differs from submission");
                            seenSubmission = true;
                        }
                    }
                    if (!entry.hasCoordinatorFrame()) continue;
                    var frame = entry.getCoordinatorFrame();
                    require(frame.getTaskId().equals(taskId), "author event belongs to another task");
                    if (frame.hasAccepted() && frame.getAccepted().getAttempt() == attempt
                            && frame.getAccepted().getRevision() == candidate.getRevision()) {
                        require(seenSubmission, "accepted review lacks exact submitted candidate");
                        return;
                    }
                    if (frame.hasRevisionRequested() || frame.hasCancellation() || frame.hasExpired()) {
                        throw new IllegalStateException("independent review did not accept the candidate");
                    }
                }
                cursor = response.getCursor();
                if (!response.getTruncated()) Thread.sleep(150);
            }
            throw new IllegalStateException("timed out waiting for independent review");
        }

        private String source(GetWorkflowAuthorContextResponse context, Methods methods) throws Exception {
            var root = JSON.createObjectNode();
            root.put("name", "normalize-record-v1");
            root.put("validateContract", true);
            root.putObject("schema").put("descriptorSetBase64",
                    Base64.getEncoder().encodeToString(context.getDescriptorSet().toByteArray()));
            root.put("inputType", WriteRecordRequest.getDescriptor().getFullName());
            var steps = root.putArray("steps");
            var normalize = steps.addObject();
            normalize.put("name", "normalize");
            normalize.put("target", fixture);
            normalize.put("method", methods.normalize().getService().getFullName() + "/"
                    + methods.normalize().getName());
            normalize.put("validate", true);
            normalize.putArray("rules").add("text = input.content");
            var write = steps.addObject();
            write.put("name", "write");
            write.put("target", fixture);
            write.put("method", methods.write().getService().getFullName() + "/"
                    + methods.write().getName());
            write.put("validate", true);
            write.putArray("rules").add("operation_id = input.operation_id")
                    .add("content = normalize.text");
            return JSON.writeValueAsString(root);
        }
    }

    private record Methods(Descriptors.MethodDescriptor normalize,
            Descriptors.MethodDescriptor write) {}

    private static Methods methods(GetWorkflowAuthorContextResponse context, FileDescriptorSet reflected,
            String fixture)
            throws Exception {
        var pinned = FileDescriptorSet.parseFrom(context.getDescriptorSet());
        Map<String, FileDescriptorProto> originals = files(pinned);
        Map<String, FileDescriptorProto> discovered = files(reflected);
        FileDescriptorProto fixtureFile = originals.values().stream()
                .filter(file -> file.getServiceList().stream().anyMatch(service ->
                        (file.getPackage().isEmpty() ? "" : file.getPackage() + ".")
                                .concat(service.getName()).equals(FIXTURE_SERVICE)))
                .findFirst().orElse(null);
        require(fixtureFile != null, "pinned descriptors omit fixture service");
        compareClosure(fixtureFile.getName(), originals, discovered, new HashSet<>());
        var linked = link(fixtureFile.getName(), originals, new HashMap<>());
        var service = linked.findServiceByName("AuthoringFixtureService");
        require(service != null && service.getFullName().equals(FIXTURE_SERVICE),
                "fixture service descriptor differs");
        var normalize = service.findMethodByName("NormalizeText");
        var write = service.findMethodByName("WriteRecord");
        require(normalize != null && write != null && !normalize.isClientStreaming()
                        && !normalize.isServerStreaming() && !write.isClientStreaming()
                        && !write.isServerStreaming(), "fixture methods are not unary");
        Set<String> calls = new HashSet<>();
        context.getPermittedCallsList().forEach(call -> {
            if (call.getTarget().equals(fixture) && !call.getTls())
                calls.add(call.getMethod());
        });
        require(calls.contains(FIXTURE_SERVICE + "/NormalizeText")
                        && calls.contains(FIXTURE_SERVICE + "/WriteRecord"),
                "fixture calls are not permitted");
        return new Methods(normalize, write);
    }

    private static void probe(ManagedChannel external, Methods methods) throws Exception {
        String original = " \tHello\r\nworld\t ";
        String expected = "Hello\nworld";
        DynamicMessage input = DynamicMessage.newBuilder(methods.normalize().getInputType())
                .setField(methods.normalize().getInputType().findFieldByName("text"), original).build();
        var normalized = DynamicGrpcCalls.call(external, methods.normalize(), input,
                CallOptions.DEFAULT.withDeadlineAfter(RPC_SECONDS, TimeUnit.SECONDS), new Metadata(), 1).getFirst();
        validate(normalized);
        String value = (String) normalized.getField(normalized.getDescriptorForType().findFieldByName("text"));
        require(value.equals(expected), "fixture normalization differs");
        String operationId = UUID.randomUUID().toString();
        DynamicMessage write = DynamicMessage.newBuilder(methods.write().getInputType())
                .setField(methods.write().getInputType().findFieldByName("operation_id"), operationId)
                .setField(methods.write().getInputType().findFieldByName("content"), value).build();
        var result = DynamicGrpcCalls.call(external, methods.write(), write,
                CallOptions.DEFAULT.withDeadlineAfter(RPC_SECONDS, TimeUnit.SECONDS), new Metadata(), 1).getFirst();
        validate(result);
        require(operationId.equals(result.getField(result.getDescriptorForType().findFieldByName("operation_id")))
                && sha(value.getBytes(StandardCharsets.UTF_8)).equals(
                        result.getField(result.getDescriptorForType().findFieldByName("content_sha256"))),
                "fixture record identity or digest differs");
        var retry = DynamicGrpcCalls.call(external, methods.write(), write,
                CallOptions.DEFAULT.withDeadlineAfter(RPC_SECONDS, TimeUnit.SECONDS), new Metadata(), 1).getFirst();
        require(result.equals(retry), "fixture retry response differs");
    }

    private static void verifyPrepared(PrepareWorkflowCandidateResponse response,
            GetWorkflowAuthorContextResponse context, String preparationId, byte[] source) {
        validate(response);
        var binding = response.getBinding();
        require(binding.getTaskId().equals(context.getTaskId())
                        && binding.getAttempt() == context.getAttempt() && binding.getRevision() == 1
                        && binding.getPreparationId().equals(preparationId)
                        && binding.getOfferEntrySha256().equals(context.getOfferEntrySha256())
                        && binding.getSourceSha256().equals(sha(source)),
                "preparation identity differs");
        WorkflowAuthoringDeliverable authored = response.getAuthored();
        validate(authored);
        require(authored.getExecutableSource().getSha256().equals(binding.getSourceSha256()),
                "prepared source differs");
        for (CheckEvidence check : authored.getDeliverable().getChecksList()) {
            require(check.getVerdict() == CheckVerdict.CHECK_VERDICT_PASSED,
                    "prepared check did not pass");
        }
    }

    private static Map<String, FileDescriptorProto> files(FileDescriptorSet set) {
        Map<String, FileDescriptorProto> result = new HashMap<>();
        for (var file : set.getFileList()) {
            require(!file.getName().isBlank() && result.putIfAbsent(file.getName(), file) == null,
                    "descriptor closure has duplicate or empty file names");
        }
        return result;
    }

    private static void compareClosure(String name, Map<String, FileDescriptorProto> pinned,
            Map<String, FileDescriptorProto> reflected, Set<String> seen) {
        if (!seen.add(name)) return;
        var expected = pinned.get(name);
        require(expected != null && expected.equals(reflected.get(name)),
                "reflected fixture descriptors differ from pinned context");
        expected.getDependencyList().forEach(dependency -> compareClosure(dependency, pinned, reflected, seen));
    }

    private static Descriptors.FileDescriptor link(String name, Map<String, FileDescriptorProto> files,
            Map<String, Descriptors.FileDescriptor> linked) throws Descriptors.DescriptorValidationException {
        return link(name, files, linked, new HashSet<>());
    }

    private static Descriptors.FileDescriptor link(String name, Map<String, FileDescriptorProto> files,
            Map<String, Descriptors.FileDescriptor> linked, Set<String> active)
            throws Descriptors.DescriptorValidationException {
        if (linked.containsKey(name)) return linked.get(name);
        require(active.add(name), "descriptor import cycle");
        var file = files.get(name);
        require(file != null, "descriptor import is absent");
        var dependencies = new Descriptors.FileDescriptor[file.getDependencyCount()];
        for (int i = 0; i < dependencies.length; i++) {
            dependencies[i] = link(file.getDependency(i), files, linked, active);
        }
        var resolved = Descriptors.FileDescriptor.buildFrom(file, dependencies);
        linked.put(name, resolved);
        active.remove(name);
        return resolved;
    }

    private static void validate(Message message) {
        rejectUnknown(message);
        var result = ProtoValidator.forMessageType(message.getDescriptorForType()).validate(message);
        require(result.valid(), "invalid protobuf response");
    }

    private static void rejectUnknown(Message message) {
        require(message.getUnknownFields().asMap().isEmpty(), "unknown protobuf fields");
        if (message instanceof Any any && any.is(WorkflowAuthoringDeliverable.class)) {
            try {
                rejectUnknown(any.unpack(WorkflowAuthoringDeliverable.class));
            } catch (com.google.protobuf.InvalidProtocolBufferException malformed) {
                throw new IllegalStateException("malformed authored payload", malformed);
            }
        }
        for (var field : message.getAllFields().entrySet()) {
            if (field.getKey().getJavaType() != FieldDescriptor.JavaType.MESSAGE) continue;
            if (field.getKey().isRepeated()) {
                for (Object nested : (List<?>) field.getValue()) rejectUnknown((Message) nested);
            } else {
                rejectUnknown((Message) field.getValue());
            }
        }
    }

    private static String sha(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (Exception impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) throw new IllegalStateException(message);
    }
}
