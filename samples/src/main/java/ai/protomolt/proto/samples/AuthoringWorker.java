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
import ai.protomolt.proto.samples.authoring.v1.AuthoringWorkerPending;
import ai.protomolt.proto.samples.authoring.v1.AuthoringWorkerState;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringDeliverable;
import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowAuthorContextRequest;
import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowAuthorContextResponse;
import ai.protomolt.proto.workflow.authoring.v1.PrepareWorkflowCandidateRequest;
import ai.protomolt.proto.workflow.authoring.v1.PrepareWorkflowCandidateResponse;
import ai.protomolt.proto.workflow.authoring.v1.ReadWorkflowAuthorEventsRequest;
import ai.protomolt.proto.workflow.authoring.v1.ReadWorkflowAuthorAssignmentsRequest;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowAuthorAssignment;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowAuthorTaskServiceGrpc;
import ai.protomolt.proto.workflow.authoring.v1.EnsureWorkflowAuthorRegistrationRequest;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowPreparationServiceGrpc;
import ai.protomolt.proto.validate.ProtoValidator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
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
import java.nio.file.Path;
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
                    + "<fixture-host:port> <task-uuid> <attempt> <worker-id> OR "
                    + "--discover <coordinator-host:port> <fixture-host:port> <worker-id> <state-dir>");
        }
        String token = System.getenv("PROTOMOLT_AUTHOR_TOKEN");
        if (token == null || token.isBlank()) throw new IllegalArgumentException("author token is required");
        if (args[0].equals("--discover")) {
            new IdleAuthoringWorkerRun(args[1], args[2], args[3], Path.of(args[4]), token).run();
        } else {
            new AuthoringWorkerRun(args[0], args[1], args[2], Integer.parseInt(args[3]), args[4], token).run();
        }
    }

    /** Persistent discovery mode; all authority still comes from the author token. */
    private record IdleAuthoringWorkerRun(String coordinator, String fixture, String workerId,
            Path stateDirectory, String token) {
        void run() throws Exception {
            try (var store = AuthoringWorkerStateStore.open(stateDirectory, coordinator, fixture, workerId)) {
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
                    boolean registrationNeedsEnsure = true;
                    boolean ready = false;
                    while (!Thread.currentThread().isInterrupted()) {
                        boolean changed = false;
                        try {
                            if (registrationNeedsEnsure) {
                                register(tasks);
                                registrationNeedsEnsure = false;
                                if (!ready) {
                                    System.out.println("AuthoringWorker ready");
                                    System.out.flush();
                                    ready = true;
                                }
                            }
                            for (int index = 0; index < store.state().getPendingCount();) {
                                WorkflowAuthorAssignment assignment = store.state().getPending(index).getAssignment();
                                if (process(tasks, preparation, external, store, assignment)) changed = true;
                                if (index < store.state().getPendingCount()
                                        && store.state().getPending(index).getAssignment().equals(assignment)) index++;
                            }
                            changed |= discover(tasks, store);
                        } catch (Exception failure) {
                            if (!retryable(failure)) throw failure;
                            registrationNeedsEnsure = true;
                        }
                        if (!changed) Thread.sleep(150);
                    }
                } finally {
                    external.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
                    control.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
                }
            }
        }

        private void register(WorkflowAuthorTaskServiceGrpc.WorkflowAuthorTaskServiceBlockingStub tasks) {
            var response = tasks.withDeadlineAfter(RPC_SECONDS, TimeUnit.SECONDS)
                    .ensureWorkflowAuthorRegistration(EnsureWorkflowAuthorRegistrationRequest.newBuilder()
                            .setRegistration(RegisterWorkerRequest.newBuilder()
                            .setWorkerId(workerId).setProvider("scripted")
                            .setModel("authoring-worker").setModelVersion("1")
                            .addCapabilities(WorkerCapability.newBuilder().setName("workflow-authoring"))
                            .build()).build());
            validate(response);
            require(response.getRegistration().getOk() && response.getRegistration().getAdmitted()
                    && response.getRegistration().getWorkerId().equals(workerId), "author registration refused");
        }

        private boolean discover(WorkflowAuthorTaskServiceGrpc.WorkflowAuthorTaskServiceBlockingStub tasks,
                AuthoringWorkerStateStore store) throws Exception {
            AuthoringWorkerState before = store.state();
            int capacity = 64 - before.getPendingCount();
            if (capacity == 0) return false;
            var response = tasks.withDeadlineAfter(RPC_SECONDS, TimeUnit.SECONDS)
                    .readWorkflowAuthorAssignments(ReadWorkflowAuthorAssignmentsRequest.newBuilder()
                            .setAfterCursor(before.getDiscoveryCursor()).setMaxAssignments(capacity).build());
            validate(response);
            require(response.getWorkerId().equals(workerId)
                    && response.getAfterCursor() == before.getDiscoveryCursor()
                    && response.getCursor() >= before.getDiscoveryCursor()
                    && response.getCursor() - before.getDiscoveryCursor() <= 256
                    && response.getAssignmentsCount() <= capacity,
                    "author assignment page identity or bound differs");
            require(!response.getTruncated() || response.getCursor() > before.getDiscoveryCursor(),
                    "author assignment page made no progress");
            var next = before.toBuilder().setDiscoveryCursor(response.getCursor());
            long previous = before.getDiscoveryCursor();
            for (WorkflowAuthorAssignment assignment : response.getAssignmentsList()) {
                require(assignment.getCursor() > previous && assignment.getCursor() <= response.getCursor(),
                        "author assignments are not in cursor order");
                previous = assignment.getCursor();
                boolean duplicate = false;
                for (AuthoringWorkerPending existing : next.getPendingList()) {
                    if (existing.getAssignment().getTaskId().equals(assignment.getTaskId())
                            && existing.getAssignment().getAttempt() == assignment.getAttempt()) {
                        require(existing.getAssignment().getOfferEntrySha256()
                                        .equals(assignment.getOfferEntrySha256()),
                                "discovered assignment conflicts with saved original offer");
                        duplicate = true;
                    }
                }
                if (!duplicate) {
                    next.addPending(AuthoringWorkerPending.newBuilder().setAssignment(assignment));
                }
            }
            if (response.getCursor() == before.getDiscoveryCursor() && response.getAssignmentsCount() == 0) return false;
            store.commit(next.build());
            return true;
        }

        private boolean process(WorkflowAuthorTaskServiceGrpc.WorkflowAuthorTaskServiceBlockingStub tasks,
                WorkflowPreparationServiceGrpc.WorkflowPreparationServiceBlockingStub preparation,
                ManagedChannel external, AuthoringWorkerStateStore store,
                WorkflowAuthorAssignment assignment) throws Exception {
            EventResult events = readEvents(tasks, store, assignment);
            if (events == EventResult.RETIRED) return true;
            if (events == EventResult.BLOCKED) return false;
            AuthoringWorkerPending pending = pending(store.state(), assignment);
            if (pending.hasSubmission()) {
                if (pending.getSubmissionCursor() != 0) return false;
                var request = pending.getSubmission();
                var reply = tasks.withDeadlineAfter(RPC_SECONDS, TimeUnit.SECONDS)
                        .submitWorkflowCandidate(request);
                validate(reply);
                require(reply.getOk() && reply.getTaskId().equals(assignment.getTaskId())
                        && reply.getAttempt() == assignment.getAttempt() && reply.getRevision() == 1,
                        "candidate submission retry differs");
                return true;
            }
            GetWorkflowAuthorContextResponse context;
            try {
                context = tasks.withDeadlineAfter(RPC_SECONDS, TimeUnit.SECONDS)
                        .getWorkflowAuthorContext(GetWorkflowAuthorContextRequest.newBuilder()
                                .setTaskId(assignment.getTaskId()).setAttempt(assignment.getAttempt()).build());
            } catch (StatusRuntimeException inactive) {
                if (inactive.getStatus().getCode() == Status.Code.FAILED_PRECONDITION) return false;
                throw inactive;
            }
            validate(context);
            require(context.getTaskId().equals(assignment.getTaskId())
                            && context.getAttempt() == assignment.getAttempt()
                            && context.getOfferEntry().getWorkerId().equals(workerId)
                            && context.getOfferEntrySha256().equals(assignment.getOfferEntrySha256())
                            && deterministicSha(context.getOfferEntry()).equals(assignment.getOfferEntrySha256()),
                    "author context differs from discovered original offer");
            var accepted = tasks.withDeadlineAfter(RPC_SECONDS, TimeUnit.SECONDS)
                    .acceptWorkflowTask(AcceptTaskRequest.newBuilder().setWorkerId(workerId)
                            .setTaskId(assignment.getTaskId()).setAttempt(assignment.getAttempt()).build());
            validate(accepted);
            require(accepted.getOk() && accepted.getTaskId().equals(assignment.getTaskId())
                    && accepted.getAttempt() == assignment.getAttempt(), "task acceptance differs");

            var reflected = ReflectionClient.discover(external, 10_000);
            Methods methods = methods(context, reflected.descriptorSet(), fixture);
            if (!pending.hasProbe()) {
                String normalized = normalize(external, methods);
                require(normalized.equals(AuthoringWorkerStateStore.PROBE_CONTENT),
                        "fixture normalization differs");
                var probe = WriteRecordRequest.newBuilder()
                        .setOperationId(AuthoringWorkerStateStore.probeOperationId(assignment, workerId))
                        .setContent(normalized).build();
                store.commit(replace(store.state(), assignment,
                        pending.toBuilder().setProbe(probe).build()));
                pending = pending(store.state(), assignment);
            }
            writeProbe(external, methods, pending.getProbe());
            if (!pending.hasPreparation()) {
                byte[] source = new AuthoringWorkerRun(coordinator, fixture, assignment.getTaskId(),
                        assignment.getAttempt(), workerId, token).source(context, methods)
                        .getBytes(StandardCharsets.UTF_8);
                var request = PrepareWorkflowCandidateRequest.newBuilder()
                        .setTaskId(assignment.getTaskId()).setAttempt(assignment.getAttempt())
                        .setRevision(1)
                        .setPreparationId(AuthoringWorkerStateStore.preparationId(assignment, workerId))
                        .setExecutableSourceJson(ByteString.copyFrom(source)).build();
                store.commit(replace(store.state(), assignment,
                        pending.toBuilder().setPreparation(request).build()));
                pending = pending(store.state(), assignment);
            }
            PrepareWorkflowCandidateRequest intent = pending.getPreparation();
            var prepared = preparation.withDeadlineAfter(60, TimeUnit.SECONDS)
                    .prepareWorkflowCandidate(intent);
            verifyPrepared(prepared, context, intent.getPreparationId(),
                    intent.getExecutableSourceJson().toByteArray());
            var authored = prepared.getAuthored();
            var deliverable = authored.getDeliverable();
            var candidate = CompletionCandidate.newBuilder().setAttempt(assignment.getAttempt()).setRevision(1)
                    .setSummary("Scripted fixture probe and coordinator-observed preparation completed")
                    .addAllEvidence(deliverable.getChecksList())
                    .addArtifacts(deliverable.getWorkflowArtifact())
                    .addArtifacts(authored.getExecutableSource())
                    .addArtifacts(deliverable.getReceipt())
                    .setResult(Any.pack(authored)).build();
            var submission = SubmitCandidateRequest.newBuilder().setWorkerId(workerId)
                    .setTaskId(assignment.getTaskId()).setCandidate(candidate).build();
            store.commit(replace(store.state(), assignment,
                    pending.toBuilder().setSubmission(submission).build()));
            var submitted = tasks.withDeadlineAfter(RPC_SECONDS, TimeUnit.SECONDS)
                    .submitWorkflowCandidate(submission);
            validate(submitted);
            require(submitted.getOk() && submitted.getTaskId().equals(assignment.getTaskId())
                    && submitted.getAttempt() == assignment.getAttempt() && submitted.getRevision() == 1,
                    "candidate submission differs");
            return true;
        }

        private EventResult readEvents(WorkflowAuthorTaskServiceGrpc.WorkflowAuthorTaskServiceBlockingStub tasks,
                AuthoringWorkerStateStore store, WorkflowAuthorAssignment assignment) throws Exception {
            AuthoringWorkerPending original = pending(store.state(), assignment);
            // Without an exact local submission intent, a prior candidate must
            // remain visible even if an earlier page advanced the saved cursor.
            long cursor = original.hasSubmission() ? original.getReviewCursor() : 0;
            long observedSubmission = original.getSubmissionCursor();
            boolean blocked = false;
            boolean sawUnknownCandidate = false;
            while (true) {
                var response = tasks.withDeadlineAfter(RPC_SECONDS, TimeUnit.SECONDS)
                        .readWorkflowAuthorEvents(ReadWorkflowAuthorEventsRequest.newBuilder()
                                .setTaskId(assignment.getTaskId()).setAttempt(assignment.getAttempt())
                                .setAfterCursor(cursor).setMaxEvents(32).build());
                validate(response);
                require(response.getTaskId().equals(assignment.getTaskId())
                                && response.getAttempt() == assignment.getAttempt()
                                && response.getAfterCursor() == cursor && response.getCursor() >= cursor
                                && response.getCursor() - cursor <= 256,
                        "author event page identity or bound differs");
                long previous = cursor;
                for (var observed : response.getEventsList()) {
                    require(observed.getCursor() > previous && observed.getCursor() <= response.getCursor(),
                            "author events are not in cursor order");
                    previous = observed.getCursor();
                    var entry = observed.getEntry();
                    require(entry.getWorkerId().equals(workerId), "author event belongs to another worker");
                    if (entry.hasWorkerFrame()) {
                        var frame = entry.getWorkerFrame();
                        require(frame.getTaskId().equals(assignment.getTaskId()),
                                "author event belongs to another task");
                        if (frame.hasCompletion() && frame.getCompletion().getAttempt() == assignment.getAttempt()) {
                            if (!original.hasSubmission()) {
                                sawUnknownCandidate = true;
                            } else if (frame.getCompletion().equals(original.getSubmission().getCandidate())) {
                                observedSubmission = observed.getCursor();
                            } else {
                                throw new IllegalStateException("recorded candidate differs from saved submission");
                            }
                        }
                        if (frame.hasCancelled() && frame.getCancelled().getAttempt() == assignment.getAttempt()) {
                            store.commit(remove(store.state(), assignment));
                            return EventResult.RETIRED;
                        }
                    } else if (entry.hasCoordinatorFrame()) {
                        var frame = entry.getCoordinatorFrame();
                        require(frame.getTaskId().equals(assignment.getTaskId()),
                                "author event belongs to another task");
                        if (frame.hasAccepted() && frame.getAccepted().getAttempt() == assignment.getAttempt()) {
                            require(!original.hasSubmission() || (observedSubmission != 0
                                            && frame.getAccepted().getRevision() == 1),
                                    "accepted review lacks exact saved candidate");
                            store.commit(remove(store.state(), assignment));
                            System.out.println("AuthoringWorker accepted task=" + assignment.getTaskId()
                                    + " attempt=" + assignment.getAttempt());
                            System.out.flush();
                            return EventResult.RETIRED;
                        }
                        if ((frame.hasCancellation() && frame.getCancellation().getAttempt() == assignment.getAttempt())
                                || (frame.hasExpired() && frame.getExpired().getAttempt() == assignment.getAttempt())) {
                            store.commit(remove(store.state(), assignment));
                            return EventResult.RETIRED;
                        }
                        if (frame.hasRevisionRequested()
                                && frame.getRevisionRequested().getAttempt() == assignment.getAttempt()) {
                            blocked = true;
                        }
                    }
                }
                if (!blocked && !sawUnknownCandidate
                        && response.getCursor() > pending(store.state(), assignment).getReviewCursor()) {
                    var current = pending(store.state(), assignment);
                    store.commit(replace(store.state(), assignment,
                            current.toBuilder().setReviewCursor(response.getCursor())
                                    .setSubmissionCursor(observedSubmission).build()));
                }
                if (!response.getTruncated()) {
                    if (blocked) {
                        System.err.println("AuthoringWorker revision requested task="
                                + assignment.getTaskId() + " attempt=" + assignment.getAttempt());
                    }
                    return blocked || sawUnknownCandidate ? EventResult.BLOCKED : EventResult.CURRENT;
                }
                require(response.getCursor() > cursor, "author event page made no progress");
                cursor = response.getCursor();
            }
        }

        private String normalize(ManagedChannel external, Methods methods) throws Exception {
            DynamicMessage input = DynamicMessage.newBuilder(methods.normalize().getInputType())
                    .setField(methods.normalize().getInputType().findFieldByName("text"),
                            " \tHello\r\nworld\t ").build();
            var output = DynamicGrpcCalls.call(external, methods.normalize(), input,
                    CallOptions.DEFAULT.withDeadlineAfter(RPC_SECONDS, TimeUnit.SECONDS),
                    new Metadata(), 1).getFirst();
            validate(output);
            return (String) output.getField(output.getDescriptorForType().findFieldByName("text"));
        }

        private void writeProbe(ManagedChannel external, Methods methods, WriteRecordRequest intent)
                throws Exception {
            DynamicMessage input = DynamicMessage.newBuilder(methods.write().getInputType())
                    .setField(methods.write().getInputType().findFieldByName("operation_id"),
                            intent.getOperationId())
                    .setField(methods.write().getInputType().findFieldByName("content"),
                            intent.getContent()).build();
            var output = DynamicGrpcCalls.call(external, methods.write(), input,
                    CallOptions.DEFAULT.withDeadlineAfter(RPC_SECONDS, TimeUnit.SECONDS),
                    new Metadata(), 1).getFirst();
            validate(output);
            require(intent.getOperationId().equals(output.getField(
                            output.getDescriptorForType().findFieldByName("operation_id")))
                            && sha(intent.getContentBytes().toByteArray()).equals(output.getField(
                            output.getDescriptorForType().findFieldByName("content_sha256"))),
                    "fixture record identity or digest differs");
        }

        private static AuthoringWorkerPending pending(AuthoringWorkerState state,
                WorkflowAuthorAssignment assignment) {
            return state.getPendingList().stream().filter(item -> item.getAssignment().equals(assignment))
                    .findFirst().orElseThrow(() -> new IllegalStateException("saved assignment disappeared"));
        }

        private static AuthoringWorkerState replace(AuthoringWorkerState state,
                WorkflowAuthorAssignment assignment, AuthoringWorkerPending replacement) {
            var updated = state.toBuilder();
            for (int index = 0; index < state.getPendingCount(); index++) {
                if (state.getPending(index).getAssignment().equals(assignment)) {
                    return updated.setPending(index, replacement).build();
                }
            }
            throw new IllegalStateException("saved assignment disappeared");
        }

        private static AuthoringWorkerState remove(AuthoringWorkerState state,
                WorkflowAuthorAssignment assignment) {
            var updated = state.toBuilder();
            for (int index = 0; index < state.getPendingCount(); index++) {
                if (state.getPending(index).getAssignment().equals(assignment)) {
                    return updated.removePending(index).build();
                }
            }
            throw new IllegalStateException("saved assignment disappeared");
        }

        private static boolean retryable(Throwable failure) {
            for (Throwable current = failure; current != null; current = current.getCause()) {
                if (current instanceof StatusRuntimeException transport) {
                    var code = transport.getStatus().getCode();
                    return code == Status.Code.UNAVAILABLE || code == Status.Code.DEADLINE_EXCEEDED
                            || code == Status.Code.ABORTED;
                }
            }
            return false;
        }

        private enum EventResult { CURRENT, BLOCKED, RETIRED }
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

    private static String deterministicSha(Message message) {
        byte[] bytes = new byte[message.getSerializedSize()];
        CodedOutputStream output = CodedOutputStream.newInstance(bytes);
        output.useDeterministicSerialization();
        try {
            message.writeTo(output);
            output.checkNoSpaceLeft();
        } catch (java.io.IOException failure) {
            throw new IllegalStateException("could not serialize author offer", failure);
        }
        return sha(bytes);
    }

    private static void require(boolean condition, String message) {
        if (!condition) throw new IllegalStateException(message);
    }
}
