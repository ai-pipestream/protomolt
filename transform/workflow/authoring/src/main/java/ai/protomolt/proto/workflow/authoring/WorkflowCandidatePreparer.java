package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.actions.CatalogContract;
import ai.protomolt.proto.actions.Fields;
import ai.protomolt.proto.delegation.TranscriptRepository;
import ai.protomolt.proto.delegation.v1.CheckEvidence;
import ai.protomolt.proto.delegation.v1.CheckVerdict;
import ai.protomolt.proto.grpc.workflow.ArtifactRepository;
import ai.protomolt.proto.grpc.workflow.RunEvidenceRepository;
import ai.protomolt.proto.grpc.workflow.WorkflowValidation;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.grpc.workflow.v1.RunEvidence;
import ai.protomolt.proto.grpc.workflow.v1.RunStatus;
import ai.protomolt.proto.receipt.TrustSnapshot;
import ai.protomolt.proto.receipt.WorkRecords;
import ai.protomolt.proto.meta.SensitivityMasker;
import ai.protomolt.proto.samples.starter.v1.WorkflowAcceptanceFixture;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringDeliverable;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringPolicy;
import ai.protomolt.proto.samples.starter.v1.WorkflowDeliverable;
import ai.protomolt.proto.workflow.CompiledWorkflow;
import ai.protomolt.proto.workflow.RecordSigning;
import ai.protomolt.proto.workflow.WorkRecordProjector;
import ai.protomolt.proto.workflow.WorkflowAuthoringFixtures;
import ai.protomolt.proto.workflow.WorkflowAuthoringPreflight;
import ai.protomolt.proto.workflow.WorkflowAuthoringReceipt;
import ai.protomolt.proto.workflow.WorkflowCompiler;
import ai.protomolt.proto.workflow.WorkflowJson;
import ai.protomolt.proto.workflow.WorkflowReplay;
import ai.protomolt.proto.workflow.WorkflowRunRecorder;
import ai.protomolt.proto.workflow.WorkflowRunner;
import ai.protomolt.proto.workflow.authoring.v1.PrepareWorkflowCandidateRequest;
import ai.protomolt.proto.workflow.authoring.v1.PrepareWorkflowCandidateResponse;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowPreparationFailure;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowPreparationFailureReason;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowPreparationIntent;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowPreparationRecord;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import io.grpc.Context;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/** Coordinator-owned, bounded preparation of a candidate; never submits or accepts it. */
public final class WorkflowCandidatePreparer {
    /** Additional trusted, starter-specific source restriction applied before reservation. */
    @FunctionalInterface
    public interface SourceTemplate {
        void verify(CompiledWorkflow workflow, String exactSourceJson, WorkflowAuthoringPolicy policy);
    }

    private final TranscriptRepository transcripts;
    private final WorkflowPreparationRepository ledger;
    private final ArtifactRepository artifacts;
    private final RunEvidenceRepository runs;
    private final WorkflowRunner runner;
    private final ActionContext actions;
    private final ArtifactReference policyReference;
    private final Supplier<TrustSnapshot> trust;
    private final RecordSigning signing;
    private final Clock clock;
    private final SourceTemplate template;

    public WorkflowCandidatePreparer(TranscriptRepository transcripts,
            WorkflowPreparationRepository ledger, ArtifactRepository artifacts,
            RunEvidenceRepository runs, WorkflowRunner runner, ActionContext actions,
            ArtifactReference policyReference, Supplier<TrustSnapshot> trust,
            RecordSigning signing, Clock clock, SourceTemplate template) {
        this.transcripts = Objects.requireNonNull(transcripts);
        this.ledger = Objects.requireNonNull(ledger);
        this.artifacts = Objects.requireNonNull(artifacts);
        this.runs = Objects.requireNonNull(runs);
        this.runner = Objects.requireNonNull(runner);
        this.actions = Objects.requireNonNull(actions);
        this.policyReference = Objects.requireNonNull(policyReference);
        this.trust = Objects.requireNonNull(trust);
        this.signing = Objects.requireNonNull(signing);
        this.clock = Objects.requireNonNull(clock);
        this.template = Objects.requireNonNull(template);
        // Opt-in preparation must refuse startup if its current signing identity
        // cannot produce a receipt accepted by the configured verifier trust.
        WorkflowPreparationSigning.verify(this.signing, this.trust.get(), this.clock);
    }

    public PrepareWorkflowCandidateResponse prepare(PrepareWorkflowCandidateRequest request, Caller caller)
            throws WorkflowPreparationException {
        try {
            Objects.requireNonNull(request, "request");
            Objects.requireNonNull(caller, "caller");
            WorkflowPreparationValidation.validate(request, 2 * 1024 * 1024);
            return ledger.withExclusiveIntent(request.getTaskId(), request.getAttempt(),
                    request.getRevision(), session -> prepareLocked(request, caller, session));
        } catch (WorkflowPreparationException failure) {
            throw failure;
        } catch (WorkflowPreparationConflictException conflict) {
            throw error(WorkflowPreparationException.Kind.CONFLICT, "workflow-preparation-conflict", conflict);
        } catch (SecurityException denied) {
            throw error(WorkflowPreparationException.Kind.PERMISSION_DENIED,
                    "workflow-preparation-holder-mismatch", denied);
        } catch (IllegalArgumentException invalid) {
            throw error(WorkflowPreparationException.Kind.INVALID_INPUT, "invalid-workflow-preparation", invalid);
        } catch (IOException storage) {
            throw error(WorkflowPreparationException.Kind.CORRUPT_EVIDENCE,
                    "workflow-preparation-storage-failed", storage);
        } catch (Exception other) {
            throw error(WorkflowPreparationException.Kind.UNAVAILABLE,
                    "workflow-preparation-unavailable", other);
        }
    }

    private PrepareWorkflowCandidateResponse prepareLocked(PrepareWorkflowCandidateRequest request,
            Caller caller, WorkflowPreparationRepository.Session session) throws Exception {
        requireActiveCaller();
        WorkflowPreparationRecord existing = session.current().orElse(null);
        var mode = existing != null && existing.hasCompleted()
                ? WorkflowPreparationAdmission.Mode.REPLAY_COMPLETED
                : WorkflowPreparationAdmission.Mode.EXECUTE;
        var admitted = inspect(request, caller, mode);
        if (existing != null) {
            if (!existing.getIntent().getRequest().equals(request)
                    || !existing.getIntent().getBinding().equals(admitted.binding())
                    || !existing.getIntent().getSelectedOffer().equals(admitted.selectedOffer())
                    || !existing.getIntent().getPolicy().equals(policyReference)) {
                throw error(WorkflowPreparationException.Kind.CONFLICT,
                        "workflow-preparation-conflict", null);
            }
            if (existing.hasFailed()) throw terminal(existing.getFailed(), null);
        }

        // The configured policy, exact source and all fixture bytes are admitted before
        // a new intent is reserved. Content-addressed preflight artifacts may remain orphaned.
        byte[] policyBytes;
        WorkflowAuthoringPolicy policy;
        try {
            policyBytes = resolve(policyReference, 4 * 1024 * 1024);
            policy = WorkflowAuthoringPolicy.parseFrom(policyBytes);
            WorkflowPreparationValidation.validate(policy, 4 * 1024 * 1024);
        } catch (IllegalArgumentException | IOException invalidPolicy) {
            throw error(WorkflowPreparationException.Kind.CORRUPT_EVIDENCE,
                    "workflow-preparation-policy-unavailable", invalidPolicy);
        }
        byte[] pinnedDescriptors = pinnedDescriptors(policy);
        String sourceJson = strictSource(request);
        ObjectNode source;
        try {
            var parsed = actions.objectMapper().readTree(sourceJson);
            if (!(parsed instanceof ObjectNode object)) throw new IllegalArgumentException("source must be a JSON object");
            source = object;
        } catch (IOException parse) {
            throw error(WorkflowPreparationException.Kind.INVALID_INPUT, "invalid-workflow-source", parse);
        }
        CompiledWorkflow parsed;
        try {
            Message sourceMessage = CatalogContract.read(source,
                    CatalogContract.request("CompiledWorkflow"), "workflow source");
            CatalogContract.validate(sourceMessage, sourceMessage.getDescriptorForType(), "workflow source");
            Message schema = Fields.message(sourceMessage, "schema");
            if (!Fields.string(schema, "type").isEmpty()
                    || !Fields.map(schema, "sources").isEmpty()
                    || !Fields.string(schema, "root").isEmpty()
                    || Fields.string(schema, "descriptorSetBase64").isEmpty()
                    || !Arrays.equals(java.util.Base64.getDecoder().decode(
                            Fields.string(schema, "descriptorSetBase64")), pinnedDescriptors)) {
                throw new IllegalArgumentException("source schema differs from pinned descriptors");
            }
            parsed = WorkflowJson.parse(sourceMessage, actions);
        } catch (WorkflowJson.WorkflowParseException | ai.protomolt.proto.actions.ActionException parse) {
            throw error(WorkflowPreparationException.Kind.INVALID_INPUT, "invalid-workflow-source", parse);
        }
        var durable = WorkflowCompiler.compile(parsed);
        ArtifactReference sourceRef = existing != null && existing.hasCompleted()
                ? existing.getCompleted().getAuthored().getExecutableSource()
                : artifacts.save(request.getExecutableSourceJson().toByteArray(), "application/json", false);
        ArtifactReference workflowRef = existing != null && existing.hasCompleted()
                ? existing.getCompleted().getAuthored().getDeliverable().getWorkflowArtifact()
                : artifacts.save(durable.toByteArray(), "application/x-protobuf", false);
        WorkflowAuthoringPreflight.Result preflight;
        try {
            preflight = WorkflowAuthoringPreflight.verify(durable, workflowRef, sourceRef,
                    policy.getDescriptors(), new WorkflowAuthoringPreflight.Policy(policy.getDescriptors(),
                            policy.getPermittedCallsList().stream().map(call ->
                                    new WorkflowAuthoringPreflight.PermittedCall(call.getTarget(),
                                            call.getMethod(), call.getTls())).toList()), actions, artifacts);
        } catch (IllegalArgumentException | IOException sourceFailure) {
            // A configured descriptor artifact changing or disappearing is not
            // repairable by the worker's source request.
            if (!Arrays.equals(pinnedDescriptors, pinnedDescriptors(policy))) {
                throw error(WorkflowPreparationException.Kind.CORRUPT_EVIDENCE,
                        "workflow-preparation-descriptors-changed", sourceFailure);
            }
            throw sourceFailure;
        }
        template.verify(preflight.workflow(), sourceJson, policy);
        var fixtures = policy.getFixturesList().stream().map(WorkflowCandidatePreparer::fixture).toList();
        var checkedFixtures = WorkflowAuthoringFixtures.admit(preflight, fixtures, fixtures, artifacts);
        DynamicMessage firstInput = firstInput(policy, preflight.workflow());
        requireRecordable(firstInput);
        var outputType = preflight.workflow().output() != null ? preflight.workflow().output().type()
                : preflight.workflow().steps().getLast().method().getOutputType();
        requireRecordable(DynamicMessage.parseFrom(outputType,
                resolve(policy.getFixtures(0).getExpectedOutput(), 4 * 1024 * 1024)));
        WorkflowPreparationSigning.verify(signing, requiredTrust(), clock);
        String runId = "prepare-" + request.getPreparationId();
        var intent = WorkflowPreparationIntent.newBuilder().setVersion(1).setRequest(request)
                .setBinding(admitted.binding()).setHolder(caller.name())
                .setSelectedOffer(admitted.selectedOffer()).setPolicy(policyReference)
                .setPolicyProto(ByteString.copyFrom(policyBytes)).setRunId(runId).build();
        WorkflowPreparationValidation.validateIntent(intent);
        requireActiveCaller();
        if (existing == null) existing = session.reserveOrMatch(intent);
        else if (!existing.getIntent().equals(intent)) {
            throw error(WorkflowPreparationException.Kind.CONFLICT, "workflow-preparation-conflict", null);
        }

        if (existing.hasCompleted()) {
            verifyCompleted(existing.getCompleted(), intent, policy, preflight, durable, workflowRef, sourceRef);
            inspectSame(request, caller, WorkflowPreparationAdmission.Mode.REPLAY_COMPLETED, admitted);
            return existing.getCompleted();
        }
        if (!existing.hasPending()) throw terminal(existing.getFailed(), null);

        // A reserved intent must finish or remain retryable when its caller disappears.
        // Keep context values but let the runner's own call and workflow budgets govern
        // fixture and recorded-run RPCs. Reassignment is still checked by inspectSame.
        Context detachedContext = Context.current().fork();
        Context previous = detachedContext.attach();
        try {
            RunEvidence run = runs.find(runId).orElse(null);
            if (run != null) {
                verifyRunIdentity(run, intent, durable, preflight, firstInput);
                if (run.getStatus() != RunStatus.RUN_STATUS_SUCCEEDED) {
                    if (run.getStatus() != RunStatus.RUN_STATUS_FAILED) {
                        throw error(WorkflowPreparationException.Kind.CORRUPT_EVIDENCE,
                                "workflow-preparation-run-status-invalid", null);
                    }
                    throw terminal(session.fail(failure(intent,
                            WorkflowPreparationFailureReason.WORKFLOW_PREPARATION_FAILURE_REASON_RECORDED_RUN_FAILED))
                            .getFailed(), null);
                }
                verifyRun(run, intent, durable, preflight, firstInput);
            }

            if (run == null) {
                inspectSame(request, caller, WorkflowPreparationAdmission.Mode.EXECUTE, admitted);
                try {
                    WorkflowAuthoringFixtures.execute(checkedFixtures, artifacts, runner);
                } catch (WorkflowRunner.WorkflowExecutionException failure) {
                    if (failure.kind() == WorkflowRunner.FailureKind.GRPC
                            || failure.kind() == WorkflowRunner.FailureKind.DEADLINE) {
                        throw transport(failure);
                    }
                    throw terminal(session.fail(failure(intent,
                            WorkflowPreparationFailureReason.WORKFLOW_PREPARATION_FAILURE_REASON_FIXTURE_REJECTED))
                            .getFailed(), failure);
                } catch (IllegalArgumentException rejected) {
                    throw terminal(session.fail(failure(intent,
                            WorkflowPreparationFailureReason.WORKFLOW_PREPARATION_FAILURE_REASON_FIXTURE_REJECTED))
                            .getFailed(), rejected);
                }
            }
            if (run == null) {
                inspectSame(request, caller, WorkflowPreparationAdmission.Mode.EXECUTE, admitted);
                try {
                    run = new WorkflowRunRecorder(runner, artifacts, runs)
                            .record(runId, null, preflight.workflow(), firstInput);
                } catch (WorkflowRunner.WorkflowExecutionException | IOException recordingFailure) {
                    run = runs.find(runId).orElse(null);
                    if (run == null) {
                        if (recordingFailure instanceof WorkflowRunner.WorkflowExecutionException execution) {
                            throw transport(execution);
                        }
                        throw error(WorkflowPreparationException.Kind.UNAVAILABLE,
                                "workflow-preparation-recording-uncertain", recordingFailure);
                    }
                    verifyRunIdentity(run, intent, durable, preflight, firstInput);
                    if (run.getStatus() != RunStatus.RUN_STATUS_SUCCEEDED) {
                        if (run.getStatus() != RunStatus.RUN_STATUS_FAILED) {
                            throw error(WorkflowPreparationException.Kind.CORRUPT_EVIDENCE,
                                    "workflow-preparation-run-status-invalid", null);
                        }
                        throw terminal(session.fail(failure(intent,
                                WorkflowPreparationFailureReason.WORKFLOW_PREPARATION_FAILURE_REASON_RECORDED_RUN_FAILED))
                                .getFailed(), recordingFailure);
                    }
                }
            }
            verifyRun(run, intent, durable, preflight, firstInput);
            try {
                verifyRecordedOutput(run, policy, preflight.workflow());
            } catch (WorkflowPreparationException mismatch) {
                if (mismatch.kind() != WorkflowPreparationException.Kind.INVALID_UPSTREAM) throw mismatch;
                throw terminal(session.fail(failure(intent,
                        WorkflowPreparationFailureReason.WORKFLOW_PREPARATION_FAILURE_REASON_FIXTURE_REJECTED))
                        .getFailed(), mismatch);
            }
            inspectSame(request, caller, WorkflowPreparationAdmission.Mode.EXECUTE, admitted);
            WorkflowPreparationSigning.verify(signing, requiredTrust(), clock);
            var receipt = sign(run);
            var response = response(intent, policy, durable, workflowRef, sourceRef, receipt, run);
            WorkflowAuthoringReceipt.verify(receipt, run, WorkflowValidation.fingerprint(durable),
                    requiredTrust(), artifacts);
            try {
                WorkflowPreparationValidation.validateResponse(response);
            } catch (IllegalArgumentException invalid) {
                throw error(WorkflowPreparationException.Kind.INVALID_UPSTREAM,
                        "invalid-workflow-preparation-response", invalid);
            }
            inspectSame(request, caller, WorkflowPreparationAdmission.Mode.EXECUTE, admitted);
            var completed = session.complete(response);
            inspectSame(request, caller, WorkflowPreparationAdmission.Mode.REPLAY_COMPLETED, admitted);
            return completed.getCompleted();
        } finally {
            detachedContext.detach(previous);
        }
    }

    private static void requireActiveCaller() throws WorkflowPreparationException {
        Context caller = Context.current();
        if (!caller.isCancelled()) return;
        boolean deadline = caller.getDeadline() != null && caller.getDeadline().isExpired();
        throw error(deadline ? WorkflowPreparationException.Kind.DEADLINE
                        : WorkflowPreparationException.Kind.UNAVAILABLE,
                deadline ? "workflow-preparation-deadline" : "workflow-preparation-unavailable", null);
    }

    private WorkflowPreparationAdmission.Snapshot inspect(PrepareWorkflowCandidateRequest request,
            Caller caller, WorkflowPreparationAdmission.Mode mode) throws WorkflowPreparationException {
        try {
            return WorkflowPreparationAdmission.inspect(transcripts, request, caller, clock, mode, policyReference);
        } catch (SecurityException denied) {
            throw error(WorkflowPreparationException.Kind.PERMISSION_DENIED,
                    "workflow-preparation-holder-mismatch", denied);
        } catch (IllegalArgumentException inactive) {
            throw error(WorkflowPreparationException.Kind.INACTIVE,
                    "workflow-preparation-inactive", inactive);
        }
    }

    private void inspectSame(PrepareWorkflowCandidateRequest request, Caller caller,
            WorkflowPreparationAdmission.Mode mode, WorkflowPreparationAdmission.Snapshot prior)
            throws WorkflowPreparationException {
        var latest = inspect(request, caller, mode);
        if (!latest.binding().equals(prior.binding())
                || !latest.selectedOffer().equals(prior.selectedOffer())) {
            throw error(WorkflowPreparationException.Kind.INACTIVE,
                    "workflow-preparation-offer-changed", null);
        }
    }

    private void verifyCompleted(PrepareWorkflowCandidateResponse response,
            WorkflowPreparationIntent intent, WorkflowAuthoringPolicy policy,
            WorkflowAuthoringPreflight.Result preflight, ai.protomolt.proto.grpc.workflow.v1.Workflow durable,
            ArtifactReference workflowRef, ArtifactReference sourceRef) throws Exception {
        try {
            WorkflowPreparationValidation.validateResponse(response);
            var authored = response.getAuthored();
            var deliverable = authored.getDeliverable();
            if (!response.getBinding().equals(intent.getBinding())
                    || !deliverable.getWorkflow().equals(durable)
                    || !deliverable.getWorkflowArtifact().equals(workflowRef)
                    || !deliverable.getDescriptors().equals(policy.getDescriptors())
                    || !authored.getExecutableSource().equals(sourceRef)
                    || !authored.getAcceptanceFixturesList().equals(policy.getFixturesList())
                    || !deliverable.getRunId().equals(intent.getRunId())) {
                throw new IllegalArgumentException("completed response differs from reserved source or policy");
            }
            RunEvidence run = runs.find(intent.getRunId()).orElseThrow(() ->
                    new IllegalArgumentException("completed run is missing"));
            verifyRun(run, intent, durable, preflight, firstInput(policy, preflight.workflow()));
            verifyRecordedOutput(run, policy, preflight.workflow());
            WorkflowAuthoringReceipt.verify(deliverable.getReceipt(), run,
                    WorkflowValidation.fingerprint(durable), requiredTrust(), artifacts);
        } catch (IllegalArgumentException invalid) {
            throw error(WorkflowPreparationException.Kind.CORRUPT_EVIDENCE,
                    "workflow-preparation-evidence-corrupt", invalid);
        }
    }

    private void verifyRun(RunEvidence run, WorkflowPreparationIntent intent,
            ai.protomolt.proto.grpc.workflow.v1.Workflow durable,
            WorkflowAuthoringPreflight.Result preflight, DynamicMessage firstInput) throws Exception {
        verifyRunIdentity(run, intent, durable, preflight, firstInput);
        if (run.getStatus() != RunStatus.RUN_STATUS_SUCCEEDED || !run.hasOutputArtifact()) {
            throw error(WorkflowPreparationException.Kind.CORRUPT_EVIDENCE,
                    "workflow-preparation-run-mismatch", null);
        }
        try {
            var replay = WorkflowReplay.replay(durable, run, preflight.workflow().files(), artifacts);
            if (!replay.ok()) throw error(WorkflowPreparationException.Kind.CORRUPT_EVIDENCE,
                    "workflow-preparation-run-replay-failed", null);
        } catch (IllegalArgumentException | IOException corrupt) {
            throw error(WorkflowPreparationException.Kind.CORRUPT_EVIDENCE,
                    "workflow-preparation-run-replay-failed", corrupt);
        }
    }

    private void verifyRunIdentity(RunEvidence run, WorkflowPreparationIntent intent,
            ai.protomolt.proto.grpc.workflow.v1.Workflow durable,
            WorkflowAuthoringPreflight.Result preflight, DynamicMessage firstInput)
            throws WorkflowPreparationException {
        try {
            WorkflowValidation.validate(run);
            if (!run.getRunId().equals(intent.getRunId())
                    || !run.getWorkflowFingerprint().equals(WorkflowValidation.fingerprint(durable))
                    || !run.getWorkflowName().equals(durable.getName())
                    || !run.getWorkflowVersion().isEmpty()
                    || !run.getDependenciesList().equals(durable.getDependenciesList())) {
                throw new IllegalArgumentException("run identity differs from reserved workflow");
            }
            var recordedInput = DynamicMessage.parseFrom(preflight.workflow().inputType(),
                    resolve(run.getInputArtifact(), 4 * 1024 * 1024));
            if (!recordedInput.equals(firstInput)) {
                throw new IllegalArgumentException("run input differs from caller fixture");
            }
            if (run.hasOutputArtifact()) resolve(run.getOutputArtifact(), 4 * 1024 * 1024);
            for (var step : run.getStepsList()) {
                if (step.hasRequestArtifact()) resolve(step.getRequestArtifact(), 4 * 1024 * 1024);
                if (step.hasResponseArtifact()) resolve(step.getResponseArtifact(), 4 * 1024 * 1024);
            }
        } catch (IllegalArgumentException | IOException corrupt) {
            throw error(WorkflowPreparationException.Kind.CORRUPT_EVIDENCE,
                    "workflow-preparation-run-evidence-corrupt", corrupt);
        }
    }

    private void verifyRecordedOutput(RunEvidence run, WorkflowAuthoringPolicy policy,
            CompiledWorkflow workflow) throws Exception {
        try {
            var outputType = workflow.output() != null ? workflow.output().type()
                    : workflow.steps().getLast().method().getOutputType();
            var expected = DynamicMessage.parseFrom(outputType,
                    resolve(policy.getFixtures(0).getExpectedOutput(), 4 * 1024 * 1024));
            requireRecordable(expected);
            var actual = DynamicMessage.parseFrom(outputType,
                    resolve(run.getOutputArtifact(), 4 * 1024 * 1024));
            if (!actual.equals(expected)) {
                throw error(WorkflowPreparationException.Kind.INVALID_UPSTREAM,
                        "recorded-workflow-output-differs-from-caller-fixture", null);
            }
        } catch (IllegalArgumentException | IOException corrupt) {
            throw error(WorkflowPreparationException.Kind.CORRUPT_EVIDENCE,
                    "workflow-preparation-output-evidence-corrupt", corrupt);
        }
    }

    private ArtifactReference sign(RunEvidence run) throws IOException {
        var now = clock.instant();
        var issuance = new WorkRecordProjector.Issuance("receipt-" + run.getRunId(),
                signing.issuer(), signing.signer().keyId(), timestamp(now), "");
        var signed = signing.signer().sign(WorkRecordProjector.project(run, issuance));
        return artifacts.save(signed.toByteArray(), "application/x-protobuf", false);
    }

    private PrepareWorkflowCandidateResponse response(WorkflowPreparationIntent intent,
            WorkflowAuthoringPolicy policy, ai.protomolt.proto.grpc.workflow.v1.Workflow durable,
            ArtifactReference workflowRef, ArtifactReference sourceRef, ArtifactReference receipt,
            RunEvidence run) {
        var checks = new ArrayList<CheckEvidence>();
        for (String name : WorkflowAuthoringReviewer.REQUIRED_CHECKS) {
            checks.add(CheckEvidence.newBuilder().setCheckName(name)
                    .setVerdict(CheckVerdict.CHECK_VERDICT_PASSED).setRanAt(run.getCompletedAt())
                    .addArtifacts(workflowRef).addArtifacts(sourceRef).addArtifacts(receipt).build());
        }
        var deliverable = WorkflowDeliverable.newBuilder().setWorkflow(durable)
                .setWorkflowArtifact(workflowRef).setDescriptors(policy.getDescriptors())
                .addFixtures(run.getInputArtifact()).addFixtures(run.getOutputArtifact())
                .addAllChecks(checks).setRunId(run.getRunId()).setReceipt(receipt).build();
        var authored = WorkflowAuthoringDeliverable.newBuilder().setDeliverable(deliverable)
                .setExecutableSource(sourceRef).addAllAcceptanceFixtures(policy.getFixturesList()).build();
        return PrepareWorkflowCandidateResponse.newBuilder().setBinding(intent.getBinding())
                .setAuthored(authored).build();
    }

    private DynamicMessage firstInput(WorkflowAuthoringPolicy policy, CompiledWorkflow workflow)
            throws IOException {
        return DynamicMessage.parseFrom(workflow.inputType(),
                resolve(policy.getFixtures(0).getInput(), 4 * 1024 * 1024));
    }

    private static void requireRecordable(Message message) throws WorkflowPreparationException {
        var masked = SensitivityMasker.mask(message, Set.of("pii", "secret"),
                SensitivityMasker.Strategy.REMOVE);
        if (!masked.unresolvedPaths().isEmpty() || !masked.message().equals(message)) {
            throw error(WorkflowPreparationException.Kind.INVALID_INPUT,
                    "preparation-fixture-cannot-be-recorded-exactly", null);
        }
    }

    private byte[] resolve(ArtifactReference ref, int maximum) throws IOException {
        WorkflowValidation.validate(ref);
        if (Long.compareUnsigned(ref.getSizeBytes(), maximum) > 0) {
            throw new IllegalArgumentException("preparation artifact exceeds bound");
        }
        var stored = artifacts.find(ref.getSha256()).orElseThrow(() ->
                new IllegalArgumentException("preparation artifact is missing"));
        if (!stored.reference().equals(ref)) throw new IllegalArgumentException("preparation artifact metadata differs");
        byte[] bytes = stored.content();
        if (bytes.length != ref.getSizeBytes()
                || !WorkRecords.sha256Hex(bytes).equals(ref.getSha256())) {
            throw new IOException("preparation artifact content differs from reference");
        }
        return bytes;
    }

    private byte[] pinnedDescriptors(WorkflowAuthoringPolicy policy)
            throws WorkflowPreparationException {
        try {
            byte[] bytes = resolve(policy.getDescriptors(), 4 * 1024 * 1024);
            FileDescriptorSet closure = FileDescriptorSet.parseFrom(bytes);
            var names = new HashSet<String>();
            for (var file : closure.getFileList()) {
                if (file.getName().isBlank() || !names.add(file.getName())) {
                    throw new IllegalArgumentException("descriptor set has duplicate or empty file name");
                }
            }
            for (var file : closure.getFileList()) {
                for (String dependency : file.getDependencyList()) {
                    if (!names.contains(dependency)) {
                        throw new IllegalArgumentException("descriptor set omits imported file");
                    }
                }
            }
            return bytes;
        } catch (IllegalArgumentException | IOException invalid) {
            throw error(WorkflowPreparationException.Kind.CORRUPT_EVIDENCE,
                    "workflow-preparation-descriptors-unavailable", invalid);
        }
    }

    private TrustSnapshot requiredTrust() {
        TrustSnapshot snapshot = trust.get();
        if (snapshot == null) throw new IllegalArgumentException("receipt trust is unavailable");
        return snapshot;
    }

    private static String strictSource(PrepareWorkflowCandidateRequest request) {
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .decode(ByteBuffer.wrap(request.getExecutableSourceJson().toByteArray())).toString();
        } catch (CharacterCodingException malformed) {
            throw new IllegalArgumentException("executable source is not UTF-8", malformed);
        }
    }

    private static WorkflowAuthoringFixtures.Fixture fixture(WorkflowAcceptanceFixture value) {
        return new WorkflowAuthoringFixtures.Fixture(value.getName(), value.getInput(), value.getExpectedOutput());
    }

    private static WorkflowPreparationFailure failure(WorkflowPreparationIntent intent,
            WorkflowPreparationFailureReason reason) {
        return WorkflowPreparationFailure.newBuilder().setBinding(intent.getBinding())
                .setRunId(intent.getRunId()).setReason(reason).build();
    }

    private static WorkflowPreparationException terminal(WorkflowPreparationFailure failure, Throwable cause) {
        return new WorkflowPreparationException(WorkflowPreparationException.Kind.TERMINAL_FAILED,
                "preparation-attempt-failed", cause, failure);
    }

    private static WorkflowPreparationException transport(WorkflowRunner.WorkflowExecutionException failure) {
        return error(failure.kind() == WorkflowRunner.FailureKind.DEADLINE
                ? WorkflowPreparationException.Kind.DEADLINE : WorkflowPreparationException.Kind.UNAVAILABLE,
                failure.kind() == WorkflowRunner.FailureKind.DEADLINE
                        ? "workflow-preparation-deadline" : "workflow-preparation-unavailable", failure);
    }

    private static WorkflowPreparationException error(WorkflowPreparationException.Kind kind,
            String message, Throwable cause) {
        return new WorkflowPreparationException(kind, message, cause);
    }

    private static Timestamp timestamp(Instant instant) {
        return Timestamp.newBuilder().setSeconds(instant.getEpochSecond()).setNanos(instant.getNano()).build();
    }
}
