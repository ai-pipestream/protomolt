package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.delegation.v1.AcceptanceCheck;
import ai.protomolt.proto.delegation.v1.CheckEvidence;
import ai.protomolt.proto.delegation.v1.CheckVerdict;
import ai.protomolt.proto.delegation.v1.DelegateResponse;
import ai.protomolt.proto.delegation.v1.Lane;
import ai.protomolt.proto.delegation.v1.TaskOffer;
import ai.protomolt.proto.delegation.v1.TaskSpec;
import ai.protomolt.proto.delegation.v1.TranscriptEntry;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.grpc.workflow.v1.ServiceDependency;
import ai.protomolt.proto.grpc.workflow.v1.StepCompletion;
import ai.protomolt.proto.grpc.workflow.v1.Workflow;
import ai.protomolt.proto.grpc.workflow.v1.WorkflowStep;
import ai.protomolt.proto.samples.starter.v1.WorkflowAcceptanceFixture;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringDeliverable;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringPolicy;
import ai.protomolt.proto.samples.starter.v1.WorkflowDeliverable;
import ai.protomolt.proto.samples.starter.v1.WorkflowPermittedCall;
import ai.protomolt.proto.workflow.authoring.v1.PrepareWorkflowCandidateRequest;
import ai.protomolt.proto.workflow.authoring.v1.PrepareWorkflowCandidateResponse;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowPreparationBinding;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowPreparationFailure;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowPreparationFailureReason;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowPreparationIntent;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowPreparationRecord;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Cross-instance and corruption tests for the preparation ledger implementation. */
class FileSystemWorkflowPreparationRepositoryTest {
    private static final String TASK_A = "00000000-0000-4000-8000-000000000001";
    private static final String TASK_B = "00000000-0000-4000-8000-000000000003";
    private static final String PREP_A = "00000000-0000-4000-8000-000000000002";
    private static final String PREP_B = "00000000-0000-4000-8000-000000000004";
    private static final String HOLDER = "worker-one";

    @Test
    void reservationAndCompletedRetrySurviveRepositoryReopen(@TempDir Path directory) throws Exception {
        var first = new FileSystemWorkflowPreparationRepository(directory);
        var intent = intent(TASK_A, 1, 1, PREP_A, "{}");
        var reserved = with(first, intent, session -> session.reserveOrMatch(intent));
        assertThat(reserved.hasPending()).isTrue();

        var reopened = new FileSystemWorkflowPreparationRepository(directory);
        assertThat(reopened.find(TASK_A, 1, 1)).contains(reserved);
        var response = response(intent);
        var completed = with(reopened, intent, session -> session.complete(response));
        assertThat(completed.hasCompleted()).isTrue();

        var secondReopen = new FileSystemWorkflowPreparationRepository(directory);
        WorkflowPreparationRecord retried = with(secondReopen, intent, session -> session.complete(response));
        assertThat(retried).isEqualTo(completed);
        assertThat(secondReopen.find(TASK_A, 1, 1)).contains(completed);
    }

    @Test
    void sourceOrPreparationIdChangesConflictOnTheReservedTuple(@TempDir Path directory) throws Exception {
        var repository = new FileSystemWorkflowPreparationRepository(directory);
        var original = intent(TASK_A, 1, 1, PREP_A, "{}");
        with(repository, original, session -> session.reserveOrMatch(original));

        var changedSource = intent(TASK_A, 1, 1, PREP_A, "{ }");
        var changedId = intent(TASK_A, 1, 1, PREP_B, "{}");
        assertConflict(() -> with(repository, changedSource,
                session -> session.reserveOrMatch(changedSource)));
        assertConflict(() -> with(repository, changedId,
                session -> session.reserveOrMatch(changedId)));
        assertThat(repository.find(TASK_A, 1, 1)).contains(with(repository, original,
                WorkflowPreparationRepository.Session::current).orElseThrow());
    }

    @Test
    void samePreparationUuidConflictsAcrossTupleIncludingLaterAttempt(@TempDir Path directory)
            throws Exception {
        var repository = new FileSystemWorkflowPreparationRepository(directory);
        var original = intent(TASK_A, 1, 1, PREP_A, "{}");
        with(repository, original, session -> session.reserveOrMatch(original));

        for (var conflicting : List.of(intent(TASK_A, 2, 1, PREP_A, "{}"),
                intent(TASK_B, 1, 1, PREP_A, "{}"))) {
            assertConflict(() -> with(repository, conflicting, session -> session.reserveOrMatch(conflicting)));
        }
        assertThat(repository.find(TASK_A, 1, 1)).isPresent();
        assertThat(repository.find(TASK_A, 2, 1)).isEmpty();
        assertThat(repository.find(TASK_B, 1, 1)).isEmpty();
    }

    @Test
    void terminalOutcomeIsImmutableAndExactFailureRetryIsStable(@TempDir Path directory) throws Exception {
        var repository = new FileSystemWorkflowPreparationRepository(directory);
        var intent = intent(TASK_A, 1, 1, PREP_A, "{}");
        var reserved = with(repository, intent, session -> session.reserveOrMatch(intent));
        var response = response(intent);
        var completed = with(repository, intent, session -> session.complete(response));
        WorkflowPreparationRecord exactRetry = with(repository, intent, session -> session.complete(response));
        assertThat(exactRetry).isEqualTo(completed);
        var changedResponse = response.toBuilder().setBinding(response.getBinding().toBuilder()
                .setRevision(2)).build();
        assertConflict(() -> with(repository, intent, session -> session.complete(changedResponse)));
        var failure = failure(intent, WorkflowPreparationFailureReason.WORKFLOW_PREPARATION_FAILURE_REASON_FIXTURE_REJECTED);
        assertConflict(() -> with(repository, intent, session -> session.fail(failure)));
        assertThat(repository.find(TASK_A, 1, 1)).contains(completed);

        var failedRepo = new FileSystemWorkflowPreparationRepository(directory.resolve("failed"));
        with(failedRepo, intent, session -> session.reserveOrMatch(intent));
        var failed = with(failedRepo, intent, session -> session.fail(failure));
        WorkflowPreparationRecord exactFailureRetry = with(failedRepo, intent, session -> session.fail(failure));
        assertThat(exactFailureRetry).isEqualTo(failed);
        var otherReason = failure.toBuilder().setReason(
                WorkflowPreparationFailureReason.WORKFLOW_PREPARATION_FAILURE_REASON_RECORDED_RUN_FAILED).build();
        assertConflict(() -> with(failedRepo, intent, session -> session.fail(otherReason)));
        assertConflict(() -> with(failedRepo, intent, session -> session.complete(response)));
        assertThat(failedRepo.find(TASK_A, 1, 1)).contains(failed);
        assertThat(reserved.hasPending()).isTrue();
    }

    @Test
    void corruptedUnknownNoncanonicalAndOversizedRecordsFailClosed(@TempDir Path directory) throws Exception {
        var repository = new FileSystemWorkflowPreparationRepository(directory);
        var intent = intent(TASK_A, 1, 1, PREP_A, "{}");
        with(repository, intent, session -> session.reserveOrMatch(intent));
        Path record = recordPath(directory);
        byte[] valid = Files.readAllBytes(record);

        Files.write(record, new byte[] {0x0a, 0x02, 0x08});
        assertCorrupt(() -> repository.find(TASK_A, 1, 1));
        Files.write(record, valid);

        WorkflowPreparationRecord parsed = WorkflowPreparationRecord.parseFrom(valid);
        var sourceDigestMismatch = parsed.toBuilder().setIntent(parsed.getIntent().toBuilder()
                .setBinding(parsed.getIntent().getBinding().toBuilder().setSourceSha256("f".repeat(64))))
                .build();
        Files.write(record, sourceDigestMismatch.toByteArray());
        assertCorrupt(() -> repository.find(TASK_A, 1, 1));
        Files.write(record, valid);

        var offerDigestMismatch = parsed.toBuilder().setIntent(parsed.getIntent().toBuilder()
                .setBinding(parsed.getIntent().getBinding().toBuilder().setOfferEntrySha256("f".repeat(64))))
                .build();
        Files.write(record, offerDigestMismatch.toByteArray());
        assertCorrupt(() -> repository.find(TASK_A, 1, 1));
        Files.write(record, valid);

        var policyDigestMismatch = parsed.toBuilder().setIntent(parsed.getIntent().toBuilder()
                .setPolicyProto(mutatedPolicy(parsed.getIntent().getPolicyProto()))).build();
        Files.write(record, policyDigestMismatch.toByteArray());
        assertCorrupt(() -> repository.find(TASK_A, 1, 1));
        Files.write(record, valid);

        // Append an unknown varint field 99. A parser may preserve it, but the ledger must reject it.
        byte[] withUnknown = java.util.Arrays.copyOf(valid, valid.length + 3);
        withUnknown[valid.length] = (byte) 0x98;
        withUnknown[valid.length + 1] = 0x06;
        withUnknown[valid.length + 2] = 0x01;
        Files.write(record, withUnknown);
        assertCorrupt(() -> repository.find(TASK_A, 1, 1));
        Files.write(record, valid);

        // Duplicate version=1 is parseable but has a second wire spelling.
        byte[] duplicateScalar = java.util.Arrays.copyOf(valid, valid.length + 2);
        duplicateScalar[valid.length] = 0x08;
        duplicateScalar[valid.length + 1] = 0x01;
        Files.write(record, duplicateScalar);
        assertCorrupt(() -> repository.find(TASK_A, 1, 1));

        Files.write(record, new byte[16 * 1024 * 1024 + 1]);
        assertCorrupt(() -> repository.find(TASK_A, 1, 1));
        assertThat(repository.find(TASK_A, 1, 2)).isEmpty();
    }

    @Test
    void sourceAndOfferDigestMismatchAreRejectedBeforeReservation(@TempDir Path directory) throws Exception {
        var repository = new FileSystemWorkflowPreparationRepository(directory);
        var base = intent(TASK_A, 1, 1, PREP_A, "{}");
        var wrongSource = base.toBuilder().setBinding(base.getBinding().toBuilder()
                .setSourceSha256("f".repeat(64))).build();
        assertThatThrownBy(() -> with(repository, wrongSource,
                session -> session.reserveOrMatch(wrongSource))).isInstanceOf(Exception.class);
        var wrongOffer = base.toBuilder().setBinding(base.getBinding().toBuilder()
                .setOfferEntrySha256("f".repeat(64))).build();
        assertThatThrownBy(() -> with(repository, wrongOffer,
                session -> session.reserveOrMatch(wrongOffer))).isInstanceOf(Exception.class);
        var wrongPolicy = base.toBuilder().setPolicyProto(mutatedPolicy(base.getPolicyProto())).build();
        assertThatThrownBy(() -> with(repository, wrongPolicy,
                session -> session.reserveOrMatch(wrongPolicy))).isInstanceOf(Exception.class);
        assertThat(repository.find(TASK_A, 1, 1)).isEmpty();
    }

    @Test
    void callbackFailureReleasesLockAndSessionExpiresAfterReturn(@TempDir Path directory) throws Exception {
        var repository = new FileSystemWorkflowPreparationRepository(directory);
        var intent = intent(TASK_A, 1, 1, PREP_A, "{}");
        AtomicInteger calls = new AtomicInteger();
        assertThatThrownBy(() -> repository.withExclusiveIntent(TASK_A, 1, 1, session -> {
            calls.incrementAndGet();
            session.reserveOrMatch(intent);
            throw new IOException("simulated caller failure");
        })).isInstanceOf(IOException.class).hasMessageContaining("simulated caller failure");
        assertThat(calls).hasValue(1);

        WorkflowPreparationRepository.Session[] escaped = new WorkflowPreparationRepository.Session[1];
        with(repository, intent, session -> {
            escaped[0] = session;
            return session.current();
        });
        assertThatThrownBy(escaped[0]::current).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> escaped[0].reserveOrMatch(intent)).isInstanceOf(IllegalStateException.class);
        assertThat(repository.find(TASK_A, 1, 1)).isPresent();
    }

    @Test
    void activeSessionRejectsUseFromAnotherThread(@TempDir Path directory) throws Exception {
        var repository = new FileSystemWorkflowPreparationRepository(directory);
        var intent = intent(TASK_A, 1, 1, PREP_A, "{}");
        boolean rejected = repository.withExclusiveIntent(TASK_A, 1, 1, session -> {
            try (var executor = Executors.newSingleThreadExecutor()) {
                Future<Boolean> result = executor.submit(() -> {
                    try {
                        session.current();
                        return false;
                    } catch (IllegalStateException expected) {
                        return true;
                    }
                });
                return result.get(2, TimeUnit.SECONDS);
            }
        });
        assertThat(rejected).isTrue();
        with(repository, intent, session -> session.reserveOrMatch(intent));
        assertThat(repository.find(TASK_A, 1, 1)).isPresent();
    }

    @Test
    void prepublishFailurePreservesAbsenceAndPriorPendingState(@TempDir Path directory) throws Exception {
        var source = new FileSystemWorkflowPreparationRepository(directory.resolve("absent"),
                (temporary, target) -> { throw new IOException("before rename"); });
        var intent = intent(TASK_A, 1, 1, PREP_A, "{}");
        assertThatThrownBy(() -> with(source, intent, session -> session.reserveOrMatch(intent)))
                .isInstanceOf(IOException.class).hasMessageContaining("before rename");
        assertThat(source.find(TASK_A, 1, 1)).isEmpty();

        Path existingDirectory = directory.resolve("existing");
        var good = new FileSystemWorkflowPreparationRepository(existingDirectory);
        var pending = with(good, intent, session -> session.reserveOrMatch(intent));
        var failing = new FileSystemWorkflowPreparationRepository(existingDirectory,
                (temporary, target) -> { throw new IOException("before terminal rename"); });
        assertThatThrownBy(() -> with(failing, intent,
                session -> session.complete(response(intent))))
                .isInstanceOf(IOException.class).hasMessageContaining("before terminal rename");
        assertThat(failing.find(TASK_A, 1, 1)).contains(pending);
    }

    @Test
    void afterRenameFailureRecoversCommittedRecordThroughReadBarrier(@TempDir Path directory)
            throws Exception {
        AtomicInteger afterRename = new AtomicInteger();
        var firstProcess = new FileSystemWorkflowPreparationRepository(directory,
                (temporary, target) -> {}, target -> {
                    if (afterRename.incrementAndGet() == 1) throw new IOException("directory sync uncertain");
                });
        var intent = intent(TASK_A, 1, 1, PREP_A, "{}");
        assertThatThrownBy(() -> with(firstProcess, intent,
                session -> session.reserveOrMatch(intent))).isInstanceOf(IOException.class)
                .hasMessageContaining("directory sync uncertain");
        assertThat(afterRename).hasValue(1);

        var reopened = new FileSystemWorkflowPreparationRepository(directory);
        WorkflowPreparationRecord recovered = reopened.find(TASK_A, 1, 1).orElseThrow();
        assertThat(recovered.hasPending()).isTrue();
        WorkflowPreparationRecord retry = with(reopened, intent, session -> session.reserveOrMatch(intent));
        assertThat(retry).isEqualTo(recovered);
    }

    @Test
    void rawNoncanonicalPolicyBytesAreHashedExactlyAndRemainInsideCanonicalRecord(@TempDir Path directory)
            throws Exception {
        byte[] rawPolicy = noncanonicalPolicyBytes();
        WorkflowPreparationIntent intent = intent(TASK_A, 1, 1, PREP_A, "{}", rawPolicy);
        WorkflowAuthoringPolicy parsedPolicy = WorkflowAuthoringPolicy.parseFrom(rawPolicy);
        assertThat(parsedPolicy.toByteArray()).isNotEqualTo(rawPolicy);
        assertThat(sha256(rawPolicy)).isEqualTo(intent.getPolicy().getSha256());

        var repository = new FileSystemWorkflowPreparationRepository(directory);
        WorkflowPreparationRecord reserved = with(repository, intent, session -> session.reserveOrMatch(intent));
        assertThat(reserved.getIntent().getPolicyProto().toByteArray()).containsExactly(rawPolicy);
        var reopened = new FileSystemWorkflowPreparationRepository(directory);
        assertThat(reopened.find(TASK_A, 1, 1).orElseThrow().getIntent().getPolicyProto().toByteArray())
                .containsExactly(rawPolicy);
        // Outer record wire bytes are still checked for their own deterministic canonical form.
        byte[] outer = Files.readAllBytes(recordPath(directory));
        assertThat(WorkflowPreparationRecord.parseFrom(outer).toByteArray()).containsExactly(outer);
    }

    @Test
    void sameTupleCallbacksSerializeButDifferentTupleProceedsConcurrently(@TempDir Path directory)
            throws Exception {
        var repositoryA = new FileSystemWorkflowPreparationRepository(directory);
        var repositoryB = new FileSystemWorkflowPreparationRepository(directory);
        CountDownLatch firstInside = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        CountDownLatch differentInside = new CountDownLatch(1);
        CountDownLatch secondInside = new CountDownLatch(1);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<?> first = executor.submit(() -> repositoryA.withExclusiveIntent(TASK_A, 1, 1, session -> {
                firstInside.countDown();
                if (!releaseFirst.await(5, TimeUnit.SECONDS)) throw new AssertionError("first callback timed out");
                return null;
            }));
            assertThat(firstInside.await(2, TimeUnit.SECONDS)).isTrue();
            Future<?> different = executor.submit(() -> repositoryB.withExclusiveIntent(TASK_B, 1, 1, session -> {
                differentInside.countDown();
                return null;
            }));
            assertThat(differentInside.await(2, TimeUnit.SECONDS)).isTrue();
            Future<?> second = executor.submit(() -> repositoryB.withExclusiveIntent(TASK_A, 1, 1, session -> {
                secondInside.countDown();
                return null;
            }));
            assertThat(secondInside.await(150, TimeUnit.MILLISECONDS)).isFalse();
            releaseFirst.countDown();
            first.get(5, TimeUnit.SECONDS);
            second.get(5, TimeUnit.SECONDS);
            different.get(5, TimeUnit.SECONDS);
            assertThat(secondInside.getCount()).isZero();
        } finally {
            releaseFirst.countDown();
        }
    }

    @Test
    void globalPreparationUuidRaceAllowsExactlyOneTuple(@TempDir Path directory) throws Exception {
        var repoA = new FileSystemWorkflowPreparationRepository(directory);
        var repoB = new FileSystemWorkflowPreparationRepository(directory);
        var left = intent(TASK_A, 1, 1, PREP_A, "{}");
        var right = intent(TASK_B, 1, 1, PREP_A, "{}");
        CountDownLatch start = new CountDownLatch(1);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<Boolean> a = executor.submit(() -> reserveResult(repoA, left, start));
            Future<Boolean> b = executor.submit(() -> reserveResult(repoB, right, start));
            start.countDown();
            assertThat(List.of(a.get(5, TimeUnit.SECONDS), b.get(5, TimeUnit.SECONDS)))
                    .containsExactlyInAnyOrder(true, false);
        }
        assertThat(repoA.find(TASK_A, 1, 1).isPresent() ^ repoA.find(TASK_B, 1, 1).isPresent()).isTrue();
    }

    private static boolean reserveResult(FileSystemWorkflowPreparationRepository repository,
            WorkflowPreparationIntent intent, CountDownLatch start) throws Exception {
        start.await();
        try {
            with(repository, intent, session -> session.reserveOrMatch(intent));
            return true;
        } catch (Exception expectedConflict) {
            return false;
        }
    }

    private static <T> T with(FileSystemWorkflowPreparationRepository repository,
            WorkflowPreparationIntent intent, WorkflowPreparationRepository.Work<T> work) throws Exception {
        return repository.withExclusiveIntent(intent.getBinding().getTaskId(), intent.getBinding().getAttempt(),
                intent.getBinding().getRevision(), work);
    }

    static WorkflowPreparationIntent intent(String taskId, int attempt, int revision,
            String preparationId, String source) {
        return intent(taskId, attempt, revision, preparationId, source, policy().toByteArray());
    }

    private static WorkflowPreparationIntent intent(String taskId, int attempt, int revision,
            String preparationId, String source, byte[] policy) {
        byte[] sourceBytes = source.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        var request = PrepareWorkflowCandidateRequest.newBuilder().setTaskId(taskId).setAttempt(attempt)
                .setRevision(revision).setPreparationId(preparationId)
                .setExecutableSourceJson(ByteString.copyFrom(sourceBytes)).build();
        ArtifactReference policyRef = artifact(sha256(policy), "application/x-protobuf", policy.length);
        var offer = offerEntry(taskId, attempt, policyRef);
        var binding = WorkflowPreparationBinding.newBuilder().setTaskId(taskId).setAttempt(attempt)
                .setRevision(revision).setPreparationId(preparationId)
                .setOfferEntrySha256(sha256(offer.toByteArray())).setSourceSha256(sha256(sourceBytes)).build();
        return WorkflowPreparationIntent.newBuilder().setVersion(1).setRequest(request).setBinding(binding)
                .setHolder(HOLDER).setSelectedOffer(offer)
                .setPolicy(policyRef)
                .setPolicyProto(ByteString.copyFrom(policy)).setRunId("prepare-" + preparationId).build();
    }

    private static TranscriptEntry offerEntry(String taskId, int attempt, ArtifactReference policyRef) {
        DelegateResponse frame = DelegateResponse.newBuilder()
                .setFrameId("00000000-0000-4000-8000-000000000010").setTaskId(taskId).setSeq(1)
                .setSentAt(Timestamp.newBuilder().setSeconds(1))
                .setOffer(TaskOffer.newBuilder().setAttempt(attempt)
                        .setSpec(TaskSpec.newBuilder().setObjective("prepare workflow")
                                .addRequiredChecks(AcceptanceCheck.newBuilder().setName("check"))
                                .addContext(policyRef))
                        .setLeaseDuration(Duration.newBuilder().setSeconds(30))
                        .setExpiresAt(Timestamp.newBuilder().setSeconds(60)))
                .build();
        return TranscriptEntry.newBuilder().setLane(Lane.LANE_COORDINATOR).setWorkerId(HOLDER)
                .setCoordinatorFrame(frame).build();
    }

    private static WorkflowAuthoringPolicy policy() {
        var ref = artifact("a".repeat(64), "application/x-protobuf", 24);
        return WorkflowAuthoringPolicy.newBuilder().setDescriptors(ref)
                .addFixtures(WorkflowAcceptanceFixture.newBuilder().setName("fixture")
                        .setInput(ref).setExpectedOutput(ref))
                .addPermittedCalls(WorkflowPermittedCall.newBuilder().setTarget("local")
                        .setMethod("example.v1.Echo/Run")).build();
    }

    private static byte[] noncanonicalPolicyBytes() throws IOException {
        WorkflowAuthoringPolicy value = policy();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        CodedOutputStream output = CodedOutputStream.newInstance(bytes);
        // Emit the same valid protobuf fields in descending tag order.
        output.writeMessage(3, value.getPermittedCalls(0));
        output.writeMessage(2, value.getFixtures(0));
        output.writeMessage(1, value.getDescriptors());
        output.flush();
        return bytes.toByteArray();
    }

    private static PrepareWorkflowCandidateResponse response(WorkflowPreparationIntent intent) {
        ArtifactReference evidence = artifact("a".repeat(64), "application/x-protobuf", 24);
        Workflow workflow = Workflow.newBuilder().setName("prepared-workflow")
                .setInputType("example.v1.Input").setValidateContract(true)
                .addDependencies(ServiceDependency.newBuilder().setAlias("echo")
                        .setServiceProfile("example.v1.Echo").setEndpoint("local")
                        .setDescriptorFingerprint("a".repeat(64)))
                .addSteps(WorkflowStep.newBuilder().setName("call").setDependency("echo")
                        .setMethod("example.v1.Echo/Run").setCompletion(StepCompletion.STEP_COMPLETION_LIVE))
                .setDeadline(Duration.newBuilder().setSeconds(30)).build();
        WorkflowDeliverable deliverable = WorkflowDeliverable.newBuilder().setWorkflow(workflow)
                .setWorkflowArtifact(evidence).setDescriptors(evidence).addFixtures(evidence)
                .addChecks(CheckEvidence.newBuilder().setCheckName("prepare")
                        .setVerdict(CheckVerdict.CHECK_VERDICT_PASSED)
                        .setRanAt(Timestamp.newBuilder().setSeconds(1)).addArtifacts(evidence))
                .setRunId(intent.getRunId()).setReceipt(evidence).build();
        WorkflowAuthoringDeliverable authored = WorkflowAuthoringDeliverable.newBuilder()
                .setDeliverable(deliverable)
                .setExecutableSource(artifact(intent.getBinding().getSourceSha256(), "application/json", 24))
                .addAcceptanceFixtures(WorkflowAcceptanceFixture.newBuilder().setName("fixture")
                        .setInput(evidence).setExpectedOutput(evidence)).build();
        return PrepareWorkflowCandidateResponse.newBuilder().setBinding(intent.getBinding())
                .setAuthored(authored).build();
    }

    private static WorkflowPreparationFailure failure(WorkflowPreparationIntent intent,
            WorkflowPreparationFailureReason reason) {
        return WorkflowPreparationFailure.newBuilder().setBinding(intent.getBinding()).setReason(reason)
                .setRunId(intent.getRunId()).build();
    }

    private static ArtifactReference artifact(String hash, String mediaType, int size) {
        return ArtifactReference.newBuilder().setSha256(hash).setMediaType(mediaType).setSizeBytes(size).build();
    }

    private static String sha256(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static ByteString mutatedPolicy(ByteString policy) {
        byte[] changed = policy.toByteArray();
        changed[0] ^= 0x01;
        return ByteString.copyFrom(changed);
    }

    private static Path recordPath(Path directory) throws IOException {
        try (var paths = Files.walk(directory)) {
            return paths.filter(path -> path.getFileName().toString().endsWith(".pb")).findFirst()
                    .orElseThrow(() -> new AssertionError("no preparation record written"));
        }
    }

    private static void assertConflict(ThrowingCall call) {
        assertThatThrownBy(call::run).isInstanceOf(WorkflowPreparationConflictException.class);
    }

    private static void assertCorrupt(ThrowingCall call) {
        assertThatThrownBy(call::run).isInstanceOf(Exception.class);
    }

    @FunctionalInterface private interface ThrowingCall { Object run() throws Exception; }
}
