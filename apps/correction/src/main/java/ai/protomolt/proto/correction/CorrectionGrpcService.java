package ai.protomolt.proto.correction;

import ai.protomolt.proto.correction.v1.*;
import ai.protomolt.proto.receipt.RecordVerifier;
import ai.protomolt.proto.receipt.SignedWorkRecord;
import ai.protomolt.proto.receipt.TrustSnapshot;
import ai.protomolt.proto.grpc.workflow.v1.WorkflowAssessment;
import ai.protomolt.proto.validate.ValidationResult;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.Semaphore;

/** Authenticated fixed-workflow RPC adapter; the server owns its interceptor. */
final class CorrectionGrpcService extends CorrectionServiceGrpc.CorrectionServiceImplBase
        implements AutoCloseable {
    private final ContactCorrection correction;
    private final TrustSnapshot trust;
    private final Path workspace;
    private final Semaphore capacity = new Semaphore(1);
    private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();

    CorrectionGrpcService(ContactCorrection correction, TrustSnapshot trust, Path workspace) {
        this.correction = correction;
        this.trust = trust;
        this.workspace = workspace;
    }

    @Override public void runCorrection(RunCorrectionRequest request,
                                        StreamObserver<RunCorrectionResponse> observer) {
        try {
            ValidationResult.validate(request).throwIfInvalid();
        } catch (RuntimeException invalid) {
            observer.onError(Status.INVALID_ARGUMENT.withDescription("invalid correction request")
                    .asRuntimeException());
            return;
        }
        if (!ContactCorrection.NAME.equals(request.getWorkflowName())) {
            observer.onError(Status.INVALID_ARGUMENT.withDescription("unsupported workflow name")
                    .asRuntimeException());
            return;
        }
        if (!capacity.tryAcquire()) {
            observer.onError(Status.RESOURCE_EXHAUSTED.withDescription("correction worker busy")
                    .asRuntimeException());
            return;
        }
        Context call = Context.current();
        AtomicReference<Thread> worker = new AtomicReference<>();
        Context.CancellationListener cancellation = ignored -> {
            Thread active = worker.get();
            if (active != null) active.interrupt();
        };
        call.addListener(cancellation, Runnable::run);
        workers.execute(() -> {
            worker.set(Thread.currentThread());
            try {
                if (call.isCancelled()) return;
                String runId = request.getRunId();
                ContactCorrection.Outcome result = correction.run(runId, request.getSource());
                var check = correction.verify(result, trust);
                if (!check.ok()) throw new IOException("recorded correction verification failed: " + check.reason());
                CorrectionOutcome reply = outcome(runId, result.assessment(),
                        result.executionReceipt(), result.assessmentReceipt());
                markCompleted(runId);
                if (!call.isCancelled()) {
                    RunCorrectionResponse response = RunCorrectionResponse.newBuilder()
                            .setOutcome(reply).build();
                    ValidationResult.validate(response).throwIfInvalid();
                    observer.onNext(response);
                    observer.onCompleted();
                }
            } catch (FileAlreadyExistsException duplicate) {
                if (!call.isCancelled()) observer.onError(Status.ALREADY_EXISTS
                        .withDescription("correction run id already used").asRuntimeException());
            } catch (Throwable failure) {
                if (!call.isCancelled()) observer.onError(Status.INTERNAL
                        .withDescription("correction run failed; inspect recorded evidence")
                        .asRuntimeException());
            } finally {
                worker.set(null);
                call.removeListener(cancellation);
                capacity.release();
            }
        });
    }

    @Override public void getCorrection(GetCorrectionRequest request,
                                        StreamObserver<GetCorrectionResponse> observer) {
        try {
            ValidationResult.validate(request).throwIfInvalid();
        } catch (RuntimeException invalid) {
            observer.onError(Status.INVALID_ARGUMENT.withDescription("invalid correction lookup")
                    .asRuntimeException());
            return;
        }
        String runId = request.getRunId();
        Path output = workspace.resolve("outcomes").resolve(runId);
        if (!Files.isRegularFile(output.resolve("completed"))) {
            observer.onError(Status.NOT_FOUND.withDescription("completed correction not found")
                    .asRuntimeException());
            return;
        }
        try {
            var check = ContactCorrection.verifyStored(workspace, runId, trust);
            if (!check.ok()) throw new IOException(check.reason());
            CorrectionOutcome reply = outcome(runId,
                    WorkflowAssessment.parseFrom(Files.readAllBytes(output.resolve("assessment.pb"))),
                    SignedWorkRecord.parseFrom(Files.readAllBytes(output.resolve("execution.pb"))),
                    SignedWorkRecord.parseFrom(Files.readAllBytes(output.resolve("receipt.pb"))));
            GetCorrectionResponse response = GetCorrectionResponse.newBuilder()
                    .setOutcome(reply).build();
            ValidationResult.validate(response).throwIfInvalid();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception invalid) {
            observer.onError(Status.DATA_LOSS.withDescription("stored correction failed verification")
                    .asRuntimeException());
        }
    }

    private CorrectionOutcome outcome(String runId, WorkflowAssessment assessment,
                                      SignedWorkRecord execution, SignedWorkRecord assessed) {
        if (!runId.equals(assessment.getRunId())
                || !RecordVerifier.verify(execution.toByteArray(), trust).verified()
                || !RecordVerifier.verify(assessed.toByteArray(), trust).verified()) {
            throw new IllegalStateException("correction receipt/run binding failed");
        }
        CorrectionOutcome reply = CorrectionOutcome.newBuilder().setRunId(runId)
                .setAssessment(assessment).setExecutionReceipt(execution)
                .setAssessmentReceipt(assessed).build();
        ValidationResult.validate(reply).throwIfInvalid();
        return reply;
    }

    private void markCompleted(String runId) throws IOException {
        Path output = workspace.resolve("outcomes").resolve(runId);
        Path temporary = Files.createTempFile(output, ".completed-", ".tmp");
        try {
            Files.move(temporary, output.resolve("completed"), StandardCopyOption.ATOMIC_MOVE);
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    @Override public void close() { workers.shutdownNow(); }
}
