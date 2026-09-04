package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.mesh.runtime.v1.CancelRunRequest;
import ai.protomolt.proto.mesh.runtime.v1.DurableFlowRun;
import ai.protomolt.proto.mesh.runtime.v1.FlowLifecycleServiceGrpc;
import ai.protomolt.proto.mesh.runtime.v1.GetDeploymentRequest;
import ai.protomolt.proto.mesh.runtime.v1.GetPublishedFlowRequest;
import ai.protomolt.proto.mesh.runtime.v1.GetRunRequest;
import ai.protomolt.proto.mesh.runtime.v1.PublishFlowRequest;
import ai.protomolt.proto.mesh.runtime.v1.PublishFlowResponse;
import ai.protomolt.proto.mesh.runtime.v1.ReadRunHistoryRequest;
import ai.protomolt.proto.mesh.runtime.v1.ReadRunHistoryResponse;
import ai.protomolt.proto.mesh.runtime.v1.ReplayRunRequest;
import ai.protomolt.proto.mesh.runtime.v1.ResumeRunRequest;
import ai.protomolt.proto.mesh.runtime.v1.SetDeploymentRequest;
import ai.protomolt.proto.mesh.runtime.v1.StartRunRequest;
import ai.protomolt.proto.mesh.runtime.v1.ValidateFlowRequest;
import com.google.protobuf.Message;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.io.UncheckedIOException;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.function.Supplier;

/** Protobuf-only gRPC surface for the durable flow lifecycle. */
public final class FlowLifecycleGrpcService
        extends FlowLifecycleServiceGrpc.FlowLifecycleServiceImplBase {

    private final DurableFlowCoordinator coordinator;

    public FlowLifecycleGrpcService(DurableFlowCoordinator coordinator) {
        this.coordinator = Objects.requireNonNull(coordinator, "coordinator");
    }

    @Override
    public void validateFlow(
            ValidateFlowRequest request,
            StreamObserver<ai.protomolt.proto.mesh.runtime.v1.FlowValidationReport> response) {
        unary(response, () -> coordinator.validate(
                request.getVersion(), request.getDefinition()));
    }

    @Override
    public void publishFlow(
            PublishFlowRequest request,
            StreamObserver<PublishFlowResponse> response) {
        unary(response, () -> coordinator.publish(
                request.getVersion(), request.getDefinition()));
    }

    @Override
    public void getPublishedFlow(
            GetPublishedFlowRequest request,
            StreamObserver<ai.protomolt.proto.mesh.runtime.v1.PublishedFlowVersion> response) {
        unary(response, () -> {
            validate(request);
            return coordinator.published(request.getWorkflowName(), request.getVersion());
        });
    }

    @Override
    public void setDeployment(
            SetDeploymentRequest request,
            StreamObserver<ai.protomolt.proto.mesh.runtime.v1.DeploymentPointer> response) {
        unary(response, () -> {
            validate(request);
            OptionalLong expected = request.hasExpectedRevision()
                    ? OptionalLong.of(request.getExpectedRevision()) : OptionalLong.empty();
            return coordinator.deploy(
                    request.getWorkflowName(), request.getVersion(), expected);
        });
    }

    @Override
    public void getDeployment(
            GetDeploymentRequest request,
            StreamObserver<ai.protomolt.proto.mesh.runtime.v1.DeploymentPointer> response) {
        unary(response, () -> {
            validate(request);
            return coordinator.deployment(request.getWorkflowName());
        });
    }

    @Override
    public void startRun(
            StartRunRequest request,
            StreamObserver<DurableFlowRun> response) {
        unary(response, () -> {
            validate(request);
            return coordinator.start(
                    request.getWorkflowName(), request.getRunId(), request.getInput());
        });
    }

    @Override
    public void resumeRun(
            ResumeRunRequest request,
            StreamObserver<DurableFlowRun> response) {
        unary(response, () -> {
            validate(request);
            return coordinator.resume(request.getRunId());
        });
    }

    @Override
    public void getRun(
            GetRunRequest request,
            StreamObserver<DurableFlowRun> response) {
        unary(response, () -> {
            validate(request);
            return coordinator.get(request.getRunId());
        });
    }

    @Override
    public void readRunHistory(
            ReadRunHistoryRequest request,
            StreamObserver<ReadRunHistoryResponse> response) {
        unary(response, () -> {
            validate(request);
            FlowLifecycleStore.HistoryPage page = coordinator.history(
                    request.getRunId(), request.getAfterSequence(), request.getLimit());
            return ReadRunHistoryResponse.newBuilder()
                    .addAllEvents(page.events())
                    .setNextSequence(page.nextSequence())
                    .setTerminal(page.terminal())
                    .build();
        });
    }

    @Override
    public void cancelRun(
            CancelRunRequest request,
            StreamObserver<DurableFlowRun> response) {
        unary(response, () -> {
            validate(request);
            return coordinator.cancel(request.getRunId(), request.getReason());
        });
    }

    @Override
    public void replayRun(
            ReplayRunRequest request,
            StreamObserver<DurableFlowRun> response) {
        unary(response, () -> {
            validate(request);
            return coordinator.replay(request.getSourceRunId(), request.getRunId(),
                    request.getFrontierSequencesList());
        });
    }

    private static void validate(Message request) {
        RemoteValidation.annotations(request);
    }

    private static <T> void unary(
            StreamObserver<T> response, Supplier<T> operation) {
        try {
            response.onNext(operation.get());
            response.onCompleted();
        } catch (LifecycleNotFoundException missing) {
            response.onError(Status.NOT_FOUND.withDescription(missing.getMessage())
                    .asRuntimeException());
        } catch (LifecycleConflictException conflict) {
            response.onError(Status.ABORTED.withDescription(conflict.getMessage())
                    .asRuntimeException());
        } catch (IllegalArgumentException invalid) {
            response.onError(Status.INVALID_ARGUMENT.withDescription(invalid.getMessage())
                    .asRuntimeException());
        } catch (FlowExecutionSuspendedException unavailable) {
            response.onError(Status.UNAVAILABLE.withDescription(unavailable.getMessage())
                    .asRuntimeException());
        } catch (UncheckedIOException storage) {
            response.onError(Status.INTERNAL
                    .withDescription("durable flow lifecycle storage failed")
                    .withCause(storage).asRuntimeException());
        } catch (RuntimeException failure) {
            response.onError(Status.INTERNAL
                    .withDescription("durable flow lifecycle failed")
                    .withCause(failure).asRuntimeException());
        }
    }
}
