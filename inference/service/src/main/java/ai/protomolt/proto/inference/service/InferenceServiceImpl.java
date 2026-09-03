package ai.protomolt.proto.inference.service;

import ai.protomolt.proto.inference.spi.ChunkObserver;
import ai.protomolt.proto.inference.spi.InferenceEngines;
import ai.protomolt.proto.inference.spi.InferenceException;
import ai.protomolt.proto.inference.spi.UnknownModelException;
import ai.protomolt.proto.inference.v1.DescribeModelRequest;
import ai.protomolt.proto.inference.v1.DescribeModelResponse;
import ai.protomolt.proto.inference.v1.GenerateRequest;
import ai.protomolt.proto.inference.v1.GenerateResponse;
import ai.protomolt.proto.inference.v1.GenerateStreamRequest;
import ai.protomolt.proto.inference.v1.GenerateStreamResponse;
import ai.protomolt.proto.inference.v1.InferenceServiceGrpc;
import ai.protomolt.proto.inference.v1.ListModelsRequest;
import ai.protomolt.proto.inference.v1.ListModelsResponse;
import ai.protomolt.proto.validate.ValidationResult;
import com.google.protobuf.Message;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * The generic {@code InferenceService} gRPC surface wrapped around the SPI
 * facade ({@link InferenceEngines}).
 *
 * <p>Every request is validated against the validate.v1 rules declared on the
 * contract before any provider is touched; violations answer INVALID_ARGUMENT
 * with the rule paths. Provider calls are blocking, so they run on a
 * virtual-thread executor — the gRPC event loop never waits on a model.
 * Errors map to the closest honest status: unknown model or provider id to
 * NOT_FOUND, everything else to INTERNAL with the provider's message
 * verbatim. Nothing is swallowed or defaulted.</p>
 *
 * <p>Instances are thread-safe. {@link #close()} shuts the executor; the
 * underlying providers and catalog are owned by the caller.</p>
 */
public final class InferenceServiceImpl extends InferenceServiceGrpc.InferenceServiceImplBase
        implements AutoCloseable {

    private final InferenceEngines engines;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    /**
     * Creates the service over the given facade.
     *
     * @param engines the catalog-plus-providers facade to serve
     */
    public InferenceServiceImpl(InferenceEngines engines) {
        this.engines = engines;
    }

    @Override
    public void generate(GenerateRequest request, StreamObserver<GenerateResponse> observer) {
        executor.submit(() -> {
            try {
                validate(request);
                observer.onNext(engines.generate(request));
                observer.onCompleted();
            } catch (RuntimeException e) {
                observer.onError(status(e));
            }
        });
    }

    @Override
    public void generateStream(GenerateStreamRequest request,
                               StreamObserver<GenerateStreamResponse> observer) {
        try {
            validate(request);
        } catch (RuntimeException e) {
            observer.onError(status(e));
            return;
        }
        executor.submit(() -> {
            try {
                engines.generateStream(request, new ChunkObserver() {
                    @Override
                    public void onNext(GenerateStreamResponse chunk) {
                        synchronized (observer) {
                            observer.onNext(chunk);
                        }
                    }

                    @Override
                    public void onComplete() {
                        synchronized (observer) {
                            observer.onCompleted();
                        }
                    }

                    @Override
                    public void onError(InferenceException error) {
                        synchronized (observer) {
                            observer.onError(status(error));
                        }
                    }
                });
            } catch (RuntimeException e) {
                observer.onError(status(e));
            }
        });
    }

    @Override
    public void listModels(ListModelsRequest request, StreamObserver<ListModelsResponse> observer) {
        try {
            validate(request);
            observer.onNext(engines.listModels(request));
            observer.onCompleted();
        } catch (RuntimeException e) {
            observer.onError(status(e));
        }
    }

    @Override
    public void describeModel(DescribeModelRequest request,
                              StreamObserver<DescribeModelResponse> observer) {
        try {
            validate(request);
            observer.onNext(engines.describe(request));
            observer.onCompleted();
        } catch (RuntimeException e) {
            observer.onError(status(e));
        }
    }

    /** Enforces the contract's declared validate.v1 rules; throws on any violation. */
    private static void validate(Message request) {
        ValidationResult.validate(request).throwIfInvalid();
    }

    /** Maps an internal failure to the closest honest gRPC status. */
    private static StatusRuntimeException status(RuntimeException e) {
        if (e instanceof ValidationResult.ValidationException validation) {
            return Status.INVALID_ARGUMENT
                    .withDescription("request violates its declared rules: "
                            + validation.result().violations())
                    .withCause(e)
                    .asRuntimeException();
        }
        if (e instanceof UnknownModelException) {
            return Status.NOT_FOUND.withDescription(e.getMessage()).withCause(e)
                    .asRuntimeException();
        }
        if (e instanceof InferenceException) {
            return Status.INTERNAL.withDescription(e.getMessage()).withCause(e)
                    .asRuntimeException();
        }
        return Status.INTERNAL.withDescription(e.toString()).withCause(e).asRuntimeException();
    }

    @Override
    public void close() {
        executor.close();
    }
}
