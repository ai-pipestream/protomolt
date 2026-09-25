package ai.protomolt.proto.correction;

import ai.protomolt.proto.authz.grpc.ApiTokenServerInterceptor;
import ai.protomolt.proto.inference.spi.*;
import ai.protomolt.proto.inference.v1.*;
import ai.protomolt.proto.validate.ProtoValidator;
import io.grpc.*;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import io.grpc.stub.StreamObserver;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

/** Candidate bridge: authenticated loopback gRPC to the local Kimi CLI over ACP. */
public final class KimiInferenceBridge {
    private KimiInferenceBridge() {}
    public static void main(String[] args) throws Exception {
        String token = System.getenv("PROTOMOLT_INFERENCE_API_TOKEN");
        if (token == null || token.isBlank()) throw new IllegalArgumentException("PROTOMOLT_INFERENCE_API_TOKEN required");
        String executable = System.getenv().getOrDefault("PROTOMOLT_KIMI_EXECUTABLE", "kimi");
        String model = System.getenv().getOrDefault("PROTOMOLT_KIMI_MODEL", "kimi-code/k3");
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 29930;
        Path workspace = Path.of(args.length > 1 ? args[1] : "build/kimi-bridge");
        var provider = new KimiCliProvider(executable, workspace);
        var engines = new InferenceEngines(new InferenceCatalog(), List.of(provider));
        engines.register(ModelEntry.newBuilder().setId("starter-correction").setProvider(provider.id())
                .setBackendModel(model).setEndpoint("local-acp://kimi")
                .setCapabilities(ModelCapabilities.newBuilder().setStructuredOutput(false)).build());
        var executor = Executors.newVirtualThreadPerTaskExecutor();
        var capacity = new Semaphore(1);
        var service = new InferenceServiceGrpc.InferenceServiceImplBase() {
            @Override public void generate(GenerateRequest request, StreamObserver<GenerateResponse> observer) {
                var validation = ProtoValidator.forMessageType(request.getDescriptorForType()).validate(request);
                if (!validation.valid()) {
                    observer.onError(Status.INVALID_ARGUMENT.withDescription("invalid generation request").asRuntimeException());
                    return;
                }
                if (!capacity.tryAcquire()) {
                    observer.onError(Status.RESOURCE_EXHAUSTED.withDescription("Kimi bridge is busy").asRuntimeException());
                    return;
                }
                Context context = Context.current();
                executor.execute(() -> {
                    Thread worker = Thread.currentThread();
                    Context.CancellationListener listener = ignored -> worker.interrupt();
                    context.addListener(listener, Runnable::run);
                    try {
                        if (context.isCancelled()) return;
                        var result = engines.generate(request);
                        if (!context.isCancelled()) {
                            if (!ProtoValidator.forMessageType(result.getDescriptorForType()).validate(result).valid()) {
                                throw new InferenceException("invalid provider response");
                            }
                            observer.onNext(result);
                            observer.onCompleted();
                        }
                    } catch (Exception error) {
                        if (!context.isCancelled()) observer.onError(Status.INTERNAL
                                .withDescription("Kimi generation failed").asRuntimeException());
                    } finally {
                        context.removeListener(listener);
                        capacity.release();
                    }
                });
            }
            @Override public void describeModel(DescribeModelRequest request, StreamObserver<DescribeModelResponse> observer) {
                try { observer.onNext(engines.describe(request)); observer.onCompleted(); }
                catch (RuntimeException error) { observer.onError(Status.NOT_FOUND.asRuntimeException()); }
            }
            @Override public void listModels(ListModelsRequest request, StreamObserver<ListModelsResponse> observer) {
                observer.onNext(engines.listModels(request)); observer.onCompleted();
            }
        };
        var server = NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", port))
                .maxInboundMessageSize(1048576).executor(executor)
                .intercept(new ApiTokenServerInterceptor(token, null)).addService(service)
                .addService(ProtoReflectionServiceV1.newInstance()).build().start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> { server.shutdownNow(); executor.shutdownNow(); }));
        System.out.println("Kimi bridge listening on loopback port " + server.getPort() + "; native-structured-output=false");
        server.awaitTermination();
    }
}
