package ai.pipestream.proto.grpc.invoke;

import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Channel;
import io.grpc.reflection.v1.ServerReflectionGrpc;
import io.grpc.reflection.v1.ServerReflectionRequest;
import io.grpc.reflection.v1.ServerReflectionResponse;
import io.grpc.stub.StreamObserver;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A gRPC server-reflection client. Reflection is a bidi stream
 * ({@code ServerReflectionInfo(stream request) returns (stream response)}), so it cannot ride
 * on {@link DynamicGrpcCalls}; this drives the stream request-at-a-time through a bounded queue,
 * which matches reflection's one-response-per-request shape.
 *
 * <p>{@link #discover} lists the server's services and then walks the descriptor graph
 * (each service's containing file, then every transitive dependency by filename) into a single
 * {@link FileDescriptorSet} an agent can feed to any descriptor-driven action.</p>
 */
public final class ReflectionClient {

    /** What a reflection walk found: the advertised service names and their full descriptor set. */
    public record Result(List<String> services, FileDescriptorSet descriptorSet) {
    }

    private ReflectionClient() {
    }

    /**
     * Reflects the server reachable on {@code channel}: lists services and resolves every
     * service's file and its transitive dependencies.
     *
     * @throws ReflectionException on a reflection error response, a stream failure, or timeout
     */
    public static Result discover(Channel channel, long timeoutMs) throws ReflectionException {
        try (Stream stream = new Stream(channel, timeoutMs)) {
            List<String> services = stream.listServices();
            Map<String, FileDescriptorProto> files = new LinkedHashMap<>();
            Deque<String> pending = new ArrayDeque<>();

            for (String service : services) {
                // The reflection well-known service is an implementation detail, not app schema.
                if (service.equals("grpc.reflection.v1.ServerReflection")
                        || service.equals("grpc.reflection.v1alpha.ServerReflection")) {
                    continue;
                }
                for (FileDescriptorProto file : stream.fileContainingSymbol(service)) {
                    if (files.putIfAbsent(file.getName(), file) == null) {
                        pending.add(file.getName());
                    }
                }
            }
            while (!pending.isEmpty()) {
                FileDescriptorProto file = files.get(pending.poll());
                for (String dependency : file.getDependencyList()) {
                    if (files.containsKey(dependency)) {
                        continue;
                    }
                    for (FileDescriptorProto fetched : stream.fileByFilename(dependency)) {
                        if (files.putIfAbsent(fetched.getName(), fetched) == null) {
                            pending.add(fetched.getName());
                        }
                    }
                }
            }
            return new Result(services,
                    FileDescriptorSet.newBuilder().addAllFile(files.values()).build());
        }
    }

    /** One reflection bidi stream, driven synchronously. */
    private static final class Stream implements AutoCloseable {
        private final long timeoutMs;
        private final BlockingQueue<Object> responses = new ArrayBlockingQueue<>(64);
        private final StreamObserver<ServerReflectionRequest> requests;
        private static final Object COMPLETED = new Object();

        Stream(Channel channel, long timeoutMs) {
            this.timeoutMs = timeoutMs;
            ServerReflectionGrpc.ServerReflectionStub stub = ServerReflectionGrpc.newStub(channel);
            this.requests = stub.serverReflectionInfo(new StreamObserver<>() {
                @Override
                public void onNext(ServerReflectionResponse value) {
                    responses.offer(value);
                }

                @Override
                public void onError(Throwable t) {
                    responses.offer(t);
                }

                @Override
                public void onCompleted() {
                    responses.offer(COMPLETED);
                }
            });
        }

        List<String> listServices() throws ReflectionException {
            ServerReflectionResponse response = exchange(
                    ServerReflectionRequest.newBuilder().setListServices("*").build());
            return response.getListServicesResponse().getServiceList().stream()
                    .map(io.grpc.reflection.v1.ServiceResponse::getName)
                    .toList();
        }

        List<FileDescriptorProto> fileContainingSymbol(String symbol) throws ReflectionException {
            return files(exchange(ServerReflectionRequest.newBuilder()
                    .setFileContainingSymbol(symbol).build()));
        }

        List<FileDescriptorProto> fileByFilename(String filename) throws ReflectionException {
            return files(exchange(ServerReflectionRequest.newBuilder()
                    .setFileByFilename(filename).build()));
        }

        private static List<FileDescriptorProto> files(ServerReflectionResponse response)
                throws ReflectionException {
            try {
                List<FileDescriptorProto> parsed = new java.util.ArrayList<>();
                for (var bytes : response.getFileDescriptorResponse().getFileDescriptorProtoList()) {
                    parsed.add(FileDescriptorProto.parseFrom(bytes));
                }
                return parsed;
            } catch (InvalidProtocolBufferException e) {
                throw new ReflectionException("Server returned an unparseable file descriptor", e);
            }
        }

        private ServerReflectionResponse exchange(ServerReflectionRequest request)
                throws ReflectionException {
            requests.onNext(request);
            Object taken;
            try {
                taken = responses.poll(timeoutMs, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ReflectionException("Interrupted while reflecting", e);
            }
            if (taken == null) {
                throw new ReflectionException("Reflection timed out after " + timeoutMs + "ms");
            }
            if (taken == COMPLETED) {
                throw new ReflectionException("Reflection stream closed before responding");
            }
            if (taken instanceof Throwable t) {
                throw new ReflectionException("Reflection stream failed: " + t.getMessage(), t);
            }
            ServerReflectionResponse response = (ServerReflectionResponse) taken;
            if (response.hasErrorResponse()) {
                throw new ReflectionException("Reflection error " + response.getErrorResponse()
                        .getErrorCode() + ": " + response.getErrorResponse().getErrorMessage());
            }
            return response;
        }

        @Override
        public void close() {
            try {
                requests.onCompleted();
            } catch (RuntimeException ignored) {
                // Best-effort half-close; the channel is the caller's to shut down.
            }
        }
    }
}
