package ai.protomolt.proto.grpc.invoke;

import ai.protomolt.proto.grpc.policy.OutboundChannelPolicy;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Channel;
import io.grpc.Status;
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

    /** Maximum total wall-clock time for one reflection walk. */
    public static final long MAX_TIMEOUT_MS = 60_000;

    /** Maximum advertised services accepted from one endpoint. */
    public static final int MAX_SERVICES = 4_096;

    /** Maximum distinct descriptor files accumulated by one reflection walk. */
    public static final int MAX_FILES = 4_096;

    /** Maximum serialized descriptor-set size accumulated by one reflection walk. */
    public static final int MAX_DESCRIPTOR_SET_BYTES = 16 * 1024 * 1024;

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
        if (timeoutMs < 1 || timeoutMs > MAX_TIMEOUT_MS) {
            throw new ReflectionException("Reflection timeout must be from 1 to "
                    + MAX_TIMEOUT_MS + "ms");
        }
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        try {
            return discover(new V1Stream(channel, timeoutMs, deadlineNanos));
        } catch (ReflectionException stableFailure) {
            if (Status.fromThrowable(stableFailure).getCode() != Status.Code.UNIMPLEMENTED) {
                throw stableFailure;
            }
            return discover(new V1AlphaStream(channel, timeoutMs, deadlineNanos));
        }
    }

    private static Result discover(ReflectionStream stream) throws ReflectionException {
        try (stream) {
            List<String> services = stream.listServices();
            if (services.size() > MAX_SERVICES) {
                throw new ReflectionException("Server advertised more than " + MAX_SERVICES
                        + " services");
            }
            Map<String, FileDescriptorProto> files = new LinkedHashMap<>();
            Deque<String> pending = new ArrayDeque<>();
            long descriptorBytes = 0;

            for (String service : services) {
                // The reflection well-known service is an implementation detail, not app schema.
                if (service.equals("grpc.reflection.v1.ServerReflection")
                        || service.equals("grpc.reflection.v1alpha.ServerReflection")) {
                    continue;
                }
                descriptorBytes = addFiles(stream.fileContainingSymbol(service), files, pending,
                        descriptorBytes);
            }
            while (!pending.isEmpty()) {
                FileDescriptorProto file = files.get(pending.poll());
                for (String dependency : file.getDependencyList()) {
                    if (files.containsKey(dependency)) {
                        continue;
                    }
                    descriptorBytes = addFiles(stream.fileByFilename(dependency), files, pending,
                            descriptorBytes);
                }
            }
            FileDescriptorSet descriptorSet = FileDescriptorSet.newBuilder()
                    .addAllFile(files.values()).build();
            if (descriptorSet.getSerializedSize() > MAX_DESCRIPTOR_SET_BYTES) {
                throw new ReflectionException("Reflected descriptor set exceeds "
                        + MAX_DESCRIPTOR_SET_BYTES + " bytes");
            }
            return new Result(services, descriptorSet);
        }
    }

    /**
     * Policy-aware overload for callers that already own a channel. Target and channel-slot
     * validation belongs to the factory that created the channel; this overload additionally
     * enforces the host's reflection deadline at the protocol boundary.
     */
    public static Result discover(Channel channel, long timeoutMs, OutboundChannelPolicy policy)
            throws ReflectionException {
        if (policy == null) {
            throw new IllegalArgumentException("channel policy must not be null");
        }
        policy.validateDeadline(timeoutMs);
        return discover(channel, timeoutMs);
    }

    private static long addFiles(List<FileDescriptorProto> candidates,
                                 Map<String, FileDescriptorProto> files,
                                 Deque<String> pending, long descriptorBytes)
            throws ReflectionException {
        long total = descriptorBytes;
        for (FileDescriptorProto file : candidates) {
            if (files.containsKey(file.getName())) {
                continue;
            }
            if (files.size() == MAX_FILES) {
                throw new ReflectionException("Reflected schema exceeds " + MAX_FILES
                        + " descriptor files");
            }
            total += file.getSerializedSize();
            if (total > MAX_DESCRIPTOR_SET_BYTES) {
                throw new ReflectionException("Reflected descriptor set exceeds "
                        + MAX_DESCRIPTOR_SET_BYTES + " bytes");
            }
            files.put(file.getName(), file);
            pending.add(file.getName());
        }
        return total;
    }

    /** The protocol operations used while walking a reflected descriptor graph. */
    private interface ReflectionStream extends AutoCloseable {
        List<String> listServices() throws ReflectionException;

        List<FileDescriptorProto> fileContainingSymbol(String symbol) throws ReflectionException;

        List<FileDescriptorProto> fileByFilename(String filename) throws ReflectionException;

        @Override
        void close();
    }

    /** One stable v1 reflection bidi stream, driven synchronously. */
    private static final class V1Stream implements ReflectionStream {
        private final long timeoutMs;
        private final long deadlineNanos;
        private final BlockingQueue<Object> responses = new ArrayBlockingQueue<>(64);
        private final StreamObserver<ServerReflectionRequest> requests;
        private static final Object COMPLETED = new Object();

        V1Stream(Channel channel, long timeoutMs, long deadlineNanos) {
            this.timeoutMs = timeoutMs;
            this.deadlineNanos = deadlineNanos;
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

        @Override
        public List<String> listServices() throws ReflectionException {
            ServerReflectionResponse response = exchange(
                    ServerReflectionRequest.newBuilder().setListServices("*").build());
            return response.getListServicesResponse().getServiceList().stream()
                    .map(io.grpc.reflection.v1.ServiceResponse::getName)
                    .toList();
        }

        @Override
        public List<FileDescriptorProto> fileContainingSymbol(String symbol)
                throws ReflectionException {
            return files(exchange(ServerReflectionRequest.newBuilder()
                    .setFileContainingSymbol(symbol).build()));
        }

        @Override
        public List<FileDescriptorProto> fileByFilename(String filename)
                throws ReflectionException {
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
                long remainingNanos = deadlineNanos - System.nanoTime();
                taken = remainingNanos <= 0 ? null
                        : responses.poll(remainingNanos, TimeUnit.NANOSECONDS);
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

    /** One legacy v1alpha reflection bidi stream, driven synchronously. */
    private static final class V1AlphaStream implements ReflectionStream {
        private final long timeoutMs;
        private final long deadlineNanos;
        private final BlockingQueue<Object> responses = new ArrayBlockingQueue<>(64);
        private final StreamObserver<io.grpc.reflection.v1alpha.ServerReflectionRequest> requests;
        private static final Object COMPLETED = new Object();

        V1AlphaStream(Channel channel, long timeoutMs, long deadlineNanos) {
            this.timeoutMs = timeoutMs;
            this.deadlineNanos = deadlineNanos;
            var stub = io.grpc.reflection.v1alpha.ServerReflectionGrpc.newStub(channel);
            this.requests = stub.serverReflectionInfo(new StreamObserver<>() {
                @Override
                public void onNext(io.grpc.reflection.v1alpha.ServerReflectionResponse value) {
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

        @Override
        public List<String> listServices() throws ReflectionException {
            var response = exchange(io.grpc.reflection.v1alpha.ServerReflectionRequest.newBuilder()
                    .setListServices("*").build());
            return response.getListServicesResponse().getServiceList().stream()
                    .map(io.grpc.reflection.v1alpha.ServiceResponse::getName)
                    .toList();
        }

        @Override
        public List<FileDescriptorProto> fileContainingSymbol(String symbol)
                throws ReflectionException {
            return files(exchange(io.grpc.reflection.v1alpha.ServerReflectionRequest.newBuilder()
                    .setFileContainingSymbol(symbol).build()));
        }

        @Override
        public List<FileDescriptorProto> fileByFilename(String filename)
                throws ReflectionException {
            return files(exchange(io.grpc.reflection.v1alpha.ServerReflectionRequest.newBuilder()
                    .setFileByFilename(filename).build()));
        }

        private static List<FileDescriptorProto> files(
                io.grpc.reflection.v1alpha.ServerReflectionResponse response)
                throws ReflectionException {
            try {
                List<FileDescriptorProto> parsed = new java.util.ArrayList<>();
                for (var bytes : response.getFileDescriptorResponse()
                        .getFileDescriptorProtoList()) {
                    parsed.add(FileDescriptorProto.parseFrom(bytes));
                }
                return parsed;
            } catch (InvalidProtocolBufferException e) {
                throw new ReflectionException("Server returned an unparseable file descriptor", e);
            }
        }

        private io.grpc.reflection.v1alpha.ServerReflectionResponse exchange(
                io.grpc.reflection.v1alpha.ServerReflectionRequest request)
                throws ReflectionException {
            requests.onNext(request);
            Object taken;
            try {
                long remainingNanos = deadlineNanos - System.nanoTime();
                taken = remainingNanos <= 0 ? null
                        : responses.poll(remainingNanos, TimeUnit.NANOSECONDS);
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
            var response = (io.grpc.reflection.v1alpha.ServerReflectionResponse) taken;
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
