package ai.protomolt.proto.emit.renderer;

import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.emit.v1.RenderRequest;
import ai.protomolt.proto.emit.v1.RenderResponse;
import ai.protomolt.proto.emit.v1.RendererCapability;
import ai.protomolt.proto.emit.v1.RendererServiceGrpc;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import io.grpc.Channel;
import io.grpc.Status;
import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * One host-configured remote capability and exact source profile. Channel creation,
 * credentials and outbound policy remain in the host. Request data cannot select
 * an endpoint. Unary transport is deliberately limited to bounded artifacts.
 */
public final class GrpcRendererProvider implements RendererProvider {
    private final RendererCapability capability;
    private final Descriptor source;
    private final Descriptor options;
    private final RendererServiceGrpc.RendererServiceFutureStub stub;
    private final Duration deadline;
    private final int maxDescriptorBytes;

    public GrpcRendererProvider(RendererCapability capability, Descriptor source, Descriptor options,
                                Channel configuredChannel, Duration deadline, int maxDescriptorBytes) {
        this.capability = Objects.requireNonNull(capability);
        this.source = Objects.requireNonNull(source);
        this.options = Objects.requireNonNull(options);
        this.deadline = Objects.requireNonNull(deadline);
        if (deadline.isNegative() || deadline.isZero() || maxDescriptorBytes < 1
                || capability.getMaxOutputBytes() <= 0
                || capability.getMaxOutputBytes() > Integer.MAX_VALUE - 65536)
            throw new IllegalArgumentException("Positive bounded transport limits required");
        this.maxDescriptorBytes = maxDescriptorBytes;
        this.stub = RendererServiceGrpc.newFutureStub(Objects.requireNonNull(configuredChannel))
                .withMaxInboundMessageSize(capability.getMaxOutputBytes() + 65536);
    }

    @Override public RendererCapability capability() { return capability; }
    @Override public Descriptor optionsDescriptor() { return options; }

    @Override public void check(Message input, Message ignored) throws RenderFailure {
        if (!source.getFullName().equals(input.getDescriptorForType().getFullName())
                || !DescriptorFingerprints.fingerprintOf(source)
                    .equals(DescriptorFingerprints.fingerprintOf(input.getDescriptorForType())))
            throw new RenderFailure(RenderFailure.Kind.UNSUPPORTED, "Remote renderer source descriptor mismatch");
        // This bounded wire contract carries no per-occurrence Any admission evidence.
        // A matching type URL or a registered descriptor cannot supply that authority.
        var pending = new java.util.ArrayDeque<Message>(); pending.add(input);
        while (!pending.isEmpty()) {
            Message value = pending.removeFirst();
            if (value.getDescriptorForType().getFullName().equals("google.protobuf.Any"))
                throw new RenderFailure(RenderFailure.Kind.UNSUPPORTED,
                        "Remote renderer requires an explicit projection of retained Any content");
            for (var field : value.getAllFields().entrySet()) {
                if (field.getKey().getJavaType() != com.google.protobuf.Descriptors.FieldDescriptor.JavaType.MESSAGE) continue;
                if (field.getKey().isRepeated())
                    for (Object item : (java.util.List<?>) field.getValue()) pending.add((Message) item);
                else pending.add((Message) field.getValue());
            }
        }
        if (DescriptorFingerprints.closure(source).getSerializedSize() > maxDescriptorBytes)
            throw new RenderFailure(RenderFailure.Kind.LIMIT, "Renderer descriptor limit exceeded");
    }

    @Override public void render(Message input, Message options, OutputStream output, Cancellation cancellation)
            throws IOException {
        cancellation.check();
        check(input, options);
        var request = RenderRequest.newBuilder().setCapability(capability)
                .setInputType(source.getFullName())
                .setInputFingerprint(DescriptorFingerprints.fingerprintOf(source))
                .setInputDescriptors(DescriptorFingerprints.closure(source).toByteString())
                .setInput(input.toByteString()).setOptions(options.toByteString()).build();
        var future = stub.withDeadlineAfter(deadline.toNanos(), TimeUnit.NANOSECONDS).render(request);
        try {
            RenderResponse response;
            while (true) {
                cancellation.check();
                try { response = future.get(50, TimeUnit.MILLISECONDS); break; }
                catch (TimeoutException waiting) { /* Poll cancellation while gRPC owns the deadline. */ }
            }
            cancellation.check();
            if (!response.getUnknownFields().asMap().isEmpty() || !response.getCapability().equals(capability))
                throw new RenderFailure(RenderFailure.Kind.INVALID, "Remote renderer returned a different capability");
            if (response.getContent().size() > capability.getMaxOutputBytes())
                throw new RenderFailure(RenderFailure.Kind.LIMIT, "Remote renderer output limit exceeded");
            response.getContent().writeTo(output);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RenderFailure(RenderFailure.Kind.CANCELLED, "Remote rendering interrupted", e);
        } catch (ExecutionException e) {
            Status status = Status.fromThrowable(e.getCause());
            RenderFailure.Kind kind = switch (status.getCode()) {
                case CANCELLED -> RenderFailure.Kind.CANCELLED;
                case UNAVAILABLE, DEADLINE_EXCEEDED -> RenderFailure.Kind.UNAVAILABLE;
                case RESOURCE_EXHAUSTED -> RenderFailure.Kind.LIMIT;
                case PERMISSION_DENIED, UNAUTHENTICATED -> RenderFailure.Kind.DENIED;
                case INVALID_ARGUMENT -> RenderFailure.Kind.INVALID;
                case UNIMPLEMENTED -> RenderFailure.Kind.UNSUPPORTED;
                default -> RenderFailure.Kind.FAILED;
            };
            throw new RenderFailure(kind, "Remote renderer refused or failed: " + status.getCode(), e.getCause());
        } finally {
            if (!future.isDone()) future.cancel(true);
        }
    }
}
