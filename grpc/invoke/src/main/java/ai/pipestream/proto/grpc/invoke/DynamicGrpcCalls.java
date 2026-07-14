package ai.pipestream.proto.grpc.invoke;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.MetadataUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * gRPC calls driven entirely by descriptors: the wire {@link MethodDescriptor} is built from a
 * protobuf {@link Descriptors.MethodDescriptor} with {@link DynamicMessage} marshallers, so any
 * method a descriptor set describes is callable with no generated stubs. This is the machinery
 * behind the {@code grpc-invoke} action, usable on its own as a library.
 */
public final class DynamicGrpcCalls {

    private DynamicGrpcCalls() {
    }

    /** The wire-level method descriptor for a protobuf method, marshalling dynamic messages. */
    public static MethodDescriptor<DynamicMessage, DynamicMessage> methodDescriptor(
            Descriptors.MethodDescriptor method) {
        return MethodDescriptor.<DynamicMessage, DynamicMessage>newBuilder()
                .setType(methodType(method))
                .setFullMethodName(MethodDescriptor.generateFullMethodName(
                        method.getService().getFullName(), method.getName()))
                .setRequestMarshaller(marshaller(method.getInputType()))
                .setResponseMarshaller(marshaller(method.getOutputType()))
                .build();
    }

    /** The gRPC call type of a protobuf method. */
    public static MethodDescriptor.MethodType methodType(Descriptors.MethodDescriptor method) {
        if (method.isClientStreaming()) {
            return method.isServerStreaming()
                    ? MethodDescriptor.MethodType.BIDI_STREAMING
                    : MethodDescriptor.MethodType.CLIENT_STREAMING;
        }
        return method.isServerStreaming()
                ? MethodDescriptor.MethodType.SERVER_STREAMING
                : MethodDescriptor.MethodType.UNARY;
    }

    /**
     * Invokes a unary or server-streaming method and returns the responses in order, at most
     * {@code maxResponses} for a stream.
     *
     * @throws io.grpc.StatusRuntimeException on any non-OK status
     * @throws IllegalArgumentException       for client-streaming or bidi methods
     */
    public static List<DynamicMessage> call(Channel channel,
                                            Descriptors.MethodDescriptor method,
                                            DynamicMessage request,
                                            CallOptions options,
                                            Metadata headers,
                                            int maxResponses) {
        if (method.isClientStreaming()) {
            throw new IllegalArgumentException(
                    "Method " + method.getFullName() + " is "
                            + methodType(method).name().toLowerCase().replace('_', '-')
                            + "; only unary and server-streaming methods can be invoked with a single request");
        }
        Channel decorated = headers.keys().isEmpty()
                ? channel
                : io.grpc.ClientInterceptors.intercept(
                        channel, MetadataUtils.newAttachHeadersInterceptor(headers));
        MethodDescriptor<DynamicMessage, DynamicMessage> descriptor = methodDescriptor(method);
        if (!method.isServerStreaming()) {
            return List.of(ClientCalls.blockingUnaryCall(decorated, descriptor, options, request));
        }
        // The size check runs before hasNext(): on an open-ended stream (a Watch-style method
        // that never completes), hasNext() blocks for the next message, so checking it after
        // the cap is reached would hang until the deadline. Cancelling the context on exit
        // terminates the RPC server-side when the cap cut the stream short.
        Context.CancellableContext context = Context.current().withCancellation();
        try {
            return context.call(() -> {
                Iterator<DynamicMessage> stream = ClientCalls.blockingServerStreamingCall(
                        decorated, descriptor, options, request);
                List<DynamicMessage> responses = new ArrayList<>();
                while (responses.size() < maxResponses && stream.hasNext()) {
                    responses.add(stream.next());
                }
                return responses;
            });
        } catch (io.grpc.StatusRuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            context.cancel(null);
        }
    }

    private static MethodDescriptor.Marshaller<DynamicMessage> marshaller(
            Descriptors.Descriptor type) {
        return new MethodDescriptor.Marshaller<>() {
            @Override
            public InputStream stream(DynamicMessage value) {
                return value.toByteString().newInput();
            }

            @Override
            public DynamicMessage parse(InputStream stream) {
                try {
                    return DynamicMessage.parseFrom(type, stream);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        };
    }
}
