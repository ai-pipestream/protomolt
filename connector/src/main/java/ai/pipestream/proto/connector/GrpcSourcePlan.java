package ai.pipestream.proto.connector;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.Metadata;

import java.util.Objects;

/**
 * What a {@link GrpcStreamSource} needs: an open channel, a server-streaming method
 * resolved from descriptors, and the request message.
 */
public record GrpcSourcePlan(Channel channel,
                             Descriptors.MethodDescriptor method,
                             DynamicMessage request,
                             CallOptions options,
                             Metadata headers) implements SourcePlan {

    public GrpcSourcePlan {
        Objects.requireNonNull(channel, "channel");
        Objects.requireNonNull(method, "method");
        Objects.requireNonNull(request, "request");
        options = options == null ? CallOptions.DEFAULT : options;
        headers = headers == null ? new Metadata() : headers;
    }

    public GrpcSourcePlan(Channel channel, Descriptors.MethodDescriptor method, DynamicMessage request) {
        this(channel, method, request, CallOptions.DEFAULT, new Metadata());
    }
}
