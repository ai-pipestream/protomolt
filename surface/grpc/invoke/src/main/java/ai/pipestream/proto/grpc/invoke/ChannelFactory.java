package ai.pipestream.proto.grpc.invoke;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

/**
 * Opens the channel for a verb invocation. The default honors the verbs' {@code tls} input:
 * plaintext unless the caller asks for TLS (system trust roots).
 */
@FunctionalInterface
public interface ChannelFactory {

    ManagedChannel open(String target, boolean tls);

    static ChannelFactory standard() {
        return (target, tls) -> {
            ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forTarget(target);
            if (!tls) {
                builder.usePlaintext();
            }
            return builder.build();
        };
    }
}
