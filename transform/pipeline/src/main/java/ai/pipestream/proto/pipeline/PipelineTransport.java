package ai.pipestream.proto.pipeline;

import ai.pipestream.proto.grpc.recipe.v1.ServiceDependency;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.DynamicMessage;

import java.util.List;

/**
 * Host-owned live transport used by {@link PipelineExecutor}. The persisted pipeline names a
 * service profile and endpoint, never a raw target or credential. A host resolves that opaque
 * dependency, applies its channel policy and credentials, and invokes the descriptor-derived
 * method through this seam. Implementations must be thread-safe: bounded fan-out invokes
 * branches concurrently on virtual threads.
 */
@FunctionalInterface
public interface PipelineTransport {

    /**
     * Invokes one method with its complete finite request lane.
     *
     * @param dependency the declared profile/endpoint reference
     * @param method resolved method descriptor
     * @param requests one request for unary/server-streaming, many for client/bidi
     * @param deadlineMillis effective remaining step deadline
     * @param maxResponses hard response bound; implementations must never return more
     */
    List<DynamicMessage> invoke(ServiceDependency dependency, MethodDescriptor method,
                                List<DynamicMessage> requests, long deadlineMillis,
                                int maxResponses);
}
