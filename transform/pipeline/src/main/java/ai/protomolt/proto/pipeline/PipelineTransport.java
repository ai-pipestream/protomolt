package ai.protomolt.proto.pipeline;

import ai.protomolt.proto.grpc.workflow.v1.ServiceDependency;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.DynamicMessage;

import java.util.List;
import java.util.Objects;

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
     * Execution identity for a live step call. Ordinary in-process runs supply an empty
     * {@code jobId}; durable coordinators bind the job UUID, step name, optional fan-out
     * branch ID (such as {@code step#0}), and 1-based execution attempt so downstream
     * side-effecting services can deduplicate replayed calls on {@link #operationId()}.
     *
     * @param jobId durable job identifier, or empty for in-process execution
     * @param stepName pipeline step name
     * @param branchId fan-out branch identifier ({@code step#index}), or empty for non-fan-out calls
     * @param attempt 1-based durable claim attempt, or {@code 0} for in-process execution
     */
    record CallContext(String jobId, String stepName, String branchId, int attempt) {
        public CallContext {
            jobId = jobId == null ? "" : jobId;
            Objects.requireNonNull(stepName, "stepName");
            branchId = branchId == null ? "" : branchId;
            if (attempt < 0) {
                throw new IllegalArgumentException("attempt must not be negative");
            }
        }

        /** Builds a call context for ordinary in-process execution without a durable job ID. */
        public static CallContext inProcess(String stepName, String branchId) {
            return new CallContext("", stepName, branchId, 0);
        }

        /**
         * Stable operation key ({@code jobId/stepName} or {@code jobId/step#index}) when running
         * under a durable job coordinator, or empty when running in process.
         */
        public String operationId() {
            if (jobId.isEmpty()) {
                return "";
            }
            return jobId + "/" + (branchId.isEmpty() ? stepName : branchId);
        }
    }

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

    /**
     * Invokes one method with step and durable job execution identity. Implementations that
     * do not inspect {@code context} inherit delegation to
     * {@link #invoke(ServiceDependency, MethodDescriptor, List, long, int)}.
     *
     * @param context step and durable job execution identity
     * @param dependency the declared profile/endpoint reference
     * @param method resolved method descriptor
     * @param requests one request for unary/server-streaming, many for client/bidi
     * @param deadlineMillis effective remaining step deadline
     * @param maxResponses hard response bound; implementations must never return more
     */
    default List<DynamicMessage> invoke(CallContext context, ServiceDependency dependency,
                                        MethodDescriptor method, List<DynamicMessage> requests,
                                        long deadlineMillis, int maxResponses) {
        return invoke(dependency, method, requests, deadlineMillis, maxResponses);
    }
}
