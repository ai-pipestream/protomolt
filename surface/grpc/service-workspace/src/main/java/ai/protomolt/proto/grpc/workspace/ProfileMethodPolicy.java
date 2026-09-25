package ai.protomolt.proto.grpc.workspace;

import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.grpc.profile.v1.MethodPolicy;
import ai.protomolt.proto.grpc.profile.v1.Operation;
import ai.protomolt.proto.grpc.profile.v1.ServiceProfile;
import java.time.Duration;

/** The same approval and deadline decision for profile invocation and reflected verbs. */
final class ProfileMethodPolicy {
    private ProfileMethodPolicy() {}

    static MethodPolicy find(ServiceProfile profile, String method) {
        return profile.getMethodPoliciesList().stream()
                .filter(candidate -> candidate.getMethod().equals(method))
                .findFirst().orElse(null);
    }

    static void requireAllowed(MethodPolicy policy, String method) throws ActionException {
        if (policy != null && (policy.getApprovalRequired()
                || policy.getOperationList().contains(Operation.OPERATION_APPROVAL_REQUIRED))) {
            throw new ActionException("approval-required",
                    "method '" + method + "' requires approval outside this action");
        }
    }

    static int deadline(MethodPolicy policy, int asked) throws ActionException {
        boolean supplied = asked != 0;
        int requested = supplied ? asked : ServiceActionSupport.DEFAULT_DEADLINE_MS;
        if (policy == null || policy.getDeadline().equals(
                com.google.protobuf.Duration.getDefaultInstance())) return requested;
        Duration configured = Duration.ofSeconds(policy.getDeadline().getSeconds(),
                policy.getDeadline().getNanos());
        long configuredMs = Math.max(1, configured.toMillis());
        if (configuredMs > Integer.MAX_VALUE) {
            throw new ActionException("invalid-profile",
                    "method deadline exceeds the supported millisecond range");
        }
        if (supplied && requested > configuredMs) {
            throw ServiceActionSupport.invalid("'deadlineMs' exceeds the registered method policy",
                    "/deadlineMs");
        }
        return supplied ? requested : Math.toIntExact(configuredMs);
    }
}
