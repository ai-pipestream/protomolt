package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.delegation.v1.TaskSpec;
import ai.protomolt.proto.workflow.authoring.v1.StartWorkflowAuthoringRequest;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowAuthoringTemplate;
import com.google.protobuf.Duration;
import com.google.protobuf.Message;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/** Versioned admission fingerprints; storage custody and atomic replay belong to the coordinator. */
final class WorkflowAuthoringStartBinding {
    private static final byte[] DOMAIN =
            "protomolt.workflow-authoring-start.v1".getBytes(StandardCharsets.UTF_8);

    private WorkflowAuthoringStartBinding() {}

    static String templateSha256(WorkflowAuthoringTemplate renderedTemplate) {
        WorkflowLaunchValidation.validate(renderedTemplate);
        return WorkflowLaunchValidation.sha256(renderedTemplate);
    }

    static String sha256(StartWorkflowAuthoringRequest request, TaskSpec renderedSpec, Duration lease) {
        WorkflowLaunchValidation.validate(request);
        WorkflowLaunchValidation.validate(renderedSpec);
        if (!request.getObjective().equals(renderedSpec.getObjective())) {
            throw new IllegalArgumentException("start objective differs from offered spec");
        }
        // The configured template uses whole seconds, bounded by the entry contract.
        if (!lease.getUnknownFields().asMap().isEmpty() || lease.getNanos() != 0
                || lease.getSeconds() < 1 || lease.getSeconds() > 86_400) {
            throw new IllegalArgumentException("start lease is outside the template contract");
        }
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(DOMAIN);
            part(digest, request);
            part(digest, renderedSpec);
            part(digest, lease);
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    private static void part(MessageDigest digest, Message message) {
        byte[] bytes = WorkflowLaunchValidation.deterministicBytes(message);
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
        digest.update(bytes);
    }
}
