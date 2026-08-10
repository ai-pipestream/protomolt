package ai.pipestream.proto.chain;

import ai.pipestream.proto.grpc.profile.ServiceProfileValidation;
import ai.pipestream.proto.prompt.PromptPacket;
import ai.pipestream.proto.prompt.PromptRenderer;
import ai.pipestream.proto.prompt.RenderPromptRequest;
import com.google.protobuf.Descriptors.Descriptor;

import java.nio.charset.StandardCharsets;

/**
 * Recomputes the prompt and schema fingerprints of a structured-generation step
 * offline: the same persona-free prompt packet the coordinator renders, hashed.
 * Recording and replay both derive evidence fingerprints here, so a drifted
 * renderer or schema surfaces identically on both sides. Raw instructions and
 * schema text are never persisted - only their lowercase SHA-256 hex.
 */
final class StructuredProvenance {

    private StructuredProvenance() {
    }

    /** Lowercase SHA-256 hex of the persona-free rendered instructions. */
    static String promptFingerprint(Descriptor targetType) {
        return sha256Hex(render(targetType).getInstructions());
    }

    /** Lowercase SHA-256 hex of the persona-free response JSON Schema. */
    static String schemaFingerprint(Descriptor targetType) {
        return sha256Hex(render(targetType).getResponseJsonSchema());
    }

    private static PromptPacket render(Descriptor targetType) {
        return PromptRenderer.create().render(targetType,
                RenderPromptRequest.newBuilder()
                        .setTargetType(targetType.getFullName())
                        .build(),
                targetType.getFile().getFullName());
    }

    private static String sha256Hex(String text) {
        return ServiceProfileValidation.sha256(text.getBytes(StandardCharsets.UTF_8));
    }
}
