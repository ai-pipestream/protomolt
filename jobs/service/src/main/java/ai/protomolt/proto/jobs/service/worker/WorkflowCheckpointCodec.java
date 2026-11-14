package ai.protomolt.proto.jobs.service.worker;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.http.json.MalformedProtobufJsonException;
import ai.protomolt.proto.workflow.WorkflowRunner;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.Base64;

/**
 * Binary-faithful encoding and decoding for persisted {@link WorkflowRunner.Checkpoint} entries.
 *
 * <p>Stores canonical protobuf wire bytes in {@code responseBase64} alongside human-inspectable
 * proto3 JSON in {@code response}. Rebuilding checkpoints prefers {@code responseBase64} so step
 * outputs retain unknown fields, raw {@code bytes}, full-range {@code uint64}/{@code fixed64}
 * values, and {@code google.protobuf.Any} payloads across worker restarts and retries without lossy
 * JSON round-tripping, while remaining backward-compatible with legacy JSON-only checkpoints.
 */
final class WorkflowCheckpointCodec {
    static final String RESPONSE_FIELD = "response";
    static final String RESPONSE_BASE64_FIELD = "responseBase64";

    private WorkflowCheckpointCodec() {}

    static void writeResponse(
            ObjectNode entry, WorkflowRunner.Checkpoint checkpoint, ActionContext context) {
        if (checkpoint.response() == null) {
            return;
        }
        entry.put(
                RESPONSE_BASE64_FIELD,
                Base64.getEncoder().encodeToString(checkpoint.response().toByteArray()));
        try {
            entry.set(
                    RESPONSE_FIELD,
                    context.objectMapper()
                            .readTree(context.transcoder().toJson(checkpoint.response())));
        } catch (Exception e) {
            // If a response holds an unregistered google.protobuf.Any type URL that proto3 JSON
            // cannot render, preserve null in the auxiliary JSON preview while keeping the
            // authoritative binary wire payload in responseBase64.
            entry.putNull(RESPONSE_FIELD);
        }
    }

    static WorkflowRunner.Checkpoint readCheckpoint(
            String name,
            boolean skipped,
            JsonNode entry,
            Descriptors.Descriptor outputType,
            ActionContext context)
            throws CorruptCheckpointException {
        if (skipped) {
            return new WorkflowRunner.Checkpoint(name, true, null);
        }
        JsonNode base64Node = entry.get(RESPONSE_BASE64_FIELD);
        if (base64Node != null && !base64Node.isNull()) {
            if (!base64Node.isTextual()) {
                throw new CorruptCheckpointException(
                        "checkpoint '" + name + "' responseBase64 is not a base64 string");
            }
            byte[] wire;
            try {
                wire = Base64.getDecoder().decode(base64Node.asText());
            } catch (IllegalArgumentException e) {
                throw new CorruptCheckpointException(
                        "checkpoint '"
                                + name
                                + "' responseBase64 is not valid base64: "
                                + e.getMessage());
            }
            try {
                return new WorkflowRunner.Checkpoint(
                        name, false, DynamicMessage.parseFrom(outputType, wire));
            } catch (InvalidProtocolBufferException e) {
                throw new CorruptCheckpointException(
                        "checkpoint '"
                                + name
                                + "' responseBase64 is not valid protobuf wire for "
                                + outputType.getFullName()
                                + ": "
                                + e.getMessage());
            }
        }
        JsonNode response = entry.get(RESPONSE_FIELD);
        if (response == null || response.isNull()) {
            return new WorkflowRunner.Checkpoint(name, false, null);
        }
        try {
            return new WorkflowRunner.Checkpoint(
                    name,
                    false,
                    context.transcoder().fromJsonDynamic(response.toString(), outputType));
        } catch (MalformedProtobufJsonException e) {
            throw new CorruptCheckpointException(
                    "checkpoint '"
                            + name
                            + "' response is not valid proto3 JSON for "
                            + outputType.getFullName()
                            + ": "
                            + e.getMessage());
        }
    }

    static final class CorruptCheckpointException extends Exception {
        CorruptCheckpointException(String message) {
            super(message);
        }
    }
}
