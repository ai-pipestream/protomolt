package ai.pipestream.proto.mesh;

import ai.pipestream.proto.mesh.v1.EntityEnvelope;
import ai.pipestream.proto.mesh.v1.EntityState;
import ai.pipestream.proto.mesh.v1.EntityStatus;
import ai.pipestream.proto.mesh.v1.TerminalState;
import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.ValidationResult;

import java.time.Instant;
import java.util.stream.Collectors;

/**
 * The mesh contract gate: the fail-fast validation entry point every routing, persistence, and
 * processor-execution boundary calls before touching an entity.
 *
 * <p>Validation composes two layers. First the contract's own {@code validate.v1} annotations
 * (field bounds plus the message-level CEL rules: exactly one body, schema identity agreement,
 * parent/depth consistency, deadline order) run through {@link ProtoValidator}. Then the checks
 * annotations cannot express run here: the payload digest and length against the exact bytes, and
 * deadline expiry against the caller's clock.
 *
 * <p>Every failure throws {@link IllegalArgumentException} with a field-precise message; nothing
 * partially valid crosses the gate.
 */
public final class MeshValidation {

    private MeshValidation() {
    }

    /**
     * Validates an entity against the wall clock. Prefer
     * {@link #validate(EntityEnvelope, Instant)} where a clock is injectable.
     *
     * @param entity the entity to validate
     */
    public static void validate(EntityEnvelope entity) {
        validate(entity, Instant.now());
    }

    /**
     * Validates an entity: annotations first, then digest/length agreement with the exact bytes
     * and deadline expiry against {@code now}. This is the method a router, repository, or
     * processor must call before acting on an entity.
     *
     * @param entity the entity to validate
     * @param now the instant expiry is judged against
     */
    public static void validate(EntityEnvelope entity, Instant now) {
        validateStructure(entity);
        if (entity.getHeader().hasDeadline()) {
            Instant deadline = Instant.ofEpochSecond(entity.getHeader().getDeadline().getSeconds(),
                    entity.getHeader().getDeadline().getNanos());
            require(deadline.isAfter(now),
                    "header.deadline (" + deadline + ") has passed at " + now
                            + "; the entity is expired");
        }
    }

    /**
     * Validates everything about an entity except deadline expiry: the validate.v1 annotations,
     * then digest and length agreement with the exact payload bytes and inline type-URL agreement
     * with the schema. Expiry is deliberately separate: it is a temporal judgment a boundary
     * makes against its own clock, not a structural property, and schema resolution must work on
     * expired entities (for example when rehydrating evidence after the fact).
     *
     * @param entity the entity to validate
     */
    public static void validateStructure(EntityEnvelope entity) {
        require(entity != null, "entity must not be null");
        validateAnnotations(entity);
        long payloadLength = entity.getHeader().getPayloadLength();
        String payloadDigest = entity.getHeader().getPayloadDigest();
        if (entity.hasPayload()) {
            byte[] bytes = entity.getPayload().getValue().toByteArray();
            require(payloadLength == bytes.length,
                    "header.payload_length is " + payloadLength
                            + " but the inline payload serializes to " + bytes.length
                            + " bytes");
            String actual = MeshDigest.sha256(bytes);
            require(payloadDigest.equals(actual),
                    "header.payload_digest does not match the inline payload SHA-256");
            String typeName = typeNameOf(entity.getPayload().getTypeUrl());
            require(typeName.equals(entity.getSchema().getTypeName()),
                    "payload.type_url names '" + typeName + "' but schema.type_name is '"
                            + entity.getSchema().getTypeName() + "'");
        } else if (entity.hasClaimCheck()) {
            var artifact = entity.getClaimCheck().getArtifact();
            require(payloadLength == artifact.getSizeBytes(),
                    "header.payload_length is " + payloadLength
                            + " but claim_check.artifact.size_bytes is "
                            + artifact.getSizeBytes());
            require(payloadDigest.equals(artifact.getSha256()),
                    "header.payload_digest does not match claim_check.artifact.sha256");
        } else {
            require(false, "entity must carry an inline payload or a claim check");
        }
    }

    /**
     * Validates a mutable entity status record: annotations plus the terminal consistency the
     * schema documents (terminal fields set exactly when the state is TERMINAL).
     *
     * @param status the status record to validate
     */
    public static void validate(EntityStatus status) {
        require(status != null, "status must not be null");
        validateAnnotations(status);
        boolean terminal = status.getState() == EntityState.ENTITY_STATE_TERMINAL;
        require(terminal == (status.getTerminalState() != TerminalState.TERMINAL_STATE_UNSPECIFIED),
                "status.terminal_state must be set exactly when status.state is TERMINAL");
        require(terminal == status.hasTerminalAt(),
                "status.terminal_at must be set exactly when status.state is TERMINAL");
    }

    /**
     * Validates a schema reference: fully qualified type name plus a well-formed canonical
     * fingerprint (and, when present, a resolvable registry URI).
     *
     * @param schema the schema reference to validate
     */
    public static void validate(ai.pipestream.proto.mesh.v1.SchemaReference schema) {
        require(schema != null, "schema must not be null");
        validateAnnotations(schema);
    }

    /** The last path segment of an {@code Any} type URL: the fully qualified type name. */
    static String typeNameOf(String typeUrl) {
        int slash = typeUrl.lastIndexOf('/');
        return slash < 0 ? typeUrl : typeUrl.substring(slash + 1);
    }

    private static void validateAnnotations(com.google.protobuf.Message message) {
        ValidationResult result = ProtoValidator.forMessageType(message.getDescriptorForType())
                .validate(message);
        if (!result.valid()) {
            throw new IllegalArgumentException("entity fails the mesh contract annotations: "
                    + result.violations().stream()
                    .map(v -> "[" + v.path() + "] " + v.ruleId() + ": " + v.message())
                    .collect(Collectors.joining("; ")));
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
