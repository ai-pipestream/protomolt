package ai.protomolt.proto.mesh;

import ai.protomolt.proto.mesh.v1.EntityEnvelope;
import ai.protomolt.proto.mesh.v1.EntityState;
import ai.protomolt.proto.mesh.v1.EntityStatus;
import ai.protomolt.proto.mesh.v1.TerminalState;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.ValidationResult;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;

import java.time.Instant;
import java.util.stream.Collectors;

/**
 * The fail-fast validation layer of the mesh contract gate.
 *
 * <p>The documented public path for admitting an entity at a routing, persistence, or
 * processor-execution boundary is {@link MeshGate#admit}, which composes these checks with
 * deadline expiry and schema-identity resolution so no caller can accidentally run only a
 * subset. The methods here remain public for the finer-grained internal callers (the resolver,
 * status-record writers) that legitimately need one layer at a time.
 *
 * <p>Validation composes three layers. Well-known-type timestamp validity runs first (an
 * out-of-range Timestamp must be rejected before any CEL comparison sees it). Then the
 * contract's own {@code validate.v1} annotations (field bounds plus the message-level CEL rules:
 * exactly one body, schema identity agreement, parent/depth consistency, deadline order) run
 * through {@link ProtoValidator}. Then the checks annotations cannot express run here: the
 * payload digest and length against the exact bytes, inline type-URL shape and agreement with
 * the schema, and deadline expiry against the caller's clock.
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
     * Validates everything about an entity except deadline expiry: well-known-type timestamp
     * validity, the validate.v1 annotations, then digest and length agreement with the exact
     * payload bytes and inline type-URL shape and agreement with the schema. Expiry is
     * deliberately separate: it is a temporal judgment a boundary makes against its own clock,
     * not a structural property, and schema resolution must work on expired entities (for
     * example when rehydrating evidence after the fact).
     *
     * @param entity the entity to validate
     */
    public static void validateStructure(EntityEnvelope entity) {
        require(entity != null, "entity must not be null");
        validateTimestamp(entity.getHeader().getCreatedAt(), "header.created_at");
        if (entity.getHeader().hasDeadline()) {
            validateTimestamp(entity.getHeader().getDeadline(), "header.deadline");
        }
        if (entity.hasClaimCheck() && entity.getClaimCheck().hasExpiresAt()) {
            validateTimestamp(entity.getClaimCheck().getExpiresAt(), "claim_check.expires_at");
        }
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
     * Validates a mutable entity status record: timestamp validity, annotations, plus the
     * terminal consistency the schema documents (terminal fields set exactly when the state is
     * TERMINAL).
     *
     * @param status the status record to validate
     */
    public static void validate(EntityStatus status) {
        require(status != null, "status must not be null");
        if (status.hasTerminalAt()) {
            validateTimestamp(status.getTerminalAt(), "status.terminal_at");
        }
        validateTimestamp(status.getUpdatedAt(), "status.updated_at");
        validateAnnotations(status);
        boolean terminal = status.getState() == EntityState.ENTITY_STATE_TERMINAL;
        require(terminal == (status.getTerminalState() != TerminalState.TERMINAL_STATE_UNSPECIFIED),
                "status.terminal_state must be set exactly when status.state is TERMINAL");
        require(terminal == status.hasTerminalAt(),
                "status.terminal_at must be set exactly when status.state is TERMINAL");
    }

    /**
     * Validates a schema reference: protobuf message full name plus a well-formed canonical
     * fingerprint (and, when present, a resolvable registry URI).
     *
     * @param schema the schema reference to validate
     */
    public static void validate(ai.protomolt.proto.mesh.v1.SchemaReference schema) {
        require(schema != null, "schema must not be null");
        validateAnnotations(schema);
    }

    /**
     * The segment after the final slash of an {@code Any} type URL: the fully qualified type
     * name. Protobuf requires at least one slash and a nonempty type name after the final
     * slash; a leading-slash URL (an empty host) is a valid URI reference and is accepted.
     */
    static String typeNameOf(String typeUrl) {
        int slash = typeUrl.lastIndexOf('/');
        require(slash >= 0,
                "payload.type_url must contain a slash: '" + typeUrl + "'");
        require(slash < typeUrl.length() - 1,
                "payload.type_url must end with the protobuf message full name: '"
                        + typeUrl + "'");
        return typeUrl.substring(slash + 1);
    }

    /**
     * Rejects a {@link Timestamp} that violates the well-known-type validity rules (seconds
     * within 0001-01-01T00:00:00Z..9999-12-31T23:59:59Z, nanos within [0, 999999999]).
     */
    private static void validateTimestamp(Timestamp value, String field) {
        require(Timestamps.isValid(value), field + " must be a valid protobuf Timestamp");
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
