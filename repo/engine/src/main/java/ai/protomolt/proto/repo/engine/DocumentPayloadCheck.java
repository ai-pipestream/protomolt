package ai.protomolt.proto.repo.engine;

import ai.protomolt.proto.descriptors.MessageWireBudget;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.Any;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Objects;

/** Internal candidate check under a trusted host's explicit validator and URL policy. */
final class DocumentPayloadCheck {
    record Limits(int maxBytes, long maxWireValues, int maxDepth, int maxSchemaTypes, int maxSchemaFields) {
        Limits {
            if (maxBytes < 1 || maxWireValues < 1 || maxDepth < 1 || maxDepth > 100
                    || maxSchemaTypes < 1 || maxSchemaFields < 1) {
                throw new IllegalArgumentException("positive payload/schema limits and depth at most 100 required");
            }
        }
    }

    private final DocumentSchemaBinding schema;
    private final Any original;
    private final DynamicMessage decoded;

    private DocumentPayloadCheck(DocumentSchemaBinding schema, Any original, DynamicMessage decoded) {
        this.schema = schema;
        this.original = original;
        this.decoded = decoded;
    }

    DocumentSchemaBinding schema() { return schema; }
    Any original() { return original; }
    DynamicMessage decoded() { return decoded; }

    /**
     * The caller supplies every required rule dialect and owns byte/decoded-memory
     * accounting. This verifies values under that validator, not policy completeness,
     * semantic correctness, durable retention, authorization or publication.
     * Nested Any and extendable payload types are unsupported until separately resolved.
     * Unknown candidate fields remain opaque and are not rule-validated. They retain
     * byte/occurrence bounds; a host must separately decide whether to permit them.
     * Control is cooperative between operations, not a hard CEL execution deadline.
     */
    static DocumentPayloadCheck check(DocumentSchemaBinding schema, Any candidate, String acceptedTypeUrl,
            ProtoValidator validator, Limits limits, Runnable control) throws InvalidProtocolBufferException {
        Objects.requireNonNull(schema, "schema");
        Objects.requireNonNull(candidate, "candidate");
        Objects.requireNonNull(acceptedTypeUrl, "acceptedTypeUrl");
        Objects.requireNonNull(validator, "validator");
        Objects.requireNonNull(limits, "limits");
        Objects.requireNonNull(control, "control");
        active(control);
        String suffix = "/" + schema.type().getFullName();
        if (acceptedTypeUrl.length() > 4096 || !acceptedTypeUrl.endsWith(suffix)
                || acceptedTypeUrl.length() <= suffix.length()) {
            throw new IllegalArgumentException("accepted type URL does not identify the bound schema");
        }
        if (!candidate.getTypeUrl().equals(acceptedTypeUrl)) {
            throw new IllegalArgumentException("candidate type URL differs from host policy");
        }
        if (!candidate.getUnknownFields().asMap().isEmpty()) {
            throw new IllegalArgumentException("unsupported Any envelope fields");
        }
        var bytes = candidate.getValue();
        if (bytes.size() > limits.maxBytes()) throw new IllegalArgumentException("payload bytes exceed limit");
        requireSupportedSchema(schema.type(), limits, control);
        validator.prepareSchema(schema.type(), limits.maxSchemaTypes(), limits.maxSchemaFields(), () -> active(control));
        new MessageWireBudget(limits.maxWireValues(), limits.maxDepth(), control).check(bytes, schema.type());
        active(control);
        var input = bytes.newCodedInput();
        input.setRecursionLimit(limits.maxDepth());
        final DynamicMessage decoded;
        try {
            decoded = DynamicMessage.parseFrom(schema.type(), input);
        } catch (java.io.IOException e) {
            if (e instanceof InvalidProtocolBufferException invalid) throw invalid;
            throw new InvalidProtocolBufferException(e);
        }
        input.checkLastTagWas(0);
        if (input.getTotalBytesRead() != bytes.size()) {
            throw new InvalidProtocolBufferException("incomplete payload decode");
        }
        active(control);
        validator.validate(decoded).throwIfInvalid();
        active(control);
        return new DocumentPayloadCheck(schema, candidate, decoded);
    }

    private static void active(Runnable control) {
        if (Thread.currentThread().isInterrupted()) {
            throw new java.util.concurrent.CancellationException("payload check interrupted");
        }
        control.run();
    }

    private static void requireSupportedSchema(Descriptor root, Limits limits, Runnable control) {
        var seen = Collections.newSetFromMap(new IdentityHashMap<Descriptor, Boolean>());
        var types = new ArrayList<Descriptor>();
        seen.add(root);
        types.add(root);
        long fields = 0;
        for (int i = 0; i < types.size(); i++) {
            active(control);
            Descriptor type = types.get(i);
            if (type.getFullName().equals("google.protobuf.Any") || type.toProto().getExtensionRangeCount() != 0) {
                throw new UnsupportedOperationException("payload schema needs nested Any or extension resolution");
            }
            fields += type.getFields().size();
            if (fields > limits.maxSchemaFields()) throw new IllegalArgumentException("schema field count exceeds limit");
            for (FieldDescriptor field : type.getFields()) {
                active(control);
                if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) continue;
                Descriptor child = field.getMessageType();
                if (seen.contains(child)) continue;
                if (types.size() == limits.maxSchemaTypes()) throw new IllegalArgumentException("schema message count exceeds limit");
                seen.add(child);
                types.add(child);
            }
        }
    }
}
