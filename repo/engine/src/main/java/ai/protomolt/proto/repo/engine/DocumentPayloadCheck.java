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
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.function.Function;
import com.google.protobuf.Message;

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
    private final Map<String, DocumentSchemaBinding> resolvedSchemas;

    private DocumentPayloadCheck(DocumentSchemaBinding schema, Any original, DynamicMessage decoded, Map<String, DocumentSchemaBinding> resolvedSchemas) {
        this.schema = schema;
        this.original = original;
        this.decoded = decoded;
        this.resolvedSchemas = Map.copyOf(resolvedSchemas);
    }

    DocumentSchemaBinding schema() { return schema; }
    Any original() { return original; }
    DynamicMessage decoded() { return decoded; }
    Map<String, DocumentSchemaBinding> resolvedSchemas() { return resolvedSchemas; }

    /**
     * The caller supplies every required rule dialect and owns byte/decoded-memory
     * accounting. This verifies values under that validator, not policy completeness,
     * semantic correctness, durable retention, authorization or publication.
     * Present Any payloads require a host-resolved binding; extendable types remain unsupported.
     * Unknown candidate fields remain opaque and are not rule-validated. They retain
     * byte/occurrence bounds; a host must separately decide whether to permit them.
     * Control is cooperative between operations, not a hard CEL execution deadline.
     */
    static DocumentPayloadCheck check(DocumentSchemaBinding schema, Any candidate, String acceptedTypeUrl,
            ProtoValidator validator, Limits limits, Runnable control) throws InvalidProtocolBufferException {
        return check(schema, candidate, acceptedTypeUrl, validator, limits, control, url -> {
            throw new IllegalArgumentException("unresolved Any type URL: " + url);
        });
    }

    /**
     * The host resolves exact URLs using authorized, pinned schema definitions. The
     * resolver may compile available source definitions; it must not choose a mutable
     * latest version or use a simple-name fallback. Each URL is resolved once per check.
     * Returned bindings are in-memory evidence, not persisted per-path admission proof.
     */
    static DocumentPayloadCheck check(DocumentSchemaBinding schema, Any candidate, String acceptedTypeUrl,
            ProtoValidator validator, Limits limits, Runnable control,
            Function<String, DocumentSchemaBinding> resolver) throws InvalidProtocolBufferException {
        Objects.requireNonNull(schema, "schema");
        Objects.requireNonNull(candidate, "candidate");
        Objects.requireNonNull(acceptedTypeUrl, "acceptedTypeUrl");
        Objects.requireNonNull(validator, "validator");
        Objects.requireNonNull(limits, "limits");
        Objects.requireNonNull(control, "control");
        Objects.requireNonNull(resolver, "resolver");
        active(control);
        requireUrl(acceptedTypeUrl, schema);
        if (!candidate.getTypeUrl().equals(acceptedTypeUrl)) {
            throw new IllegalArgumentException("candidate type URL differs from host policy");
        }
        if (!candidate.getUnknownFields().asMap().isEmpty()) {
            throw new IllegalArgumentException("unsupported Any envelope fields");
        }
        var session = new Session(validator, limits, control, resolver);
        session.bindings.put(acceptedTypeUrl, schema);
        DynamicMessage decoded = session.decode(schema, candidate.getValue(), 0);
        return new DocumentPayloadCheck(schema, candidate, decoded, session.bindings);
    }

    private static void requireUrl(String url, DocumentSchemaBinding schema) {
        String suffix = "/" + schema.type().getFullName();
        if (url.length() > 4096 || !url.endsWith(suffix) || url.length() <= suffix.length()) {
            throw new IllegalArgumentException("accepted type URL does not identify the bound schema");
        }
    }

    private static final class Session {
        private final ProtoValidator validator;
        private final Limits limits;
        private final Runnable control;
        private final Function<String, DocumentSchemaBinding> resolver;
        private final MessageWireBudget wire;
        private final Map<String, DocumentSchemaBinding> bindings = new LinkedHashMap<>();
        private final java.util.Set<Descriptor> schemas = Collections.newSetFromMap(new IdentityHashMap<>());
        private long decodedBytes;
        private long schemaFields;
        private long anyCount;

        Session(ProtoValidator validator, Limits limits, Runnable control,
                Function<String, DocumentSchemaBinding> resolver) {
            this.validator = validator;
            this.limits = limits;
            this.control = control;
            this.resolver = resolver;
            this.wire = new MessageWireBudget(limits.maxWireValues(), limits.maxDepth(), () -> active(control));
        }

        DynamicMessage decode(DocumentSchemaBinding schema, com.google.protobuf.ByteString bytes, int depth)
                throws InvalidProtocolBufferException {
            active(control);
            depth(depth);
            if (bytes.size() > limits.maxBytes() - decodedBytes) {
                throw new IllegalArgumentException("aggregate payload bytes exceed limit");
            }
            decodedBytes += bytes.size();
            prepare(schema.type());
            wire.check(bytes, schema.type());
            var input = bytes.newCodedInput();
            input.setRecursionLimit(limits.maxDepth() - depth);
            final DynamicMessage decoded;
            try {
                decoded = DynamicMessage.parseFrom(schema.type(), input);
            } catch (java.io.IOException e) {
                if (e instanceof InvalidProtocolBufferException invalid) throw invalid;
                throw new InvalidProtocolBufferException(e);
            }
            input.checkLastTagWas(0);
            if (input.getTotalBytesRead() != bytes.size()) throw new InvalidProtocolBufferException("incomplete payload decode");
            walk(decoded, depth);
            active(control);
            validator.validate(decoded).throwIfInvalid();
            active(control);
            return decoded;
        }

        void walk(Message message, int depth) throws InvalidProtocolBufferException {
            active(control);
            depth(depth);
            Descriptor type = message.getDescriptorForType();
            if (type.getFullName().equals("google.protobuf.Any")) {
                if (++anyCount > limits.maxWireValues()) throw new IllegalArgumentException("Any occurrence limit exceeded");
                if (!message.getUnknownFields().asMap().isEmpty()) throw new IllegalArgumentException("unsupported Any envelope fields");
                var urlField = type.findFieldByNumber(1);
                var valueField = type.findFieldByNumber(2);
                if (urlField == null || valueField == null || urlField.isRepeated() || valueField.isRepeated()
                        || urlField.getType() != FieldDescriptor.Type.STRING || valueField.getType() != FieldDescriptor.Type.BYTES) {
                    throw new IllegalArgumentException("invalid Any envelope descriptor");
                }
                String url = (String) message.getField(urlField);
                if (url.isEmpty() || url.length() > 4096) throw new IllegalArgumentException("invalid Any type URL");
                depth(depth + 1);
                DocumentSchemaBinding binding = bindings.get(url);
                if (binding == null) {
                    binding = resolver.apply(url);
                    active(control);
                    if (binding == null) throw new IllegalArgumentException("unresolved Any type URL: " + url);
                    requireUrl(url, binding);
                    bindings.put(url, binding);
                }
                decode(binding, (com.google.protobuf.ByteString) message.getField(valueField), depth + 1);
                return;
            }
            for (var entry : message.getAllFields().entrySet()) {
                active(control);
                var field = entry.getKey();
                if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) continue;
                if (field.isRepeated()) {
                    for (Object item : (java.util.List<?>) entry.getValue()) walk((Message) item, depth + 1);
                } else walk((Message) entry.getValue(), depth + 1);
            }
        }

        void depth(int depth) {
            if (depth > limits.maxDepth()) throw new IllegalArgumentException("aggregate payload depth exceeds limit");
        }

        void prepare(Descriptor root) {
            if (schemas.contains(root)) return;
            var pending = new ArrayList<Descriptor>();
            add(root, pending);
            for (int i = 0; i < pending.size(); i++) {
                active(control);
                var type = pending.get(i);
                if (type.toProto().getExtensionRangeCount() != 0) {
                    throw new UnsupportedOperationException("payload schema needs extension resolution");
                }
                schemaFields += type.getFields().size();
                if (schemaFields > limits.maxSchemaFields()) throw new IllegalArgumentException("schema field count exceeds limit");
                for (var field : type.getFields()) {
                    if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE) add(field.getMessageType(), pending);
                }
            }
            validator.prepareSchema(root, limits.maxSchemaTypes(), limits.maxSchemaFields(), () -> active(control));
        }

        void add(Descriptor type, ArrayList<Descriptor> pending) {
            if (schemas.contains(type)) return;
            if (schemas.size() == limits.maxSchemaTypes()) throw new IllegalArgumentException("schema message count exceeds limit");
            schemas.add(type);
            pending.add(type);
        }
    }

    private static void active(Runnable control) {
        if (Thread.currentThread().isInterrupted()) {
            throw new java.util.concurrent.CancellationException("payload check interrupted");
        }
        control.run();
    }

}
