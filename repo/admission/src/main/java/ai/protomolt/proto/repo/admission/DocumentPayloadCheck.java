package ai.protomolt.proto.repo.admission;

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
    record SchemaKey(String typeUrl, String artifactSha256) {}
    /** Host context and immutable version selection belong to the resolver, not payload bytes. */
    record ResolutionRequest(String typeUrl, java.util.List<DocumentSchemaOccurrences.Step> prefix,
            String valueSha256, long valueSizeBytes) {
        ResolutionRequest { prefix = java.util.List.copyOf(prefix); }
    }
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
    private final Map<SchemaKey, DocumentSchemaBinding> resolvedSchemas;
    private final java.util.List<DocumentSchemaOccurrences.Occurrence> occurrences;
    private final long decodedBytes;

    private DocumentPayloadCheck(DocumentSchemaBinding schema, Any original, DynamicMessage decoded, Map<SchemaKey, DocumentSchemaBinding> resolvedSchemas,
            java.util.List<DocumentSchemaOccurrences.Occurrence> occurrences, long decodedBytes) {
        this.schema = schema;
        this.original = original;
        this.decoded = decoded;
        this.resolvedSchemas = Map.copyOf(resolvedSchemas);
        this.occurrences = java.util.List.copyOf(occurrences);
        this.decodedBytes = decodedBytes;
    }

    DocumentSchemaBinding schema() { return schema; }
    Any original() { return original; }
    DynamicMessage decoded() { return decoded; }
    Map<SchemaKey, DocumentSchemaBinding> resolvedSchemas() { return resolvedSchemas; }
    java.util.List<DocumentSchemaOccurrences.Occurrence> occurrences() { return occurrences; }
    long decodedBytes() { return decodedBytes; }

    /** In-memory evidence only; neither retention nor trusted compiler provenance is established. */
    record AssetResult(DocumentPayloadCheck payload, Map<SchemaKey, DocumentSchemaAssetBinding> assets) {
        AssetResult { assets = Map.copyOf(assets); }
    }

    /**
     * Checks values against structurally bound archival assets while preserving the
     * host's independent URL policy. Every resolved asset must name the exact URL
     * requested, including its prefix. The host still verifies provenance and access.
     * One URL identifies one binding per check; the host accounts for aggregate
     * descriptor and metadata memory returned by its resolver.
     * Archival evidence refuses unknown fields and duplicate decoded map keys;
     * these cannot receive an unambiguous fully validated occurrence identity.
     */
    static AssetResult checkAssets(DocumentSchemaAssetBinding root, Any candidate, String acceptedTypeUrl,
            ProtoValidator validator, Limits limits, Runnable control,
            Function<String, DocumentSchemaAssetBinding> resolver) throws InvalidProtocolBufferException {
        return checkAssets(root, candidate, acceptedTypeUrl, validator, limits, control, resolver,
                DocumentSchemaOccurrences.Limits.DEFAULT);
    }

    static AssetResult checkAssets(DocumentSchemaAssetBinding root, Any candidate, String acceptedTypeUrl,
            ProtoValidator validator, Limits limits, Runnable control,
            Function<String, DocumentSchemaAssetBinding> resolver, DocumentSchemaOccurrences.Limits evidenceLimits)
            throws InvalidProtocolBufferException {
        Objects.requireNonNull(resolver, "resolver");
        var pinned = new LinkedHashMap<String, DocumentSchemaAssetBinding>();
        pinned.put(acceptedTypeUrl, root);
        return checkContextualAssets(root, candidate, acceptedTypeUrl, validator, limits, control,
                request -> pinned.computeIfAbsent(request.typeUrl(), resolver), evidenceLimits);
    }

    /** Resolves each occurrence separately; the host supplies authorized immutable version bindings. */
    static AssetResult checkContextualAssets(DocumentSchemaAssetBinding root, Any candidate, String acceptedTypeUrl,
            ProtoValidator validator, Limits limits, Runnable control,
            Function<ResolutionRequest, DocumentSchemaAssetBinding> resolver,
            DocumentSchemaOccurrences.Limits evidenceLimits) throws InvalidProtocolBufferException {
        var selections = assetSelections(root, acceptedTypeUrl, resolver, control);
        var payload = check(root.schema(), candidate, acceptedTypeUrl, validator, limits, control,
                selections.resolver(), new DocumentSchemaOccurrences(evidenceLimits));
        return new AssetResult(payload, selections.assets());
    }

    /** Completes the entire payload traversal after a value failure; operational failures still throw. */
    static DocumentPayloadAssessment assessContextualAssets(DocumentSchemaAssetBinding root, Any candidate,
            String acceptedTypeUrl, ProtoValidator validator, Limits limits, Runnable control,
            Function<ResolutionRequest, DocumentSchemaAssetBinding> resolver,
            DocumentSchemaOccurrences.Limits evidenceLimits, java.time.Instant evaluatedAt)
            throws InvalidProtocolBufferException {
        var selections = assetSelections(root, acceptedTypeUrl, resolver, control);
        var scan = scan(root.schema(), candidate, acceptedTypeUrl, validator, limits, control,
                selections.resolver(), new DocumentSchemaOccurrences(evidenceLimits), evaluatedAt, true);
        var session = scan.session();
        var occurrences = session.evidence.result();
        active(control);
        if (session.failure != null) return new DocumentPayloadAssessment.Invalid(evaluatedAt, candidate,
                selections.assets(), occurrences, session.decodedBytes, session.failure);
        var checked = new DocumentPayloadCheck(root.schema(), candidate, scan.decoded(), session.bindings,
                occurrences, session.decodedBytes);
        return new DocumentPayloadAssessment.Accepted(evaluatedAt, new AssetResult(checked, selections.assets()));
    }

    private record AssetSelections(Map<SchemaKey, DocumentSchemaAssetBinding> assets,
                                   Function<ResolutionRequest, DocumentSchemaBinding> resolver) {}

    private static AssetSelections assetSelections(DocumentSchemaAssetBinding root, String acceptedTypeUrl,
            Function<ResolutionRequest, DocumentSchemaAssetBinding> resolver, Runnable control) {
        Objects.requireNonNull(root, "root");
        Objects.requireNonNull(resolver, "resolver");
        Objects.requireNonNull(control, "control");
        active(control);
        if (!root.metadata().getTypeUrl().equals(acceptedTypeUrl)) {
            throw new IllegalArgumentException("schema asset type URL differs from host policy");
        }
        var assets = new LinkedHashMap<SchemaKey, DocumentSchemaAssetBinding>();
        assets.put(new SchemaKey(acceptedTypeUrl, root.schema().artifactSha256()), root);
        return new AssetSelections(assets, request -> {
            String url = request.typeUrl();
            var asset = resolver.apply(request);
            active(control);
            if (asset == null) throw new IllegalArgumentException("unresolved Any schema asset: " + url);
            if (!asset.metadata().getTypeUrl().equals(url)) {
                throw new IllegalArgumentException("resolved schema asset type URL mismatch");
            }
            var key = new SchemaKey(url, asset.schema().artifactSha256());
            var previous = assets.putIfAbsent(key, asset);
            if (previous != null && !previous.metadata().equals(asset.metadata()))
                throw new IllegalArgumentException("conflicting schema asset metadata for one identity");
            return asset.schema();
        });
    }

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
        Objects.requireNonNull(resolver, "resolver");
        var pinned = new LinkedHashMap<String, DocumentSchemaBinding>();
        pinned.put(acceptedTypeUrl, schema);
        return check(schema,candidate,acceptedTypeUrl,validator,limits,control,
                request -> pinned.computeIfAbsent(request.typeUrl(), resolver),null);
    }

    private static DocumentPayloadCheck check(DocumentSchemaBinding schema, Any candidate, String acceptedTypeUrl,
            ProtoValidator validator, Limits limits, Runnable control,
            Function<ResolutionRequest, DocumentSchemaBinding> resolver, DocumentSchemaOccurrences evidence) throws InvalidProtocolBufferException {
        var scan = scan(schema, candidate, acceptedTypeUrl, validator, limits, control, resolver, evidence,
                java.time.Instant.now(), false);
        return new DocumentPayloadCheck(schema, candidate, scan.decoded(), scan.session().bindings,
                evidence == null ? java.util.List.of() : evidence.result(), scan.session().decodedBytes);
    }

    private record Scan(DynamicMessage decoded, Session session) {}

    private static Scan scan(DocumentSchemaBinding schema, Any candidate, String acceptedTypeUrl,
            ProtoValidator validator, Limits limits, Runnable control,
            Function<ResolutionRequest, DocumentSchemaBinding> resolver, DocumentSchemaOccurrences evidence,
            java.time.Instant evaluatedAt, boolean assess) throws InvalidProtocolBufferException {
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
        var session = new Session(validator, limits, control, resolver, evidence, evaluatedAt, assess);
        session.bindings.put(new SchemaKey(acceptedTypeUrl, schema.artifactSha256()), schema);
        DynamicMessage decoded = session.decodeBoundary(acceptedTypeUrl, schema, candidate.getValue(), 0);
        return new Scan(decoded, session);
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
        private final Function<ResolutionRequest, DocumentSchemaBinding> resolver;
        private final boolean archival;
        private final DocumentSchemaOccurrences evidence;
        private final MessageWireBudget wire;
        private final Map<SchemaKey, DocumentSchemaBinding> bindings = new LinkedHashMap<>();
        private final java.util.Set<Descriptor> schemas = Collections.newSetFromMap(new IdentityHashMap<>());
        private long decodedBytes;
        private long schemaFields;
        private long anyCount;
        private final java.time.Instant evaluatedAt;
        private final boolean assess;
        private DocumentPayloadAssessment.Failure failure;

        Session(ProtoValidator validator, Limits limits, Runnable control,
                Function<ResolutionRequest, DocumentSchemaBinding> resolver, DocumentSchemaOccurrences evidence,
                java.time.Instant evaluatedAt, boolean assess) {
            this.evaluatedAt = Objects.requireNonNull(evaluatedAt);
            this.assess = assess;
            this.validator = validator;
            this.limits = limits;
            this.control = control;
            this.resolver = resolver;
            this.archival = evidence != null;
            this.evidence = evidence;
            this.wire = new MessageWireBudget(limits.maxWireValues(), limits.maxDepth(), () -> active(control));
        }

        DynamicMessage decodeBoundary(String url, DocumentSchemaBinding schema, com.google.protobuf.ByteString bytes, int depth)
                throws InvalidProtocolBufferException {
            active(control);
            depth(depth);
            if (bytes.size() > limits.maxBytes() - decodedBytes)
                throw new IllegalArgumentException("aggregate payload bytes exceed limit");
            if (evidence == null) return decode(schema, bytes, depth);
            evidence.boundary(url, bytes, schema, () -> active(control));
            try { return decode(schema, bytes, depth); }
            finally { evidence.pop(); }
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
            if (assess) {
                var invalid = validator.firstViolation(decoded, evaluatedAt);
                if (failure == null && invalid.isPresent())
                    failure = DocumentPayloadAssessment.Failure.from(evidence.prefix(), invalid.orElseThrow());
            } else validator.validate(decoded, evaluatedAt).throwIfInvalid();
            active(control);
            return decoded;
        }

        void walk(Message message, int depth) throws InvalidProtocolBufferException {
            walk(message, depth, false);
        }

        void walk(Message message, int depth, boolean mapEntry) throws InvalidProtocolBufferException {
            active(control);
            depth(depth);
            if (archival && !message.getUnknownFields().asMap().isEmpty())
                throw new IllegalArgumentException("unknown candidate fields cannot receive typed archival evidence");
            Descriptor type = message.getDescriptorForType();
            if (type.getFullName().equals("google.protobuf.Any")) {
                if (++anyCount > limits.maxWireValues()) throw new IllegalArgumentException("Any occurrence limit exceeded");
                if (!message.getUnknownFields().asMap().isEmpty()) throw new IllegalArgumentException("unsupported Any envelope fields");
                var urlField = type.findFieldByNumber(1);
                var valueField = type.findFieldByNumber(2);
                if (type.getFields().size() != 2 || urlField == null || valueField == null
                        || !urlField.getName().equals("type_url") || !valueField.getName().equals("value")
                        || urlField.isRepeated() || valueField.isRepeated() || urlField.isRequired() || valueField.isRequired()
                        || urlField.getContainingOneof() != null || valueField.getContainingOneof() != null
                        || urlField.hasDefaultValue() || valueField.hasDefaultValue()
                        || urlField.getType() != FieldDescriptor.Type.STRING || valueField.getType() != FieldDescriptor.Type.BYTES) {
                    throw new IllegalArgumentException("invalid Any envelope descriptor");
                }
                String url = (String) message.getField(urlField);
                if (url.isEmpty() || url.length() > 4096) throw new IllegalArgumentException("invalid Any type URL");
                depth(depth + 1);
                var bytes = (com.google.protobuf.ByteString) message.getField(valueField);
                if (bytes.size() > limits.maxBytes() - decodedBytes)
                    throw new IllegalArgumentException("aggregate payload bytes exceed limit");
                DocumentSchemaBinding binding = resolver.apply(new ResolutionRequest(url,
                        evidence == null ? java.util.List.of() : evidence.prefix(),
                        evidence == null ? "" : DocumentSchemaOccurrences.sha256(bytes, () -> active(control)), bytes.size()));
                active(control);
                if (binding == null) throw new IllegalArgumentException("unresolved Any type URL: " + url);
                requireUrl(url, binding);
                var key = new SchemaKey(url, binding.artifactSha256());
                var previous = bindings.putIfAbsent(key, binding);
                if (previous != null) binding = previous;
                decodeBoundary(url, binding, (com.google.protobuf.ByteString) message.getField(valueField), depth + 1);
                return;
            }
            for (var entry : message.getAllFields().entrySet()) {
                active(control);
                var field = entry.getKey();
                if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) continue;
                // A map-key step selects its value, not the synthetic entry field 2.
                boolean fieldStep = evidence != null && !mapEntry;
                if (fieldStep) evidence.push(new DocumentSchemaOccurrences.Field(field.getNumber()));
                try {
                    if (field.isRepeated()) {
                        java.util.Set<Object> keys = archival && field.isMapField() ? new java.util.HashSet<>() : null;
                        int index = 0;
                        for (Object item : (java.util.List<?>) entry.getValue()) {
                            active(control);
                            var value = (Message) item;
                            if (keys != null) {
                                // getField supplies the declared protobuf default for omitted keys.
                                Object key = value.getField(field.getMessageType().findFieldByNumber(1));
                                if (!keys.add(key)) throw new IllegalArgumentException("duplicate map keys cannot receive typed archival evidence");
                            }
                            if (evidence != null) {
                                var keyField = field.isMapField() ? field.getMessageType().findFieldByNumber(1) : null;
                                evidence.push(keyField == null ? new DocumentSchemaOccurrences.Index(index)
                                        : new DocumentSchemaOccurrences.MapKey(keyField.getType(), value.getField(keyField)));
                            }
                            try { walk(value, depth + 1, field.isMapField()); }
                            finally { if (evidence != null) evidence.pop(); }
                            index++;
                        }
                    } else walk((Message) entry.getValue(), depth + 1);
                } finally { if (fieldStep) evidence.pop(); }
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
                    if (archival && field.isMapField()) checkMapEntry(field.getMessageType());
                    if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE) add(field.getMessageType(), pending);
                }
            }
            validator.prepareSchema(root, limits.maxSchemaTypes(), limits.maxSchemaFields(), () -> active(control));
        }

        private void checkMapEntry(Descriptor entry) {
            var key = entry.findFieldByNumber(1);
            var value = entry.findFieldByNumber(2);
            if (entry.getFields().size() != 2 || key == null || value == null
                    || !key.getName().equals("key") || !value.getName().equals("value")
                    || key.isRepeated() || value.isRepeated() || key.isRequired() || value.isRequired()
                    || key.getContainingOneof() != null || value.getContainingOneof() != null
                    || key.hasDefaultValue() || value.hasDefaultValue()) {
                throw new IllegalArgumentException("invalid map entry descriptor");
            }
            switch (key.getType()) {
                case STRING, BOOL, INT32, SINT32, SFIXED32, UINT32, FIXED32,
                        INT64, SINT64, SFIXED64, UINT64, FIXED64 -> { }
                default -> throw new IllegalArgumentException("invalid map entry descriptor key type");
            }
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
