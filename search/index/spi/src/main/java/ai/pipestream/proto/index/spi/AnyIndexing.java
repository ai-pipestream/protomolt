package ai.pipestream.proto.index.spi;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.helpers.AnyHandler;
import ai.pipestream.proto.mapper.MappingException;
import ai.pipestream.proto.mapper.ProtoFieldMapper;
import com.google.protobuf.Any;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.ServiceLoader;

/**
 * Write-time expansion of {@code google.protobuf.Any} plan entries.
 *
 * <p>Plan time cannot know a single packed type, so {@link IndexingPlanFactory} keeps Any
 * as one {@link IndexFieldKind#ANY} leaf. On {@link SearchEngineIndexer#map}, this helper
 * unpacks a set Any through {@link AnyHandler} / {@link DescriptorRegistry}, plans the
 * inner descriptor with the same hint chain as the parent, and emits inner fields under
 * {@code anyField.innerPath} (proto field names; engine names prefixed with the Any
 * field's engine name, name overrides included). Unknown type URLs fail by path and type
 * URL — never a silent skip. An unset or empty Any contributes no inner fields.
 *
 * <p>Only entries whose resolved kind is {@link IndexFieldKind#ANY} expand: a hint that
 * resolves an Any field to any other kind ({@code SKIP} included) has said otherwise and
 * is left alone. Repeated Any fields and Any fields under a repeated ancestor (a
 * {@link BlockRole#CHUNKS} scope) have no single packed type per plan path, so their
 * entry is kept as-is; engines and schema generators ignore ANY entries.
 *
 * <p>Every unpacked payload is offered to the {@link AnyPayloadValidator}s discovered via
 * {@link ServiceLoader} before its fields are planned. With
 * {@code protomolt-protobuf-indexing} on the classpath, packed messages carrying declared
 * validation rules are therefore validated on the same write path as everything else.
 * A hint with {@code validate_payloads: false} opts that one field out of the gate —
 * its payloads still expand and malformed or unknown-type Anys still fail. Payloads that
 * pack further Anys expand recursively, up to {@value #MAX_EXPANSION_DEPTH} levels.
 */
public final class AnyIndexing {

    /**
     * Maximum Any-inside-Any nesting {@link #expand} follows before failing the document.
     * Mirrors the plan walk's default depth bound; deeper chains are adversarial data.
     */
    static final int MAX_EXPANSION_DEPTH = 8;

    private final AnyHandler anyHandler;
    private final DescriptorRegistry registry;
    private final IndexingPlanFactory planFactory;
    private final List<AnyPayloadValidator> payloadValidators;

    /** Uses the {@link ServiceLoader}-discovered {@link AnyPayloadValidator}s. */
    public AnyIndexing(DescriptorRegistry registry, IndexingPlanFactory planFactory) {
        this(registry, planFactory, discoverValidators());
    }

    public AnyIndexing(
            DescriptorRegistry registry,
            IndexingPlanFactory planFactory,
            List<AnyPayloadValidator> payloadValidators) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.anyHandler = new AnyHandler(registry);
        this.planFactory = Objects.requireNonNull(planFactory, "planFactory");
        this.payloadValidators = List.copyOf(payloadValidators);
    }

    public static AnyIndexing from(IndexerContext context) {
        Objects.requireNonNull(context, "context");
        return new AnyIndexing(context.descriptorRegistry(), context.planFactory());
    }

    /**
     * Fallback for engines constructed from a bare mapper (no {@link IndexerContext}):
     * inner types are planned with the default hint chain (catalog → proto options →
     * inference) instead of the parent plan's chain.
     */
    public static AnyIndexing from(ProtoFieldMapper fieldMapper) {
        Objects.requireNonNull(fieldMapper, "fieldMapper");
        return new AnyIndexing(
                fieldMapper.getDescriptorRegistry(),
                IndexingPlanFactory.defaults(new CatalogIndexingHintSource()));
    }

    /**
     * Replaces expandable ANY entries with planned inner fields. Must run before any engine
     * document is built so an unknown type URL or an invalid payload cannot emit a partial
     * document. Returns {@code plan} itself when it holds no ANY entry.
     */
    public IndexingPlan expand(Message message, IndexingPlan plan) throws MappingException {
        Objects.requireNonNull(message, "message");
        Objects.requireNonNull(plan, "plan");
        boolean hasAny = false;
        for (IndexingPlan.IndexedField field : plan.fields()) {
            hasAny |= field.type() == IndexFieldKind.ANY;
        }
        return hasAny ? expand(message, plan, "", 0) : plan;
    }

    private IndexingPlan expand(Message message, IndexingPlan plan, String pathToMessage, int depth)
            throws MappingException {
        List<IndexingPlan.IndexedField> out = new ArrayList<>(plan.fields().size());
        for (IndexingPlan.IndexedField field : plan.fields()) {
            if (field.type() != IndexFieldKind.ANY) {
                out.add(field);
                continue;
            }
            String absolutePath = pathToMessage.isEmpty()
                    ? field.path()
                    : pathToMessage + "." + field.path();
            AnyLeaf leaf = resolveLeaf(message.getDescriptorForType(), field.path(), absolutePath);
            if (leaf.field().isRepeated() || leaf.underRepeatedAncestor()) {
                out.add(field);
                continue;
            }
            if (depth >= MAX_EXPANSION_DEPTH) {
                throw new MappingException(
                        "google.protobuf.Any nesting exceeds " + MAX_EXPANSION_DEPTH + " levels",
                        absolutePath);
            }
            Any any = readAny(message, field.path(), absolutePath);
            if (any == null || isEmpty(any, absolutePath)) {
                continue;
            }
            Message unpacked = unpack(any, absolutePath);
            if (field.hint().validatePayloads()) {
                for (AnyPayloadValidator validator : payloadValidators) {
                    validator.validate(unpacked, absolutePath);
                }
            }
            IndexingPlan inner = expand(
                    unpacked,
                    planFactory.create(unpacked.getDescriptorForType()),
                    absolutePath,
                    depth + 1);
            for (IndexingPlan.IndexedField child : inner.fields()) {
                out.add(prefixed(child, field.path(), field.fieldName()));
            }
        }
        return new IndexingPlan(plan.messageFullName(), out);
    }

    private Message unpack(Any any, String absolutePath) throws MappingException {
        return unpack(anyHandler, registry, any, absolutePath);
    }

    /**
     * Unpacks with three distinct failures: a type URL without the {@code '/'} the Any spec
     * (and every renderer) requires, a type the registry does not know, and value bytes
     * that do not parse as the registered type — each named so the operator fixes the
     * right thing.
     */
    static Message unpack(
            AnyHandler anyHandler, DescriptorRegistry registry, Any any, String absolutePath)
            throws MappingException {
        String typeUrl = any.getTypeUrl();
        int slash = typeUrl.lastIndexOf('/');
        if (slash < 0) {
            throw new MappingException(
                    "google.protobuf.Any type URL '" + typeUrl
                            + "' has no '/'; the Any contract requires 'host/fully.qualified.TypeName'",
                    absolutePath);
        }
        String typeName = typeUrl.substring(slash + 1);
        if (registry.findDescriptorByFullName(typeName) == null) {
            throw new MappingException(
                    "Cannot unpack google.protobuf.Any: unknown type URL '" + typeUrl
                            + "'; register the packed type with the DescriptorRegistry",
                    absolutePath);
        }
        try {
            return anyHandler.unpack(any);
        } catch (InvalidProtocolBufferException e) {
            throw new MappingException(
                    "google.protobuf.Any value bytes do not parse as '" + typeName + "'",
                    e,
                    absolutePath);
        }
    }

    /** A default-instance Any is unset in all but presence; value bytes without a type URL are malformed. */
    static boolean isEmpty(Any any, String absolutePath) throws MappingException {
        if (!any.getTypeUrl().isEmpty()) {
            return false;
        }
        if (!any.getValue().isEmpty()) {
            throw new MappingException(
                    "google.protobuf.Any carries value bytes but no type URL", absolutePath);
        }
        return true;
    }

    private static IndexingPlan.IndexedField prefixed(
            IndexingPlan.IndexedField field, String pathPrefix, String namePrefix) {
        String path = pathPrefix.isEmpty() ? field.path() : pathPrefix + "." + field.path();
        String name = namePrefix.isEmpty() ? field.fieldName() : namePrefix + "_" + field.fieldName();
        return new IndexingPlan.IndexedField(path, name, field.hint(), field.repeated());
    }

    /** The plan path's leaf field plus whether any traversed ancestor is repeated. */
    private record AnyLeaf(FieldDescriptor field, boolean underRepeatedAncestor) {
    }

    /**
     * Resolves {@code path} against the descriptor alone, so a plan that does not fit the
     * message type fails deterministically even when the field is unset on this document.
     */
    private static AnyLeaf resolveLeaf(Descriptor root, String path, String absolutePath)
            throws MappingException {
        Descriptor current = root;
        boolean underRepeated = false;
        int start = 0;
        while (true) {
            int dot = path.indexOf('.', start);
            String segment = dot < 0 ? path.substring(start) : path.substring(start, dot);
            FieldDescriptor field = current.findFieldByName(segment);
            if (field == null) {
                throw new MappingException(
                        "ANY plan entry does not resolve on " + root.getFullName(), absolutePath);
            }
            if (dot < 0) {
                if (!isAny(field)) {
                    throw new MappingException(
                            "ANY plan entry is not a google.protobuf.Any field but "
                                    + describe(field),
                            absolutePath);
                }
                return new AnyLeaf(field, underRepeated);
            }
            if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
                throw new MappingException(
                        "ANY plan entry traverses non-message field '" + segment + "'",
                        absolutePath);
            }
            underRepeated |= field.isRepeated();
            current = field.getMessageType();
            start = dot + 1;
        }
    }

    private static boolean isAny(FieldDescriptor field) {
        return field.getJavaType() == FieldDescriptor.JavaType.MESSAGE
                && Any.getDescriptor().getFullName().equals(field.getMessageType().getFullName());
    }

    private static String describe(FieldDescriptor field) {
        return field.getJavaType() == FieldDescriptor.JavaType.MESSAGE
                ? field.getMessageType().getFullName()
                : field.getType().name();
    }

    /**
     * Reads the Any value along a path {@link #resolveLeaf} has already vetted as singular
     * messages ending in an Any. Returns null when the leaf or an intermediate is unset.
     */
    private static Any readAny(Message message, String path, String absolutePath)
            throws MappingException {
        Message current = message;
        int start = 0;
        while (true) {
            int dot = path.indexOf('.', start);
            String segment = dot < 0 ? path.substring(start) : path.substring(start, dot);
            FieldDescriptor field = current.getDescriptorForType().findFieldByName(segment);
            if (!current.hasField(field)) {
                return null;
            }
            Object value = current.getField(field);
            if (dot < 0) {
                return toAny(value, absolutePath);
            }
            current = (Message) value;
            start = dot + 1;
        }
    }

    /** Dynamic messages hold Any fields as {@link com.google.protobuf.DynamicMessage}; reserialize those. */
    static Any toAny(Object value, String absolutePath) throws MappingException {
        if (value instanceof Any any) {
            return any;
        }
        Message message = value instanceof Message.Builder builder
                ? builder.build()
                : (Message) value;
        try {
            return Any.parseFrom(message.toByteString());
        } catch (InvalidProtocolBufferException e) {
            throw new MappingException(
                    "Failed to convert dynamic google.protobuf.Any message", e, absolutePath);
        }
    }

    static List<AnyPayloadValidator> discoverValidators() {
        List<AnyPayloadValidator> validators = new ArrayList<>();
        for (AnyPayloadValidator validator : ServiceLoader.load(AnyPayloadValidator.class)) {
            validators.add(validator);
        }
        return validators;
    }
}
