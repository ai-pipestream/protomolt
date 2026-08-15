package ai.pipestream.proto.index.spi;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.helpers.AnyHandler;
import ai.pipestream.proto.mapper.MappingException;
import com.google.protobuf.Any;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Offers every {@code google.protobuf.Any} payload set on a message to the
 * {@link AnyPayloadValidator}s, without consulting an {@link IndexMapping}.
 *
 * <p>{@link AnyIndexing} gates payloads on the engine write path, where mapping expansion
 * already unpacks each mapped Any. Output paths that render the whole message rather
 * than mapping entries — the NDJSON facade foremost — still embed every packed payload, so
 * this gate walks the message instance itself: singular and repeated Any fields, Anys
 * reached through nested messages, repeated elements and map values, a root message that
 * is itself an Any, and payloads that pack further Anys, up to
 * {@value AnyIndexing#MAX_EXPANSION_DEPTH} unpacking levels. Because the walk follows
 * values instead of mapping paths, repeated Anys and Anys under repeated ancestors — inert
 * on the expansion path — are validated here, element by element.
 *
 * <p>A field whose resolved hint carries {@code validate_payloads: false} opts its own
 * payloads out of the validators (hints come from the {@link IndexingHintSource} the gate
 * was built with, descriptor options by default). Opted-out payloads are still unpacked:
 * malformed Anys and unknown type URLs keep failing, and Anys nested inside the payload
 * are gated under their own fields' settings.
 *
 * <p>Failure semantics match {@link AnyIndexing}: an unknown type URL, a type URL without
 * the {@code '/'} the Any contract requires, value bytes that do not parse as the
 * registered type, or value bytes without a type URL fail by path — never a silent skip —
 * and a validator throwing its standard's unchecked exception aborts the document. An
 * unset or default-instance Any contributes nothing. Paths name repeated elements and map
 * entries by position or key, e.g. {@code attachments[2]} or {@code extras[cover]}.
 */
public final class AnyPayloadGate {

    private final AnyHandler anyHandler;
    private final DescriptorRegistry registry;
    private final IndexingHintSource hints;
    private final List<AnyPayloadValidator> payloadValidators;

    /** Uses the {@link java.util.ServiceLoader}-discovered {@link AnyPayloadValidator}s. */
    public AnyPayloadGate(DescriptorRegistry registry) {
        this(registry, AnyIndexing.discoverValidators());
    }

    /** Field opt-outs come from descriptor options ({@link ProtoOptionsIndexingHintSource}). */
    public AnyPayloadGate(DescriptorRegistry registry, List<AnyPayloadValidator> payloadValidators) {
        this(registry, payloadValidators, new ProtoOptionsIndexingHintSource());
    }

    /** Uses the discovered validators with {@code hints} resolving per-field opt-outs. */
    public AnyPayloadGate(DescriptorRegistry registry, IndexingHintSource hints) {
        this(registry, AnyIndexing.discoverValidators(), hints);
    }

    public AnyPayloadGate(
            DescriptorRegistry registry,
            List<AnyPayloadValidator> payloadValidators,
            IndexingHintSource hints) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.anyHandler = new AnyHandler(registry);
        this.payloadValidators = List.copyOf(payloadValidators);
        this.hints = Objects.requireNonNull(hints, "hints");
    }

    /**
     * Walks {@code message} and validates every packed payload. A no-op when no
     * {@link AnyPayloadValidator} is on the classpath: without a gate to run, malformed
     * or unknown-type Anys are left for the output path's own encoder to report.
     */
    public void validate(Message message) throws MappingException {
        Objects.requireNonNull(message, "message");
        if (payloadValidators.isEmpty()) {
            return;
        }
        walk(message, "", 0);
    }

    private void walk(Message message, String path, int depth) throws MappingException {
        if (isAny(message.getDescriptorForType())) {
            // Root message, or an Any packed directly inside another Any: no field carries
            // an opt-out here, so the payload is validated.
            checkPayload(AnyIndexing.toAny(message, path), path, depth, true);
            return;
        }
        for (Map.Entry<FieldDescriptor, Object> entry : message.getAllFields().entrySet()) {
            FieldDescriptor field = entry.getKey();
            if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
                continue;
            }
            String fieldPath = path.isEmpty() ? field.getName() : path + "." + field.getName();
            if (field.isMapField()) {
                walkMapValues(field, (List<?>) entry.getValue(), fieldPath, depth);
            } else if (field.isRepeated()) {
                List<?> elements = (List<?>) entry.getValue();
                boolean anyField = isAny(field.getMessageType());
                boolean validate = anyField && validatePayloads(field);
                for (int i = 0; i < elements.size(); i++) {
                    String elementPath = fieldPath + "[" + i + "]";
                    if (anyField) {
                        checkPayload(AnyIndexing.toAny(elements.get(i), elementPath),
                                elementPath, depth, validate);
                    } else {
                        walk((Message) elements.get(i), elementPath, depth);
                    }
                }
            } else if (isAny(field.getMessageType())) {
                checkPayload(AnyIndexing.toAny(entry.getValue(), fieldPath),
                        fieldPath, depth, validatePayloads(field));
            } else {
                walk((Message) entry.getValue(), fieldPath, depth);
            }
        }
    }

    private void walkMapValues(FieldDescriptor field, List<?> entries, String fieldPath, int depth)
            throws MappingException {
        FieldDescriptor keyField = field.getMessageType().findFieldByName("key");
        FieldDescriptor valueField = field.getMessageType().findFieldByName("value");
        if (valueField.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
            return;
        }
        // The map field itself is the annotatable surface; its synthetic value field is not.
        boolean anyValues = isAny(valueField.getMessageType());
        boolean validate = anyValues && validatePayloads(field);
        for (Object element : entries) {
            Message entry = (Message) element;
            String entryPath = fieldPath + "[" + entry.getField(keyField) + "]";
            if (anyValues) {
                checkPayload(AnyIndexing.toAny(entry.getField(valueField), entryPath),
                        entryPath, depth, validate);
            } else {
                walk((Message) entry.getField(valueField), entryPath, depth);
            }
        }
    }

    private boolean validatePayloads(FieldDescriptor field) {
        return hints.resolve(field)
                .map(ResolvedFieldHint::validatePayloads)
                .orElse(true);
    }

    private void checkPayload(Any any, String path, int depth, boolean validate)
            throws MappingException {
        if (AnyIndexing.isEmpty(any, path)) {
            return;
        }
        if (depth >= AnyIndexing.MAX_EXPANSION_DEPTH) {
            throw new MappingException(
                    "google.protobuf.Any nesting exceeds " + AnyIndexing.MAX_EXPANSION_DEPTH
                            + " levels",
                    path);
        }
        Message unpacked = AnyIndexing.unpack(anyHandler, registry, any, path);
        if (validate) {
            for (AnyPayloadValidator validator : payloadValidators) {
                validator.validate(unpacked, path);
            }
        }
        walk(unpacked, path, depth + 1);
    }

    private static boolean isAny(Descriptor descriptor) {
        return Any.getDescriptor().getFullName().equals(descriptor.getFullName());
    }
}
