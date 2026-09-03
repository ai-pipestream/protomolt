package ai.protomolt.proto.search.index.protobuf;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.search.index.ndjson.NdjsonOptions;
import ai.protomolt.proto.search.index.ndjson.ProtoNdjsonWriter;
import ai.protomolt.proto.search.index.spi.AnyPayloadGate;
import ai.protomolt.proto.search.index.spi.CatalogIndexingHintSource;
import ai.protomolt.proto.search.index.spi.IndexMapping;
import ai.protomolt.proto.search.index.spi.IndexMappingFactory;
import ai.protomolt.proto.search.index.spi.ProtoOptionsIndexingHintSource;
import ai.protomolt.proto.mapper.MappingException;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.ValidationResult;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.Message;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Indexing facade: optional CEL validation, then mapping + NDJSON projection.
 * Validation and indexing stay independent — pass a validator only when chaining.
 *
 * <p>When the writer carries a {@link DescriptorRegistry} (so {@code google.protobuf.Any}
 * payloads can be rendered at all), every write method also runs the
 * {@link AnyPayloadGate} over the message: each packed payload is unpacked against that
 * same registry and validated. With a caller-supplied validator, the gate binds to it, so
 * payloads validate under the caller's rule sources and taxonomy catalog — the same
 * contract as the top-level message. Without one, the gate offers payloads to the
 * {@code AnyPayloadValidator}s on the classpath — the gate the engine write path applies
 * during mapping expansion. A registry-less writer cannot render non-empty Anys in the
 * first place, so no gate runs there.
 */
public final class ProtobufIndexer {

    private final IndexMappingFactory mappingFactory;
    private final ProtoNdjsonWriter writer;
    private final ProtoValidator validator;
    private final AnyPayloadGate anyPayloadGate;

    public ProtobufIndexer(IndexMappingFactory mappingFactory, ProtoNdjsonWriter writer) {
        this(mappingFactory, writer, null);
    }

    public ProtobufIndexer(
            IndexMappingFactory mappingFactory, ProtoNdjsonWriter writer, ProtoValidator validator) {
        this.mappingFactory = Objects.requireNonNull(mappingFactory, "mappingFactory");
        this.writer = Objects.requireNonNull(writer, "writer");
        this.validator = validator;
        // A caller-supplied validator carries its own rule sources and taxonomy catalog:
        // packed payloads must validate under exactly those, not the ServiceLoader gate's
        // shared default (which mounts no taxonomies and knows no custom sources).
        this.anyPayloadGate = writer.descriptorRegistry()
                .map(registry -> validator == null
                        ? new AnyPayloadGate(registry, mappingFactory.hints())
                        : new AnyPayloadGate(registry,
                                List.of(new DeclaredRulesAnyPayloadValidator(validator)),
                                mappingFactory.hints()))
                .orElse(null);
    }

    /** Inferring hints only — no validation. */
    public static ProtobufIndexer create() {
        return new ProtobufIndexer(IndexMappingFactory.inferringOnly(), new ProtoNdjsonWriter());
    }

    /** Catalog → proto options → inference, with optional validation before NDJSON. */
    public static ProtobufIndexer defaults(ProtoValidator validator) {
        return new ProtobufIndexer(
                IndexMappingFactory.defaults(new CatalogIndexingHintSource()),
                new ProtoNdjsonWriter(),
                validator);
    }

    /**
     * Like {@link #defaults(ProtoValidator)}, with a registry for rendering and gating
     * {@code google.protobuf.Any} payloads.
     */
    public static ProtobufIndexer defaults(ProtoValidator validator, DescriptorRegistry registry) {
        Objects.requireNonNull(registry, "registry");
        return new ProtobufIndexer(
                IndexMappingFactory.defaults(new CatalogIndexingHintSource()),
                new ProtoNdjsonWriter(NdjsonOptions.defaults(), registry),
                validator);
    }

    public static void registerExtensions(ExtensionRegistry registry) {
        Objects.requireNonNull(registry, "registry");
        ProtoOptionsIndexingHintSource.registerExtensions(registry);
        ValidationResult.registerExtensions(registry);
    }

    public Optional<ProtoValidator> validator() {
        return Optional.ofNullable(validator);
    }

    public IndexMapping mapping(Descriptor descriptor) {
        return mappingFactory.create(descriptor);
    }

    public ValidationResult validate(Message message) {
        if (validator == null) {
            return ValidationResult.ok();
        }
        return validator.validate(message);
    }

    /**
     * Validates when a validator is configured, gates Any payloads when a registry is
     * present, then encodes one NDJSON line.
     *
     * @throws MappingException when an Any payload is malformed or packs an unknown type
     */
    public String toNdjsonLine(Message message) throws MappingException {
        Objects.requireNonNull(message, "message");
        validate(message).throwIfInvalid();
        gateAnyPayloads(message);
        return writer.toJsonLine(message);
    }

    /**
     * @throws MappingException when an Any payload is malformed or packs an unknown type
     */
    public void writeNdjsonLine(Appendable out, Message message) throws MappingException {
        Objects.requireNonNull(out, "out");
        validate(message).throwIfInvalid();
        gateAnyPayloads(message);
        writer.writeLine(out, message);
    }

    /**
     * @throws MappingException when an Any payload is malformed or packs an unknown type
     */
    public void writeBulkIndex(Appendable out, String index, String id, Message document)
            throws MappingException {
        Objects.requireNonNull(out, "out");
        validate(document).throwIfInvalid();
        gateAnyPayloads(document);
        writer.writeBulkIndex(out, index, id, document);
    }

    private void gateAnyPayloads(Message message) throws MappingException {
        if (anyPayloadGate != null) {
            anyPayloadGate.validate(message);
        }
    }
}
