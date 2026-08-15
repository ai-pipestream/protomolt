package ai.pipestream.proto.indexing;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.index.ndjson.NdjsonOptions;
import ai.pipestream.proto.index.ndjson.ProtoNdjsonWriter;
import ai.pipestream.proto.index.spi.AnyPayloadGate;
import ai.pipestream.proto.index.spi.CatalogIndexingHintSource;
import ai.pipestream.proto.index.spi.IndexMapping;
import ai.pipestream.proto.index.spi.IndexMappingFactory;
import ai.pipestream.proto.index.spi.ProtoOptionsIndexingHintSource;
import ai.pipestream.proto.mapper.MappingException;
import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.ValidationResult;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.Message;

import java.util.Objects;
import java.util.Optional;

/**
 * Indexing facade: optional CEL validation, then mapping + NDJSON projection.
 * Validation and indexing stay independent — pass a validator only when chaining.
 *
 * <p>When the writer carries a {@link DescriptorRegistry} (so {@code google.protobuf.Any}
 * payloads can be rendered at all), every write method also runs the
 * {@link AnyPayloadGate} over the message: each packed payload is unpacked against that
 * same registry and offered to the {@code AnyPayloadValidator}s on the classpath — the
 * gate the engine write path applies during mapping expansion. A registry-less writer cannot
 * render non-empty Anys in the first place, so no gate runs there.
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
        this.anyPayloadGate = writer.descriptorRegistry()
                .map(registry -> new AnyPayloadGate(registry, mappingFactory.hints()))
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
