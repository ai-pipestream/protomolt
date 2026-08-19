package ai.pipestream.proto.indexing;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.index.ndjson.NdjsonOptions;
import ai.pipestream.proto.index.ndjson.ProtoNdjsonWriter;
import ai.pipestream.proto.index.spi.CatalogIndexingHintSource;
import ai.pipestream.proto.index.spi.IndexFieldKind;
import ai.pipestream.proto.index.spi.IndexMappingFactory;
import ai.pipestream.proto.index.spi.ResolvedFieldHint;
import ai.pipestream.proto.indexing.testdata.AnyEnvelope;
import ai.pipestream.proto.validate.FieldRules;
import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.ValidateProto;
import ai.pipestream.proto.validate.ValidationResult;
import ai.pipestream.proto.validate.spi.TaxonomyCatalog;
import com.google.protobuf.Any;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The catalog-aware Any-payload gate: an indexer built with an explicit validator binds
 * the payload gate to it, so a taxonomy-bound TreePath inside a packed payload checks its
 * membership against the caller's mounted catalog — the same contract the top-level
 * message gets. Without an explicit validator the ServiceLoader gate keeps the shared
 * default validator, whose empty catalog refuses taxonomy-bound payloads fail-closed.
 * The payload type is built dynamically: packed customer types are exactly the ones this
 * module never compiles against.
 */
class TaxonomyAwareAnyGateTest {

    private static final Descriptors.FileDescriptor FILE = buildFile();
    private static final Descriptors.Descriptor TREE_PATH = FILE.findMessageTypeByName("TreePath");
    private static final Descriptors.Descriptor DOC = FILE.findMessageTypeByName("Doc");

    private static final TaxonomyCatalog CATALOG = name -> "products".equals(name)
            ? Optional.of(TaxonomyCatalog.Mounted.of("products", "v7", List.of(
                    List.of("electronics", "computers", "laptops"),
                    List.of("media", "books"))))
            : Optional.empty();

    private static Descriptors.FileDescriptor buildFile() {
        DescriptorProtos.FieldOptions taxonomyRule = DescriptorProtos.FieldOptions.newBuilder()
                .setExtension(ValidateProto.field,
                        FieldRules.newBuilder().setTaxonomy("products").build())
                .build();
        DescriptorProtos.FileDescriptorProto file = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("taxonomy_any_gate_test.proto")
                .setSyntax("proto3")
                .setPackage("ai.pipestream.proto.types.v1")
                .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("TreePath")
                        .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                                .setName("segments").setNumber(1)
                                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
                                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED)))
                .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("Doc")
                        .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                                .setName("category").setNumber(1)
                                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setTypeName(".ai.pipestream.proto.types.v1.TreePath")
                                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .setOptions(taxonomyRule)))
                .build();
        try {
            return Descriptors.FileDescriptor.buildFrom(file, new Descriptors.FileDescriptor[0]);
        } catch (Descriptors.DescriptorValidationException e) {
            throw new IllegalStateException(e);
        }
    }

    private static AnyEnvelope envelope(String... segments) {
        DynamicMessage.Builder path = DynamicMessage.newBuilder(TREE_PATH);
        for (String segment : segments) {
            path.addRepeatedField(TREE_PATH.findFieldByName("segments"), segment);
        }
        DynamicMessage doc = DynamicMessage.newBuilder(DOC)
                .setField(DOC.findFieldByName("category"), path.build())
                .build();
        return AnyEnvelope.newBuilder()
                .setDocId("doc-1")
                .setPayload(Any.newBuilder()
                        .setTypeUrl("type.googleapis.com/" + DOC.getFullName())
                        .setValue(doc.toByteString()))
                .build();
    }

    private static DescriptorRegistry registry() {
        DescriptorRegistry registry = new DescriptorRegistry();
        registry.register(DOC);
        registry.register(TREE_PATH);
        return registry;
    }

    @Test
    void aMemberPathInsideAPackedPayloadPassesTheMountedCatalog() {
        ProtobufIndexer indexer = ProtobufIndexer.defaults(
                ProtoValidator.create(CATALOG), registry());
        assertThatCode(() -> indexer.toNdjsonLine(envelope("electronics", "computers")))
                .doesNotThrowAnyException();
    }

    @Test
    void aNonMemberPathInsideAPackedPayloadRefusesByMembership() {
        ProtobufIndexer indexer = ProtobufIndexer.defaults(
                ProtoValidator.create(CATALOG), registry());
        assertThatThrownBy(() -> indexer.toNdjsonLine(envelope("media", "movies")))
                .isInstanceOfSatisfying(ValidationResult.ValidationException.class, e ->
                        assertThat(e.result().violations()).singleElement().satisfies(v -> {
                            assertThat(v.ruleId()).isEqualTo("taxonomy.member");
                            assertThat(v.path()).isEqualTo("payload.category");
                            assertThat(v.message()).contains("media/movies").contains("v7");
                        }));
    }

    @Test
    void theCallerBoundGateStillHonorsHintOptOuts() {
        // Binding the gate to the caller's validator must not change which hint source
        // resolves per-field opt-outs: a validate_payloads false hint still suspends it.
        CatalogIndexingHintSource hints = new CatalogIndexingHintSource()
                .put(AnyEnvelope.getDescriptor().getFullName(), "payload",
                        ResolvedFieldHint.builder(IndexFieldKind.ANY)
                                .validatePayloads(false).build());
        ProtobufIndexer indexer = new ProtobufIndexer(
                IndexMappingFactory.defaults(hints),
                new ProtoNdjsonWriter(NdjsonOptions.defaults(), registry()),
                ProtoValidator.create(CATALOG));
        assertThatCode(() -> indexer.toNdjsonLine(envelope("media", "movies")))
                .doesNotThrowAnyException();
    }

    @Test
    void withoutACallerValidatorTheDefaultGateStaysFailClosed() {
        // The ServiceLoader gate keeps the shared default validator: no mounts, so a
        // taxonomy-bound payload refuses as unmounted rather than silently passing.
        ProtobufIndexer indexer = ProtobufIndexer.defaults(null, registry());
        assertThatThrownBy(() -> indexer.toNdjsonLine(envelope("electronics", "computers")))
                .isInstanceOfSatisfying(ValidationResult.ValidationException.class, e ->
                        assertThat(e.result().violations()).singleElement()
                                .extracting(ValidationResult.Violation::ruleId)
                                .isEqualTo("taxonomy.unmounted"));
    }
}
