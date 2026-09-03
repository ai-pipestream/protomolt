package ai.protomolt.proto.search.index.spi;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.mapper.MappingException;
import com.google.protobuf.Any;
import com.google.protobuf.AnyProto;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AnyIndexingTest {

    @Test
    void expandsPackedInnerFieldsUnderTheAnyPathPrefix() throws Exception {
        AnyFixtures fixtures = AnyFixtures.create();
        IndexMapping mapping = fixtures.factory.create(fixtures.envelope);
        IndexMapping expanded = fixtures.anyIndexing.expand(fixtures.packedEnvelope(), mapping);

        assertThat(expanded.find("payload")).isEmpty();
        assertThat(expanded.find("payload.title")).get()
                .extracting(IndexMapping.IndexedField::type)
                .isEqualTo(IndexFieldKind.KEYWORD);
        assertThat(expanded.find("payload.title")).get()
                .extracting(IndexMapping.IndexedField::fieldName)
                .isEqualTo("payload_title");
        assertThat(expanded.find("payload.page_count")).get()
                .extracting(IndexMapping.IndexedField::type)
                .isEqualTo(IndexFieldKind.INT32);
        assertThat(expanded.find("doc_id")).get()
                .extracting(IndexMapping.IndexedField::type)
                .isEqualTo(IndexFieldKind.KEYWORD);
    }

    @Test
    void nestedAnyExpandsRecursivelyWithPrefixedPathsAndNames() throws Exception {
        AnyFixtures fixtures = AnyFixtures.create();
        IndexMapping mapping = fixtures.factory.create(fixtures.envelope);
        DynamicMessage middle = DynamicMessage.newBuilder(fixtures.middle)
                .setField(fixtures.middle.findFieldByName("label"), "mid")
                .setField(fixtures.middle.findFieldByName("next"), Any.pack(fixtures.inner("Opinion", 12)))
                .build();
        DynamicMessage message = fixtures.envelope(Any.pack(middle));

        IndexMapping expanded = fixtures.anyIndexing.expand(message, mapping);

        assertThat(expanded.find("payload.label")).get()
                .extracting(IndexMapping.IndexedField::fieldName)
                .isEqualTo("payload_label");
        assertThat(expanded.find("payload.next")).isEmpty();
        assertThat(expanded.find("payload.next.title")).get()
                .extracting(IndexMapping.IndexedField::fieldName)
                .isEqualTo("payload_next_title");
        assertThat(expanded.find("payload.next.page_count")).get()
                .extracting(IndexMapping.IndexedField::type)
                .isEqualTo(IndexFieldKind.INT32);
    }

    @Test
    void unknownTypeUrlFailsWithPathAndTypeUrlAndEmitsNoInnerFields() throws Exception {
        AnyFixtures fixtures = AnyFixtures.create();
        IndexMapping mapping = fixtures.factory.create(fixtures.envelope);
        DynamicMessage message = fixtures.envelope(Any.newBuilder()
                .setTypeUrl("type.googleapis.com/ai.pipestream.test.MissingType")
                .setValue(ByteString.copyFromUtf8("x"))
                .build());

        assertThatThrownBy(() -> fixtures.anyIndexing.expand(message, mapping))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("payload")
                .hasMessageContaining("type.googleapis.com/ai.pipestream.test.MissingType");
    }

    @Test
    void unsetAnyDoesNotFailAndAddsNoInnerFields() throws Exception {
        AnyFixtures fixtures = AnyFixtures.create();
        IndexMapping mapping = fixtures.factory.create(fixtures.envelope);
        DynamicMessage message = DynamicMessage.newBuilder(fixtures.envelope)
                .setField(fixtures.envelope.findFieldByName("doc_id"), "doc-1")
                .build();

        IndexMapping expanded = fixtures.anyIndexing.expand(message, mapping);

        assertThat(expanded.find("payload")).isEmpty();
        assertThat(expanded.find("payload.title")).isEmpty();
        assertThat(expanded.find("doc_id")).isPresent();
    }

    @Test
    void emptyAnyDoesNotFailAndAddsNoInnerFields() throws Exception {
        AnyFixtures fixtures = AnyFixtures.create();
        IndexMapping mapping = fixtures.factory.create(fixtures.envelope);
        DynamicMessage message = fixtures.envelope(Any.getDefaultInstance());

        IndexMapping expanded = fixtures.anyIndexing.expand(message, mapping);

        assertThat(expanded.find("payload")).isEmpty();
        assertThat(expanded.find("payload.title")).isEmpty();
    }

    @Test
    void valueBytesWithoutTypeUrlAreMalformedNotEmpty() throws Exception {
        AnyFixtures fixtures = AnyFixtures.create();
        IndexMapping mapping = fixtures.factory.create(fixtures.envelope);
        DynamicMessage message = fixtures.envelope(Any.newBuilder()
                .setValue(ByteString.copyFromUtf8("x"))
                .build());

        assertThatThrownBy(() -> fixtures.anyIndexing.expand(message, mapping))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("no type URL")
                .hasMessageContaining("payload");
    }

    @Test
    void skipHintIsLeftAlone() throws Exception {
        AnyFixtures fixtures = AnyFixtures.create();
        CatalogIndexingHintSource catalog = new CatalogIndexingHintSource()
                .put(fixtures.envelope.getFullName(), "payload", ResolvedFieldHint.skipped())
                .put(fixtures.innerType.getFullName(), "title", ResolvedFieldHint.of(IndexFieldKind.KEYWORD));
        IndexMappingFactory factory = IndexMappingFactory.defaults(catalog);
        IndexMapping mapping = factory.create(fixtures.envelope);
        AnyIndexing anyIndexing = new AnyIndexing(fixtures.registry, factory, List.of());

        IndexMapping expanded = anyIndexing.expand(fixtures.packedEnvelope(), mapping);

        assertThat(expanded.find("payload")).get()
                .extracting(IndexMapping.IndexedField::type)
                .isEqualTo(IndexFieldKind.SKIP);
        assertThat(expanded.find("payload.title")).isEmpty();
    }

    @Test
    void explicitNonAnyHintOnAnAnyFieldSaysOtherwiseAndIsRespected() throws Exception {
        AnyFixtures fixtures = AnyFixtures.create();
        CatalogIndexingHintSource catalog = new CatalogIndexingHintSource()
                .put(fixtures.envelope.getFullName(), "payload", ResolvedFieldHint.of(IndexFieldKind.OBJECT));
        IndexMappingFactory factory = IndexMappingFactory.defaults(catalog);
        IndexMapping mapping = factory.create(fixtures.envelope);
        AnyIndexing anyIndexing = new AnyIndexing(fixtures.registry, factory, List.of());

        IndexMapping expanded = anyIndexing.expand(fixtures.packedEnvelope(), mapping);

        assertThat(expanded.find("payload")).get()
                .extracting(IndexMapping.IndexedField::type)
                .isEqualTo(IndexFieldKind.OBJECT);
        assertThat(expanded.find("payload.title")).isEmpty();
    }

    @Test
    void repeatedAnyKeepsItsInertEntry() throws Exception {
        AnyFixtures fixtures = AnyFixtures.create();
        IndexMapping mapping = fixtures.factory.create(fixtures.envelope);
        DynamicMessage message = DynamicMessage.newBuilder(fixtures.envelope)
                .addRepeatedField(
                        fixtures.envelope.findFieldByName("extras"),
                        Any.pack(fixtures.inner("Opinion", 12)))
                .build();

        IndexMapping expanded = fixtures.anyIndexing.expand(message, mapping);

        assertThat(expanded.find("extras")).get()
                .extracting(IndexMapping.IndexedField::type)
                .isEqualTo(IndexFieldKind.ANY);
        assertThat(expanded.find("extras.title")).isEmpty();
    }

    @Test
    void anyUnderARepeatedAncestorKeepsItsInertEntry() throws Exception {
        AnyFixtures fixtures = AnyFixtures.create();
        IndexMapping mapping = new IndexMapping(fixtures.envelope.getFullName(), List.of(
                new IndexMapping.IndexedField(
                        "items.payload", "items_payload", ResolvedFieldHint.of(IndexFieldKind.ANY), false)));
        DynamicMessage item = DynamicMessage.newBuilder(fixtures.item)
                .setField(fixtures.item.findFieldByName("payload"), Any.pack(fixtures.inner("Opinion", 12)))
                .build();
        DynamicMessage message = DynamicMessage.newBuilder(fixtures.envelope)
                .addRepeatedField(fixtures.envelope.findFieldByName("items"), item)
                .build();

        IndexMapping expanded = fixtures.anyIndexing.expand(message, mapping);

        assertThat(expanded.find("items.payload")).get()
                .extracting(IndexMapping.IndexedField::type)
                .isEqualTo(IndexFieldKind.ANY);
        assertThat(expanded.find("items.payload.title")).isEmpty();
    }

    @Test
    void anyEntryOnANonAnyFieldIsAMappingErrorWithThePath() throws Exception {
        AnyFixtures fixtures = AnyFixtures.create();
        IndexMapping mapping = new IndexMapping(fixtures.envelope.getFullName(), List.of(
                new IndexMapping.IndexedField(
                        "doc_id", "doc_id", ResolvedFieldHint.of(IndexFieldKind.ANY), false)));

        assertThatThrownBy(() -> fixtures.anyIndexing.expand(fixtures.packedEnvelope(), mapping))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("not a google.protobuf.Any field")
                .hasMessageContaining("doc_id");
    }

    @Test
    void anyEntryOnAMissingFieldIsAMappingErrorWithThePath() throws Exception {
        AnyFixtures fixtures = AnyFixtures.create();
        IndexMapping mapping = new IndexMapping(fixtures.envelope.getFullName(), List.of(
                new IndexMapping.IndexedField(
                        "missing", "missing", ResolvedFieldHint.of(IndexFieldKind.ANY), false)));

        assertThatThrownBy(() -> fixtures.anyIndexing.expand(fixtures.packedEnvelope(), mapping))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("does not resolve")
                .hasMessageContaining("missing");
    }

    @Test
    void nestingBeyondTheDepthCapFailsInsteadOfRecursingForever() throws Exception {
        AnyFixtures fixtures = AnyFixtures.create();
        IndexMapping mapping = fixtures.factory.create(fixtures.envelope);
        Any chain = Any.pack(fixtures.inner("deep", 1));
        for (int i = 0; i < 12; i++) {
            chain = Any.pack(DynamicMessage.newBuilder(fixtures.middle)
                    .setField(fixtures.middle.findFieldByName("next"), chain)
                    .build());
        }
        DynamicMessage message = fixtures.envelope(chain);

        assertThatThrownBy(() -> fixtures.anyIndexing.expand(message, mapping))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("nesting exceeds")
                .hasMessageContaining("payload.next");
    }

    @Test
    void mappingsWithoutAnyEntriesAreReturnedUnchanged() throws Exception {
        AnyFixtures fixtures = AnyFixtures.create();
        IndexMapping mapping = new IndexMapping(fixtures.envelope.getFullName(), List.of(
                new IndexMapping.IndexedField(
                        "doc_id", "doc_id", ResolvedFieldHint.of(IndexFieldKind.KEYWORD), false)));

        assertThat(fixtures.anyIndexing.expand(fixtures.packedEnvelope(), mapping)).isSameAs(mapping);
    }

    @Test
    void validatorsSeeEveryUnpackedPayloadWithItsAbsolutePath() throws Exception {
        AnyFixtures fixtures = AnyFixtures.create();
        List<String> paths = new ArrayList<>();
        List<String> types = new ArrayList<>();
        AnyIndexing anyIndexing = new AnyIndexing(fixtures.registry, fixtures.factory,
                List.of((unpacked, path) -> {
                    paths.add(path);
                    types.add(unpacked.getDescriptorForType().getFullName());
                }));
        DynamicMessage middle = DynamicMessage.newBuilder(fixtures.middle)
                .setField(fixtures.middle.findFieldByName("next"), Any.pack(fixtures.inner("Opinion", 12)))
                .build();

        anyIndexing.expand(fixtures.envelope(Any.pack(middle)), fixtures.factory.create(fixtures.envelope));

        assertThat(paths).containsExactly("payload", "payload.next");
        assertThat(types).containsExactly(
                fixtures.middle.getFullName(), fixtures.innerType.getFullName());
    }

    @Test
    void aThrowingValidatorAbortsTheDocumentBeforeAnyInnerFieldIsMapped() throws Exception {
        AnyFixtures fixtures = AnyFixtures.create();
        AnyIndexing anyIndexing = new AnyIndexing(fixtures.registry, fixtures.factory,
                List.of((unpacked, path) -> {
                    throw new IllegalStateException("declared rule violated at " + path);
                }));
        IndexMapping mapping = fixtures.factory.create(fixtures.envelope);

        assertThatThrownBy(() -> anyIndexing.expand(fixtures.packedEnvelope(), mapping))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("payload");
    }

    @Test
    void validatePayloadsFalseExpandsWithoutOfferingPayloadsToValidators() throws Exception {
        AnyFixtures fixtures = AnyFixtures.create();
        CatalogIndexingHintSource catalog = new CatalogIndexingHintSource()
                .put(fixtures.envelope.getFullName(), "payload",
                        ResolvedFieldHint.builder(IndexFieldKind.ANY).validatePayloads(false).build())
                .put(fixtures.innerType.getFullName(), "title", ResolvedFieldHint.of(IndexFieldKind.KEYWORD));
        IndexMappingFactory factory = IndexMappingFactory.defaults(catalog);
        List<String> offered = new ArrayList<>();
        AnyIndexing anyIndexing = new AnyIndexing(fixtures.registry, factory,
                List.of((unpacked, path) -> offered.add(path)));

        IndexMapping expanded = anyIndexing.expand(
                fixtures.packedEnvelope(), factory.create(fixtures.envelope));

        assertThat(offered).isEmpty();
        assertThat(expanded.find("payload.title")).get()
                .extracting(IndexMapping.IndexedField::type)
                .isEqualTo(IndexFieldKind.KEYWORD);
    }

    @Test
    void validatePayloadsFalseStillFailsUnknownTypeUrls() throws Exception {
        AnyFixtures fixtures = AnyFixtures.create();
        CatalogIndexingHintSource catalog = new CatalogIndexingHintSource()
                .put(fixtures.envelope.getFullName(), "payload",
                        ResolvedFieldHint.builder(IndexFieldKind.ANY).validatePayloads(false).build());
        IndexMappingFactory factory = IndexMappingFactory.defaults(catalog);
        AnyIndexing anyIndexing = new AnyIndexing(fixtures.registry, factory,
                List.of((unpacked, path) -> { }));
        DynamicMessage message = fixtures.envelope(Any.newBuilder()
                .setTypeUrl("type.googleapis.com/ai.pipestream.test.MissingType")
                .setValue(ByteString.copyFromUtf8("x"))
                .build());

        assertThatThrownBy(() -> anyIndexing.expand(message, factory.create(fixtures.envelope)))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("unknown type URL");
    }

    @Test
    void unsetAnyIsNotOfferedToValidators() throws Exception {
        AnyFixtures fixtures = AnyFixtures.create();
        List<String> paths = new ArrayList<>();
        AnyIndexing anyIndexing = new AnyIndexing(fixtures.registry, fixtures.factory,
                List.of((unpacked, path) -> paths.add(path)));
        DynamicMessage message = DynamicMessage.newBuilder(fixtures.envelope)
                .setField(fixtures.envelope.findFieldByName("doc_id"), "doc-1")
                .build();

        anyIndexing.expand(message, fixtures.factory.create(fixtures.envelope));

        assertThat(paths).isEmpty();
    }

    private record AnyFixtures(
            Descriptor envelope,
            Descriptor innerType,
            Descriptor middle,
            Descriptor item,
            DescriptorRegistry registry,
            IndexMappingFactory factory,
            AnyIndexing anyIndexing) {

        static AnyFixtures create() throws Exception {
            FileDescriptor file = anyFile();
            Descriptor envelope = file.findMessageTypeByName("Envelope");
            Descriptor inner = file.findMessageTypeByName("InnerPayload");
            Descriptor middle = file.findMessageTypeByName("Middle");
            Descriptor item = file.findMessageTypeByName("Item");
            DescriptorRegistry registry = new DescriptorRegistry();
            registry.register(inner);
            registry.register(middle);
            CatalogIndexingHintSource catalog = new CatalogIndexingHintSource()
                    .put(inner.getFullName(), "title", ResolvedFieldHint.of(IndexFieldKind.KEYWORD));
            IndexMappingFactory factory = IndexMappingFactory.defaults(catalog);
            return new AnyFixtures(envelope, inner, middle, item, registry, factory,
                    new AnyIndexing(registry, factory, List.of()));
        }

        DynamicMessage inner(String title, int pageCount) {
            return DynamicMessage.newBuilder(innerType)
                    .setField(innerType.findFieldByName("title"), title)
                    .setField(innerType.findFieldByName("page_count"), pageCount)
                    .build();
        }

        DynamicMessage envelope(Any payload) {
            return DynamicMessage.newBuilder(envelope)
                    .setField(envelope.findFieldByName("doc_id"), "doc-1")
                    .setField(envelope.findFieldByName("payload"), payload)
                    .build();
        }

        DynamicMessage packedEnvelope() {
            return envelope(Any.pack(inner("Opinion", 12)));
        }

        private static FileDescriptor anyFile() throws Exception {
            FileDescriptorProto proto = FileDescriptorProto.newBuilder()
                    .setName("any_index_doc.proto")
                    .setPackage("ai.pipestream.test")
                    .setSyntax("proto3")
                    .addDependency("google/protobuf/any.proto")
                    .addMessageType(DescriptorProto.newBuilder()
                            .setName("InnerPayload")
                            .addField(stringField("title", 1))
                            .addField(FieldDescriptorProto.newBuilder()
                                    .setName("page_count")
                                    .setNumber(2)
                                    .setType(FieldDescriptorProto.Type.TYPE_INT32)
                                    .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                    .addMessageType(DescriptorProto.newBuilder()
                            .setName("Middle")
                            .addField(stringField("label", 1))
                            .addField(anyField("next", 2, FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                    .addMessageType(DescriptorProto.newBuilder()
                            .setName("Item")
                            .addField(anyField("payload", 1, FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                    .addMessageType(DescriptorProto.newBuilder()
                            .setName("Envelope")
                            .addField(stringField("doc_id", 1))
                            .addField(anyField("payload", 2, FieldDescriptorProto.Label.LABEL_OPTIONAL))
                            .addField(anyField("extras", 3, FieldDescriptorProto.Label.LABEL_REPEATED))
                            .addField(FieldDescriptorProto.newBuilder()
                                    .setName("items")
                                    .setNumber(4)
                                    .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                                    .setTypeName(".ai.pipestream.test.Item")
                                    .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED)))
                    .build();
            return FileDescriptor.buildFrom(proto, new FileDescriptor[]{AnyProto.getDescriptor()});
        }

        private static FieldDescriptorProto.Builder stringField(String name, int number) {
            return FieldDescriptorProto.newBuilder()
                    .setName(name)
                    .setNumber(number)
                    .setType(FieldDescriptorProto.Type.TYPE_STRING)
                    .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL);
        }

        private static FieldDescriptorProto.Builder anyField(
                String name, int number, FieldDescriptorProto.Label label) {
            return FieldDescriptorProto.newBuilder()
                    .setName(name)
                    .setNumber(number)
                    .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                    .setTypeName(".google.protobuf.Any")
                    .setLabel(label);
        }
    }
}
