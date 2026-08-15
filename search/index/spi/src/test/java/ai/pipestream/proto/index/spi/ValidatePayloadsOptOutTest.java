package ai.pipestream.proto.index.spi;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.index.hints.FieldIndexHint;
import ai.pipestream.proto.index.hints.IndexingHintsProto;
import ai.pipestream.proto.mapper.MappingException;
import com.google.protobuf.Any;
import com.google.protobuf.AnyProto;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldOptions;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.MessageOptions;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.ExtensionRegistry;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Adversarial coverage for the schema-level {@code validate_payloads} opt-out, driven from
 * descriptor options rather than catalog hints: repeated and map Any fields, non-Any
 * fields, the explicit {@code true}, opt-outs carried on an unpacked payload's own fields,
 * Any-inside-Any, catalog precedence, the depth cap, mapping-time carriage, and the
 * {@link ResolvedFieldHint} component itself.
 */
class ValidatePayloadsOptOutTest {

    @Test
    void aDescriptorOptOutSuppressesTheValidatorsForThatFieldOnly() throws Exception {
        OptOutFixtures fixtures = OptOutFixtures.create();
        List<String> offered = new ArrayList<>();
        AnyPayloadGate gate = fixtures.gate(offered);
        DynamicMessage message = DynamicMessage.newBuilder(fixtures.envelope)
                .setField(fixtures.field("opted"), Any.pack(fixtures.inner("A", 1)))
                .addRepeatedField(fixtures.field("plain"), Any.pack(fixtures.inner("B", 2)))
                .build();

        gate.validate(message);

        assertThat(offered).containsExactly("plain[0]");
    }

    @Test
    void aRepeatedAnyOptOutSkipsEveryElementNotJustTheFirst() throws Exception {
        OptOutFixtures fixtures = OptOutFixtures.create();
        List<String> offered = new ArrayList<>();
        AnyPayloadGate gate = fixtures.gate(offered);
        DynamicMessage message = DynamicMessage.newBuilder(fixtures.envelope)
                .addRepeatedField(fixtures.field("attachments"), Any.pack(fixtures.inner("A", 1)))
                .addRepeatedField(fixtures.field("attachments"), Any.pack(fixtures.inner("B", 2)))
                .addRepeatedField(fixtures.field("attachments"), Any.pack(fixtures.inner("C", 3)))
                .addRepeatedField(fixtures.field("plain"), Any.pack(fixtures.inner("D", 4)))
                .build();

        gate.validate(message);

        assertThat(offered).containsExactly("plain[0]");
    }

    @Test
    void aMapValueOptOutIsResolvedOnTheMapFieldItself() throws Exception {
        // The annotatable surface is the map field; its synthetic MapEntry.value field
        // cannot carry an option at all, so the resolution must not look there.
        OptOutFixtures fixtures = OptOutFixtures.create();
        List<String> offered = new ArrayList<>();
        AnyPayloadGate gate = fixtures.gate(offered);
        DynamicMessage message = DynamicMessage.newBuilder(fixtures.envelope)
                .addRepeatedField(fixtures.field("extras"),
                        fixtures.extrasEntry("cover", Any.pack(fixtures.inner("A", 1))))
                .addRepeatedField(fixtures.field("extras"),
                        fixtures.extrasEntry("back", Any.pack(fixtures.inner("B", 2))))
                .addRepeatedField(fixtures.field("plain"), Any.pack(fixtures.inner("C", 3)))
                .build();

        gate.validate(message);

        assertThat(offered).containsExactly("plain[0]");
    }

    @Test
    void anOptOutOnAMessageFieldThatIsNotAnAnyIsInert() throws Exception {
        OptOutFixtures fixtures = OptOutFixtures.create();
        List<String> offered = new ArrayList<>();
        AnyPayloadGate gate = fixtures.gate(offered);
        DynamicMessage nested = DynamicMessage.newBuilder(fixtures.nested)
                .setField(fixtures.nested.findFieldByName("tagged"), Any.pack(fixtures.inner("A", 1)))
                .build();
        DynamicMessage message = DynamicMessage.newBuilder(fixtures.envelope)
                .setField(fixtures.field("nested"), nested)
                .build();

        gate.validate(message);

        assertThat(offered).containsExactly("nested.tagged");
    }

    @Test
    void anExplicitValidatePayloadsTrueBehavesLikeTheDefault() throws Exception {
        OptOutFixtures fixtures = OptOutFixtures.create();
        List<String> offered = new ArrayList<>();
        AnyPayloadGate gate = fixtures.gate(offered);
        DynamicMessage message = DynamicMessage.newBuilder(fixtures.envelope)
                .setField(fixtures.field("checked"), Any.pack(fixtures.inner("A", 1)))
                .build();

        gate.validate(message);

        assertThat(offered).containsExactly("checked");
    }

    @Test
    void theGateResolvesOptOutsOnTheUnpackedPayloadsOwnFields() throws Exception {
        OptOutFixtures fixtures = OptOutFixtures.create();
        List<String> offered = new ArrayList<>();
        AnyPayloadGate gate = fixtures.gate(offered);
        DynamicMessage payload = DynamicMessage.newBuilder(fixtures.payload)
                .setField(fixtures.payload.findFieldByName("title"), "p")
                .setField(fixtures.payload.findFieldByName("inner"), Any.pack(fixtures.inner("A", 1)))
                .build();
        DynamicMessage message = DynamicMessage.newBuilder(fixtures.envelope)
                .setField(fixtures.field("checked"), Any.pack(payload))
                .build();

        gate.validate(message);

        assertThat(offered).containsExactly("checked");
    }

    @Test
    void anAnyPackedDirectlyInsideAnOptedOutAnyIsStillValidated() throws Exception {
        // Documented hole: the inner Any is reached through no field of its own, so the
        // opt-out on the outer field does not cover it (AnyPayloadGate.java:89-91).
        OptOutFixtures fixtures = OptOutFixtures.create();
        List<String> offered = new ArrayList<>();
        AnyPayloadGate gate = fixtures.gate(offered);
        DynamicMessage message = DynamicMessage.newBuilder(fixtures.envelope)
                .setField(fixtures.field("opted"), Any.pack(Any.pack(fixtures.inner("A", 1))))
                .build();

        gate.validate(message);

        assertThat(offered).containsExactly("opted");
    }

    @Test
    void aCatalogHintWithoutTheOptOutOverridesTheDescriptorOptOut() throws Exception {
        // Whole-hint precedence: the catalog does not merge into the descriptor's hint, it
        // replaces it, so an unrelated catalog tag re-enables validation the schema disabled.
        OptOutFixtures fixtures = OptOutFixtures.create();
        CatalogIndexingHintSource catalog = new CatalogIndexingHintSource()
                .put(fixtures.envelope.getFullName(), "opted", ResolvedFieldHint.of(IndexFieldKind.ANY));
        List<String> offered = new ArrayList<>();
        AnyPayloadGate gate = new AnyPayloadGate(
                fixtures.registry,
                List.of((unpacked, path) -> offered.add(path)),
                IndexMappingFactory.defaults(catalog).hints());
        DynamicMessage message = DynamicMessage.newBuilder(fixtures.envelope)
                .setField(fixtures.field("opted"), Any.pack(fixtures.inner("A", 1)))
                .build();

        gate.validate(message);

        assertThat(offered).containsExactly("opted");
    }

    @Test
    void optedOutPayloadsStillHitTheDepthCap() throws Exception {
        OptOutFixtures fixtures = OptOutFixtures.create();
        AnyPayloadGate gate = fixtures.gate(new ArrayList<>());
        DynamicMessage message = DynamicMessage.newBuilder(fixtures.envelope)
                .setField(fixtures.field("opted"),
                        fixtures.chainOfAnys(AnyIndexing.MAX_EXPANSION_DEPTH + 1))
                .build();

        assertThatThrownBy(() -> gate.validate(message))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("nesting exceeds");
    }

    @Test
    void optedOutPayloadsStillFailOnUnknownTypeUrlsUnderARepeatedField() throws Exception {
        OptOutFixtures fixtures = OptOutFixtures.create();
        AnyPayloadGate gate = fixtures.gate(new ArrayList<>());
        DynamicMessage message = DynamicMessage.newBuilder(fixtures.envelope)
                .addRepeatedField(fixtures.field("attachments"), Any.pack(fixtures.inner("A", 1)))
                .addRepeatedField(fixtures.field("attachments"), Any.newBuilder()
                        .setTypeUrl("type.googleapis.com/ai.pipestream.test.optout.MissingType")
                        .build())
                .build();

        assertThatThrownBy(() -> gate.validate(message))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("attachments[1]")
                .hasMessageContaining("unknown type URL");
    }

    @Test
    void theMappingKeepsAnAnyEntryCarryingTheOptOut() throws Exception {
        OptOutFixtures fixtures = OptOutFixtures.create();
        IndexMappingFactory factory = IndexMappingFactory.defaults(new CatalogIndexingHintSource());

        IndexMapping mapping = factory.create(fixtures.envelope);

        // Type inference still applies to a hint that carries only validate_payloads.
        assertThat(mapping.find("opted")).get()
                .extracting(IndexMapping.IndexedField::type)
                .isEqualTo(IndexFieldKind.ANY);
        assertThat(mapping.find("opted")).get()
                .extracting(f -> f.hint().validatePayloads())
                .isEqualTo(false);
        assertThat(mapping.find("attachments")).get()
                .extracting(f -> f.hint().validatePayloads())
                .isEqualTo(false);
        assertThat(mapping.find("checked")).get()
                .extracting(f -> f.hint().validatePayloads())
                .isEqualTo(true);
        assertThat(mapping.find("plain")).get()
                .extracting(f -> f.hint().validatePayloads())
                .isEqualTo(true);
    }

    @Test
    void theHintsAccessorExposesTheMappingChainTheGateNeeds() throws Exception {
        OptOutFixtures fixtures = OptOutFixtures.create();
        IndexMappingFactory factory = IndexMappingFactory.defaults(new CatalogIndexingHintSource());

        assertThat(factory.hints().resolve(fixtures.field("opted")))
                .get()
                .extracting(ResolvedFieldHint::validatePayloads)
                .isEqualTo(false);
    }

    @Test
    void theExpansionPathReadsTheOptOutFromTheInnerMappingsHintChain() throws Exception {
        OptOutFixtures fixtures = OptOutFixtures.create();
        IndexMappingFactory factory = IndexMappingFactory.defaults(new CatalogIndexingHintSource());
        List<String> offered = new ArrayList<>();
        AnyIndexing anyIndexing = new AnyIndexing(fixtures.registry, factory,
                List.of((unpacked, path) -> offered.add(path)));
        DynamicMessage payload = DynamicMessage.newBuilder(fixtures.payload)
                .setField(fixtures.payload.findFieldByName("title"), "p")
                .setField(fixtures.payload.findFieldByName("inner"), Any.pack(fixtures.inner("A", 1)))
                .build();
        DynamicMessage message = DynamicMessage.newBuilder(fixtures.envelope)
                .setField(fixtures.field("checked"), Any.pack(payload))
                .build();

        IndexMapping expanded = anyIndexing.expand(message, factory.create(fixtures.envelope));

        assertThat(offered).containsExactly("checked");
        assertThat(expanded.find("checked.inner.title")).isPresent();
    }

    @Test
    void validatePayloadsRoundTripsThroughToBuilderAndParticipatesInEquality() {
        ResolvedFieldHint validating = ResolvedFieldHint.of(IndexFieldKind.ANY);
        ResolvedFieldHint optedOut = validating.toBuilder().validatePayloads(false).build();

        assertThat(validating.validatePayloads()).isTrue();
        assertThat(optedOut.validatePayloads()).isFalse();
        assertThat(optedOut).isNotEqualTo(validating);
        assertThat(optedOut.toBuilder().build()).isEqualTo(optedOut);
        assertThat(optedOut.toBuilder().validatePayloads(true).build()).isEqualTo(validating);
    }

    @Test
    void everyHintFactoryLeavesPayloadValidationOn() {
        assertThat(ResolvedFieldHint.of(IndexFieldKind.ANY).validatePayloads()).isTrue();
        assertThat(ResolvedFieldHint.skipped().validatePayloads()).isTrue();
        assertThat(ResolvedFieldHint.builder(IndexFieldKind.ANY).build().validatePayloads()).isTrue();
        assertThat(new ResolvedFieldHint(IndexFieldKind.ANY, true, true, "", 0).validatePayloads())
                .isTrue();
        assertThat(new CatalogIndexingHintSource()
                .put("type_url", ResolvedFieldHint.skipped())
                .resolve(dummyField())
                .map(ResolvedFieldHint::validatePayloads))
                .contains(true);
    }

    @Test
    void anOptOutSurvivingOnlyAsAnUnknownOptionFieldIsHonoured() throws Exception {
        // A descriptor set linked without the hint extension carries the (index) option only
        // as an unknown field; the hint source reparses it (like the validation standard)
        // rather than dropping it, or a payload the schema waved through would be rejected.
        OptOutFixtures fixtures = OptOutFixtures.linkedWithoutExtensions();
        FieldOptions options = fixtures.field("opted").getOptions();
        // Precondition: the annotation is still on the wire, so honouring it costs a reparse,
        // not a schema change — this is a read failure, not missing information.
        assertThat(options.hasExtension(IndexingHintsProto.index)).isFalse();
        assertThat(options.getUnknownFields().hasField(IndexingHintsProto.index.getNumber())).isTrue();
        ExtensionRegistry extensions = ExtensionRegistry.newInstance();
        ProtoOptionsIndexingHintSource.registerExtensions(extensions);
        assertThat(FieldOptions.parseFrom(options.toByteString(), extensions)
                .getExtension(IndexingHintsProto.index)
                .getValidatePayloads())
                .isFalse();

        List<String> offered = new ArrayList<>();
        AnyPayloadGate gate = fixtures.gate(offered);
        DynamicMessage message = DynamicMessage.newBuilder(fixtures.envelope)
                .setField(fixtures.field("opted"), Any.pack(fixtures.inner("A", 1)))
                .build();

        gate.validate(message);

        assertThat(offered).isEmpty();
    }

    private static com.google.protobuf.Descriptors.FieldDescriptor dummyField() {
        return Any.getDescriptor().findFieldByName("type_url");
    }

    private record OptOutFixtures(
            Descriptor envelope,
            Descriptor innerType,
            Descriptor middle,
            Descriptor payload,
            Descriptor nested,
            DescriptorRegistry registry) {

        static OptOutFixtures create() throws Exception {
            return of(optOutFile());
        }

        /**
         * The same schema after a descriptor-set round trip with no extension registry: the
         * {@code (index)} option survives only as an unknown field on FieldOptions.
         */
        static OptOutFixtures linkedWithoutExtensions() throws Exception {
            return of(FileDescriptorProto.parseFrom(optOutFile().toByteString()));
        }

        private static OptOutFixtures of(FileDescriptorProto proto) throws Exception {
            FileDescriptor file =
                    FileDescriptor.buildFrom(proto, new FileDescriptor[]{AnyProto.getDescriptor()});
            Descriptor inner = file.findMessageTypeByName("InnerPayload");
            Descriptor middle = file.findMessageTypeByName("Middle");
            Descriptor payload = file.findMessageTypeByName("Payload");
            DescriptorRegistry registry = new DescriptorRegistry();
            registry.register(inner);
            registry.register(middle);
            registry.register(payload);
            return new OptOutFixtures(
                    file.findMessageTypeByName("Envelope"),
                    inner,
                    middle,
                    payload,
                    file.findMessageTypeByName("Nested"),
                    registry);
        }

        /** Descriptor options only: the two-argument gate resolves through proto options. */
        AnyPayloadGate gate(List<String> offered) {
            return new AnyPayloadGate(registry, List.of((unpacked, path) -> offered.add(path)));
        }

        com.google.protobuf.Descriptors.FieldDescriptor field(String name) {
            return envelope.findFieldByName(name);
        }

        DynamicMessage inner(String title, int pageCount) {
            return DynamicMessage.newBuilder(innerType)
                    .setField(innerType.findFieldByName("title"), title)
                    .setField(innerType.findFieldByName("page_count"), pageCount)
                    .build();
        }

        DynamicMessage extrasEntry(String key, Any value) {
            Descriptor entry = envelope.findNestedTypeByName("ExtrasEntry");
            return DynamicMessage.newBuilder(entry)
                    .setField(entry.findFieldByName("key"), key)
                    .setField(entry.findFieldByName("value"), value)
                    .build();
        }

        /** A chain of {@code count} Anys: the innermost packs InnerPayload, the rest Middle. */
        Any chainOfAnys(int count) {
            Any chain = Any.pack(inner("deep", 1));
            for (int i = 1; i < count; i++) {
                chain = Any.pack(DynamicMessage.newBuilder(middle)
                        .setField(middle.findFieldByName("next"), chain)
                        .build());
            }
            return chain;
        }

        private static FileDescriptorProto optOutFile() {
            return FileDescriptorProto.newBuilder()
                    .setName("validate_payloads_opt_out_doc.proto")
                    .setPackage("ai.pipestream.test.optout")
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
                            .addField(anyField("next", 1, FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                    .addMessageType(DescriptorProto.newBuilder()
                            .setName("Payload")
                            .addField(stringField("title", 1))
                            .addField(anyField("inner", 2, FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                    .setOptions(hint(false))))
                    .addMessageType(DescriptorProto.newBuilder()
                            .setName("Nested")
                            .addField(anyField("tagged", 1, FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                    .addMessageType(DescriptorProto.newBuilder()
                            .setName("Envelope")
                            .addField(stringField("doc_id", 1))
                            .addField(anyField("opted", 2, FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                    .setOptions(hint(false)))
                            .addField(anyField("checked", 3, FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                    .setOptions(hint(true)))
                            .addField(anyField("attachments", 4, FieldDescriptorProto.Label.LABEL_REPEATED)
                                    .setOptions(hint(false)))
                            .addField(FieldDescriptorProto.newBuilder()
                                    .setName("extras")
                                    .setNumber(5)
                                    .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                                    .setTypeName(".ai.pipestream.test.optout.Envelope.ExtrasEntry")
                                    .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED)
                                    .setOptions(hint(false)))
                            .addField(FieldDescriptorProto.newBuilder()
                                    .setName("nested")
                                    .setNumber(6)
                                    .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                                    .setTypeName(".ai.pipestream.test.optout.Nested")
                                    .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                    .setOptions(hint(false)))
                            .addField(anyField("plain", 7, FieldDescriptorProto.Label.LABEL_REPEATED))
                            .addNestedType(DescriptorProto.newBuilder()
                                    .setName("ExtrasEntry")
                                    .setOptions(MessageOptions.newBuilder().setMapEntry(true))
                                    .addField(stringField("key", 1))
                                    .addField(anyField(
                                            "value", 2, FieldDescriptorProto.Label.LABEL_OPTIONAL))))
                    .build();
        }

        private static FieldOptions hint(boolean validatePayloads) {
            return FieldOptions.newBuilder()
                    .setExtension(IndexingHintsProto.index, FieldIndexHint.newBuilder()
                            .setValidatePayloads(validatePayloads)
                            .build())
                    .build();
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
