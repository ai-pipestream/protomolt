package ai.pipestream.proto.index.spi;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.mapper.MappingException;
import ai.pipestream.proto.mapper.ProtoFieldMapper;
import ai.pipestream.proto.mapper.ProtoFieldMapperImpl;
import com.google.protobuf.Any;
import com.google.protobuf.AnyProto;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.MessageOptions;
import com.google.protobuf.DescriptorProtos.OneofDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.Struct;
import com.google.protobuf.StructProto;
import com.google.protobuf.Value;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Adversarial edge cases for {@link PlanValues}: leaf-vs-intermediate repeated fields,
 * Any/Struct/map boundaries below a fan-out, presence rules per element, and the
 * plan flags engines size their schemas from.
 */
class PlanValuesEdgeCaseTest {

    // ------------------------------------------------------------------ shapes

    @Test
    void repeatedMessageAsTheWholePathStaysALeafList() throws Exception {
        Fixtures f = Fixtures.create();
        DynamicMessage doc = f.docOf(f.leaf("alpha"), f.leaf("beta"));

        Object value = PlanValues.read(f.mapper, doc, "leaves", false);

        // No fan-out: the repeated message field IS the leaf, so its elements are the value.
        assertThat(value).asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.LIST)
                .hasSize(2)
                .allSatisfy(element -> assertThat(((Message) element).getDescriptorForType().getName())
                        .isEqualTo("Leaf"));
    }

    @Test
    void fanOutToARepeatedMessageLeafFlattensElementsAcrossParents() throws Exception {
        Fixtures f = Fixtures.create();
        DynamicMessage doc = f.docWithGroups(
                f.groupOf(f.leaf("a1"), f.leaf("a2")), f.groupOf(f.leaf("b1")));

        Object value = PlanValues.read(f.mapper, doc, "groups.leaves", false);

        assertThat(value).asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.LIST)
                .hasSize(3);
    }

    @Test
    void threeLevelFanOutFlattensDepthFirstAndSkipsEmptyElements() throws Exception {
        Fixtures f = Fixtures.create();
        DynamicMessage doc = f.docWithGroups(
                f.groupOf(f.leaf("a1"), f.leaf("a2")),
                f.groupOf(),
                f.groupOf(f.leaf("c1")));

        assertThat(PlanValues.read(f.mapper, doc, "groups.leaves.text", false))
                .isEqualTo(List.of("a1", "a2", "c1"));
    }

    @Test
    void selfReferencingTypeFansOutAtEveryLevel() throws Exception {
        Fixtures f = Fixtures.create();
        DynamicMessage grandchild = f.node("leaf-1");
        DynamicMessage child = DynamicMessage.newBuilder(f.node)
                .setField(f.node.findFieldByName("name"), "child")
                .addRepeatedField(f.node.findFieldByName("children"), grandchild)
                .addRepeatedField(f.node.findFieldByName("children"), f.node("leaf-2"))
                .build();
        DynamicMessage root = DynamicMessage.newBuilder(f.node)
                .setField(f.node.findFieldByName("name"), "root")
                .addRepeatedField(f.node.findFieldByName("children"), child)
                .build();
        DynamicMessage doc = DynamicMessage.newBuilder(f.doc)
                .setField(f.doc.findFieldByName("root"), root)
                .build();

        assertThat(PlanValues.read(f.mapper, doc, "root.children.name", false))
                .isEqualTo(List.of("child"));
        assertThat(PlanValues.read(f.mapper, doc, "root.children.children.name", false))
                .isEqualTo(List.of("leaf-1", "leaf-2"));
    }

    @Test
    void fanOutUnderASingularParentReadsWhenSetAndIsMissingWhenNot() throws Exception {
        Fixtures f = Fixtures.create();
        DynamicMessage withGroup = DynamicMessage.newBuilder(f.doc)
                .setField(f.doc.findFieldByName("solo_group"), f.groupOf(f.leaf("x"), f.leaf("y")))
                .build();

        assertThat(PlanValues.read(f.mapper, withGroup, "solo_group.leaves.text", false))
                .isEqualTo(List.of("x", "y"));
        assertThat(PlanValues.read(f.mapper, f.docOf(), "solo_group.leaves.text", false)).isNull();
    }

    // ------------------------------------------------------------------ presence

    @Test
    void includeDefaultsSurfacesImplicitPresenceLeavesPerElement() throws Exception {
        Fixtures f = Fixtures.create();
        DynamicMessage scored = DynamicMessage.newBuilder(f.leaf)
                .setField(f.leaf.findFieldByName("score"), 7)
                .build();
        DynamicMessage doc = f.docOf(scored, f.leaf("no score"));

        assertThat(PlanValues.read(f.mapper, doc, "leaves.score", false)).isEqualTo(List.of(7));
        // proto3 implicit presence: the second element's 0 only appears when asked for.
        assertThat(PlanValues.read(f.mapper, doc, "leaves.score", true)).isEqualTo(List.of(7, 0));
    }

    @Test
    void explicitPresenceLeavesStayAbsentUnderIncludeDefaults() throws Exception {
        Fixtures f = Fixtures.create();
        DynamicMessage picked = DynamicMessage.newBuilder(f.leaf)
                .setField(f.leaf.findFieldByName("pick_a"), "A")
                .build();
        DynamicMessage other = DynamicMessage.newBuilder(f.leaf)
                .setField(f.leaf.findFieldByName("pick_b"), "B")
                .build();
        DynamicMessage doc = f.docOf(picked, other);

        // Oneof members have real presence: includeDefaults must not resurrect the unset arm.
        assertThat(PlanValues.read(f.mapper, doc, "leaves.pick_a", true)).isEqualTo(List.of("A"));
        assertThat(PlanValues.read(f.mapper, doc, "leaves.pick_b", true)).isEqualTo(List.of("B"));
    }

    @Test
    void everyElementEmptyReadsAsMissing() throws Exception {
        Fixtures f = Fixtures.create();
        DynamicMessage doc = f.docOf(DynamicMessage.newBuilder(f.leaf).build(),
                DynamicMessage.newBuilder(f.leaf).build());

        assertThat(PlanValues.read(f.mapper, doc, "leaves.text", false)).isNull();
        assertThat(PlanValues.read(f.mapper, doc, "leaves.meta.label", false)).isNull();
    }

    // ------------------------------------------------------------------ boundaries

    @Test
    void structKeysTraversePerElementAndUnsetStructsContributeNothing() throws Exception {
        Fixtures f = Fixtures.create();
        DynamicMessage withProps = DynamicMessage.newBuilder(f.leaf)
                .setField(f.leaf.findFieldByName("props"), Struct.newBuilder()
                        .putFields("title", Value.newBuilder().setStringValue("T1").build())
                        .build())
                .build();
        DynamicMessage doc = f.docOf(withProps, f.leaf("no props"));

        assertThat(PlanValues.read(f.mapper, doc, "leaves.props.title", false))
                .isEqualTo(List.of("T1"));
    }

    @Test
    void mapLeafUnderAFanOutConcatenatesEntriesFromEveryElement() throws Exception {
        Fixtures f = Fixtures.create();
        DynamicMessage doc = f.docOf(f.leafWithAttr("k1", "v1"), f.leafWithAttr("k2", "v2"));

        Object value = PlanValues.read(f.mapper, doc, "leaves.attrs", false);

        // Map entries stay map entries (engines still detect the map-entry list shape).
        assertThat(value).asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.LIST)
                .hasSize(2)
                .allSatisfy(entry -> assertThat(((Message) entry).getDescriptorForType()
                        .getOptions().getMapEntry()).isTrue());
    }

    @Test
    void mapKeyPathUnderAFanOutStaysWithTheMapperError() throws Exception {
        Fixtures f = Fixtures.create();
        DynamicMessage doc = f.docOf(f.leafWithAttr("k1", "v1"));

        assertThatThrownBy(() -> PlanValues.read(f.mapper, doc, "leaves.attrs.k1", false))
                .isInstanceOf(MappingException.class);
    }

    @Test
    void repeatedScalarIntermediateFailsLoudly() throws Exception {
        Fixtures f = Fixtures.create();
        DynamicMessage doc = f.docOf(f.leaf("alpha"));

        assertThatThrownBy(() -> PlanValues.read(f.mapper, doc, "leaves.tags.nope", false))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("tags");
    }

    @Test
    void unknownSegmentAboveAFanOutFailsLoudly() throws Exception {
        Fixtures f = Fixtures.create();
        DynamicMessage doc = f.docOf(f.leaf("alpha"));

        assertThatThrownBy(() -> PlanValues.read(f.mapper, doc, "bogus.text", false))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("bogus");
    }

    // ------------------------------------------------------------------ Any

    @Test
    void anyLeafUnderAFanOutUnpacksPerElement() throws Exception {
        Fixtures f = Fixtures.create();
        f.registry.register(f.inner);
        DynamicMessage doc = f.docOf(f.leafWithPayload("L1"), f.leafWithPayload("L2"));

        Object value = PlanValues.read(f.mapper, doc, "leaves.payload", false);

        assertThat(value).asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.LIST)
                .hasSize(2);
        assertThat(((Message) ((List<?>) value).get(0))
                .getField(f.inner.findFieldByName("label"))).isEqualTo("L1");
    }

    @Test
    void anyIntermediateUnderAFanOutUnpacksAndKeepsReading() throws Exception {
        Fixtures f = Fixtures.create();
        f.registry.register(f.inner);
        DynamicMessage doc = f.docOf(f.leafWithPayload("L1"), f.leafWithPayload("L2"));

        assertThat(PlanValues.read(f.mapper, doc, "leaves.payload.label", false))
                .isEqualTo(List.of("L1", "L2"));
    }

    @Test
    void unregisteredAnyTypeUnderAFanOutFailsLoudly() throws Exception {
        Fixtures f = Fixtures.create();
        // Inner deliberately NOT registered: an unknown type URL must never be a silent skip.
        DynamicMessage doc = f.docOf(f.leafWithPayload("L1"));

        assertThatThrownBy(() -> PlanValues.read(f.mapper, doc, "leaves.payload", false))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("Inner");
    }

    @Test
    // An element whose Any intermediate is unset contributes nothing, exactly like an
    // unset plain message parent.
    void unsetAnyIntermediateInsideOneElementContributesNothing() throws Exception {
        Fixtures f = Fixtures.create();
        f.registry.register(f.inner);
        DynamicMessage doc = f.docOf(f.leafWithPayload("L1"), f.leaf("no payload"));

        // Singular baseline: an unset Any parent is missing, not an error.
        assertThat(PlanValues.read(f.mapper, f.docWithSingle(f.leaf("plain")),
                "single.payload.label", false)).isNull();

        assertThat(PlanValues.read(f.mapper, doc, "leaves.payload.label", false))
                .isEqualTo(List.of("L1"));
    }

    @Test
    // An AnyIndexing-expanded child path whose packed type declares a CHUNKS scope
    // ("payload.leaves.text") fans out through the unpacked Any instead of failing.
    void anyExpandedChildrenUnderAChunksScopeAreReadable() throws Exception {
        Fixtures f = Fixtures.create();
        f.registry.register(f.group);
        CatalogIndexingHintSource catalog = new CatalogIndexingHintSource()
                .put(f.group.getFullName(), "leaves", ResolvedFieldHint.builder(IndexFieldKind.NESTED)
                        .blockRole(BlockRole.CHUNKS)
                        .build());
        IndexingPlanFactory factory =
                new IndexingPlanFactory(catalog.orElse(new InferringIndexingHintSource()));
        DynamicMessage message = DynamicMessage.newBuilder(f.doc)
                .setField(f.doc.findFieldByName("payload"),
                        Any.pack(f.groupOf(f.leaf("a1"), f.leaf("a2"))))
                .build();
        // No payload validators: this is about the plan paths, not the Any gate.
        IndexingPlan expanded = new AnyIndexing(f.registry, factory, List.of())
                .expand(message, factory.create(f.doc));

        String path = expanded.find("payload.leaves.text").orElseThrow().path();
        assertThat(PlanValues.read(f.mapper, message, path, false))
                .isEqualTo(List.of("a1", "a2"));
    }

    // ------------------------------------------------------------------ literals

    @Test
    // Plan-path segments are always field names: a leaf named "true" reads the field
    // below a fan-out, never the mapper DSL's boolean literal.
    void aLeafSegmentNamedLikeALiteralIsStillAField() throws Exception {
        Fixtures f = Fixtures.create();
        DynamicMessage doc = DynamicMessage.newBuilder(f.doc)
                .setField(f.doc.findFieldByName("single"), f.leafNamedTrue("yes"))
                .addRepeatedField(f.doc.findFieldByName("leaves"), f.leafNamedTrue("yes"))
                .addRepeatedField(f.doc.findFieldByName("leaves"), f.leafNamedTrue("also"))
                .build();

        // Singular baseline: non-first segments are never literal-parsed by the mapper.
        assertThat(PlanValues.read(f.mapper, doc, "single.true", false)).isEqualTo("yes");

        assertThat(PlanValues.read(f.mapper, doc, "leaves.true", false))
                .isEqualTo(List.of("yes", "also"));
    }

    // ------------------------------------------------------------------ plan errors

    @Test
    // Path validity must not depend on the document: an unresolvable remainder fails even
    // when the repeated field has zero elements.
    void unknownFieldBelowAnEmptyFanOutStillFailsLoudly() throws Exception {
        Fixtures f = Fixtures.create();

        assertThatThrownBy(() -> PlanValues.read(f.mapper, f.docOf(), "leaves.missing", false))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("missing");
    }

    @Test
    // A child expanded under a repeated ancestor is multi-valued at write time, so the
    // plan stamps it repeated whatever the child field's own cardinality.
    void chunkExpandedChildrenArePlannedMultiValued() throws Exception {
        Fixtures f = Fixtures.create();
        CatalogIndexingHintSource catalog = new CatalogIndexingHintSource()
                .put(f.doc.getFullName(), "leaves", ResolvedFieldHint.builder(IndexFieldKind.NESTED)
                        .blockRole(BlockRole.CHUNKS)
                        .build());
        IndexingPlan plan = new IndexingPlanFactory(
                catalog.orElse(new InferringIndexingHintSource())).create(f.doc);

        assertThat(plan.find("leaves").orElseThrow().repeated()).isTrue();
        assertThat(plan.find("leaves.text").orElseThrow().repeated()).isTrue();
    }

    // ------------------------------------------------------------------ fixtures

    private record Fixtures(
            Descriptor doc,
            Descriptor leaf,
            Descriptor inner,
            Descriptor group,
            Descriptor node,
            DescriptorRegistry registry,
            ProtoFieldMapper mapper) {

        static Fixtures create() throws Exception {
            FileDescriptor file = file();
            DescriptorRegistry registry = new DescriptorRegistry();
            return new Fixtures(
                    file.findMessageTypeByName("Doc"),
                    file.findMessageTypeByName("Leaf"),
                    file.findMessageTypeByName("Inner"),
                    file.findMessageTypeByName("Group"),
                    file.findMessageTypeByName("Node"),
                    registry,
                    new ProtoFieldMapperImpl(registry));
        }

        DynamicMessage leaf(String text) {
            return DynamicMessage.newBuilder(leaf)
                    .setField(leaf.findFieldByName("text"), text)
                    .build();
        }

        DynamicMessage leafNamedTrue(String value) {
            return DynamicMessage.newBuilder(leaf)
                    .setField(leaf.findFieldByName("true"), value)
                    .build();
        }

        DynamicMessage leafWithAttr(String key, String value) {
            Descriptor entry = leaf.findNestedTypeByName("AttrsEntry");
            return DynamicMessage.newBuilder(leaf)
                    .addRepeatedField(leaf.findFieldByName("attrs"), DynamicMessage.newBuilder(entry)
                            .setField(entry.findFieldByName("key"), key)
                            .setField(entry.findFieldByName("value"), value)
                            .build())
                    .build();
        }

        DynamicMessage leafWithPayload(String label) {
            return DynamicMessage.newBuilder(leaf)
                    .setField(leaf.findFieldByName("payload"), Any.pack(inner(label)))
                    .build();
        }

        DynamicMessage inner(String label) {
            return DynamicMessage.newBuilder(inner)
                    .setField(inner.findFieldByName("label"), label)
                    .build();
        }

        DynamicMessage node(String name) {
            return DynamicMessage.newBuilder(node)
                    .setField(node.findFieldByName("name"), name)
                    .build();
        }

        DynamicMessage groupOf(DynamicMessage... leaves) {
            DynamicMessage.Builder builder = DynamicMessage.newBuilder(group);
            FieldDescriptor field = group.findFieldByName("leaves");
            for (DynamicMessage element : leaves) {
                builder.addRepeatedField(field, element);
            }
            return builder.build();
        }

        DynamicMessage docOf(DynamicMessage... leaves) {
            DynamicMessage.Builder builder = DynamicMessage.newBuilder(doc)
                    .setField(doc.findFieldByName("doc_id"), "d1");
            FieldDescriptor field = doc.findFieldByName("leaves");
            for (DynamicMessage element : leaves) {
                builder.addRepeatedField(field, element);
            }
            return builder.build();
        }

        DynamicMessage docWithSingle(DynamicMessage single) {
            return DynamicMessage.newBuilder(doc)
                    .setField(doc.findFieldByName("single"), single)
                    .build();
        }

        DynamicMessage docWithGroups(DynamicMessage... groups) {
            DynamicMessage.Builder builder = DynamicMessage.newBuilder(doc)
                    .setField(doc.findFieldByName("doc_id"), "d1");
            FieldDescriptor field = doc.findFieldByName("groups");
            for (DynamicMessage element : groups) {
                builder.addRepeatedField(field, element);
            }
            return builder.build();
        }

        private static FileDescriptor file() throws Exception {
            String pkg = ".ai.pipestream.test.planedge";
            FileDescriptorProto proto = FileDescriptorProto.newBuilder()
                    .setName("plan_values_edge.proto")
                    .setPackage("ai.pipestream.test.planedge")
                    .setSyntax("proto3")
                    .addDependency("google/protobuf/any.proto")
                    .addDependency("google/protobuf/struct.proto")
                    .addMessageType(DescriptorProto.newBuilder()
                            .setName("Inner")
                            .addField(string("label", 1)))
                    .addMessageType(DescriptorProto.newBuilder()
                            .setName("Leaf")
                            .addField(string("text", 1))
                            // A field whose name collides with the mapper's literal vocabulary.
                            .addField(string("true", 2))
                            .addField(FieldDescriptorProto.newBuilder()
                                    .setName("score").setNumber(3)
                                    .setType(FieldDescriptorProto.Type.TYPE_INT32)
                                    .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                            .addField(message("meta", 4, pkg + ".Inner",
                                    FieldDescriptorProto.Label.LABEL_OPTIONAL))
                            .addField(message("payload", 5, ".google.protobuf.Any",
                                    FieldDescriptorProto.Label.LABEL_OPTIONAL))
                            .addField(message("attrs", 6, pkg + ".Leaf.AttrsEntry",
                                    FieldDescriptorProto.Label.LABEL_REPEATED))
                            .addField(string("tags", 7)
                                    .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED))
                            .addField(message("props", 8, ".google.protobuf.Struct",
                                    FieldDescriptorProto.Label.LABEL_OPTIONAL))
                            .addField(string("pick_a", 9).setOneofIndex(0))
                            .addField(string("pick_b", 10).setOneofIndex(0))
                            .addOneofDecl(OneofDescriptorProto.newBuilder().setName("choice"))
                            .addNestedType(DescriptorProto.newBuilder()
                                    .setName("AttrsEntry")
                                    .setOptions(MessageOptions.newBuilder().setMapEntry(true))
                                    .addField(string("key", 1))
                                    .addField(string("value", 2))))
                    .addMessageType(DescriptorProto.newBuilder()
                            .setName("Group")
                            .addField(message("leaves", 1, pkg + ".Leaf",
                                    FieldDescriptorProto.Label.LABEL_REPEATED)))
                    .addMessageType(DescriptorProto.newBuilder()
                            .setName("Node")
                            .addField(string("name", 1))
                            .addField(message("children", 2, pkg + ".Node",
                                    FieldDescriptorProto.Label.LABEL_REPEATED)))
                    .addMessageType(DescriptorProto.newBuilder()
                            .setName("Doc")
                            .addField(string("doc_id", 1))
                            .addField(message("leaves", 2, pkg + ".Leaf",
                                    FieldDescriptorProto.Label.LABEL_REPEATED))
                            .addField(message("solo", 3, pkg + ".Inner",
                                    FieldDescriptorProto.Label.LABEL_OPTIONAL))
                            .addField(message("groups", 4, pkg + ".Group",
                                    FieldDescriptorProto.Label.LABEL_REPEATED))
                            .addField(message("root", 5, pkg + ".Node",
                                    FieldDescriptorProto.Label.LABEL_OPTIONAL))
                            .addField(message("single", 6, pkg + ".Leaf",
                                    FieldDescriptorProto.Label.LABEL_OPTIONAL))
                            .addField(message("solo_group", 7, pkg + ".Group",
                                    FieldDescriptorProto.Label.LABEL_OPTIONAL))
                            .addField(message("payload", 8, ".google.protobuf.Any",
                                    FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                    .build();
            return FileDescriptor.buildFrom(proto, new FileDescriptor[]{
                    AnyProto.getDescriptor(), StructProto.getDescriptor()});
        }

        private static FieldDescriptorProto.Builder string(String name, int number) {
            return FieldDescriptorProto.newBuilder()
                    .setName(name)
                    .setNumber(number)
                    .setType(FieldDescriptorProto.Type.TYPE_STRING)
                    .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL);
        }

        private static FieldDescriptorProto.Builder message(
                String name, int number, String typeName, FieldDescriptorProto.Label label) {
            return FieldDescriptorProto.newBuilder()
                    .setName(name)
                    .setNumber(number)
                    .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                    .setTypeName(typeName)
                    .setLabel(label);
        }
    }
}
