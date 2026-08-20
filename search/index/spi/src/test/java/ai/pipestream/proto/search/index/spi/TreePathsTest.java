package ai.pipestream.proto.search.index.spi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.pipestream.proto.types.TreePath;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.FileDescriptor;
import org.junit.jupiter.api.Test;

/**
 * The canonical types.v1 TreePath resolves by name and infers TREE_PATH with
 * no hint at all — facetable out of the box, the hierarchical drill-down facet
 * being the type's platform behavior. Any other message duck-types through a
 * repeated string {@code segments} field, and emission is the ancestor chain.
 */
class TreePathsTest {

    @Test
    void theCanonicalTypeIsRecognizedByName() {
        assertThat(TreePaths.isCanonical(TreePath.getDescriptor())).isTrue();
        assertThat(TreePaths.resolve(TreePath.getDescriptor())).isPresent();
    }

    @Test
    void anyMessageWithRepeatedStringSegmentsDuckTypes() throws Exception {
        var withSegments = messageType("Duck", FieldDescriptorProto.newBuilder()
                .setName("segments").setNumber(1)
                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED));
        assertThat(TreePaths.resolve(withSegments)).isPresent();
        assertThat(TreePaths.isCanonical(withSegments)).isFalse();

        var singularSegments = messageType("Singular", FieldDescriptorProto.newBuilder()
                .setName("segments").setNumber(1)
                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL));
        assertThat(TreePaths.resolve(singularSegments)).isEmpty();

        var numericSegments = messageType("Numeric", FieldDescriptorProto.newBuilder()
                .setName("segments").setNumber(1)
                .setType(FieldDescriptorProto.Type.TYPE_INT64)
                .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED));
        assertThat(TreePaths.resolve(numericSegments)).isEmpty();
    }

    @Test
    void theAncestorChainIsRootFirstAndDelimiterJoined() {
        var segments = TreePaths.resolve(TreePath.getDescriptor()).orElseThrow();
        assertThat(TreePaths.ancestorPaths(TreePath.newBuilder()
                .addSegments("electronics").addSegments("audio").addSegments("headphones")
                .build(), segments))
                .containsExactly("electronics", "electronics/audio",
                        "electronics/audio/headphones");
        assertThat(TreePaths.ancestorPaths(TreePath.getDefaultInstance(), segments)).isEmpty();
    }

    @Test
    void canonicalFieldsInferTreePathFacetableWithNoHint() throws Exception {
        var doc = docWithTreePathFields();

        ResolvedFieldHint singular =
                InferringIndexingHintSource.infer(doc.findFieldByName("category"));
        assertThat(singular.type()).isEqualTo(IndexFieldKind.TREE_PATH);
        assertThat(singular.facetable()).isTrue();

        // Repeated too: a document may carry several taxonomy paths.
        ResolvedFieldHint repeated =
                InferringIndexingHintSource.infer(doc.findFieldByName("categories"));
        assertThat(repeated.type()).isEqualTo(IndexFieldKind.TREE_PATH);
        assertThat(repeated.facetable()).isTrue();
    }

    @Test
    void mappingRefusesTreePathOverAMessageWithoutSegments() throws Exception {
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("no_segments_doc.proto")
                .setPackage("ai.pipestream.test")
                .setSyntax("proto3")
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("NotAPath")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("label").setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("Doc")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("category").setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setTypeName(".ai.pipestream.test.NotAPath")
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                .build();
        var doc = FileDescriptor.buildFrom(file, new FileDescriptor[0])
                .findMessageTypeByName("Doc");

        var factory = new IndexMappingFactory(field ->
                java.util.Optional.of(ResolvedFieldHint.of(IndexFieldKind.TREE_PATH)));
        assertThatThrownBy(() -> factory.create(doc))
                .isInstanceOf(IndexMappingException.class)
                .hasMessageContaining("repeated string 'segments'");
    }

    @Test
    void mappingKeepsTreePathFieldsAsSingleEntries() throws Exception {
        var doc = docWithTreePathFields();
        IndexMapping mapping = IndexMappingFactory.inferringOnly().create(doc);
        assertThat(mapping.indexable())
                .extracting(IndexMapping.IndexedField::fieldName,
                        f -> f.hint().type())
                .containsExactly(
                        org.assertj.core.api.Assertions.tuple(
                                "category", IndexFieldKind.TREE_PATH),
                        org.assertj.core.api.Assertions.tuple(
                                "categories", IndexFieldKind.TREE_PATH));
    }

    private static com.google.protobuf.Descriptors.Descriptor docWithTreePathFields()
            throws Exception {
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("tree_path_doc.proto")
                .setPackage("ai.pipestream.test")
                .setSyntax("proto3")
                .addDependency("ai/pipestream/proto/types/v1/tree_path.proto")
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("Doc")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("category").setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setTypeName(".ai.pipestream.proto.types.v1.TreePath")
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("categories").setNumber(2)
                                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setTypeName(".ai.pipestream.proto.types.v1.TreePath")
                                .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED)))
                .build();
        return FileDescriptor.buildFrom(
                        file, new FileDescriptor[]{TreePath.getDescriptor().getFile()})
                .findMessageTypeByName("Doc");
    }

    private static com.google.protobuf.Descriptors.Descriptor messageType(
            String name, FieldDescriptorProto.Builder field) throws Exception {
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName(name.toLowerCase(java.util.Locale.ROOT) + ".proto")
                .setPackage("ai.pipestream.test")
                .setSyntax("proto3")
                .addMessageType(DescriptorProto.newBuilder().setName(name).addField(field))
                .build();
        return FileDescriptor.buildFrom(file, new FileDescriptor[0])
                .findMessageTypeByName(name);
    }
}
