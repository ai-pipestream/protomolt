package ai.pipestream.proto.search.index.opensearch;

import static org.assertj.core.api.Assertions.assertThat;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.search.index.spi.IndexFieldKind;
import ai.pipestream.proto.search.index.spi.IndexMapping;
import ai.pipestream.proto.search.index.spi.ResolvedFieldHint;
import ai.pipestream.proto.mapper.ProtoFieldMapperImpl;
import ai.pipestream.proto.types.TreePath;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

/**
 * TreePath through the OpenSearch mapper: each path value emits its
 * ancestor chain as a flat keyword array, so a terms aggregation counts at
 * any depth and a path-prefix filter is an exact term match. The mapping
 * generator types the field {@code keyword}.
 */
class TreePathMappingTest {

    private final OpenSearchDocumentMapper mapper =
            new OpenSearchDocumentMapper(new ProtoFieldMapperImpl(new DescriptorRegistry()));

    private static final TreePath AUDIO = TreePath.newBuilder()
            .addSegments("electronics").addSegments("audio").addSegments("headphones").build();

    @Test
    void aPathEmitsItsAncestorChain() throws Exception {
        Descriptor docType = docDescriptor(false);
        DynamicMessage message = DynamicMessage.newBuilder(docType)
                .setField(docType.findFieldByName("category"), AUDIO)
                .build();
        Map<String, Object> doc = mapper.map(message, mapping(false));
        assertThat(doc.get("category")).isEqualTo(List.of(
                "electronics", "electronics/audio", "electronics/audio/headphones"));
    }

    @Test
    void repeatedPathsMergeTheirChains() throws Exception {
        Descriptor docType = docDescriptor(true);
        var field = docType.findFieldByName("category");
        DynamicMessage message = DynamicMessage.newBuilder(docType)
                .addRepeatedField(field, AUDIO)
                .addRepeatedField(field, TreePath.newBuilder()
                        .addSegments("clearance").addSegments("audio").build())
                .build();
        Map<String, Object> doc = mapper.map(message, mapping(true));
        assertThat(doc.get("category")).isEqualTo(List.of(
                "electronics", "electronics/audio", "electronics/audio/headphones",
                "clearance", "clearance/audio"));
    }

    @Test
    void theMappingTypeIsKeyword() {
        Map<String, Object> generated = new OpenSearchMappingGenerator().generate(mapping(false));
        @SuppressWarnings("unchecked")
        Map<String, Object> properties = (Map<String, Object>) generated.get("properties");
        @SuppressWarnings("unchecked")
        Map<String, Object> category = (Map<String, Object>) properties.get("category");
        assertThat(category.get("type")).isEqualTo("keyword");
    }

    private static IndexMapping mapping(boolean repeated) {
        return new IndexMapping("ai.pipestream.test.Doc", List.of(
                new IndexMapping.IndexedField("category", "category",
                        ResolvedFieldHint.builder(IndexFieldKind.TREE_PATH)
                                .facetable(true).build(),
                        repeated)));
    }

    private static Descriptor docDescriptor(boolean repeated) throws Exception {
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("tree_path_doc_" + (repeated ? "repeated" : "singular") + ".proto")
                .setPackage("ai.pipestream.test")
                .setSyntax("proto3")
                .addDependency(TreePath.getDescriptor().getFile().getName())
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("Doc")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("category")
                                .setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setTypeName("." + TreePath.getDescriptor().getFullName())
                                .setLabel(repeated
                                        ? FieldDescriptorProto.Label.LABEL_REPEATED
                                        : FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                .build();
        return FileDescriptor.buildFrom(
                        file, new FileDescriptor[]{TreePath.getDescriptor().getFile()})
                .findMessageTypeByName("Doc");
    }
}
