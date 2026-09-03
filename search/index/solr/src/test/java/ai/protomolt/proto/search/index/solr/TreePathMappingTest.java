package ai.protomolt.proto.search.index.solr;

import static org.assertj.core.api.Assertions.assertThat;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.search.index.spi.IndexFieldKind;
import ai.protomolt.proto.search.index.spi.IndexMapping;
import ai.protomolt.proto.search.index.spi.ResolvedFieldHint;
import ai.protomolt.proto.mapper.ProtoFieldMapperImpl;
import ai.protomolt.proto.types.TreePath;
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
 * TreePath through the Solr mapper: each path value emits its ancestor
 * chain into one multi-valued string field, so faceting counts at any
 * depth and a path-prefix filter is an exact term match. The schema
 * declares the field multiValued even for a singular proto field — a
 * single path still emits its whole chain.
 */
class TreePathMappingTest {

    private final SolrDocumentMapper mapper =
            new SolrDocumentMapper(new ProtoFieldMapperImpl(new DescriptorRegistry()));

    private static final TreePath AUDIO = TreePath.newBuilder()
            .addSegments("electronics").addSegments("audio").addSegments("headphones").build();

    @Test
    void aPathEmitsItsAncestorChain() throws Exception {
        Descriptor docType = docDescriptor();
        DynamicMessage message = DynamicMessage.newBuilder(docType)
                .setField(docType.findFieldByName("category"), AUDIO)
                .build();
        Map<String, Object> doc = mapper.map(message, mapping());
        assertThat(doc.get("category")).isEqualTo(List.of(
                "electronics", "electronics/audio", "electronics/audio/headphones"));
    }

    @Test
    void theSchemaFieldIsAlwaysMultiValued() {
        SolrSchemaGenerator.SolrSchema schema = new SolrSchemaGenerator().generate(mapping());
        Map<String, Object> field = schema.fields().stream()
                .filter(f -> "category".equals(f.get("name")))
                .findFirst().orElseThrow();
        assertThat(field.get("type")).isEqualTo("string");
        // The proto field is singular, but the chain is multi-valued by construction.
        assertThat(field.get("multiValued")).isEqualTo(true);
        assertThat(field.get("docValues")).isEqualTo(true);
    }

    private static IndexMapping mapping() {
        return new IndexMapping("ai.pipestream.test.Doc", List.of(
                new IndexMapping.IndexedField("category", "category",
                        ResolvedFieldHint.builder(IndexFieldKind.TREE_PATH)
                                .facetable(true).build())));
    }

    private static Descriptor docDescriptor() throws Exception {
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("tree_path_doc.proto")
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
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                .build();
        return FileDescriptor.buildFrom(
                        file, new FileDescriptor[]{TreePath.getDescriptor().getFile()})
                .findMessageTypeByName("Doc");
    }
}
