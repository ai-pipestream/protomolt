package ai.pipestream.proto.search.index.solr;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.search.index.spi.BlockRole;
import ai.pipestream.proto.search.index.spi.CatalogIndexingHintSource;
import ai.pipestream.proto.search.index.spi.IndexFieldKind;
import ai.pipestream.proto.search.index.spi.IndexMapping;
import ai.pipestream.proto.search.index.spi.IndexMappingFactory;
import ai.pipestream.proto.search.index.spi.InferringIndexingHintSource;
import ai.pipestream.proto.search.index.spi.ResolvedFieldHint;
import ai.pipestream.proto.mapper.ProtoFieldMapperImpl;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Fan-out edge cases for the Solr mapper, plus the schema contract that the flattened
 * values have to satisfy (Solr rejects a second value for a single-valued field).
 */
class SolrDocumentMapperFanOutTest {

    private final SolrDocumentMapper mapper =
            new SolrDocumentMapper(new ProtoFieldMapperImpl(new DescriptorRegistry()));

    @Test
    void fanOutValuesLandAsOneMultiValuedDocumentField() throws Exception {
        Fixture f = Fixture.create();
        DynamicMessage message = f.docOf(f.chunk("alpha"), f.chunk("beta"));
        IndexMapping mapping = new IndexMapping(f.doc.getFullName(), List.of(
                new IndexMapping.IndexedField("chunks.text", "chunks_text",
                        ResolvedFieldHint.of(IndexFieldKind.TEXT), true)));

        assertThat(mapper.map(message, mapping))
                .containsEntry("chunks_text", List.of("alpha", "beta"));
    }

    @Test
    void messageLeavesUnderAFanOutBecomeJsonStrings() throws Exception {
        Fixture f = Fixture.create();
        DynamicMessage message = f.docOf(f.chunkWithMeta("m1"), f.chunkWithMeta("m2"));
        IndexMapping mapping = new IndexMapping(f.doc.getFullName(), List.of(
                new IndexMapping.IndexedField("chunks.meta", "chunks_meta",
                        ResolvedFieldHint.of(IndexFieldKind.OBJECT), true)));

        // Solr documents are flat: each element message is its own compact JSON string.
        assertThat(mapper.map(message, mapping))
                .containsEntry("chunks_meta", List.of("{\"label\":\"m1\"}", "{\"label\":\"m2\"}"));
    }

    @Test
    // A CHUNKS child is multi-valued at write time whatever its own cardinality, so the
    // generated schema must declare multiValued or Solr rejects the fan-out document.
    void chunkExpandedFieldsAreDeclaredMultiValued() throws Exception {
        Fixture f = Fixture.create();
        CatalogIndexingHintSource catalog = new CatalogIndexingHintSource()
                .put(f.doc.getFullName(), "chunks", ResolvedFieldHint.builder(IndexFieldKind.NESTED)
                        .blockRole(BlockRole.CHUNKS)
                        .build());
        IndexMapping mapping = new IndexMappingFactory(
                catalog.orElse(new InferringIndexingHintSource())).create(f.doc);

        Map<String, Object> document =
                mapper.map(f.docOf(f.chunk("alpha"), f.chunk("beta")), mapping);
        assertThat(document.get("text")).isEqualTo(List.of("alpha", "beta"));

        Map<String, Object> textField = new SolrSchemaGenerator().generate(mapping).fields().stream()
                .filter(field -> "text".equals(field.get("name")))
                .findFirst()
                .orElseThrow();
        assertThat(textField).containsEntry("multiValued", true);
    }

    private record Fixture(Descriptor doc, Descriptor chunk, Descriptor inner) {

        static Fixture create() throws Exception {
            FileDescriptor file = file();
            return new Fixture(
                    file.findMessageTypeByName("Doc"),
                    file.findMessageTypeByName("Chunk"),
                    file.findMessageTypeByName("Inner"));
        }

        DynamicMessage docOf(DynamicMessage... chunks) {
            DynamicMessage.Builder builder = DynamicMessage.newBuilder(doc)
                    .setField(doc.findFieldByName("doc_id"), "d1");
            for (DynamicMessage element : chunks) {
                builder.addRepeatedField(doc.findFieldByName("chunks"), element);
            }
            return builder.build();
        }

        DynamicMessage chunk(String text) {
            return DynamicMessage.newBuilder(chunk)
                    .setField(chunk.findFieldByName("text"), text)
                    .build();
        }

        DynamicMessage chunkWithMeta(String label) {
            return DynamicMessage.newBuilder(chunk)
                    .setField(chunk.findFieldByName("meta"), DynamicMessage.newBuilder(inner)
                            .setField(inner.findFieldByName("label"), label)
                            .build())
                    .build();
        }

        private static FileDescriptor file() throws Exception {
            String pkg = ".ai.pipestream.test.solrfanout";
            FileDescriptorProto proto = FileDescriptorProto.newBuilder()
                    .setName("solr_fanout.proto")
                    .setPackage("ai.pipestream.test.solrfanout")
                    .setSyntax("proto3")
                    .addMessageType(DescriptorProto.newBuilder()
                            .setName("Inner")
                            .addField(string("label", 1)))
                    .addMessageType(DescriptorProto.newBuilder()
                            .setName("Chunk")
                            .addField(string("text", 1))
                            .addField(message("meta", 2, pkg + ".Inner",
                                    FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                    .addMessageType(DescriptorProto.newBuilder()
                            .setName("Doc")
                            .addField(string("doc_id", 1))
                            .addField(message("chunks", 2, pkg + ".Chunk",
                                    FieldDescriptorProto.Label.LABEL_REPEATED)))
                    .build();
            return FileDescriptor.buildFrom(proto, new FileDescriptor[]{});
        }

        private static FieldDescriptorProto.Builder string(String name, int number) {
            return FieldDescriptorProto.newBuilder()
                    .setName(name).setNumber(number)
                    .setType(FieldDescriptorProto.Type.TYPE_STRING)
                    .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL);
        }

        private static FieldDescriptorProto.Builder message(
                String name, int number, String typeName, FieldDescriptorProto.Label label) {
            return FieldDescriptorProto.newBuilder()
                    .setName(name).setNumber(number)
                    .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                    .setTypeName(typeName)
                    .setLabel(label);
        }
    }
}
