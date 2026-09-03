package ai.protomolt.proto.search.index.lucene;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.search.index.spi.IndexFieldKind;
import ai.protomolt.proto.search.index.spi.IndexMapping;
import ai.protomolt.proto.search.index.spi.ResolvedFieldHint;
import ai.protomolt.proto.mapper.ProtoFieldMapperImpl;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Fan-out edge cases for the Lucene mapper: a mapping path under a repeated ancestor now
 * yields several values for one field, so every field shape it emits has to be one Lucene
 * accepts more than once per document.
 */
class ProtoLuceneMapperFanOutTest {

    private final ProtoLuceneMapper mapper =
            new ProtoLuceneMapper(new ProtoFieldMapperImpl(new DescriptorRegistry()));

    @Test
    void fanOutEmitsOneIndexableFieldPerValue() throws Exception {
        Fixture f = Fixture.create();
        Document document = mapper.map(f.docOf(f.chunk("alpha", 3), f.chunk("beta", 5)),
                f.mapping(new IndexMapping.IndexedField("chunks.text", "chunks_text",
                        ResolvedFieldHint.of(IndexFieldKind.TEXT), true)));

        assertThat(document.getValues("chunks_text")).containsExactly("alpha", "beta");
    }

    @Test
    void numericFanOutEmitsOnePointAndOneStoredValuePerElement() throws Exception {
        Fixture f = Fixture.create();
        Document document = mapper.map(f.docOf(f.chunk("alpha", 3), f.chunk("beta", 5)),
                f.mapping(new IndexMapping.IndexedField("chunks.score", "chunks_score",
                        ResolvedFieldHint.of(IndexFieldKind.INT64), true)));

        assertThat(Arrays.stream(document.getFields("chunks_score"))
                .filter(field -> field.fieldType().stored())
                .map(IndexableField::numericValue))
                .containsExactly(3L, 5L);
    }

    @Test
    void facetableFanOutUsesMultiValuedDocValues() throws Exception {
        Fixture f = Fixture.create();
        Document document = mapper.map(f.docOf(f.chunk("alpha", 3), f.chunk("beta", 5)),
                f.mapping(new IndexMapping.IndexedField("chunks.text", "chunks_text",
                        ResolvedFieldHint.builder(IndexFieldKind.KEYWORD).facetable(true).build(),
                        true)));

        assertThat(Arrays.stream(document.getFields("chunks_text"))
                .map(field -> field.fieldType().docValuesType())
                .filter(type -> type != DocValuesType.NONE))
                .containsExactly(DocValuesType.SORTED_SET, DocValuesType.SORTED_SET);
        assertThatCode(() -> index(document)).doesNotThrowAnyException();
    }

    @Test
    // Multi-valued reads must sort through SORTED_SET doc values: a single-valued
    // SortedDocValuesField per element would be rejected by Lucene at index time.
    void sortableFanOutStillProducesAnIndexableDocument() throws Exception {
        Fixture f = Fixture.create();
        Document document = mapper.map(f.docOf(f.chunk("alpha", 3), f.chunk("beta", 5)),
                f.mapping(new IndexMapping.IndexedField("chunks.text", "chunks_text",
                        ResolvedFieldHint.builder(IndexFieldKind.KEYWORD).sortable(true).build(),
                        true)));

        assertThatCode(() -> index(document)).doesNotThrowAnyException();
    }

    private static void index(Document document) throws Exception {
        try (Directory directory = new ByteBuffersDirectory();
             IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer()))) {
            writer.addDocument(document);
            writer.commit();
        }
    }

    private record Fixture(Descriptor doc, Descriptor chunk) {

        static Fixture create() throws Exception {
            FileDescriptor file = file();
            return new Fixture(
                    file.findMessageTypeByName("Doc"),
                    file.findMessageTypeByName("Chunk"));
        }

        IndexMapping mapping(IndexMapping.IndexedField... fields) {
            return new IndexMapping(doc.getFullName(), List.of(fields));
        }

        DynamicMessage docOf(DynamicMessage... chunks) {
            DynamicMessage.Builder builder = DynamicMessage.newBuilder(doc)
                    .setField(doc.findFieldByName("doc_id"), "d1");
            for (DynamicMessage element : chunks) {
                builder.addRepeatedField(doc.findFieldByName("chunks"), element);
            }
            return builder.build();
        }

        DynamicMessage chunk(String text, long score) {
            return DynamicMessage.newBuilder(chunk)
                    .setField(chunk.findFieldByName("text"), text)
                    .setField(chunk.findFieldByName("score"), score)
                    .build();
        }

        private static FileDescriptor file() throws Exception {
            FileDescriptorProto proto = FileDescriptorProto.newBuilder()
                    .setName("lucene_fanout.proto")
                    .setPackage("ai.pipestream.test.lucenefanout")
                    .setSyntax("proto3")
                    .addMessageType(DescriptorProto.newBuilder()
                            .setName("Chunk")
                            .addField(FieldDescriptorProto.newBuilder()
                                    .setName("text").setNumber(1)
                                    .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                    .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                            .addField(FieldDescriptorProto.newBuilder()
                                    .setName("score").setNumber(2)
                                    .setType(FieldDescriptorProto.Type.TYPE_INT64)
                                    .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                    .addMessageType(DescriptorProto.newBuilder()
                            .setName("Doc")
                            .addField(FieldDescriptorProto.newBuilder()
                                    .setName("doc_id").setNumber(1)
                                    .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                    .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                            .addField(FieldDescriptorProto.newBuilder()
                                    .setName("chunks").setNumber(2)
                                    .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                                    .setTypeName(".ai.pipestream.test.lucenefanout.Chunk")
                                    .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED)))
                    .build();
            return FileDescriptor.buildFrom(proto, new FileDescriptor[]{});
        }
    }
}
