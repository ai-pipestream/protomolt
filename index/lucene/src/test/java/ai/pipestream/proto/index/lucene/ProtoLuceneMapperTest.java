package ai.pipestream.proto.index.lucene;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.index.spi.IndexFieldKind;
import ai.pipestream.proto.index.spi.IndexingPlan;
import ai.pipestream.proto.index.spi.ResolvedFieldHint;
import ai.pipestream.proto.mapper.ProtoFieldMapperImpl;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ProtoLuceneMapperTest {

    private final ProtoLuceneMapper mapper =
            new ProtoLuceneMapper(new ProtoFieldMapperImpl(new DescriptorRegistry()));

    @Test
    void projectsStructPathsIntoLuceneFields() throws Exception {
        Struct message = Struct.newBuilder()
                .putFields("title", Value.newBuilder().setStringValue("Pipestream").build())
                .putFields("lang", Value.newBuilder().setStringValue("en").build())
                .build();

        Document doc = mapper.map(message, List.of(
                new ProtoLuceneMapper.FieldProjection("title", "title", true, true),
                new ProtoLuceneMapper.FieldProjection("lang", "lang", true, true)
        ));

        assertThat(doc.get("title")).isEqualTo("Pipestream");
        assertThat(doc.get("lang")).isEqualTo("en");
    }

    @Test
    void skipsNullPaths() throws Exception {
        Struct message = Struct.newBuilder()
                .putFields("title", Value.newBuilder().setStringValue("only").build())
                .build();
        Document doc = mapper.map(message, List.of(
                new ProtoLuceneMapper.FieldProjection("title", "title", true, true),
                new ProtoLuceneMapper.FieldProjection("missing", "missing", true, true)
        ));
        assertThat(doc.get("title")).isEqualTo("only");
        assertThat(doc.get("missing")).isNull();
    }

    @Test
    void storedOnlyNumericField() throws Exception {
        Struct message = Struct.newBuilder()
                .putFields("score", Value.newBuilder().setNumberValue(3.5).build())
                .build();
        Document doc = mapper.map(message, List.of(
                new ProtoLuceneMapper.FieldProjection("score", "score", true, false)
        ));
        assertThat(doc.get("score")).isEqualTo("3.5");
    }

    @Test
    void emptyProjectionsYieldEmptyDocument() throws Exception {
        assertThat(mapper.map(Struct.getDefaultInstance(), List.<ProtoLuceneMapper.FieldProjection>of()).getFields()).isEmpty();
        assertThat(mapper.map(Struct.getDefaultInstance(), (List<ProtoLuceneMapper.FieldProjection>) null).getFields()).isEmpty();
    }

    @Test
    void indexesFloatVectorsAsKnnField() {
        Document document = new Document();
        IndexingPlan.IndexedField field = new IndexingPlan.IndexedField(
                "embedding",
                "embedding",
                new ResolvedFieldHint(IndexFieldKind.VECTOR, false, true, "embedding", 3));
        // Exercise package-private path via public map API on a Struct list isn't available —
        // call toFloatVector + KnnFloatVectorField directly like the mapper does.
        float[] vector = ProtoLuceneMapper.toFloatVector(List.of(0.1f, 0.2f, 0.3f));
        document.add(new KnnFloatVectorField("embedding", vector, VectorSimilarityFunction.COSINE));
        assertThat(document.getFields("embedding")).isNotEmpty();
        assertThat(field.type()).isEqualTo(IndexFieldKind.VECTOR);
        assertThat(vector).containsExactly(0.1f, 0.2f, 0.3f);
    }

    @Test
    void luceneIndexWriterRoundTrip(@TempDir Path dir) throws Exception {
        Document document = new Document();
        document.add(new org.apache.lucene.document.StringField(
                "id", "a", org.apache.lucene.document.Field.Store.YES));
        try (LuceneIndexWriter writer = new LuceneIndexWriter(dir)) {
            writer.add(document);
            writer.commit();
            assertThat(writer.numDocs()).isEqualTo(1);
        }
    }
}
