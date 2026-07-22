package ai.pipestream.proto.index.lucene;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.index.spi.IndexFieldKind;
import ai.pipestream.proto.index.spi.IndexingPlan;
import ai.pipestream.proto.index.spi.ResolvedFieldHint;
import ai.pipestream.proto.mapper.ProtoFieldMapperImpl;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Lucene engine end to end: documents mapped from dynamic messages by an
 * {@link IndexingPlan} land in a real index and behave as the hints promise — analyzed
 * full-text search, exact keyword terms, numeric sort, point ranges, and kNN vector
 * search. The per-field analyzers are wired from {@link LuceneFieldSpecs}, the way a host
 * application is expected to.
 */
class LuceneEngineEndToEndTest {

    private static Descriptor descriptor;
    private static IndexingPlan plan;
    private static Directory directory;
    private static IndexSearcher searcher;
    private static DirectoryReader reader;

    @BeforeAll
    static void indexCorpus() throws Exception {
        descriptor = bookDescriptor();
        plan = new IndexingPlan(descriptor.getFullName(), List.of(
                new IndexingPlan.IndexedField("title", "title",
                        ResolvedFieldHint.builder(IndexFieldKind.TEXT)
                                .analyzer("english")
                                .build()),
                new IndexingPlan.IndexedField("genre", "genre",
                        ResolvedFieldHint.builder(IndexFieldKind.KEYWORD)
                                .sortable(true)
                                .facetable(true)
                                .build()),
                new IndexingPlan.IndexedField("rank", "rank",
                        ResolvedFieldHint.builder(IndexFieldKind.INT64)
                                .sortable(true)
                                .build()),
                new IndexingPlan.IndexedField("embedding", "embedding",
                        ResolvedFieldHint.builder(IndexFieldKind.VECTOR)
                                .vectorDims(4)
                                .build())));

        // Analyzer names travel on the hints; the host maps them onto implementations.
        Map<String, Analyzer> perField = new HashMap<>();
        for (LuceneFieldSpecs.FieldSpec spec : LuceneFieldSpecs.from(plan).fields()) {
            if ("english".equals(spec.analyzer())) {
                perField.put(spec.name(), new EnglishAnalyzer());
            } else if (spec.kind() == IndexFieldKind.KEYWORD) {
                perField.put(spec.name(), new KeywordAnalyzer());
            }
        }
        Analyzer analyzer = new PerFieldAnalyzerWrapper(new StandardAnalyzer(), perField);

        ProtoLuceneMapper mapper = new ProtoLuceneMapper(
                new ProtoFieldMapperImpl(new DescriptorRegistry()));
        directory = new ByteBuffersDirectory();
        try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(analyzer))) {
            writer.addDocument(mapper.map(
                    book("Running with Scissors", "memoir", 3, 1f, 0f, 0f, 0f), plan));
            writer.addDocument(mapper.map(
                    book("The Silent Library", "mystery", 1, 0f, 1f, 0f, 0f), plan));
            writer.addDocument(mapper.map(
                    book("Runs in the Family", "mystery", 2, 0.9f, 0.1f, 0f, 0f), plan));
        }
        reader = DirectoryReader.open(directory);
        searcher = new IndexSearcher(reader);
    }

    @AfterAll
    static void close() throws Exception {
        reader.close();
        directory.close();
    }

    private static Document doc(TopDocs hits, int i) throws Exception {
        return searcher.storedFields().document(hits.scoreDocs[i].doc);
    }

    @Test
    void analyzedTextSearchMatchesAcrossInflections() throws Exception {
        // The english analyzer stems both "Running" and "Runs" to "run" at index time.
        TopDocs hits = searcher.search(new TermQuery(new Term("title", "run")), 10);
        assertThat(hits.totalHits.value()).isEqualTo(2);
    }

    @Test
    void keywordFieldMatchesExactTermsOnly() throws Exception {
        TopDocs hits = searcher.search(new TermQuery(new Term("genre", "mystery")), 10);
        assertThat(hits.totalHits.value()).isEqualTo(2);
        assertThat(searcher.search(new TermQuery(new Term("genre", "myst")), 10)
                .totalHits.value()).isZero();
    }

    @Test
    void sortableNumericHintDrivesRealSorting() throws Exception {
        // rank is sortable-only, so it carries plain NUMERIC doc values.
        TopDocs hits = searcher.search(new TermQuery(new Term("genre", "mystery")), 10,
                new Sort(new org.apache.lucene.search.SortField(
                        "rank", org.apache.lucene.search.SortField.Type.LONG, true)));
        assertThat(doc(hits, 0).get("title")).isEqualTo("Runs in the Family");
        assertThat(doc(hits, 1).get("title")).isEqualTo("The Silent Library");
    }

    @Test
    void numericPointsAnswerRangeQueries() throws Exception {
        TopDocs hits = searcher.search(LongPoint.newRangeQuery("rank", 2, 3), 10);
        assertThat(hits.totalHits.value()).isEqualTo(2);
    }

    @Test
    void vectorHintEnablesKnnSearch() throws Exception {
        TopDocs hits = searcher.search(
                new KnnFloatVectorQuery("embedding", new float[] {1f, 0f, 0f, 0f}, 2), 2);
        assertThat(hits.scoreDocs).hasSize(2);
        assertThat(doc(hits, 0).get("title")).isEqualTo("Running with Scissors");
        assertThat(doc(hits, 1).get("title")).isEqualTo("Runs in the Family");
    }

    // ---- fixture ----

    private static DynamicMessage book(String title, String genre, long rank, float... vector)
            throws Exception {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        builder.setField(descriptor.findFieldByName("title"), title);
        builder.setField(descriptor.findFieldByName("genre"), genre);
        builder.setField(descriptor.findFieldByName("rank"), rank);
        FieldDescriptor embedding = descriptor.findFieldByName("embedding");
        for (float component : vector) {
            builder.addRepeatedField(embedding, component);
        }
        return builder.build();
    }

    private static Descriptor bookDescriptor() throws Exception {
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("library/book.proto")
                .setPackage("library")
                .setSyntax("proto3")
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("Book")
                        .addField(field("title", 1, FieldDescriptorProto.Type.TYPE_STRING, false))
                        .addField(field("genre", 2, FieldDescriptorProto.Type.TYPE_STRING, false))
                        .addField(field("rank", 3, FieldDescriptorProto.Type.TYPE_INT64, false))
                        .addField(field("embedding", 4, FieldDescriptorProto.Type.TYPE_FLOAT, true)))
                .build();
        return FileDescriptor.buildFrom(file, new FileDescriptor[0])
                .findMessageTypeByName("Book");
    }

    private static FieldDescriptorProto.Builder field(String name, int number,
                                                      FieldDescriptorProto.Type type,
                                                      boolean repeated) {
        FieldDescriptorProto.Builder builder = FieldDescriptorProto.newBuilder()
                .setName(name)
                .setNumber(number)
                .setType(type);
        builder.setLabel(repeated
                ? FieldDescriptorProto.Label.LABEL_REPEATED
                : FieldDescriptorProto.Label.LABEL_OPTIONAL);
        return builder;
    }
}
