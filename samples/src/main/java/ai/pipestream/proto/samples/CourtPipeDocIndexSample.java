package ai.pipestream.proto.samples;

import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.SearchMetadata;
import ai.pipestream.data.v1.SemanticProcessingResult;
import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.index.lucene.LuceneIndexWriter;
import ai.pipestream.proto.index.lucene.ProtoLuceneMapper;
import ai.pipestream.proto.index.ndjson.ProtoNdjsonWriter;
import ai.pipestream.proto.index.spi.CatalogIndexingHintSource;
import ai.pipestream.proto.index.spi.IndexFieldKind;
import ai.pipestream.proto.index.spi.IndexingPlan;
import ai.pipestream.proto.index.spi.IndexingPlanFactory;
import ai.pipestream.proto.index.spi.ResolvedFieldHint;
import ai.pipestream.proto.mapper.ProtoFieldMapperImpl;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.FSDirectory;

import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

/**
 * End-to-end demo: court {@link PipeDoc} fixtures → projected NDJSON → Lucene
 * (text + HNSW float vectors already present on the protobuf).
 *
 * <p>Usage:
 * {@code ./gradlew :samples:runCourtPipeDocToLucene
 *   --args='/path/to/fixtures/court/opensearch-sink /tmp/court-out 50'}
 */
public final class CourtPipeDocIndexSample {

    private CourtPipeDocIndexSample() {
    }

    public static void main(String[] args) throws Exception {
        Path fixtures = Path.of(args.length > 0
                ? args[0]
                : "/work/worktrees/proto-nd-json-worktree/opensearch-sink-input-court"
                        + "/src/main/resources/fixtures/court/opensearch-sink");
        Path outDir = Path.of(args.length > 1 ? args[1] : "build/court-index-out");
        int limit = args.length > 2 ? Integer.parseInt(args[2]) : 50;

        Files.createDirectories(outDir);
        Path ndjsonPath = outDir.resolve("pipedocs.ndjson");
        Path lucenePath = outDir.resolve("lucene");
        Files.createDirectories(lucenePath);

        CatalogIndexingHintSource catalog = pipeDocCatalog();
        IndexingPlanFactory plans = IndexingPlanFactory.defaults(catalog);
        ProtoFieldMapperImpl fieldMapper = new ProtoFieldMapperImpl(DescriptorRegistry.create());
        ProtoLuceneMapper luceneMapper = new ProtoLuceneMapper(fieldMapper);
        ProtoNdjsonWriter ndjson = new ProtoNdjsonWriter();

        List<Path> files;
        try (Stream<Path> stream = Files.list(fixtures)) {
            files = stream
                    .filter(p -> p.getFileName().toString().endsWith(".pb.gz"))
                    .sorted(Comparator.comparing(p -> p.getFileName().toString()))
                    .limit(limit)
                    .toList();
        }
        if (files.isEmpty()) {
            throw new IllegalStateException("No .pb.gz fixtures under " + fixtures.toAbsolutePath());
        }

        int indexed = 0;
        int withVectors = 0;
        try (BufferedWriter ndjsonOut = Files.newBufferedWriter(ndjsonPath);
                LuceneIndexWriter lucene = new LuceneIndexWriter(lucenePath)) {
            for (Path file : files) {
                PipeDoc doc = readGzipped(file);
                Struct projection = project(doc);
                ndjson.writeLine(ndjsonOut, projection);

                IndexingPlan plan = plans.create(PipeDoc.getDescriptor());
                Document luceneDoc = luceneMapper.map(doc, plan);
                // Document-level embedding lives under repeated semantic_results;
                // catalog paths cannot index into repeated fields yet — attach explicitly.
                float[] vector = firstDocumentEmbedding(doc);
                if (vector.length > 0) {
                    luceneDoc.add(new KnnFloatVectorField(
                            "embedding", vector, VectorSimilarityFunction.COSINE));
                    luceneDoc.add(new StoredField("embedding_dims", vector.length));
                    withVectors++;
                }
                // Ensure id/title/body are present even when nested getValue misses optionals.
                ensureTextFields(luceneDoc, doc);
                lucene.add(luceneDoc);
                indexed++;
            }
            lucene.commit();
            System.out.printf(
                    Locale.ROOT,
                    "Indexed %d PipeDocs (%d with doc-level vectors) →%n  NDJSON: %s%n  Lucene: %s (%d docs)%n",
                    indexed,
                    withVectors,
                    ndjsonPath.toAbsolutePath(),
                    lucenePath.toAbsolutePath(),
                    lucene.numDocs());
        }

        try (var reader = DirectoryReader.open(FSDirectory.open(lucenePath))) {
            System.out.printf(Locale.ROOT, "Verified Lucene reader numDocs=%d%n", reader.numDocs());
        }
    }

    static CatalogIndexingHintSource pipeDocCatalog() {
        String pipeDoc = PipeDoc.getDescriptor().getFullName();
        String search = SearchMetadata.getDescriptor().getFullName();
        return new CatalogIndexingHintSource()
                .put(pipeDoc, "doc_id", ResolvedFieldHint.of(IndexFieldKind.KEYWORD))
                .put(search, "title", withName(IndexFieldKind.TEXT, "title"))
                .put(search, "body", withName(IndexFieldKind.TEXT, "body"))
                .put(search, "language", withName(IndexFieldKind.KEYWORD, "language"))
                .put(search, "source_uri", withName(IndexFieldKind.KEYWORD, "source_uri"))
                .put(search, "document_type", withName(IndexFieldKind.KEYWORD, "document_type"))
                .put(search, "author", withName(IndexFieldKind.KEYWORD, "author"))
                .put(search, "semantic_results", ResolvedFieldHint.skipped())
                .put(search, "vector_set_directives", ResolvedFieldHint.skipped())
                .put(search, "source_field_analytics", ResolvedFieldHint.skipped())
                .put(search, "doc_outline", ResolvedFieldHint.skipped())
                .put(search, "discovered_links", ResolvedFieldHint.skipped())
                .put(search, "custom_fields", ResolvedFieldHint.skipped())
                .put(search, "keywords", ResolvedFieldHint.skipped())
                .put(search, "tags", ResolvedFieldHint.skipped())
                .put(search, "quality_index", ResolvedFieldHint.skipped())
                .put(pipeDoc, "blob_bag", ResolvedFieldHint.skipped())
                .put(pipeDoc, "structured_data", ResolvedFieldHint.skipped())
                .put(pipeDoc, "parsed_metadata", ResolvedFieldHint.skipped())
                .put(pipeDoc, "ownership", ResolvedFieldHint.skipped())
                .put(pipeDoc, "doc_id_derivation", ResolvedFieldHint.skipped());
    }

    private static ResolvedFieldHint withName(IndexFieldKind kind, String name) {
        ResolvedFieldHint base = ResolvedFieldHint.of(kind);
        return new ResolvedFieldHint(base.type(), base.stored(), base.indexed(), name, base.vectorDims());
    }

    static Struct project(PipeDoc doc) {
        SearchMetadata sm = doc.getSearchMetadata();
        Struct.Builder b = Struct.newBuilder()
                .putFields("doc_id", str(doc.getDocId()))
                .putFields("title", str(sm.getTitle()))
                .putFields("body", str(sm.getBody()))
                .putFields("language", str(sm.getLanguage()))
                .putFields("source_uri", str(sm.getSourceUri()))
                .putFields("document_type", str(sm.getDocumentType()));
        float[] vector = firstDocumentEmbedding(doc);
        if (vector.length > 0) {
            ListValue.Builder list = ListValue.newBuilder();
            for (float f : vector) {
                list.addValues(Value.newBuilder().setNumberValue(f).build());
            }
            b.putFields("embedding", Value.newBuilder().setListValue(list).build());
            b.putFields("embedding_dims", Value.newBuilder().setNumberValue(vector.length).build());
        }
        return b.build();
    }

    static float[] firstDocumentEmbedding(PipeDoc doc) {
        for (SemanticProcessingResult result : doc.getSearchMetadata().getSemanticResultsList()) {
            if (result.getChunksCount() == 0) {
                continue;
            }
            // Prefer document-level centroids / embeddings when present.
            String gran = result.getGranularity().name();
            if (gran.contains("DOCUMENT") || result.getChunksCount() == 1) {
                List<Float> floats = result.getChunks(0).getEmbeddingInfo().getVectorList();
                if (!floats.isEmpty()) {
                    float[] out = new float[floats.size()];
                    for (int i = 0; i < floats.size(); i++) {
                        out[i] = floats.get(i);
                    }
                    return out;
                }
            }
        }
        return new float[0];
    }

    private static void ensureTextFields(Document luceneDoc, PipeDoc doc) {
        if (luceneDoc.get("doc_id") == null && !doc.getDocId().isEmpty()) {
            luceneDoc.add(new StringField("doc_id", doc.getDocId(), org.apache.lucene.document.Field.Store.YES));
        }
        SearchMetadata sm = doc.getSearchMetadata();
        if (luceneDoc.get("title") == null && sm.hasTitle()) {
            luceneDoc.add(new TextField("title", sm.getTitle(), org.apache.lucene.document.Field.Store.YES));
        }
        if (luceneDoc.get("body") == null && sm.hasBody()) {
            luceneDoc.add(new TextField("body", sm.getBody(), org.apache.lucene.document.Field.Store.YES));
        }
    }

    private static Value str(String s) {
        return Value.newBuilder().setStringValue(s == null ? "" : s).build();
    }

    private static PipeDoc readGzipped(Path path) throws Exception {
        try (var in = new GZIPInputStream(Files.newInputStream(path))) {
            return PipeDoc.parseFrom(in.readAllBytes());
        }
    }
}
