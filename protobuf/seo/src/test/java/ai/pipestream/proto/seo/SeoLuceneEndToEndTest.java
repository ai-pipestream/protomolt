package ai.pipestream.proto.seo;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.index.lucene.LuceneFieldSpecs;
import ai.pipestream.proto.index.lucene.ProtoLuceneMapper;
import ai.pipestream.proto.index.spi.IndexFieldKind;
import ai.pipestream.proto.index.spi.IndexMapping;
import ai.pipestream.proto.mapper.ProtoFieldMapperImpl;
import ai.pipestream.proto.seo.v1.Article;
import ai.pipestream.proto.seo.v1.DublinCore;
import ai.pipestream.proto.seo.v1.Offer;
import ai.pipestream.proto.seo.v1.Person;
import ai.pipestream.proto.seo.v1.Product;
import ai.pipestream.proto.seo.v1.SearchStandard;
import ai.pipestream.proto.validate.ProtoValidator;
import com.google.protobuf.Timestamp;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The search-metadata standard end to end on the Lucene engine: validated
 * {@link SearchStandard} messages mapped through {@link ProtoLuceneMapper} with the mapping
 * {@link SeoIndexing#mappingFor} derives from the annotations land in a real index and
 * behave as the hints promise — analyzed full-text titles, exact keyword SKU terms, and
 * numeric date points answering range queries. Per-field analyzers are wired from
 * {@link LuceneFieldSpecs}, the way a host application is expected to.
 */
class SeoLuceneEndToEndTest {

    private static Directory directory;
    private static DirectoryReader reader;
    private static IndexSearcher searcher;

    @BeforeAll
    static void indexCorpus() throws Exception {
        IndexMapping mapping = SeoIndexing.mappingFor(SearchStandard.getDescriptor());

        // Analyzer names travel on the hints; the host maps them onto implementations.
        Map<String, Analyzer> perField = new HashMap<>();
        for (LuceneFieldSpecs.FieldSpec spec : LuceneFieldSpecs.from(mapping).fields()) {
            if (spec.kind() == IndexFieldKind.KEYWORD) {
                perField.put(spec.name(), new KeywordAnalyzer());
            }
        }
        Analyzer analyzer = new PerFieldAnalyzerWrapper(new StandardAnalyzer(), perField);

        ProtoLuceneMapper mapper = new ProtoLuceneMapper(
                new ProtoFieldMapperImpl(new DescriptorRegistry()));
        ProtoValidator validator = ProtoValidator.forMessageType(SearchStandard.getDescriptor());
        directory = new ByteBuffersDirectory();
        try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(analyzer))) {
            for (SearchStandard doc
                    : new SearchStandard[] {articleDoc(), productDoc(), olderArticleDoc()}) {
                // The standard's own validation gates the write path, as intended.
                validator.validate(doc).throwIfInvalid();
                writer.addDocument(mapper.map(doc, mapping));
            }
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
    void analyzedTitleTokenFindsTheDocument() throws Exception {
        // "Deep Learning Handbook" was analyzed at index time; the lowercase
        // single-token term matches, proving the TEXT hint drove analysis.
        TopDocs hits = searcher.search(new TermQuery(new Term("dublin_core_title", "handbook")), 10);
        assertThat(hits.totalHits.value()).isEqualTo(1);
        assertThat(doc(hits, 0).get("dublin_core_title")).isEqualTo("Deep Learning Handbook");
    }

    @Test
    void skuMatchesAsExactKeywordTermOnly() throws Exception {
        TopDocs hits = searcher.search(new TermQuery(new Term("product_sku", "SKU-42-BLUE")), 10);
        assertThat(hits.totalHits.value()).isEqualTo(1);
        assertThat(doc(hits, 0).get("product_name")).isEqualTo("Blue Widget");
        // A KEYWORD field is not analyzed: the lowercased term must not match.
        assertThat(searcher.search(new TermQuery(new Term("product_sku", "sku-42-blue")), 10)
                .totalHits.value()).isZero();
    }

    @Test
    void datePublishedAnswersRangeQueries() throws Exception {
        // The DATE hint emits epoch-millis points; only the 2024 article is in range.
        TopDocs hits = searcher.search(LongPoint.newRangeQuery(
                "article_date_published",
                Instant.parse("2023-01-01T00:00:00Z").toEpochMilli(),
                Instant.parse("2025-01-01T00:00:00Z").toEpochMilli()), 10);
        assertThat(hits.totalHits.value()).isEqualTo(1);
        assertThat(doc(hits, 0).get("article_headline"))
                .isEqualTo("Transformers explained simply");
    }

    @Test
    void offerCurrencyUnderTheRepeatedScopeIsSearchable() throws Exception {
        TopDocs hits = searcher.search(
                new TermQuery(new Term("product_offers_price_currency", "USD")), 10);
        assertThat(hits.totalHits.value()).isEqualTo(1);
    }

    // ---- fixtures ----

    private static Timestamp at(String instant) {
        Instant parsed = Instant.parse(instant);
        return Timestamp.newBuilder()
                .setSeconds(parsed.getEpochSecond())
                .setNanos(parsed.getNano())
                .build();
    }

    private static SearchStandard articleDoc() {
        return SearchStandard.newBuilder()
                .setDublinCore(DublinCore.newBuilder()
                        .setTitle("Deep Learning Handbook")
                        .setLanguage("en"))
                .setCanonicalUrl("https://example.com/articles/transformers")
                .addKeywords("transformers")
                .setArticle(Article.newBuilder()
                        .setHeadline("Transformers explained simply")
                        .setAuthorPerson(Person.newBuilder().setName("Ada Lovelace"))
                        .setDatePublished(at("2024-03-15T00:00:00Z")))
                .build();
    }

    private static SearchStandard olderArticleDoc() {
        return SearchStandard.newBuilder()
                .setDublinCore(DublinCore.newBuilder().setTitle("Garden Almanac"))
                .setArticle(Article.newBuilder()
                        .setHeadline("Older piece about gardens")
                        .setDatePublished(at("2020-01-05T00:00:00Z")))
                .build();
    }

    private static SearchStandard productDoc() {
        return SearchStandard.newBuilder()
                .setDublinCore(DublinCore.newBuilder().setTitle("Widget Catalog"))
                .setProduct(Product.newBuilder()
                        .setName("Blue Widget")
                        .setSku("SKU-42-BLUE")
                        .addOffers(Offer.newBuilder()
                                .setPrice("19.99")
                                .setPriceCurrency("USD")
                                .setAvailability(Offer.Availability.AVAILABILITY_IN_STOCK)))
                .build();
    }
}
