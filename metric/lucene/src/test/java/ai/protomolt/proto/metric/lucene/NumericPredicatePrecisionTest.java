package ai.protomolt.proto.metric.lucene;

import ai.protomolt.proto.metric.Aggregate;
import ai.protomolt.proto.metric.MemberRole;
import ai.protomolt.proto.metric.MetricBackend;
import ai.protomolt.proto.metric.TimeGrain;
import ai.protomolt.proto.metric.spi.CompiledMetricQuery;
import ai.protomolt.proto.metric.spi.MetricMapping;
import ai.protomolt.proto.metric.spi.MetricRowPredicates;
import ai.protomolt.proto.search.index.spi.IndexFieldKind;
import ai.protomolt.proto.search.index.spi.ResolvedFieldHint;
import java.util.List;
import java.util.Map;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.NumericUtils;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class NumericPredicatePrecisionTest {
    private static MetricMapping.MetricMember member(String name) {
        return new MetricMapping.MetricMember(name, MemberRole.MEMBER_ROLE_DIMENSION,
                Aggregate.AGGREGATE_UNSPECIFIED, name, name, MetricMapping.FieldKind.NUMERIC,
                List.of(), "", List.of(), TimeGrain.TIME_GRAIN_UNSPECIFIED, "", "");
    }

    @Test void mixedNumericFiltersSelectOnlyTheExactInt64Row() throws Exception {
        var mapping = new MetricMapping("numbers", "test.Numbers", Map.of(
                "count", member("count"), "score", member("score")));
        var hints = Map.of("count", ResolvedFieldHint.builder(IndexFieldKind.INT64).facetable(true).build(),
                "score", ResolvedFieldHint.builder(IndexFieldKind.DOUBLE).facetable(true).build());
        try (var directory = new ByteBuffersDirectory(); var writer = new IndexWriter(directory, new IndexWriterConfig())) {
            for (long count : new long[] {9007199254740992L, 9007199254740993L, 9007199254740994L}) {
                var doc = new Document();
                doc.add(new SortedNumericDocValuesField("count", count));
                doc.add(new SortedNumericDocValuesField("score", NumericUtils.doubleToSortableLong(2.5)));
                writer.addDocument(doc);
            }
            try (var reader = DirectoryReader.open(writer)) {
                var searcher = new IndexSearcher(reader);
                for (String expression : List.of(
                        "count == 9007199254740993 && score > 1.5",
                        "this.count in [9007199254740993] && this.score == 2.5",
                        "count == 9007199254740993 && count > 1.5")) {
                    var predicate = MetricRowPredicates.compile(mapping, expression);
                    var query = new CompiledMetricQuery("numbers", MetricBackend.METRIC_BACKEND_LUCENE,
                            List.of(), List.of(), List.of(), List.of(), List.of(), List.of(), List.of(predicate), 10);
                    var hits = searcher.search(LuceneMetricQueries.filterQuery(query, hints), 10);
                    assertThat(hits.totalHits.value()).as(expression).isEqualTo(1);
                    assertThat(hits.scoreDocs[0].doc).isEqualTo(1);
                }
            }
        }
    }
}
