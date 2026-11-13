package ai.protomolt.proto.metric.lucene;

import ai.protomolt.proto.metric.Aggregate;
import ai.protomolt.proto.metric.MemberRole;
import ai.protomolt.proto.metric.MetricBackend;
import ai.protomolt.proto.metric.TimeGrain;
import ai.protomolt.proto.metric.spi.CompiledMetricQuery;
import ai.protomolt.proto.metric.spi.MetricMapping;
import ai.protomolt.proto.metric.spi.MetricRowPredicates;
import ai.protomolt.proto.search.index.spi.DateResolution;
import ai.protomolt.proto.search.index.spi.IndexFieldKind;
import ai.protomolt.proto.search.index.spi.ResolvedFieldHint;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class TimestampPredicatePrecisionTest {
    @Test void secondResolutionHonorsExclusiveAndFractionalBoundsOnBothSidesOfEpoch() throws Exception {
        var member = new MetricMapping.MetricMember("ts", MemberRole.MEMBER_ROLE_DIMENSION,
                Aggregate.AGGREGATE_UNSPECIFIED, "ts", "ts", MetricMapping.FieldKind.DATE,
                List.of(), "", List.of(), TimeGrain.TIME_GRAIN_UNSPECIFIED, "", "");
        var mapping = new MetricMapping("dates", "test.Dates", Map.of("ts", member));
        var hints = Map.of("ts", ResolvedFieldHint.builder(IndexFieldKind.DATE)
                .dateResolution(DateResolution.SECONDS).facetable(true).build());
        try (var directory = new ByteBuffersDirectory(); var writer = new IndexWriter(directory, new IndexWriterConfig())) {
            for (long seconds : new long[] {-2, -1, 0, 1, 2}) {
                var doc = new Document();
                doc.add(new SortedNumericDocValuesField("ts", seconds));
                writer.addDocument(doc);
            }
            try (var reader = DirectoryReader.open(writer)) {
                var searcher = new IndexSearcher(reader);
                for (long bound : new long[] {-1500, -1000, -1, 0, 1, 999, 1000, 1500}) {
                    for (String op : new String[] {">", ">=", "<", "<=", "=="}) {
                        String expression = "ts " + op + " timestamp(\"" + Instant.ofEpochMilli(bound) + "\")";
                        var predicate = MetricRowPredicates.compile(mapping, expression);
                        var query = new CompiledMetricQuery("dates", MetricBackend.METRIC_BACKEND_LUCENE,
                                List.of(), List.of(), List.of(), List.of(), List.of(), List.of(), List.of(predicate), 10);
                        int expected = 0;
                        for (long seconds : new long[] {-2, -1, 0, 1, 2}) {
                            long millis = seconds * 1000;
                            if (switch (op) {
                                case ">" -> millis > bound;
                                case ">=" -> millis >= bound;
                                case "<" -> millis < bound;
                                case "<=" -> millis <= bound;
                                default -> millis == bound;
                            }) expected++;
                        }
                        assertThat(searcher.count(LuceneMetricQueries.filterQuery(query, hints)))
                                .as(expression).isEqualTo(expected);
                    }
                }
            }
        }
    }
}
