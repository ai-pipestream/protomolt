package ai.protomolt.proto.metric.lucene;

import ai.protomolt.proto.metric.spi.CompiledMetricQuery;
import ai.protomolt.proto.metric.spi.CompiledMetricQuery.EqualsFilter;
import ai.protomolt.proto.search.index.spi.IndexMapping;
import ai.protomolt.proto.search.index.spi.ResolvedFieldHint;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

/**
 * Builds the Lucene filter query for a compiled metric query, and the
 * field-name index of resolved hints the cursors and date-range bounds
 * consult. Pure functions over the compiled query; no Lucene reader is
 * touched here, so reader ownership stays with the executor's caller.
 */
final class LuceneMetricQueries {

    private LuceneMetricQueries() {
    }

    /** The equals, date-range and path-prefix filters as one boolean query. */
    static Query filterQuery(
            CompiledMetricQuery query, Map<String, ResolvedFieldHint> hints) {
        if (query.filters().isEmpty() && query.dateRanges().isEmpty()
                && query.pathPrefixes().isEmpty()) {
            return new MatchAllDocsQuery();
        }
        BooleanQuery.Builder all = new BooleanQuery.Builder();
        for (CompiledMetricQuery.PathPrefixFilter prefix : query.pathPrefixes()) {
            // The mapper indexed the whole ancestor chain as terms, so
            // descendant-or-self is one exact term match, never a scan.
            all.add(new TermQuery(new Term(prefix.fieldName(), prefix.path())),
                    BooleanClause.Occur.MUST);
        }
        for (EqualsFilter filter : query.filters()) {
            BooleanQuery.Builder any = new BooleanQuery.Builder();
            for (String value : filter.values()) {
                any.add(new TermQuery(new Term(filter.fieldName(), value)),
                        BooleanClause.Occur.SHOULD);
            }
            all.add(any.build(), BooleanClause.Occur.MUST);
        }
        for (CompiledMetricQuery.DateRangeFilter range : query.dateRanges()) {
            // The bounds arrive as inclusive UTC epoch millis; a field that
            // stores seconds compares in seconds (day bounds divide evenly
            // downward, so inclusiveness survives the floor).
            ResolvedFieldHint hint = hints.get(range.fieldName());
            boolean seconds = hint != null && hint.dateResolution() != null
                    && "SECONDS".equals(hint.dateResolution().name());
            long lower = range.gteEpochMillis() == null
                    ? Long.MIN_VALUE
                    : seconds ? Math.floorDiv(range.gteEpochMillis(), 1000)
                            : range.gteEpochMillis();
            long upper = range.lteEpochMillis() == null
                    ? Long.MAX_VALUE
                    : seconds ? Math.floorDiv(range.lteEpochMillis(), 1000)
                            : range.lteEpochMillis();
            all.add(org.apache.lucene.document.SortedNumericDocValuesField
                    .newSlowRangeQuery(range.fieldName(), lower, upper),
                    BooleanClause.Occur.MUST);
        }
        return all.build();
    }

    /** The mapping's indexable fields keyed by field name. */
    static Map<String, ResolvedFieldHint> hintsByFieldName(IndexMapping mapping) {
        Map<String, ResolvedFieldHint> hints = new HashMap<>();
        for (IndexMapping.IndexedField field : mapping.indexable()) {
            hints.put(field.fieldName(), field.hint());
        }
        return hints;
    }
}
