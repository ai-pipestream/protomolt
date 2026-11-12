package ai.protomolt.proto.metric.lucene;

import ai.protomolt.proto.metric.spi.CompiledMetricQuery;
import ai.protomolt.proto.metric.spi.CompiledMetricQuery.DateRangeFilter;
import ai.protomolt.proto.metric.spi.CompiledMetricQuery.EqualsFilter;
import ai.protomolt.proto.metric.spi.CompiledMetricQuery.NumericRangeFilter;
import ai.protomolt.proto.metric.spi.CompiledMetricQuery.RowPredicate;
import ai.protomolt.proto.search.index.spi.IndexMapping;
import ai.protomolt.proto.search.index.spi.ResolvedFieldHint;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.NumericUtils;

/**
 * Builds the Lucene filter query for a compiled metric query, and the
 * field-name index of resolved hints the cursors and range bounds consult.
 * Pure functions over the compiled query; no Lucene reader is touched here,
 * so reader ownership stays with the executor's caller.
 */
final class LuceneMetricQueries {

    private LuceneMetricQueries() {
    }

    /**
     * The equals, date-range, numeric-range, path-prefix, and CEL row
     * predicates as one boolean query.
     */
    static Query filterQuery(
            CompiledMetricQuery query, Map<String, ResolvedFieldHint> hints) {
        if (query.filters().isEmpty()
                && query.dateRanges().isEmpty()
                && query.pathPrefixes().isEmpty()
                && query.numericRanges().isEmpty()
                && query.predicates().isEmpty()) {
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
            all.add(equalsQuery(filter), BooleanClause.Occur.MUST);
        }
        for (DateRangeFilter range : query.dateRanges()) {
            all.add(dateRangeQuery(range, hints), BooleanClause.Occur.MUST);
        }
        for (NumericRangeFilter range : query.numericRanges()) {
            all.add(numericRangeQuery(range, hints), BooleanClause.Occur.MUST);
        }
        for (RowPredicate predicate : query.predicates()) {
            all.add(predicateQuery(predicate, hints), BooleanClause.Occur.MUST);
        }
        return all.build();
    }

    private static Query equalsQuery(EqualsFilter filter) {
        BooleanQuery.Builder any = new BooleanQuery.Builder();
        for (String value : filter.values()) {
            any.add(new TermQuery(new Term(filter.fieldName(), value)),
                    BooleanClause.Occur.SHOULD);
        }
        return any.build();
    }

    private static Query dateRangeQuery(
            DateRangeFilter range, Map<String, ResolvedFieldHint> hints) {
        // The bounds arrive as inclusive UTC epoch millis; a field that
        // stores seconds needs ceil for the lower bound and floor for the upper.
        // Flooring both includes the second just before a fractional lower bound.
        ResolvedFieldHint hint = hints.get(range.fieldName());
        boolean seconds = hint != null && hint.dateResolution() != null
                && "SECONDS".equals(hint.dateResolution().name());
        long lower = range.gteEpochMillis() == null
                ? Long.MIN_VALUE
                : seconds ? Math.ceilDiv(range.gteEpochMillis(), 1000)
                        : range.gteEpochMillis();
        long upper = range.lteEpochMillis() == null
                ? Long.MAX_VALUE
                : seconds ? Math.floorDiv(range.lteEpochMillis(), 1000)
                        : range.lteEpochMillis();
        if (lower > upper) {
            return new MatchNoDocsQuery();
        }
        return SortedNumericDocValuesField.newSlowRangeQuery(
                range.fieldName(), lower, upper);
    }

    private static Query numericRangeQuery(
            NumericRangeFilter range, Map<String, ResolvedFieldHint> hints) {
        ResolvedFieldHint hint = hints.get(range.fieldName());
        String kind = hint == null || hint.type() == null ? "" : hint.type().name();
        if (kind.endsWith("DOUBLE")) {
            double lower;
            double upper;
            if (range.integral()) {
                lower = range.gteLong() == null
                        ? Double.NEGATIVE_INFINITY
                        : (double) range.gteLong();
                upper = range.lteLong() == null
                        ? Double.POSITIVE_INFINITY
                        : (double) range.lteLong();
            } else {
                lower = range.lowerDouble() == null
                        ? Double.NEGATIVE_INFINITY
                        : range.lowerInclusive()
                                ? (range.lowerDouble() == 0.0 ? -0.0 : range.lowerDouble())
                                : Math.nextUp(range.lowerDouble() == 0.0 ? +0.0 : range.lowerDouble());
                upper = range.upperDouble() == null
                        ? Double.POSITIVE_INFINITY
                        : range.upperInclusive()
                                ? (range.upperDouble() == 0.0 ? +0.0 : range.upperDouble())
                                : Math.nextDown(range.upperDouble() == 0.0 ? -0.0 : range.upperDouble());
            }
            if (lower > upper) {
                return new MatchNoDocsQuery();
            }
            if (hint == null || hint.facetable() || !hint.indexed()) {
                return SortedNumericDocValuesField.newSlowRangeQuery(
                        range.fieldName(),
                        NumericUtils.doubleToSortableLong(lower),
                        NumericUtils.doubleToSortableLong(upper));
            }
            return DoublePoint.newRangeQuery(
                    range.fieldName(), lower, upper);
        }
        if (kind.endsWith("FLOAT")) {
            double rawLower = range.integral()
                    ? (range.gteLong() == null ? Double.NEGATIVE_INFINITY : (double) range.gteLong())
                    : (range.lowerDouble() == null ? Double.NEGATIVE_INFINITY : range.lowerDouble());
            boolean lowerInc = range.integral() || range.lowerInclusive();
            double rawUpper = range.integral()
                    ? (range.lteLong() == null ? Double.POSITIVE_INFINITY : (double) range.lteLong())
                    : (range.upperDouble() == null ? Double.POSITIVE_INFINITY : range.upperDouble());
            boolean upperInc = range.integral() || range.upperInclusive();
            float lower = toFloatLowerBound(rawLower, lowerInc);
            float upper = toFloatUpperBound(rawUpper, upperInc);
            if (lower > upper) {
                return new MatchNoDocsQuery();
            }
            if (hint == null || hint.facetable() || !hint.indexed()) {
                return SortedNumericDocValuesField.newSlowRangeQuery(
                        range.fieldName(),
                        NumericUtils.floatToSortableInt(lower),
                        NumericUtils.floatToSortableInt(upper));
            }
            return FloatPoint.newRangeQuery(
                    range.fieldName(), lower, upper);
        }
        long lower;
        long upper;
        if (range.integral()) {
            lower = range.gteLong() == null ? Long.MIN_VALUE : range.gteLong();
            upper = range.lteLong() == null ? Long.MAX_VALUE : range.lteLong();
        } else {
            if (range.lowerDouble() == null) {
                lower = Long.MIN_VALUE;
            } else {
                double d = range.lowerDouble();
                double ceil = Math.ceil(d);
                if (ceil > Long.MAX_VALUE) {
                    return new MatchNoDocsQuery();
                }
                if (ceil <= Long.MIN_VALUE) {
                    lower = Long.MIN_VALUE;
                } else {
                    long c = (long) ceil;
                    if (!range.lowerInclusive() && ceil == d) {
                        if (c == Long.MAX_VALUE) {
                            return new MatchNoDocsQuery();
                        }
                        lower = c + 1L;
                    } else {
                        lower = c;
                    }
                }
            }
            if (range.upperDouble() == null) {
                upper = Long.MAX_VALUE;
            } else {
                double d = range.upperDouble();
                double floor = Math.floor(d);
                if (floor < Long.MIN_VALUE) {
                    return new MatchNoDocsQuery();
                }
                if (floor >= Long.MAX_VALUE) {
                    upper = Long.MAX_VALUE;
                } else {
                    long f = (long) floor;
                    if (!range.upperInclusive() && floor == d) {
                        if (f == Long.MIN_VALUE) {
                            return new MatchNoDocsQuery();
                        }
                        upper = f - 1L;
                    } else {
                        upper = f;
                    }
                }
            }
        }
        if (lower > upper) {
            return new MatchNoDocsQuery();
        }
        return SortedNumericDocValuesField.newSlowRangeQuery(
                range.fieldName(), lower, upper);
    }

    private static float toFloatLowerBound(double bound, boolean inclusive) {
        if (bound == Double.NEGATIVE_INFINITY) {
            return Float.NEGATIVE_INFINITY;
        }
        float f = (float) bound;
        if (inclusive) {
            if (f == 0.0f) {
                return -0.0f;
            }
            return (double) f < bound ? Math.nextUp(f) : f;
        }
        if (f == 0.0f && bound == 0.0) {
            return Math.nextUp(+0.0f);
        }
        return (double) f > bound ? f : Math.nextUp(f);
    }

    private static float toFloatUpperBound(double bound, boolean inclusive) {
        if (bound == Double.POSITIVE_INFINITY) {
            return Float.POSITIVE_INFINITY;
        }
        float f = (float) bound;
        if (inclusive) {
            if (f == 0.0f) {
                return +0.0f;
            }
            return (double) f > bound ? Math.nextDown(f) : f;
        }
        if (f == 0.0f && bound == 0.0) {
            return Math.nextDown(-0.0f);
        }
        return (double) f < bound ? f : Math.nextDown(f);
    }

    private static Query predicateQuery(
            RowPredicate predicate, Map<String, ResolvedFieldHint> hints) {
        return switch (predicate) {
            case RowPredicate.And and -> {
                BooleanQuery.Builder b = new BooleanQuery.Builder();
                for (RowPredicate child : and.children()) {
                    b.add(predicateQuery(child, hints), BooleanClause.Occur.MUST);
                }
                yield b.build();
            }
            case RowPredicate.Or or -> {
                BooleanQuery.Builder b = new BooleanQuery.Builder();
                for (RowPredicate child : or.children()) {
                    b.add(predicateQuery(child, hints), BooleanClause.Occur.SHOULD);
                }
                yield b.build();
            }
            case RowPredicate.Not not -> {
                BooleanQuery.Builder b = new BooleanQuery.Builder();
                if (not.fieldNames().isEmpty()) {
                    b.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
                } else {
                    for (String fieldName : not.fieldNames()) {
                        b.add(new FieldExistsQuery(fieldName), BooleanClause.Occur.MUST);
                    }
                }
                b.add(predicateQuery(not.child(), hints), BooleanClause.Occur.MUST_NOT);
                yield b.build();
            }
            case RowPredicate.Terms terms -> equalsQuery(terms.filter());
            case RowPredicate.NumericRange num -> numericRangeQuery(num.filter(), hints);
            case RowPredicate.DateRange date -> dateRangeQuery(date.filter(), hints);
        };
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
