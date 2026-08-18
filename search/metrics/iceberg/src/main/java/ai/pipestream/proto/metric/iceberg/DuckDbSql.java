package ai.pipestream.proto.metric.iceberg;

import ai.pipestream.proto.metric.Aggregate;
import ai.pipestream.proto.metric.spi.CompiledMetricQuery;
import ai.pipestream.proto.metric.spi.CompiledMetricQuery.Dimension;
import ai.pipestream.proto.metric.spi.CompiledMetricQuery.DimensionKind;
import ai.pipestream.proto.metric.spi.CompiledMetricQuery.EqualsFilter;
import ai.pipestream.proto.metric.spi.CompiledMetricQuery.Measure;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

/**
 * Renders one compiled metric query as a single DuckDB statement over
 * {@code read_parquet}. Columns come from each member's {@code fieldPath}
 * (struct extraction per nesting level); date dimensions label their
 * buckets in UTC with exactly the Lucene backend's formats, so the same
 * query answers the same labels on either engine. Filter values bind as
 * parameters; identifiers and file paths are escaped, never interpolated
 * raw.
 *
 * @param text the statement
 * @param parameters the bind values, in placeholder order
 */
record DuckDbSql(String text, List<String> parameters) {

    static DuckDbSql render(CompiledMetricQuery query, List<String> files) {
        List<String> parameters = new ArrayList<>();
        StringJoiner select = new StringJoiner(", ");
        for (Dimension dimension : query.dimensions()) {
            select.add(dimensionExpression(dimension));
        }
        for (Measure measure : query.measures()) {
            select.add(measureExpression(measure, parameters));
        }

        StringJoiner where = new StringJoiner(" AND ");
        for (Dimension dimension : query.dimensions()) {
            String column = column(dimension.fieldPath());
            where.add(dimension.kind() == DimensionKind.TERM
                    ? "(" + column + " IS NOT NULL AND " + column + " <> '')"
                    : column + " IS NOT NULL");
        }
        for (EqualsFilter filter : query.filters()) {
            where.add(filterCondition(filter, parameters));
        }
        for (CompiledMetricQuery.DateRangeFilter range : query.dateRanges()) {
            // Inclusive UTC epoch-millis bounds over the timestamp column,
            // matching the Lucene backend's doc-value comparison exactly.
            String instant = "epoch_ms(timezone('UTC', " + column(range.fieldPath()) + "))";
            if (range.gteEpochMillis() != null) {
                where.add(instant + " >= CAST(? AS BIGINT)");
                parameters.add(Long.toString(range.gteEpochMillis()));
            }
            if (range.lteEpochMillis() != null) {
                where.add(instant + " <= CAST(? AS BIGINT)");
                parameters.add(Long.toString(range.lteEpochMillis()));
            }
        }

        StringBuilder sql = new StringBuilder("SELECT ").append(select)
                .append(" FROM read_parquet(").append(fileList(files)).append(")");
        if (where.length() > 0) {
            sql.append(" WHERE ").append(where);
        }
        if (!query.dimensions().isEmpty()) {
            StringJoiner positions = new StringJoiner(", ");
            for (int i = 1; i <= query.dimensions().size(); i++) {
                positions.add(Integer.toString(i));
            }
            sql.append(" GROUP BY ").append(positions)
                    .append(" ORDER BY ").append(positions)
                    .append(" LIMIT ").append(query.limit());
        }
        return new DuckDbSql(sql.toString(), List.copyOf(parameters));
    }

    private static String dimensionExpression(Dimension dimension) {
        String column = column(dimension.fieldPath());
        return switch (dimension.kind()) {
            case TERM -> column;
            case BOOLEAN -> "CAST(" + column + " AS VARCHAR)";
            case DATE -> dateLabel(column, dimension);
        };
    }

    /** The Lucene backend's bucket labels, produced in SQL over UTC. */
    private static String dateLabel(String column, Dimension dimension) {
        String utc = "timezone('UTC', " + column + ")";
        return switch (dimension.grain()) {
            case TIME_GRAIN_DAY -> "strftime(" + utc + ", '%Y-%m-%d')";
            case TIME_GRAIN_WEEK -> "printf('%d-W%02d', CAST(isoyear(" + utc
                    + ") AS INT), CAST(weekofyear(" + utc + ") AS INT))";
            case TIME_GRAIN_MONTH -> "strftime(" + utc + ", '%Y-%m')";
            case TIME_GRAIN_QUARTER -> "printf('%d-Q%d', CAST(year(" + utc
                    + ") AS INT), CAST(quarter(" + utc + ") AS INT))";
            default -> "CAST(year(" + utc + ") AS VARCHAR)";
        };
    }

    private static String measureExpression(Measure measure, List<String> parameters) {
        String reduced = switch (measure.aggregate()) {
            case AGGREGATE_COUNT -> "COUNT(*)";
            case AGGREGATE_COUNT_DISTINCT ->
                    "COUNT(DISTINCT " + column(measure.fieldPath()) + ")";
            default -> measure.aggregate().name().substring("AGGREGATE_".length())
                    + "(CAST(" + column(measure.fieldPath()) + " AS DOUBLE))";
        };
        if (measure.rowFilters().isEmpty()) {
            return reduced;
        }
        StringJoiner conditions = new StringJoiner(" AND ");
        for (EqualsFilter filter : measure.rowFilters()) {
            conditions.add(filterCondition(filter, parameters));
        }
        return reduced + " FILTER (WHERE " + conditions + ")";
    }

    private static String filterCondition(EqualsFilter filter, List<String> parameters) {
        String column = column(filter.fieldPath());
        StringJoiner placeholders = new StringJoiner(", ");
        for (String value : filter.values()) {
            parameters.add(value);
            placeholders.add(filter.kind() == DimensionKind.BOOLEAN
                    ? "CAST(? AS BOOLEAN)" : "?");
        }
        return column + " IN (" + placeholders + ")";
    }

    /** Struct extraction per path segment: {@code "a"['b']['c']}. */
    private static String column(String fieldPath) {
        String[] segments = fieldPath.split("\\.");
        StringBuilder column = new StringBuilder(
                "\"" + segments[0].replace("\"", "\"\"") + "\"");
        for (int i = 1; i < segments.length; i++) {
            column.append("['").append(segments[i].replace("'", "''")).append("']");
        }
        return column.toString();
    }

    private static String fileList(List<String> files) {
        StringJoiner list = new StringJoiner(", ", "[", "]");
        for (String file : files) {
            list.add("'" + file.replace("'", "''") + "'");
        }
        return list.toString();
    }
}
