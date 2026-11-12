package ai.protomolt.proto.metric.iceberg;

import ai.protomolt.proto.metric.Aggregate;
import ai.protomolt.proto.metric.MemberRole;
import ai.protomolt.proto.metric.MetricBackend;
import ai.protomolt.proto.metric.TimeGrain;
import ai.protomolt.proto.metric.spi.CompiledMetricQuery;
import ai.protomolt.proto.metric.spi.MetricMapping;
import ai.protomolt.proto.metric.spi.MetricRowPredicates;
import java.time.Instant;
import java.nio.file.Path;
import java.sql.DriverManager;
import org.junit.jupiter.api.io.TempDir;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class TimestampPredicatePrecisionTest {
    @TempDir Path temp;
    @Test void secondResolutionHonorsExclusiveAndFractionalBoundsOnBothSidesOfEpoch() throws Exception {
        var member = new MetricMapping.MetricMember("ts", MemberRole.MEMBER_ROLE_DIMENSION,
                Aggregate.AGGREGATE_UNSPECIFIED, "ts", "ts", MetricMapping.FieldKind.DATE,
                List.of(), "", List.of(), TimeGrain.TIME_GRAIN_UNSPECIFIED, "", "");
        var mapping = new MetricMapping("dates", "test.Dates", Map.of("ts", member));
        try (var connection = DriverManager.getConnection("jdbc:duckdb:");
                var setup = connection.createStatement()) {
            setup.execute("CREATE TABLE timestamps AS SELECT make_timestamp(x * 1000000) AS ts FROM range(-2, 3) t(x)");
            var file = temp.resolve("timestamps.parquet").toString();
            setup.execute("COPY timestamps TO '" + file.replace("'", "''") + "' (FORMAT PARQUET)");
            for (long bound : new long[] {-1500, -1000, -1, 0, 1, 999, 1000, 1500}) {
                for (String op : new String[] {">", ">=", "<", "<=", "=="}) {
                    String expression = "ts " + op + " timestamp(\"" + Instant.ofEpochMilli(bound) + "\")";
                    var predicate = MetricRowPredicates.compile(mapping, expression);
                    var query = new CompiledMetricQuery("dates", MetricBackend.METRIC_BACKEND_ICEBERG,
                            List.of(new CompiledMetricQuery.Measure("hits", "", "", Aggregate.AGGREGATE_COUNT, List.of())),
                            List.of(), List.of(), List.of(), List.of(), List.of(), List.of(predicate), 10);
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
                    var sql = DuckDbSql.render(query, List.of(file));
                    try (var statement = connection.prepareStatement(sql.text())) {
                        for (int i = 0; i < sql.parameters().size(); i++) statement.setObject(i + 1, sql.parameters().get(i));
                        try (var rows = statement.executeQuery()) {
                            assertThat(rows.next()).isTrue();
                            assertThat(rows.getLong(1)).as(expression).isEqualTo(expected);
                        }
                    }
                }
            }
        }
    }
}
