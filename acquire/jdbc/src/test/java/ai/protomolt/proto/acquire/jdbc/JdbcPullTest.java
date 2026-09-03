package ai.protomolt.proto.acquire.jdbc;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Watermark comparison semantics and env config refusals, no database needed. */
class JdbcPullTest {

    @Test
    void numericMarksCompareNumericallyNotLexically() {
        assertThat(JdbcPull.compareMarks("9", "10"))
                .as("auto-increment ids must not compare as strings")
                .isNegative();
        assertThat(JdbcPull.compareMarks("10", "9")).isPositive();
        assertThat(JdbcPull.compareMarks("10", "10.0")).isZero();
    }

    @Test
    void timestampMarksCompareLexically() {
        assertThat(JdbcPull.compareMarks(
                "2026-01-01 10:00:00+00", "2026-01-02 10:00:00+00")).isNegative();
        assertThat(JdbcPull.compareMarks(
                "2026-01-02 10:00:00+00", "2026-01-02 10:00:00+00")).isZero();
    }

    @Test
    void missingApiKeyAndUrlAreRefusedByVariableName() {
        assertThatThrownBy(() -> JdbcPullModule.Config.fromEnvironment(
                Map.of(JdbcPullModule.ENV_URL, "jdbc:postgresql://src/db")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("PROTOMOLT_ACQUIRE_API_KEY");
        assertThatThrownBy(() -> JdbcPullModule.Config.fromEnvironment(
                Map.of(JdbcPullModule.ENV_API_KEY, "key")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("PROTOMOLT_ACQUIRE_JDBC_URL");
    }
}
