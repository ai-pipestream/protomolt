package ai.protomolt.proto.repo.catalog;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * An unavailable catalog is retried, never recorded as an outcome, so the SQLState
 * classification must admit only connection, resource and shutdown classes and must keep the
 * original failure as the cause.
 */
class CatalogExceptionTest {

    @ParameterizedTest
    @ValueSource(strings = {"08000", "08006", "08P01", "53000", "53300", "57P01", "57P03"})
    void connectionResourceAndShutdownStatesAreUnavailable(String state) {
        assertThat(CatalogException.unavailable(new SQLException("x", state))).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"23505", "40001", "42P01", "57014", "57000", "5", "0", ""})
    void otherStatesIncludingQueryCancelAreNotUnavailable(String state) {
        assertThat(CatalogException.unavailable(new SQLException("x", state))).isFalse();
    }

    @Test
    void aMissingStateIsNotUnavailableUnlessTheTypeSaysSo() {
        assertThat(CatalogException.unavailable(new SQLException("x"))).isFalse();
        assertThat(CatalogException.unavailable(new SQLTransientConnectionException("pool timeout"))).isTrue();
        assertThat(CatalogException.unavailable(new SQLTransientConnectionException("x", "23505"))).isTrue();
    }

    @Test
    void fromNamesTheOperationAndKeepsTheCauseAndClassification() {
        var cause = new SQLException("connection refused", "08001");
        var failure = CatalogException.from("save revision", cause);
        assertThat(failure).hasMessage("save revision failed: connection refused").hasCause(cause);
        assertThat(failure.getCause()).isSameAs(cause);
        assertThat(failure.unavailable()).isTrue();

        var duplicate = CatalogException.from("insert part", new SQLException(null, "23505"));
        assertThat(duplicate).hasMessage("insert part failed: null");
        assertThat(duplicate.unavailable()).isFalse();
        assertThat(duplicate.alreadyExists()).isTrue();
        assertThat(failure.alreadyExists()).isFalse();
        assertThat(CatalogException.from("cancel", new SQLException("cancelled", "57014")).alreadyExists()).isFalse();
    }

    @Test
    void thePublicConstructorCarriesItsArgumentsVerbatim() {
        var failure = new CatalogException("m", null, true);
        assertThat(failure).hasMessage("m").hasNoCause();
        assertThat(failure.unavailable()).isTrue();
        assertThat(new CatalogException("m", null, false).unavailable()).isFalse();
    }
}
