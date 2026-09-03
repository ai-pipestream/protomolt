package ai.protomolt.proto.repo.container.ledger;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link ListDocumentsFilter}'s page-size fallback: the documented
 * "values &lt;= 0 fall back to 100" rule, exercised at the boundaries.
 */
class ListDocumentsFilterTest {

    @Test
    void allIsUnfilteredOnTheFirstHundredRows() {
        ListDocumentsFilter filter = ListDocumentsFilter.all();

        assertThat(filter.driveName()).isNull();
        assertThat(filter.connectorId()).isNull();
        assertThat(filter.crawlId()).isNull();
        assertThat(filter.accountId()).isNull();
        assertThat(filter.effectiveLimit()).isEqualTo(100);
        assertThat(filter.offset()).isZero();
    }

    @Test
    void nonPositiveLimitsFallBackToOneHundred() {
        assertThat(new ListDocumentsFilter(null, null, null, null, 0, 0).effectiveLimit())
                .isEqualTo(100);
        assertThat(new ListDocumentsFilter(null, null, null, null, -7, 0).effectiveLimit())
                .isEqualTo(100);
    }

    @Test
    void positiveLimitsAreKeptAtTheBoundaries() {
        assertThat(new ListDocumentsFilter(null, null, null, null, 1, 0).effectiveLimit())
                .isEqualTo(1);
        assertThat(new ListDocumentsFilter(null, null, null, null, 250, 0).effectiveLimit())
                .isEqualTo(250);
    }
}
