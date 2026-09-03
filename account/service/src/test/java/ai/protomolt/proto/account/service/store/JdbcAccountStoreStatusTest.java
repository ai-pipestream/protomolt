package ai.protomolt.proto.account.service.store;

import ai.protomolt.proto.account.v1.AccountStatus;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The {@link JdbcAccountStore} status spelling: the database column carries
 * the enum name minus the {@code ACCOUNT_STATUS_} prefix. Round-trips for
 * every enum value, and the unknown-spelling failure mode (an unclassified
 * store failure — INTERNAL on the wire — never a client-blaming
 * IllegalArgumentException). Static methods: no containers.
 */
class JdbcAccountStoreStatusTest {

    @Test
    void everyEnumValueRoundTrips() {
        for (AccountStatus status : AccountStatus.values()) {
            // UNRECOGNIZED is proto3's synthetic unknown-value sentinel, not a
            // status: it carries no ACCOUNT_STATUS_ prefix and can never be
            // set through the generated builders, so the DB spelling mapping
            // does not cover it.
            if (status == AccountStatus.UNRECOGNIZED) {
                continue;
            }
            assertThat(JdbcAccountStore.statusFromDb(JdbcAccountStore.statusToDb(status)))
                    .isEqualTo(status);
        }
    }

    @Test
    void theDbSpellingDropsThePrefix() {
        assertThat(JdbcAccountStore.statusToDb(AccountStatus.ACCOUNT_STATUS_ACTIVE))
                .isEqualTo("ACTIVE");
        assertThat(JdbcAccountStore.statusToDb(AccountStatus.ACCOUNT_STATUS_SUSPENDED))
                .isEqualTo("SUSPENDED");
        assertThat(JdbcAccountStore.statusToDb(AccountStatus.ACCOUNT_STATUS_DEACTIVATED))
                .isEqualTo("DEACTIVATED");
        assertThat(JdbcAccountStore.statusToDb(AccountStatus.ACCOUNT_STATUS_UNSPECIFIED))
                .isEqualTo("UNSPECIFIED");
    }

    @Test
    void unknownSpellingIsAnUnclassifiedStoreFailure() {
        assertThatThrownBy(() -> JdbcAccountStore.statusFromDb("BOGUS"))
                .isInstanceOfSatisfying(AccountStoreException.class, e -> {
                    assertThat(e.kind()).isEqualTo(AccountStoreException.Kind.NONE);
                    assertThat(e.getMessage()).contains("BOGUS");
                    assertThat(e.getCause()).isInstanceOf(IllegalArgumentException.class);
                });
    }
}
