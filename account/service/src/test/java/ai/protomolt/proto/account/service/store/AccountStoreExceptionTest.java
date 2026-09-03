package ai.protomolt.proto.account.service.store;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link AccountStoreException}: the store layer's failure type. The Kind is
 * the wire contract the gRPC layer maps on, so the factories' classifications
 * and messages are pinned here. No containers.
 */
class AccountStoreExceptionTest {

    @Test
    void wrapIsUnclassifiedAndKeepsTheCause() {
        java.sql.SQLException cause = new java.sql.SQLException("connection reset");
        AccountStoreException e = AccountStoreException.wrap("transactional work failed", cause);

        assertThat(e.kind()).isEqualTo(AccountStoreException.Kind.NONE);
        assertThat(e.getMessage()).isEqualTo("transactional work failed");
        assertThat(e.getCause()).isSameAs(cause);
    }

    @Test
    void notFoundCarriesTheAccountId() {
        AccountStoreException e = AccountStoreException.notFound("acct-ghost");

        assertThat(e.kind()).isEqualTo(AccountStoreException.Kind.NOT_FOUND);
        assertThat(e.getMessage()).contains("acct-ghost");
        assertThat(e.getCause()).isNull();
    }

    @Test
    void conflictCarriesTheAccountId() {
        AccountStoreException e = AccountStoreException.conflict("acct-taken");

        assertThat(e.kind()).isEqualTo(AccountStoreException.Kind.CONFLICT);
        assertThat(e.getMessage()).contains("acct-taken");
        assertThat(e.getCause()).isNull();
    }

    @Test
    void theKindsAreDistinct() {
        // GrpcErrors switches on the kind: the three factories must never
        // collapse onto one classification.
        assertThat(AccountStoreException.wrap("x", new RuntimeException()).kind())
                .isNotEqualTo(AccountStoreException.notFound("x").kind());
        assertThat(AccountStoreException.notFound("x").kind())
                .isNotEqualTo(AccountStoreException.conflict("x").kind());
        assertThat(AccountStoreException.conflict("x").kind())
                .isNotEqualTo(AccountStoreException.wrap("x", new RuntimeException()).kind());
    }
}
