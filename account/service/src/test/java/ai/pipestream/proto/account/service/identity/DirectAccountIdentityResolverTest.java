package ai.pipestream.proto.account.service.identity;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The default {@link IdentityResolver}: a pass-through for direct account
 * ids. No containers — the contract is pure: which (identity, type) pairs
 * resolve, and how the trimming behaves.
 */
class DirectAccountIdentityResolverTest {

    private final DirectAccountIdentityResolver resolver = new DirectAccountIdentityResolver();

    @Test
    void accountIdTypeResolvesToItself() {
        assertThat(resolver.resolveAccountId("acct-1", DirectAccountIdentityResolver.ACCOUNT_ID_TYPE))
                .contains("acct-1");
    }

    @Test
    void blankOrMissingTypeDefaultsToPassThrough() {
        assertThat(resolver.resolveAccountId("acct-2", null)).contains("acct-2");
        assertThat(resolver.resolveAccountId("acct-2", "  ")).contains("acct-2");
    }

    @Test
    void identityAndTypeAreTrimmed() {
        assertThat(resolver.resolveAccountId("  acct-3  ", " account-id ")).contains("acct-3");
    }

    @Test
    void unknownIdentityTypesResolveToEmpty() {
        // External principals are for the adapters that replace this resolver.
        assertThat(resolver.resolveAccountId("user@corp.example", "user-principal-name"))
                .isEmpty();
        assertThat(resolver.resolveAccountId("S-1-5-21-...", "windows-sid")).isEmpty();
    }

    @Test
    void blankIdentityNeverResolves() {
        assertThat(resolver.resolveAccountId(null, "account-id")).isEmpty();
        assertThat(resolver.resolveAccountId("", "account-id")).isEmpty();
        assertThat(resolver.resolveAccountId("   ", null)).isEmpty();
    }
}
