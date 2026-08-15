package ai.pipestream.proto.intake.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.pipestream.proto.intake.service.identity.InMemoryApiKeyIdentityResolver;
import org.junit.jupiter.api.Test;

class IntakeServiceMainTest {

    @Test
    void missingKeySpecIsRejectedLoudly() {
        assertThatThrownBy(() -> IntakeServiceMain.resolverFromEnvironment(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(IntakeServiceMain.ENV_KEYS);
        assertThatThrownBy(() -> IntakeServiceMain.resolverFromEnvironment("  "))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void parsesUnrestrictedAndDatasourceScopedEntries() {
        InMemoryApiKeyIdentityResolver resolver =
                IntakeServiceMain.resolverFromEnvironment(
                        "k1=acct-a; k2=acct-b@ds-1, ds-2 ;");
        assertThat(resolver.resolve("k1")).hasValueSatisfying(scope -> {
            assertThat(scope.accountId()).isEqualTo("acct-a");
            assertThat(scope.allowsDatasource("anything")).isTrue();
        });
        assertThat(resolver.resolve("k2")).hasValueSatisfying(scope -> {
            assertThat(scope.accountId()).isEqualTo("acct-b");
            assertThat(scope.allowsDatasource("ds-1")).isTrue();
            assertThat(scope.allowsDatasource("ds-2")).isTrue();
            assertThat(scope.allowsDatasource("ds-3")).isFalse();
        });
        assertThat(resolver.resolve("k3")).isEmpty();
    }

    @Test
    void oidcEnvSelectsTheIntrospectionStoreAndDemandsClientCredentials() {
        var oidc =
                IntakeServiceMain.selectResolver(
                        java.util.Map.of(
                                IntakeServiceMain.ENV_OIDC_URL, "http://idp/introspect",
                                IntakeServiceMain.ENV_OIDC_CLIENT_ID, "door",
                                IntakeServiceMain.ENV_OIDC_CLIENT_SECRET, "s3cret"));
        assertThat(oidc)
                .isInstanceOf(
                        ai.pipestream.proto.intake.service.identity.OidcIntrospectionResolver.class);

        assertThatThrownBy(
                        () ->
                                IntakeServiceMain.selectResolver(
                                        java.util.Map.of(
                                                IntakeServiceMain.ENV_OIDC_URL, "http://idp/introspect")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(IntakeServiceMain.ENV_OIDC_CLIENT_ID);

        // No OIDC url falls back to the env-seeded store.
        var seeded =
                IntakeServiceMain.selectResolver(
                        java.util.Map.of(IntakeServiceMain.ENV_KEYS, "k=acct"));
        assertThat(seeded).isInstanceOf(InMemoryApiKeyIdentityResolver.class);
    }

    @Test
    void malformedEntriesAreRejectedByName() {
        assertThatThrownBy(() -> IntakeServiceMain.resolverFromEnvironment("just-a-key"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("just-a-key");
        assertThatThrownBy(() -> IntakeServiceMain.resolverFromEnvironment("k1=acct@"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("datasource");
    }
}
