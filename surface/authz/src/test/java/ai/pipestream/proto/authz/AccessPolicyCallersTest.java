package ai.pipestream.proto.authz;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.pipestream.proto.actions.Caller;
import ai.pipestream.proto.actions.Scopes;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class AccessPolicyCallersTest {

    private final AccessPolicyCallers resolver =
            new AccessPolicyCallers(AccessPoliciesTest.policy());

    @Test
    void aKnownCredentialResolvesToItsPrincipal() {
        Caller caller = resolver.resolve("reader-token").orElseThrow();
        assertThat(caller.name()).isEqualTo("ci-reader");
        assertThat(caller.scopes()).containsExactly(Scopes.SCHEMA_READ);
        assertThat(caller.unrestricted()).isFalse();
    }

    @Test
    void rotationWithGraceResolvesBothCredentialsToOnePrincipal() {
        assertThat(resolver.resolve("writer-token").orElseThrow().name())
                .isEqualTo("publisher");
        assertThat(resolver.resolve("writer-token-old").orElseThrow().name())
                .isEqualTo("publisher");
    }

    @Test
    void anUnknownCredentialResolvesEmpty() {
        assertThat(resolver.resolve("guessed-token")).isEqualTo(Optional.empty());
    }

    @Test
    void theDigestIsTheLowercaseHexOfTheUtf8Credential() {
        assertThat(AccessPolicyCallers.sha256Hex(""))
                .isEqualTo("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    }

    @Test
    void aMalformedPolicyRefusesAtConstruction() {
        assertThatThrownBy(() -> new AccessPolicyCallers(AccessPolicy.getDefaultInstance()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aNullCredentialIsTheTransportsBug() {
        assertThatThrownBy(() -> resolver.resolve(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void aPrincipalsBudgetsRideItsCaller() {
        AccessPolicy policy = AccessPolicy.newBuilder()
                .addPrincipals(Principal.newBuilder()
                        .setName("meterme")
                        .addCredentialSha256(
                                AccessPolicyCallers.sha256Hex("metered-credential"))
                        .addScopes(Scopes.SEARCH_QUERY)
                        .addScopes(Scopes.METRICS_QUERY)
                        .addBudgets(ScopeBudget.newBuilder()
                                .setScope(Scopes.SEARCH_QUERY)
                                .setRequestsPerMinute(5)
                                .setMaxPayloadBytes(1024)))
                .build();
        Caller caller = new AccessPolicyCallers(policy)
                .resolve("metered-credential").orElseThrow();
        assertThat(caller.budgets())
                .containsOnlyKeys(Scopes.SEARCH_QUERY);
        assertThat(caller.budgets().get(Scopes.SEARCH_QUERY))
                .isEqualTo(new Caller.Budget(5, 1024));
    }
}
