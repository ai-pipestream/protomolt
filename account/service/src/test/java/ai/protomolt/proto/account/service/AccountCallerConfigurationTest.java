package ai.protomolt.proto.account.service;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.authz.AccessPolicy;
import ai.protomolt.proto.authz.AccessPolicyCallers;
import ai.protomolt.proto.authz.AccountBinding;
import ai.protomolt.proto.authz.AccountBindings;
import ai.protomolt.proto.authz.AuthenticatedCaller;
import ai.protomolt.proto.authz.Principal;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The account host's environment caller seam: the required operator token, the
 * credential stores, the account bindings, and every refusal naming its variable.
 */
class AccountCallerConfigurationTest {

    private static final String TOKEN = "config-test-operator-token";
    private static final String ALICE_TOKEN = "config-test-alice-token";

    @TempDir
    Path dir;

    private Path policyFile() throws IOException {
        Path file = dir.resolve("access-policy.binpb");
        Files.write(file, AccessPolicy.newBuilder()
                .addPrincipals(Principal.newBuilder().setName("alice")
                        .addCredentialSha256(AccessPolicyCallers.sha256Hex(ALICE_TOKEN))
                        .addScopes(Scopes.SEARCH_QUERY))
                .build().toByteArray());
        return file;
    }

    private Path bindingsFile() throws IOException {
        Path file = dir.resolve("account-bindings.binpb");
        Files.write(file, AccountBindings.newBuilder()
                .addBindings(AccountBinding.newBuilder().setPrincipal("alice")
                        .addAccountIds("acct-a").addAccountIds("acct-b"))
                .build().toByteArray());
        return file;
    }

    private static Map<String, String> env(String... pairs) {
        Map<String, String> env = new HashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            env.put(pairs[i], pairs[i + 1]);
        }
        return env;
    }

    @Test
    void aMissingOperatorTokenRefusesNamingTheVariable() {
        assertThatThrownBy(() -> AccountCallerConfiguration.fromEnvironment(env()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("PROTOMOLT_API_TOKEN is unset");
    }

    @Test
    void aBlankOperatorTokenRefusesNamingTheVariable() {
        assertThatThrownBy(() -> AccountCallerConfiguration.fromEnvironment(
                env("PROTOMOLT_API_TOKEN", "  ")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("PROTOMOLT_API_TOKEN is blank");
    }

    @Test
    void theTokenAloneAuthenticatesOnlyTheOperator() {
        AccountCallerConfiguration config = AccountCallerConfiguration.fromEnvironment(
                env("PROTOMOLT_API_TOKEN", TOKEN));
        assertThat(config.apiToken()).isEqualTo(TOKEN);
        assertThat(config.resolver()).isNull();
        assertThat(config.bindings().bindingOf(AuthenticatedCaller.unbound(
                Caller.scoped("alice", Set.of(Scopes.SEARCH_QUERY))))).isEmpty();
    }

    @Test
    void aBlankAccessPolicyVariableRefusesNamingIt() {
        assertThatThrownBy(() -> AccountCallerConfiguration.fromEnvironment(
                env("PROTOMOLT_API_TOKEN", TOKEN, "PROTOMOLT_ACCESS_POLICY", " ")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("PROTOMOLT_ACCESS_POLICY is set but blank");
    }

    @Test
    void anUnreadableAccessPolicyRefusesNamingTheVariableAndKeepsTheCause() {
        String missing = dir.resolve("absent.binpb").toString();
        assertThatThrownBy(() -> AccountCallerConfiguration.fromEnvironment(
                env("PROTOMOLT_API_TOKEN", TOKEN, "PROTOMOLT_ACCESS_POLICY", missing)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("PROTOMOLT_ACCESS_POLICY")
                .hasMessageContaining(missing)
                .hasCauseInstanceOf(IOException.class);
    }

    @Test
    void bindingsWithoutACallerStoreRefuseAtBoot() throws IOException {
        String bindings = bindingsFile().toString();
        assertThatThrownBy(() -> AccountCallerConfiguration.fromEnvironment(
                env("PROTOMOLT_API_TOKEN", TOKEN, "PROTOMOLT_ACCOUNT_BINDINGS", bindings)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("PROTOMOLT_ACCOUNT_BINDINGS is set but no caller store is")
                .hasMessageContaining("PROTOMOLT_ACCESS_POLICY")
                .hasMessageContaining("PROTOMOLT_AUTHZ_OIDC_INTROSPECTION_URL");
    }

    @Test
    void aPolicyAndBindingsResolveAScopedCallerToItsAccounts() throws IOException {
        AccountCallerConfiguration config = AccountCallerConfiguration.fromEnvironment(env(
                "PROTOMOLT_API_TOKEN", TOKEN,
                "PROTOMOLT_ACCESS_POLICY", policyFile().toString(),
                "PROTOMOLT_ACCOUNT_BINDINGS", bindingsFile().toString()));
        AuthenticatedCaller alice = config.resolver().resolveAuthenticated(ALICE_TOKEN)
                .orElseThrow();
        assertThat(alice.caller().name()).isEqualTo("alice");
        assertThat(alice.caller().unrestricted()).isFalse();
        assertThat(config.resolver().resolveAuthenticated(TOKEN)).isEmpty();
        assertThat(config.bindings().bindingOf(alice).orElseThrow().getAccountIdsList())
                .containsExactly("acct-a", "acct-b");
    }

    @Test
    void aPolicyWithoutBindingsBindsNoAccount() throws IOException {
        AccountCallerConfiguration config = AccountCallerConfiguration.fromEnvironment(env(
                "PROTOMOLT_API_TOKEN", TOKEN,
                "PROTOMOLT_ACCESS_POLICY", policyFile().toString()));
        AuthenticatedCaller alice = config.resolver().resolveAuthenticated(ALICE_TOKEN)
                .orElseThrow();
        assertThat(config.bindings().bindingOf(alice)).isEmpty();
    }

    @Test
    void theOperatorTokenNeverReachesToString() {
        AccountCallerConfiguration config = AccountCallerConfiguration.fromEnvironment(
                env("PROTOMOLT_API_TOKEN", TOKEN));
        assertThat(config.toString()).doesNotContain(TOKEN).contains("<redacted>");
    }
}
