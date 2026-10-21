package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.authz.AccessPolicy;
import ai.protomolt.proto.authz.AccessPolicyCallers;
import ai.protomolt.proto.authz.AccountBinding;
import ai.protomolt.proto.authz.AccountBindingPolicies;
import ai.protomolt.proto.authz.AccountBindings;
import ai.protomolt.proto.authz.AuthenticatedCaller;
import ai.protomolt.proto.authz.Principal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RepoCallerConfigurationTest {

    @Test
    void noCallerStoreMeansOnlyTheOperatorAuthenticates() {
        var configuration = RepoCallerConfiguration.fromEnvironment(Map.of());
        assertThat(configuration.resolver()).isNull();
        assertThat(configuration.bindings().bindingOf(
                AuthenticatedCaller.unbound(Caller.scoped("alice", Set.of("search-query"))))).isEmpty();
    }

    @Test
    void thePolicyAndTheBindingsComposeIntoOneCallerSeam(@TempDir Path dir) throws Exception {
        Path policy = dir.resolve("policy.binpb");
        Files.write(policy, AccessPolicy.newBuilder().addPrincipals(Principal.newBuilder().setName("alice")
                .addCredentialSha256(AccessPolicyCallers.sha256Hex("alice-credential"))
                .addScopes("search-query")).build().toByteArray());
        Path bindings = dir.resolve("bindings.binpb");
        Files.write(bindings, AccountBindings.newBuilder().addBindings(AccountBinding.newBuilder()
                .setPrincipal("alice").addAccountIds("acct-a")).build().toByteArray());
        var configuration = RepoCallerConfiguration.fromEnvironment(Map.of(
                RepoCallerConfiguration.ENV_ACCESS_POLICY, policy.toString(),
                AccountBindingPolicies.ENV_ACCOUNT_BINDINGS, bindings.toString()));
        var alice = configuration.resolver().resolveAuthenticated("alice-credential").orElseThrow();
        assertThat(alice.caller().name()).isEqualTo("alice");
        assertThat(configuration.resolver().resolveAuthenticated("unknown-credential")).isEmpty();
        assertThat(configuration.bindings().bindingOf(alice))
                .map(binding -> List.copyOf(binding.getAccountIdsList())).contains(List.of("acct-a"));
    }

    @Test
    void bindingsWithoutACallerStoreRefuseNamingTheVariables(@TempDir Path dir) throws Exception {
        Path bindings = dir.resolve("bindings.binpb");
        Files.write(bindings, AccountBindings.newBuilder().addBindings(AccountBinding.newBuilder()
                .setPrincipal("alice").addAccountIds("acct-a")).build().toByteArray());
        assertThatThrownBy(() -> RepoCallerConfiguration.fromEnvironment(Map.of(
                AccountBindingPolicies.ENV_ACCOUNT_BINDINGS, bindings.toString())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("PROTOMOLT_ACCOUNT_BINDINGS is set but no caller store is: account bindings need"
                        + " PROTOMOLT_ACCESS_POLICY or PROTOMOLT_AUTHZ_OIDC_INTROSPECTION_URL");
    }

    @Test
    void aBlankOrUnreadablePolicyRefusesNamingTheVariable(@TempDir Path dir) {
        assertThatThrownBy(() -> RepoCallerConfiguration.fromEnvironment(Map.of(
                RepoCallerConfiguration.ENV_ACCESS_POLICY, " ")))
                .hasMessage("PROTOMOLT_ACCESS_POLICY is set but blank: name an access policy file or unset it");
        Path missing = dir.resolve("missing.binpb");
        assertThatThrownBy(() -> RepoCallerConfiguration.fromEnvironment(Map.of(
                RepoCallerConfiguration.ENV_ACCESS_POLICY, missing.toString())))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageStartingWith("failed to read the access policy named by PROTOMOLT_ACCESS_POLICY at " + missing);
    }
}
