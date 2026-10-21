package ai.protomolt.proto.authz;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.actions.Caller;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AccountBindingPoliciesTest {

    static AccountBindings bindings() {
        return AccountBindings.newBuilder()
                .addBindings(AccountBinding.newBuilder()
                        .setPrincipal("alice@example.com")
                        .addAccountIds("acct-a")
                        .addAccountIds("acct-shared")
                        .addAclIdentities(AclIdentity.newBuilder()
                                .setIdentityType("user-principal-name")
                                .setIdentity("alice@example.com")))
                .addBindings(AccountBinding.newBuilder()
                        .setPrincipal("search-bot")
                        .addAccountIds("acct-b"))
                .build();
    }

    static AuthenticatedCaller scoped(String name) {
        return AuthenticatedCaller.unbound(Caller.scoped(name, Set.of("search-query")));
    }

    @Test
    void aWellFormedDocumentPassesAndReturnsItself() {
        AccountBindings document = bindings();
        assertThat(AccountBindingPolicies.requireWellFormed(document)).isSameAs(document);
    }

    @Test
    void theDeclaredRulesRefuseByPath() {
        AccountBindings noAccounts = AccountBindings.newBuilder()
                .addBindings(AccountBinding.newBuilder().setPrincipal("alice")).build();
        assertThatThrownBy(() -> AccountBindingPolicies.requireWellFormed(noAccounts))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("account bindings are invalid: ")
                .hasMessageContaining("account_ids");
        AccountBindings empty = AccountBindings.getDefaultInstance();
        assertThatThrownBy(() -> AccountBindingPolicies.requireWellFormed(empty))
                .hasMessageContaining("bindings");
        AccountBindings repeated = AccountBindings.newBuilder()
                .addBindings(AccountBinding.newBuilder().setPrincipal("alice")
                        .addAccountIds("acct-a").addAccountIds("acct-a")).build();
        assertThatThrownBy(() -> AccountBindingPolicies.requireWellFormed(repeated))
                .hasMessageContaining("account_ids");
        AccountBindings badType = AccountBindings.newBuilder()
                .addBindings(AccountBinding.newBuilder().setPrincipal("alice").addAccountIds("acct-a")
                        .addAclIdentities(AclIdentity.newBuilder()
                                .setIdentityType("User Principal").setIdentity("alice"))).build();
        assertThatThrownBy(() -> AccountBindingPolicies.requireWellFormed(badType))
                .hasMessageContaining("identity_type");
    }

    @Test
    void aDuplicatePrincipalRefusesByName() {
        AccountBindings duplicate = bindings().toBuilder()
                .addBindings(AccountBinding.newBuilder().setPrincipal("search-bot").addAccountIds("acct-c"))
                .build();
        assertThatThrownBy(() -> AccountBindingPolicies.requireWellFormed(duplicate))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("account bindings duplicate principal 'search-bot'");
    }

    @Test
    void aCaseVariantOfAnIdentityIsTheSameIdentity() {
        AccountBindings duplicate = AccountBindings.newBuilder()
                .addBindings(AccountBinding.newBuilder().setPrincipal("alice").addAccountIds("acct-a")
                        .addAclIdentities(AclIdentity.newBuilder()
                                .setIdentityType("user-principal-name").setIdentity("alice@example.com"))
                        .addAclIdentities(AclIdentity.newBuilder()
                                .setIdentityType("user-principal-name").setIdentity("Alice@Example.com")))
                .build();
        assertThatThrownBy(() -> AccountBindingPolicies.requireWellFormed(duplicate))
                .hasMessage("principal 'alice' binds ACL identity user-principal-name"
                        + " 'Alice@Example.com' twice");
    }

    @Test
    void thePublicIdentityTypeRefuses() {
        AccountBindings publicIdentity = AccountBindings.newBuilder()
                .addBindings(AccountBinding.newBuilder().setPrincipal("alice").addAccountIds("acct-a")
                        .addAclIdentities(AclIdentity.newBuilder()
                                .setIdentityType("public").setIdentity("public")))
                .build();
        assertThatThrownBy(() -> AccountBindingPolicies.requireWellFormed(publicIdentity))
                .hasMessage("principal 'alice' binds the 'public' identity type, which every caller"
                        + " already matches");
    }

    @Test
    void theResolverAnswersByPrincipalNameAndRefusesProcessAuthority() {
        AccountBindingResolver resolver = AccountBindingResolver.of(bindings());
        assertThat(resolver.bindingOf(scoped("alice@example.com")))
                .map(binding -> List.copyOf(binding.getAccountIdsList())).contains(List.of("acct-a", "acct-shared"));
        assertThat(resolver.bindingOf(scoped("search-bot")))
                .map(binding -> List.copyOf(binding.getAccountIdsList())).contains(List.of("acct-b"));
        assertThat(resolver.bindingOf(scoped("Alice@Example.com"))).isEmpty();
        assertThat(resolver.bindingOf(scoped("nobody"))).isEmpty();
        AuthenticatedCaller operator = AuthenticatedCaller.unbound(Caller.operator());
        assertThatThrownBy(() -> resolver.bindingOf(operator))
                .hasMessage("process authority is never account-bound");
        assertThatThrownBy(() -> AccountBindingResolver.none().bindingOf(operator))
                .hasMessage("process authority is never account-bound");
        assertThat(AccountBindingResolver.none().bindingOf(scoped("alice@example.com"))).isEmpty();
    }

    @Test
    void theResolverRefusesAnIllFormedDocument() {
        assertThatThrownBy(() -> AccountBindingResolver.of(AccountBindings.getDefaultInstance()))
                .hasMessageStartingWith("account bindings are invalid: ");
    }

    @Test
    void theResolverIgnoresTheCredentialIdentity() {
        AccountBindingResolver resolver = AccountBindingResolver.of(bindings());
        AuthenticatedCaller keyed = new AuthenticatedCaller(Caller.scoped("search-bot", Set.of("search-query")),
                Optional.of(new CredentialBinding("issuer", UUID.randomUUID(), 1)));
        assertThat(resolver.bindingOf(keyed)).map(AccountBinding::getPrincipal).contains("search-bot");
    }

    @Test
    void loadReadsBinaryProtobufOnly(@TempDir Path dir) throws Exception {
        Path binary = dir.resolve("bindings.binpb");
        Files.write(binary, bindings().toByteArray());
        assertThat(AccountBindingPolicies.load(binary)).isEqualTo(bindings());
        Path pb = dir.resolve("bindings.pb");
        Files.write(pb, bindings().toByteArray());
        assertThat(AccountBindingPolicies.load(pb)).isEqualTo(bindings());
        Path json = dir.resolve("bindings.json");
        Files.writeString(json, "{}");
        assertThatThrownBy(() -> AccountBindingPolicies.load(json))
                .hasMessage("account bindings file 'bindings.json' must end in .binpb or .pb");
        Path garbage = dir.resolve("garbage.binpb");
        Files.write(garbage, new byte[] {(byte) 0xff, (byte) 0xff, (byte) 0xff});
        assertThatThrownBy(() -> AccountBindingPolicies.load(garbage))
                .hasMessageStartingWith("account bindings file 'garbage.binpb' does not parse: ");
        Path invalid = dir.resolve("invalid.binpb");
        Files.write(invalid, AccountBindings.getDefaultInstance().toByteArray());
        assertThatThrownBy(() -> AccountBindingPolicies.load(invalid))
                .hasMessageStartingWith("account bindings are invalid: ");
    }

    @Test
    void theEnvironmentNamesTheFile(@TempDir Path dir) throws Exception {
        assertThat(AccountBindingPolicies.fromEnvironment(Map.of())).isEmpty();
        assertThatThrownBy(() -> AccountBindingPolicies.fromEnvironment(
                Map.of(AccountBindingPolicies.ENV_ACCOUNT_BINDINGS, " ")))
                .hasMessage("PROTOMOLT_ACCOUNT_BINDINGS is set but blank: name a .binpb account"
                        + " bindings file or unset it");
        Path missing = dir.resolve("missing.binpb");
        assertThatThrownBy(() -> AccountBindingPolicies.fromEnvironment(
                Map.of(AccountBindingPolicies.ENV_ACCOUNT_BINDINGS, missing.toString())))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageStartingWith("failed to read the account bindings named by"
                        + " PROTOMOLT_ACCOUNT_BINDINGS at " + missing);
        Path file = dir.resolve("bindings.binpb");
        Files.write(file, bindings().toByteArray());
        Optional<AccountBindingResolver> resolver = AccountBindingPolicies.fromEnvironment(
                Map.of(AccountBindingPolicies.ENV_ACCOUNT_BINDINGS, file.toString()));
        assertThat(resolver).isPresent();
        assertThat(resolver.get().bindingOf(scoped("search-bot"))).isPresent();
    }
}
