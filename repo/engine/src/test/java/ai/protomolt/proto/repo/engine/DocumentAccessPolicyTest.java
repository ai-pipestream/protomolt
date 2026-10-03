package ai.protomolt.proto.repo.engine;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.v1.*;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Set;
import static org.assertj.core.api.Assertions.*;

class DocumentAccessPolicyTest {
    private static Principal identity(String type, String value) {
        return Principal.newBuilder().setIdentityType(type).setIdentity(value).build();
    }
    private static AccessRule rule(String type, String value, Access access) {
        return AccessRule.newBuilder().setIdentityType(type).setIdentity(value).setAccess(access).build();
    }
    private static DocumentSecurity policy(AccessRule... rules) {
        return DocumentSecurity.newBuilder().addAllPermissions(List.of(rules)).build();
    }
    private static final RepositoryCaller ALICE = new RepositoryCaller("login-name", false,
            Set.of("account-a"), Set.of(identity("cmis-user", "Alice"), identity("oauth-group", "Editors")));
    private static final AccessRule READ = rule("cmis-user", "alice", Access.ACCESS_READ);
    private static boolean read(DocumentSecurity policy) {
        return DocumentAccessPolicy.allows(ALICE, "account-a", policy, null, Access.ACCESS_READ);
    }

    @Test void accountMembershipPrecedesPublicAndExplicitGrants() {
        var publicRead = policy(rule("public", "public", Access.ACCESS_READ));
        assertThat(read(publicRead)).isTrue();
        assertThat(DocumentAccessPolicy.allows(ALICE, "account-b", publicRead, null, Access.ACCESS_READ)).isFalse();
        assertThat(DocumentAccessPolicy.allows(ALICE, "ACCOUNT-A", policy(READ), null, Access.ACCESS_READ)).isFalse();
        assertThat(DocumentAccessPolicy.allows(new RepositoryCaller("alice", false), "account-a",
                policy(READ), null, Access.ACCESS_READ)).isFalse();
    }

    @Test void matchesTypedPrincipalAndGroupIdentitiesWithoutUsingLoginName() {
        assertThat(read(policy(READ))).isTrue();
        assertThat(read(policy(rule("oauth-group", "EDITORS", Access.ACCESS_READ)))).isTrue();
        assertThat(read(policy(rule("cmis-user", "Editors", Access.ACCESS_READ)))).isFalse();
        assertThat(read(policy(rule("cmis-user", "login-name", Access.ACCESS_READ)))).isFalse();
    }

    @Test void denyWinsRegardlessOfRuleOrderOrGrantIdentity() {
        var deny = rule("oauth-group", "editors", Access.ACCESS_DENY);
        assertThat(read(policy(READ, deny))).isFalse();
        assertThat(read(policy(deny, READ))).isFalse();
        assertThat(read(policy(READ, rule("public", "public", Access.ACCESS_DENY)))).isFalse();
    }

    @Test void readAndWriteRemainSeparateGrants() {
        assertThat(read(policy(rule("cmis-user", "alice", Access.ACCESS_WRITE)))).isFalse();
        assertThat(DocumentAccessPolicy.allows(ALICE, "account-a", policy(READ), null, Access.ACCESS_WRITE)).isFalse();
        assertThat(DocumentAccessPolicy.allows(ALICE, "account-a",
                policy(rule("cmis-user", "alice", Access.ACCESS_WRITE)), null, Access.ACCESS_WRITE)).isTrue();
    }

    @Test void missingEmptyAndMalformedPoliciesFailClosed() {
        assertThat(read(null)).isFalse();
        assertThat(read(policy())).isFalse();
        for (var malformed : List.of(AccessRule.getDefaultInstance(),
                READ.toBuilder().setAccessValue(99).build(),
                rule("public", "someone", Access.ACCESS_READ),
                rule("", "alice", Access.ACCESS_READ))) {
            assertThatThrownBy(() -> read(policy(READ, malformed)))
                    .isInstanceOf(RepositoryException.class);
        }
        assertThatThrownBy(() -> DocumentAccessPolicy.allows(ALICE, "", policy(READ), null, Access.ACCESS_READ))
                .isInstanceOf(RepositoryException.class);
    }

    @Test void inheritanceMustBeResolvedAndItsDeniesOverrideExplicitGrants() {
        var inherited = policy(READ).toBuilder().setInheritanceEnabled(true).build();
        assertThatThrownBy(() -> read(inherited)).isInstanceOf(RepositoryException.class);
        assertThat(DocumentAccessPolicy.allows(ALICE, "account-a", inherited, List.of(), Access.ACCESS_READ)).isTrue();
        var deny = rule("cmis-user", "alice", Access.ACCESS_DENY);
        assertThat(DocumentAccessPolicy.allows(ALICE, "account-a", inherited, List.of(deny), Access.ACCESS_READ)).isFalse();
        assertThat(DocumentAccessPolicy.allows(ALICE, "account-a", policy(READ), List.of(deny), Access.ACCESS_READ)).isTrue();
    }

    @Test void unknownPolicyFieldsCannotBeIgnoredByDirectCallers() {
        var unknown = com.google.protobuf.UnknownFieldSet.newBuilder().addField(100,
                com.google.protobuf.UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build();
        assertThatThrownBy(() -> read(policy(READ).toBuilder().setUnknownFields(unknown).build()))
                .isInstanceOf(RepositoryException.class);
        assertThatThrownBy(() -> read(policy(READ.toBuilder().setUnknownFields(unknown).build())))
                .isInstanceOf(RepositoryException.class);
        assertThatThrownBy(() -> new RepositoryCaller("alice", false, Set.of("account-a"),
                Set.of(identity("cmis-user", "alice").toBuilder().setUnknownFields(unknown).build())))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test void operatorBypassDoesNotConcealMalformedOwnershipOrPolicy() {
        var operator = new RepositoryCaller("operator", true);
        var deny = policy(rule("public", "public", Access.ACCESS_DENY));
        assertThat(DocumentAccessPolicy.allows(operator, "account-a", deny, null, Access.ACCESS_READ)).isTrue();
        assertThat(DocumentAccessPolicy.allows(operator, "account-a", null, null, Access.ACCESS_READ)).isTrue();
        assertThatThrownBy(() -> DocumentAccessPolicy.allows(operator, "", deny, null, Access.ACCESS_READ))
                .isInstanceOf(RepositoryException.class);
        assertThatThrownBy(() -> DocumentAccessPolicy.allows(operator, "account-a",
                policy(AccessRule.getDefaultInstance()), null, Access.ACCESS_READ)).isInstanceOf(RepositoryException.class);
    }

    @Test void callerBindingsAreImmutableAndNeverImplicit() {
        var accounts = new java.util.HashSet<>(Set.of("account-a"));
        var identities = new java.util.HashSet<>(ALICE.identities());
        var caller = new RepositoryCaller("alice", false, accounts, identities);
        accounts.add("account-b");
        identities.clear();
        assertThat(caller.accountIds()).containsExactly("account-a");
        assertThat(caller.identities()).hasSize(2);
        assertThatThrownBy(() -> caller.accountIds().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> new RepositoryCaller("alice", false, Set.of(" "), Set.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RepositoryCaller("alice", false, Set.of(), Set.of(Principal.getDefaultInstance())))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(new RepositoryCaller("alice", false).identities()).isEmpty();
    }
}
