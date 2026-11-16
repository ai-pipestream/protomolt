package ai.protomolt.proto.repo.accession.external;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.repo.v1.Access;
import ai.protomolt.proto.repo.v1.AccessRule;
import ai.protomolt.proto.repo.v1.DocumentSecurity;
import ai.protomolt.proto.repo.v1.Principal;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ExternalBindingSecurityTest {

    private static Principal principal(String type, String identity) {
        return Principal.newBuilder().setIdentityType(type).setIdentity(identity).build();
    }

    @Test
    void accountMembersReadAndOnlyNamedWritersWrite() {
        DocumentSecurity security = ExternalBindingStore.bindingSecurity(Set.of(
                principal("service", "binding-admin"), principal("user", "ops")));
        assertThat(security.getPermissionsList()).containsExactly(
                AccessRule.newBuilder().setIdentityType("public").setIdentity("public")
                        .setAccess(Access.ACCESS_READ).build(),
                AccessRule.newBuilder().setIdentityType("service").setIdentity("binding-admin")
                        .setAccess(Access.ACCESS_WRITE).build(),
                AccessRule.newBuilder().setIdentityType("user").setIdentity("ops")
                        .setAccess(Access.ACCESS_WRITE).build());
        assertThat(security.getPermissionsList())
                .filteredOn(rule -> rule.getIdentityType().equals("public"))
                .extracting(AccessRule::getAccess)
                .containsExactly(Access.ACCESS_READ);
    }

    @Test
    void refusesNoWritersAPublicWriterAndBlankIdentities() {
        assertThatThrownBy(() -> ExternalBindingStore.bindingSecurity(Set.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least one");
        assertThatThrownBy(() -> ExternalBindingStore.bindingSecurity(
                Set.of(principal("PUBLIC", "public"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot be public");
        assertThatThrownBy(() -> ExternalBindingStore.bindingSecurity(
                Set.of(principal("service", " "))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("blank");
    }
}
