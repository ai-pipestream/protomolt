package ai.protomolt.proto.repo.spi;

import ai.protomolt.proto.repo.v1.IdentityLink;
import ai.protomolt.proto.repo.v1.Principal;
import com.google.protobuf.UnknownFieldSet;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CallerIdentityExpansionTest {

    private static Principal p(String type, String identity) {
        return Principal.newBuilder().setIdentityType(type).setIdentity(identity).build();
    }

    private static IdentityLink link(Principal from, Principal to) {
        return IdentityLink.newBuilder().setFrom(from).setTo(to).build();
    }

    @Test
    void linkEndpointsKeepTypeAndIdentitySeparate() {
        Principal source = p("user\u0000realm", "alice");
        Principal other = p("user", "realm\u0000alice");
        Principal group = p("group", "staff");
        var expansion = new CallerIdentityExpansion(List.of(link(source, group)));

        assertThat(expansion.expand(Set.of(source))).contains(group);
        assertThat(expansion.expand(Set.of(other))).containsExactly(other);
    }

    @Test
    void expandsLinksUsingThePolicyUnicodeComparison() {
        Principal group = p("group", "staff");
        var expansion = new CallerIdentityExpansion(List.of(link(p("Key", "İbrahim"), group)));
        assertThat(expansion.expand(Set.of(p("key", "ibrahim")))).contains(group);
    }

    @Test
    void followsLinksTransitivelyAndCaseInsensitively() {
        CallerIdentityExpansion expansion = new CallerIdentityExpansion(List.of(
                link(p("user-principal-name", "Ada@Example.com"), p("atlassian-account", "acc-1")),
                link(p("atlassian-account", "acc-1"), p("atlassian-group", "g-1")),
                link(p("atlassian-group", "g-1"), p("atlassian-group", "g-parent")),
                link(p("atlassian-group", "g-parent"), p("atlassian-group", "g-1")),
                link(p("atlassian-account", "acc-2"), p("atlassian-group", "g-2"))));

        assertThat(expansion.expand(Set.of(p("USER-PRINCIPAL-NAME", "ada@example.com"))))
                .extracting(x -> x.getIdentityType() + ":" + x.getIdentity()).containsExactly(
                        "USER-PRINCIPAL-NAME:ada@example.com", "atlassian-account:acc-1",
                        "atlassian-group:g-1", "atlassian-group:g-parent");
        assertThat(expansion.expand(Set.of(p("user-principal-name", "bob@example.com")))).hasSize(1);
    }

    @Test
    void expandsACallerButNotProcessAuthority() {
        CallerIdentityExpansion expansion = new CallerIdentityExpansion(List.of(
                link(p("user-principal-name", "ada@example.com"), p("atlassian-account", "acc-1"))));
        RepositoryCaller ada = new RepositoryCaller("ada", false, Set.of("ACC"),
                Set.of(p("user-principal-name", "ada@example.com")), Optional.empty());

        assertThat(expansion.expand(ada).identities()).contains(p("atlassian-account", "acc-1"));
        assertThat(expansion.expand(ada).accountIds()).containsExactly("ACC");
        RepositoryCaller process = new RepositoryCaller("system", true);
        assertThat(expansion.expand(process)).isSameAs(process);
    }

    @Test
    void rejectsMalformedLinksAndCallerPrincipals() {
        UnknownFieldSet unknown = UnknownFieldSet.newBuilder()
                .addField(99, UnknownFieldSet.Field.newBuilder().addVarint(1L).build())
                .build();
        assertThatThrownBy(() -> new CallerIdentityExpansion(List.of(
                link(p("atlassian-account", ""), p("atlassian-group", "g-1")))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CallerIdentityExpansion(List.of(
                link(p("public", "someone"), p("atlassian-group", "g-1")))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CallerIdentityExpansion(List.of(
                IdentityLink.newBuilder()
                        .setFrom(p("atlassian-account", "acc-1"))
                        .setTo(p("atlassian-group", "g-1"))
                        .setUnknownFields(unknown)
                        .build())))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CallerIdentityExpansion(List.of(
                link(p("atlassian-account", "acc-1").toBuilder().setUnknownFields(unknown).build(),
                        p("atlassian-group", "g-1")))))
                .isInstanceOf(IllegalArgumentException.class);

        CallerIdentityExpansion empty = new CallerIdentityExpansion(List.of());
        assertThatThrownBy(() -> empty.expand(Collections.singleton((Principal) null)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> empty.expand(Set.of(p("", "acc-1"))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void refusesAnExpansionPastTheIdentityLimit() {
        var links = List.of(
                link(p("user-principal-name", "ada@example.com"), p("atlassian-account", "acc-1")),
                link(p("atlassian-account", "acc-1"), p("atlassian-group", "g-1")),
                link(p("atlassian-group", "g-1"), p("atlassian-group", "g-2")));
        Set<Principal> ada = Set.of(p("user-principal-name", "ada@example.com"));

        assertThat(new CallerIdentityExpansion(links, 4).expand(ada)).hasSize(4);
        assertThatThrownBy(() -> new CallerIdentityExpansion(links, 3).expand(ada))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("exceeds 3 identities");
        assertThatThrownBy(() -> new CallerIdentityExpansion(links, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
