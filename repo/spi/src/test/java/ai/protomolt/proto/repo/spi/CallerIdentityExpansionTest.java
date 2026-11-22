package ai.protomolt.proto.repo.spi;

import ai.protomolt.proto.repo.v1.IdentityLink;
import ai.protomolt.proto.repo.v1.Principal;
import org.junit.jupiter.api.Test;

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
    void rejectsABlankLink() {
        assertThatThrownBy(() -> new CallerIdentityExpansion(List.of(
                link(p("atlassian-account", ""), p("atlassian-group", "g-1")))))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
