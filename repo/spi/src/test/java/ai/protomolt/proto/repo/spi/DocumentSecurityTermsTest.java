package ai.protomolt.proto.repo.spi;

import ai.protomolt.proto.repo.v1.Access;
import ai.protomolt.proto.repo.v1.AccessRule;
import ai.protomolt.proto.repo.v1.AccessRuleSet;
import ai.protomolt.proto.repo.v1.DocumentSecurity;
import ai.protomolt.proto.repo.v1.Principal;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DocumentSecurityTermsTest {

    private static AccessRule rule(String type, String identity, Access access) {
        return AccessRule.newBuilder().setIdentityType(type).setIdentity(identity).setAccess(access).build();
    }

    private static final DocumentSecurity SECURITY = DocumentSecurity.newBuilder()
            .addPermissions(rule("atlassian-group", "G-Staff", Access.ACCESS_READ))
            .addPermissions(rule("atlassian-account", "acc-w", Access.ACCESS_WRITE))
            .addPermissions(rule("atlassian-account", "acc-banned", Access.ACCESS_DENY))
            .addRequiredRuleSets(AccessRuleSet.newBuilder().setSource("confluence-owner:200")
                    .addRules(rule("atlassian-account", "Acc-1", Access.ACCESS_READ))
                    .addRules(rule("atlassian-group", "g-hr", Access.ACCESS_READ))
                    .addRules(rule("atlassian-account", "acc-x", Access.ACCESS_DENY)))
            .build();

    @Test
    void readRulesAllowDenyRulesFromAnywhereDenyAndEachSetIsAGate() {
        var terms = DocumentSecurityTerms.of(SECURITY, List.of());

        assertThat(terms.allow()).containsExactly("atlassian-group:g-staff");
        assertThat(terms.deny()).containsExactly("atlassian-account:acc-banned", "atlassian-account:acc-x");
        assertThat(terms.gates()).singleElement().satisfies(gate -> {
            assertThat(gate.source()).isEqualTo("confluence-owner:200");
            assertThat(gate.allow()).containsExactly("atlassian-account:acc-1", "atlassian-group:g-hr");
        });
    }

    @Test
    void termsAnswerWhatThePolicyAnswersForRead() {
        var terms = DocumentSecurityTerms.of(SECURITY, List.of());
        List<String> pool = List.of("atlassian-group:g-staff", "atlassian-account:acc-1", "atlassian-group:g-hr",
                "atlassian-account:acc-w", "atlassian-account:acc-banned", "atlassian-account:acc-x");
        for (int mask = 0; mask < 1 << pool.size(); mask++) {
            Set<Principal> identities = new LinkedHashSet<>();
            for (int i = 0; i < pool.size(); i++) {
                if ((mask & 1 << i) != 0) {
                    String[] parts = pool.get(i).split(":", 2);
                    identities.add(Principal.newBuilder().setIdentityType(parts[0])
                            .setIdentity(parts[1].toUpperCase(java.util.Locale.ROOT)).build());
                }
            }
            var caller = new RepositoryCaller("reader", false, Set.of("acct"), identities);
            assertThat(terms.reads(DocumentSecurityTerms.callerTerms(identities)))
                    .as("identities %s", identities)
                    .isEqualTo(DocumentAccessPolicy.allows(caller, "acct", SECURITY, List.of(), Access.ACCESS_READ));
        }
    }

    @Test
    void publicRulesMatchEveryCallerAndInheritedRulesCountWhenInheritanceIsOn() {
        var security = DocumentSecurity.newBuilder().setInheritanceEnabled(true).build();
        var inherited = new ArrayList<>(List.of(rule("public", "public", Access.ACCESS_READ)));
        var terms = DocumentSecurityTerms.of(security, inherited);

        assertThat(terms.allow()).containsExactly(DocumentSecurityTerms.PUBLIC);
        assertThat(terms.reads(DocumentSecurityTerms.callerTerms(Set.of()))).isTrue();
        assertThatThrownBy(() -> DocumentSecurityTerms.of(security, null)).isInstanceOf(RepositoryException.class);
        assertThatThrownBy(() -> DocumentSecurityTerms.of(DocumentSecurity.newBuilder()
                .addPermissions(rule("public", "someone", Access.ACCESS_READ)).build(), List.of()))
                .isInstanceOf(RepositoryException.class);
    }
}
