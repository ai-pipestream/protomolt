package ai.protomolt.proto.repo.spi;

import ai.protomolt.proto.repo.v1.Access;
import ai.protomolt.proto.repo.v1.AccessRule;
import ai.protomolt.proto.repo.v1.AccessRuleSet;
import ai.protomolt.proto.repo.v1.DocumentSecurity;
import ai.protomolt.proto.repo.v1.Principal;
import com.google.protobuf.UnknownFieldSet;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DocumentSecurityTermsTest {

    private static AccessRule rule(String type, String identity, Access access) {
        return AccessRule.newBuilder().setIdentityType(type).setIdentity(identity).setAccess(access).build();
    }

    private static Principal principal(String type, String identity) {
        return Principal.newBuilder().setIdentityType(type).setIdentity(identity).build();
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
                    identities.add(principal(parts[0], parts[1].toUpperCase(Locale.ROOT)));
                }
            }
            var caller = new RepositoryCaller("reader", false, Set.of("acct"), identities);
            assertThat(terms.reads(DocumentSecurityTerms.callerTerms(identities)))
                    .as("identities %s", identities)
                    .isEqualTo(DocumentAccessPolicy.allows(caller, "acct", SECURITY, List.of(), Access.ACCESS_READ));
        }
    }

    @Test
    void colonsAndPercentsInIdentityTypesNeverCollideAcrossBoundaries() {
        var security = DocumentSecurity.newBuilder()
                .addPermissions(rule("a", "b:c", Access.ACCESS_READ))
                .addPermissions(rule("a%3ab", "c", Access.ACCESS_DENY))
                .addRequiredRuleSets(AccessRuleSet.newBuilder()
                        .setSource("gate:1")
                        .addRules(rule("urn:custom", "claim:read", Access.ACCESS_READ)))
                .build();
        var terms = DocumentSecurityTerms.of(security, List.of());

        assertThat(terms.allow()).containsExactly("a:b:c");
        assertThat(terms.deny()).containsExactly("a%253ab:c");
        assertThat(terms.gates().getFirst().allow()).containsExactly("urn%3acustom:claim:read");

        List<Principal> candidates = List.of(
                principal("a", "b:c"),
                principal("a:b", "c"),
                principal("a%3ab", "c"),
                principal("urn:custom", "claim:read"),
                principal("urn", "custom:claim:read"));
        for (int mask = 0; mask < 1 << candidates.size(); mask++) {
            Set<Principal> identities = new LinkedHashSet<>();
            for (int i = 0; i < candidates.size(); i++) {
                if ((mask & 1 << i) != 0) {
                    identities.add(candidates.get(i));
                }
            }
            var caller = new RepositoryCaller("reader", false, Set.of("acct"), identities);
            assertThat(terms.reads(DocumentSecurityTerms.callerTerms(identities)))
                    .as("identities %s", identities)
                    .isEqualTo(DocumentAccessPolicy.allows(caller, "acct", security, List.of(), Access.ACCESS_READ));
        }
    }

    @Test
    void callerTermsRejectsNullBlankOrUnknownFieldPrincipals() {
        UnknownFieldSet unknown = UnknownFieldSet.newBuilder()
                .addField(77, UnknownFieldSet.Field.newBuilder().addVarint(1L).build())
                .build();
        assertThatThrownBy(() -> DocumentSecurityTerms.callerTerms(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> DocumentSecurityTerms.callerTerms(Collections.singleton((Principal) null)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DocumentSecurityTerms.callerTerms(Set.of(principal("", "acc-1"))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DocumentSecurityTerms.callerTerms(Set.of(principal("email", " "))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DocumentSecurityTerms.callerTerms(
                Set.of(principal("email", "a@b.c").toBuilder().setUnknownFields(unknown).build())))
                .isInstanceOf(IllegalArgumentException.class);
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

    @Test
    void writesTheExhaustiveSearchFixture(@TempDir Path out) throws Exception {
        Map<String, DocumentSecurity> documents = new LinkedHashMap<>();
        documents.put("exhaustive:gated", SECURITY);
        documents.put("exhaustive:ungated", SECURITY.toBuilder().clearRequiredRuleSets().build());
        documents.put("exhaustive:write-only", DocumentSecurity.newBuilder()
                .addPermissions(rule("atlassian-account", "acc-w", Access.ACCESS_WRITE)).build());
        documents.put("exhaustive:gate-only", DocumentSecurity.newBuilder()
                .addRequiredRuleSets(SECURITY.getRequiredRuleSets(0)).build());
        documents.put("exhaustive:no-rules", DocumentSecurity.getDefaultInstance());
        List<String> pool = List.of("atlassian-group:g-staff", "atlassian-account:acc-1", "atlassian-group:g-hr",
                "atlassian-account:acc-w", "atlassian-account:acc-banned", "atlassian-account:acc-x");

        StringBuilder items = new StringBuilder();
        Map<String, DocumentSecurityTerms> terms = new LinkedHashMap<>();
        for (var document : documents.entrySet()) {
            DocumentSecurityTerms t = DocumentSecurityTerms.of(document.getValue(), List.of());
            terms.put(document.getKey(), t);
            items.append("{\"_id\":").append(json(document.getKey()))
                    .append(",\"acl_allow\":").append(json(t.allow()))
                    .append(",\"acl_deny\":").append(json(t.deny()))
                    .append(",\"acl_gates\":[");
            for (int i = 0; i < t.gates().size(); i++) {
                items.append(i == 0 ? "" : ",").append("{\"source\":").append(json(t.gates().get(i).source()))
                        .append(",\"allow\":").append(json(t.gates().get(i).allow())).append('}');
            }
            items.append("],\"acl_complete\":true}\n");
        }
        StringBuilder callers = new StringBuilder("[");
        Set<Boolean> seen = new LinkedHashSet<>();
        for (int mask = 0; mask < 1 << pool.size(); mask++) {
            Set<Principal> identities = new LinkedHashSet<>();
            List<String> names = new ArrayList<>();
            for (int i = 0; i < pool.size(); i++) {
                if ((mask & 1 << i) != 0) {
                    String[] parts = pool.get(i).split(":", 2);
                    identities.add(principal(parts[0], parts[1]));
                    names.add(pool.get(i));
                }
            }
            var caller = new RepositoryCaller("reader", false, Set.of("acct"), identities);
            callers.append(mask == 0 ? "\n" : ",\n").append("{\"caller\":").append(json("mask-" + mask))
                    .append(",\"identities\":").append(json(names))
                    .append(",\"terms\":").append(json(List.copyOf(DocumentSecurityTerms.callerTerms(identities))))
                    .append(",\"reads\":{");
            int n = 0;
            for (var document : documents.entrySet()) {
                boolean allowed = DocumentAccessPolicy.allows(caller, "acct", document.getValue(), List.of(),
                        Access.ACCESS_READ);
                assertThat(terms.get(document.getKey()).reads(DocumentSecurityTerms.callerTerms(identities)))
                        .as("mask %d on %s", mask, document.getKey()).isEqualTo(allowed);
                seen.add(allowed);
                callers.append(n++ == 0 ? "" : ",").append(json(document.getKey())).append(':').append(allowed);
            }
            callers.append("}}");
        }
        callers.append("\n]\n");
        assertThat(seen).containsExactlyInAnyOrder(true, false);
        Path itemsFile = out.resolve("exhaustive-items.ndjson");
        Path callersFile = out.resolve("exhaustive-callers.json");
        Files.writeString(itemsFile, items);
        Files.writeString(callersFile, callers);
        assertThat(Files.readAllLines(itemsFile)).hasSize(documents.size());
        assertThat(Files.readString(callersFile)).contains("\"mask-0\"");
    }

    private static String json(String value) {
        StringBuilder out = new StringBuilder("\"");
        for (char c : value.toCharArray()) {
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                default -> {
                    if (c < 0x20) out.append(String.format("\\u%04x", (int) c));
                    else out.append(c);
                }
            }
        }
        return out.append('"').toString();
    }

    private static String json(List<String> values) {
        StringBuilder out = new StringBuilder("[");
        for (int i = 0; i < values.size(); i++) out.append(i == 0 ? "" : ",").append(json(values.get(i)));
        return out.append(']').toString();
    }
}
