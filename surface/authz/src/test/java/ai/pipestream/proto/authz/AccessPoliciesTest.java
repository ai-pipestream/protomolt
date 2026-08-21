package ai.pipestream.proto.authz;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.pipestream.proto.actions.Scopes;
import com.google.protobuf.util.JsonFormat;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AccessPoliciesTest {

    static final String READER_DIGEST = AccessPolicyCallers.sha256Hex("reader-token");
    static final String WRITER_DIGEST = AccessPolicyCallers.sha256Hex("writer-token");
    static final String GRACE_DIGEST = AccessPolicyCallers.sha256Hex("writer-token-old");

    static AccessPolicy policy() {
        return AccessPolicy.newBuilder()
                .addPrincipals(Principal.newBuilder()
                        .setName("ci-reader")
                        .addCredentialSha256(READER_DIGEST)
                        .addScopes(Scopes.SCHEMA_READ))
                .addPrincipals(Principal.newBuilder()
                        .setName("publisher")
                        .addCredentialSha256(WRITER_DIGEST)
                        .addCredentialSha256(GRACE_DIGEST)
                        .addScopes(Scopes.SCHEMA_READ)
                        .addScopes(Scopes.SCHEMA_WRITE))
                .build();
    }

    @Test
    void aWellFormedPolicyPassesAndReturnsItself() {
        AccessPolicy policy = policy();
        assertThat(AccessPolicies.requireWellFormed(policy)).isSameAs(policy);
    }

    @Test
    void aDuplicatePrincipalNameRefusesNamingIt() {
        AccessPolicy duplicate = policy().toBuilder()
                .addPrincipals(Principal.newBuilder()
                        .setName("ci-reader")
                        .addCredentialSha256(AccessPolicyCallers.sha256Hex("another"))
                        .addScopes(Scopes.SCHEMA_READ))
                .build();
        assertThatThrownBy(() -> AccessPolicies.requireWellFormed(duplicate))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duplicates principal 'ci-reader'");
    }

    @Test
    void aDigestReusedAcrossPrincipalsRefusesNamingThePrincipalNotTheDigest() {
        AccessPolicy duplicate = policy().toBuilder()
                .addPrincipals(Principal.newBuilder()
                        .setName("impostor")
                        .addCredentialSha256(READER_DIGEST)
                        .addScopes(Scopes.SCHEMA_READ))
                .build();
        assertThatThrownBy(() -> AccessPolicies.requireWellFormed(duplicate))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("impostor")
                .hasMessageNotContaining(READER_DIGEST);
    }

    @Test
    void anUnknownScopeRefusesNamingIt() {
        AccessPolicy typo = AccessPolicy.newBuilder()
                .addPrincipals(Principal.newBuilder()
                        .setName("x")
                        .addCredentialSha256(READER_DIGEST)
                        .addScopes("schema-red"))
                .build();
        assertThatThrownBy(() -> AccessPolicies.requireWellFormed(typo))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown scope 'schema-red'");
    }

    @Test
    void theDocumentsOwnDeclaredRulesAreEnforced() {
        assertThatThrownBy(() -> AccessPolicies.requireWellFormed(
                AccessPolicy.getDefaultInstance()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("invalid");
        assertThatThrownBy(() -> AccessPolicies.requireWellFormed(AccessPolicy.newBuilder()
                .addPrincipals(Principal.newBuilder()
                        .setName("x")
                        .addCredentialSha256("not-a-digest")
                        .addScopes(Scopes.SCHEMA_READ))
                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("invalid");
        assertThatThrownBy(() -> AccessPolicies.requireWellFormed(AccessPolicy.newBuilder()
                .addPrincipals(Principal.newBuilder()
                        .setName("no scopes")
                        .addCredentialSha256(READER_DIGEST))
                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("invalid");
    }

    @Test
    void aJsonPolicyFileLoadsAndVerifies(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("policy.json");
        Files.writeString(file, JsonFormat.printer().print(policy()));
        AccessPolicy loaded = AccessPolicies.load(file);
        assertThat(loaded).isEqualTo(policy());
    }

    @Test
    void aBinaryPolicyFileLoadsAndVerifies(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("policy.binpb");
        Files.write(file, policy().toByteArray());
        assertThat(AccessPolicies.load(file)).isEqualTo(policy());
    }

    @Test
    void anUnknownJsonFieldRefusesInsteadOfDroppingIt(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("policy.json");
        Files.writeString(file,
                "{\"principals\": [], \"grantsEverything\": true}");
        assertThatThrownBy(() -> AccessPolicies.load(file))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not parse");
    }

    @Test
    void anUnrecognizedExtensionRefusesNamingTheAcceptedForms(@TempDir Path dir)
            throws Exception {
        Path file = dir.resolve("policy.yaml");
        Files.writeString(file, "principals: []");
        assertThatThrownBy(() -> AccessPolicies.load(file))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(".json, .binpb, or .pb");
    }

    @Test
    void aLoadedPolicyStillFailsWellFormedness(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("policy.binpb");
        Files.write(file, AccessPolicy.getDefaultInstance().toByteArray());
        assertThatThrownBy(() -> AccessPolicies.load(file))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("invalid");
    }

    @Test
    void budgetsAreValidatedByName() {
        AccessPolicy budgeted = policy().toBuilder()
                .setPrincipals(0, policy().getPrincipals(0).toBuilder()
                        .addBudgets(ScopeBudget.newBuilder()
                                .setScope(Scopes.SCHEMA_READ)
                                .setRequestsPerMinute(100)))
                .build();
        assertThat(AccessPolicies.requireWellFormed(budgeted)).isSameAs(budgeted);

        AccessPolicy unheld = policy().toBuilder()
                .setPrincipals(0, policy().getPrincipals(0).toBuilder()
                        .addBudgets(ScopeBudget.newBuilder()
                                .setScope(Scopes.SCHEMA_WRITE)
                                .setRequestsPerMinute(1)))
                .build();
        assertThatThrownBy(() -> AccessPolicies.requireWellFormed(unheld))
                .hasMessageContaining("ci-reader")
                .hasMessageContaining("does not hold");

        AccessPolicy doubled = budgeted.toBuilder()
                .setPrincipals(0, budgeted.getPrincipals(0).toBuilder()
                        .addBudgets(ScopeBudget.newBuilder()
                                .setScope(Scopes.SCHEMA_READ)
                                .setMaxPayloadBytes(10)))
                .build();
        assertThatThrownBy(() -> AccessPolicies.requireWellFormed(doubled))
                .hasMessageContaining("twice");

        AccessPolicy empty = policy().toBuilder()
                .setPrincipals(0, policy().getPrincipals(0).toBuilder()
                        .addBudgets(ScopeBudget.newBuilder()
                                .setScope(Scopes.SCHEMA_READ)))
                .build();
        assertThatThrownBy(() -> AccessPolicies.requireWellFormed(empty))
                .hasMessageContaining("caps nothing");
    }

    @Test
    void metricAccessIsValidatedByName() {
        AccessPolicy restricted = policy().toBuilder()
                .setPrincipals(0, policy().getPrincipals(0).toBuilder()
                        .setMetricAccess(MetricAccess.newBuilder()
                                .addDeny(MetricMemberDeny.newBuilder()
                                        .setMappingSubject("orders")
                                        .addMembers("revenue"))
                                .addRowFilters(MetricRowFilter.newBuilder()
                                        .setMappingSubject("orders")
                                        .setMember("segment")
                                        .addEquals("smb"))))
                .build();
        assertThat(AccessPolicies.requireWellFormed(restricted)).isSameAs(restricted);

        AccessPolicy nothing = policy().toBuilder()
                .setPrincipals(0, policy().getPrincipals(0).toBuilder()
                        .setMetricAccess(MetricAccess.getDefaultInstance()))
                .build();
        assertThatThrownBy(() -> AccessPolicies.requireWellFormed(nothing))
                .hasMessageContaining("restricts nothing");

        AccessPolicy doubledDeny = policy().toBuilder()
                .setPrincipals(0, policy().getPrincipals(0).toBuilder()
                        .setMetricAccess(MetricAccess.newBuilder()
                                .addDeny(MetricMemberDeny.newBuilder()
                                        .setMappingSubject("orders").addMembers("a"))
                                .addDeny(MetricMemberDeny.newBuilder()
                                        .setMappingSubject("orders").addMembers("b"))))
                .build();
        assertThatThrownBy(() -> AccessPolicies.requireWellFormed(doubledDeny))
                .hasMessageContaining("'orders' twice");

        AccessPolicy doubledFilter = policy().toBuilder()
                .setPrincipals(0, policy().getPrincipals(0).toBuilder()
                        .setMetricAccess(MetricAccess.newBuilder()
                                .addRowFilters(MetricRowFilter.newBuilder()
                                        .setMappingSubject("orders")
                                        .setMember("segment").addEquals("smb"))
                                .addRowFilters(MetricRowFilter.newBuilder()
                                        .setMappingSubject("orders")
                                        .setMember("segment").addEquals("mid"))))
                .build();
        assertThatThrownBy(() -> AccessPolicies.requireWellFormed(doubledFilter))
                .hasMessageContaining("filters member 'segment'")
                .hasMessageContaining("twice");

        AccessPolicy blankEquals = policy().toBuilder()
                .setPrincipals(0, policy().getPrincipals(0).toBuilder()
                        .setMetricAccess(MetricAccess.newBuilder()
                                .addRowFilters(MetricRowFilter.newBuilder()
                                        .setMappingSubject("orders")
                                        .setMember("segment"))))
                .build();
        assertThatThrownBy(() -> AccessPolicies.requireWellFormed(blankEquals))
                .hasMessageContaining("invalid");
    }
}
