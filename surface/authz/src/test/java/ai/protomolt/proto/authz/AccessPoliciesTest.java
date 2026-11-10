package ai.protomolt.proto.authz;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.types.DateRange;
import ai.protomolt.proto.types.DoubleRange;
import ai.protomolt.proto.types.LongRange;
import com.google.protobuf.util.JsonFormat;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
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

    @Test
    void metricRangeAndCelRowFiltersValidateAtPolicyLoad() {
        AccessPolicy valid = policy().toBuilder()
                .setPrincipals(0, policy().getPrincipals(0).toBuilder()
                        .setMetricAccess(MetricAccess.newBuilder()
                                .addRowFilters(MetricRowFilter.newBuilder()
                                        .setMappingSubject("orders")
                                        .setMember("created_at")
                                        .setRange(DateRange.newBuilder()
                                                .setBegin("2026-07-01")
                                                .setEnd("2026-07-31")))
                                .addRowFilters(MetricRowFilter.newBuilder()
                                        .setMappingSubject("orders")
                                        .setMember("revenue")
                                        .setLongRange(LongRange.newBuilder()
                                                .setBegin(50L)
                                                .setEnd(500L)))
                                .addRowFilters(MetricRowFilter.newBuilder()
                                        .setMappingSubject("orders")
                                        .setMember("avg_ratio")
                                        .setDoubleRange(DoubleRange.newBuilder()
                                                .setBegin(0.1)
                                                .setEnd(0.9)))
                                .addRowFilters(MetricRowFilter.newBuilder()
                                        .setMappingSubject("orders")
                                        .setCel("this.segment == 'smb' && this.amount >= 50"))
                                .addRowFilters(MetricRowFilter.newBuilder()
                                        .setMappingSubject("orders")
                                        .setCel("this.paying == true"))))
                .build();
        assertThat(AccessPolicies.requireWellFormed(valid)).isSameAs(valid);

        // Multiple filter forms on a single MetricRowFilter -> rejected.
        AccessPolicy twoForms = policy().toBuilder()
                .setPrincipals(0, policy().getPrincipals(0).toBuilder()
                        .setMetricAccess(MetricAccess.newBuilder()
                                .addRowFilters(MetricRowFilter.newBuilder()
                                        .setMappingSubject("orders")
                                        .setMember("segment")
                                        .addEquals("smb")
                                        .setCel("this.segment == 'smb'"))))
                .build();
        assertThatThrownBy(() -> AccessPolicies.requireWellFormed(twoForms))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exactly one of equals, range, long_range, double_range, or cel");

        // Missing member on a range filter -> rejected.
        AccessPolicy rangeWithoutMember = policy().toBuilder()
                .setPrincipals(0, policy().getPrincipals(0).toBuilder()
                        .setMetricAccess(MetricAccess.newBuilder()
                                .addRowFilters(MetricRowFilter.newBuilder()
                                        .setMappingSubject("orders")
                                        .setLongRange(LongRange.newBuilder()
                                                .setBegin(10L)))))
                .build();
        assertThatThrownBy(() -> AccessPolicies.requireWellFormed(rangeWithoutMember))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires a member name");

        // Empty integer range after exclusive bounds (5, 6) -> rejected.
        AccessPolicy emptyLongRange = policy().toBuilder()
                .setPrincipals(0, policy().getPrincipals(0).toBuilder()
                        .setMetricAccess(MetricAccess.newBuilder()
                                .addRowFilters(MetricRowFilter.newBuilder()
                                        .setMappingSubject("orders")
                                        .setMember("revenue")
                                        .setLongRange(LongRange.newBuilder()
                                                .setBegin(5L)
                                                .setEnd(6L)
                                                .setIncludeHead(false)
                                                .setIncludeTail(false)))))
                .build();
        assertThatThrownBy(() -> AccessPolicies.requireWellFormed(emptyLongRange))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("empty long_range");

        // Empty date range after exclusive bounds -> rejected.
        AccessPolicy emptyDateRange = policy().toBuilder()
                .setPrincipals(0, policy().getPrincipals(0).toBuilder()
                        .setMetricAccess(MetricAccess.newBuilder()
                                .addRowFilters(MetricRowFilter.newBuilder()
                                        .setMappingSubject("orders")
                                        .setMember("created_at")
                                        .setRange(DateRange.newBuilder()
                                                .setBegin("2026-07-10")
                                                .setEnd("2026-07-11")
                                                .setIncludeHead(false)
                                                .setIncludeTail(false)))))
                .build();
        assertThatThrownBy(() -> AccessPolicies.requireWellFormed(emptyDateRange))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("empty date range");

        // Syntax error in CEL -> rejected.
        AccessPolicy badCelSyntax = policy().toBuilder()
                .setPrincipals(0, policy().getPrincipals(0).toBuilder()
                        .setMetricAccess(MetricAccess.newBuilder()
                                .addRowFilters(MetricRowFilter.newBuilder()
                                        .setMappingSubject("orders")
                                        .setCel("this.segment =="))))
                .build();
        assertThatThrownBy(() -> AccessPolicies.requireWellFormed(badCelSyntax))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("invalid cel");

        // Non-boolean / constant literal CEL -> rejected.
        AccessPolicy nonBooleanCel = policy().toBuilder()
                .setPrincipals(0, policy().getPrincipals(0).toBuilder()
                        .setMetricAccess(MetricAccess.newBuilder()
                                .addRowFilters(MetricRowFilter.newBuilder()
                                        .setMappingSubject("orders")
                                        .setCel("this.amount + 1"))))
                .build();
        assertThatThrownBy(() -> AccessPolicies.requireWellFormed(nonBooleanCel))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("boolean predicate");

        AccessPolicy constantLiteralCel = policy().toBuilder()
                .setPrincipals(0, policy().getPrincipals(0).toBuilder()
                        .setMetricAccess(MetricAccess.newBuilder()
                                .addRowFilters(MetricRowFilter.newBuilder()
                                        .setMappingSubject("orders")
                                        .setCel("true"))))
                .build();
        assertThatThrownBy(() -> AccessPolicies.requireWellFormed(constantLiteralCel))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("boolean predicate");

        // Duplicate CEL filter on same subject -> rejected.
        AccessPolicy duplicateCel = policy().toBuilder()
                .setPrincipals(0, policy().getPrincipals(0).toBuilder()
                        .setMetricAccess(MetricAccess.newBuilder()
                                .addRowFilters(MetricRowFilter.newBuilder()
                                        .setMappingSubject("orders")
                                        .setCel("this.paying == true"))
                                .addRowFilters(MetricRowFilter.newBuilder()
                                        .setMappingSubject("orders")
                                        .setCel("this.paying == true"))))
                .build();
        assertThatThrownBy(() -> AccessPolicies.requireWellFormed(duplicateCel))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duplicates CEL filter");
    }

    @Test
    void aNonFiniteDoubleRangeBoundIsRefusedAtPolicyLoad() {
        for (DoubleRange range : List.of(
                DoubleRange.newBuilder().setBegin(Double.NaN).setEnd(1.0).build(),
                DoubleRange.newBuilder().setBegin(0.0).setEnd(Double.POSITIVE_INFINITY).build(),
                DoubleRange.newBuilder().setBegin(Double.NEGATIVE_INFINITY).build(),
                DoubleRange.newBuilder().setEnd(Double.NaN).setIncludeTail(false).build())) {
            AccessPolicy nonFinite = policy().toBuilder()
                    .setPrincipals(0, policy().getPrincipals(0).toBuilder()
                            .setMetricAccess(MetricAccess.newBuilder()
                                    .addRowFilters(MetricRowFilter.newBuilder()
                                            .setMappingSubject("orders")
                                            .setMember("avg_ratio")
                                            .setDoubleRange(range))))
                    .build();
            assertThatThrownBy(() -> AccessPolicies.requireWellFormed(nonFinite))
                    .as(range.toString())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("row_filters[0].double_range.")
                    .hasMessageContaining("must be finite");
        }
    }
}
