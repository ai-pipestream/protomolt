package ai.protomolt.proto.samples;

import ai.protomolt.proto.samples.starter.v1.CoordinationReport;
import ai.protomolt.proto.validate.ProtoValidator;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Checks that the sample report's scalar, collection, and cross-field rules execute at runtime. */
class CoordinationReportContractTest {
    private static final ProtoValidator VALIDATOR =
            ProtoValidator.forMessageType(CoordinationReport.getDescriptor());

    private static CoordinationReport report(String headline, String... findings) {
        var builder = CoordinationReport.newBuilder().setHeadline(headline);
        for (String finding : findings) builder.addFindings(finding);
        return builder.setFindingCount(findings.length).build();
    }

    @Test
    void acceptsHeadlineAndFindingCollectionEndpointsWithMatchingCount() {
        var smallest = report("12345678", "x");
        var largest = report("h".repeat(256), "f".repeat(1024), "f".repeat(1024),
                "f".repeat(1024), "f".repeat(1024), "f".repeat(1024), "f".repeat(1024),
                "f".repeat(1024), "f".repeat(1024));

        assertThat(VALIDATOR.validate(smallest).violations()).isEmpty();
        assertThat(VALIDATOR.validate(largest).violations()).isEmpty();
    }

    @Test
    void rejectsHeadlineAndListBounds() {
        assertViolation(report("short", "finding"), "headline", "string.min_len");
        assertViolation(report("h".repeat(257), "finding"), "headline", "string.max_len");
        assertViolation(report("valid headline"), "findings", "repeated.min_items");
        assertViolation(report("valid headline", "a", "b", "c", "d", "e", "f", "g", "h", "i"),
                "findings", "repeated.max_items");
        assertViolation(report("valid headline", ""), "findings[0]", "string.min_len");
        assertViolation(report("valid headline", "f".repeat(1025)), "findings[0]", "string.max_len");
    }

    @Test
    void rejectsFindingCountsOutsideBoundsAndCountsThatDisagreeWithTheList() {
        var base = report("valid headline", "first");
        assertViolation(base.toBuilder().setFindingCount(0).build(), "finding_count", "int32.gte_lte");
        assertViolation(base.toBuilder().setFindingCount(9).build(), "finding_count", "int32.gte_lte");
        assertViolation(base.toBuilder().setFindingCount(2).build(), "", "report-count-matches-findings");
    }

    private static void assertViolation(CoordinationReport report, String path, String ruleId) {
        assertThat(VALIDATOR.validate(report).violations())
                .anySatisfy(v -> {
                    assertThat(v.path()).isEqualTo(path);
                    assertThat(v.ruleId()).isEqualTo(ruleId);
                });
    }

}
