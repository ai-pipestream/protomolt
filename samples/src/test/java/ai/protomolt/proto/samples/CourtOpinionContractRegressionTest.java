package ai.protomolt.proto.samples;

import static org.assertj.core.api.Assertions.assertThat;

import ai.protomolt.proto.validate.ProtoValidator;
import court.v1.OpinionMetadataOuterClass.OpinionMetadata;
import org.junit.jupiter.api.Test;

/**
 * Deterministic regression cases against the repository's court-decoration schema.
 * The source check here is deliberately limited to printed docket support; a full
 * semantic evaluator needs the configured evidence and policy from the host.
 */
class CourtOpinionContractRegressionTest {
    private static final String SOURCE = """
            UNITED STATES COURT OF APPEALS
            Parker v. City of Northhaven, No. 23-1042 (2024).
            The court affirms the district court's judgment.
            """;

    @Test
    void incompleteWithReasonIsValidButRoutesToReview() {
        OpinionMetadata result = OpinionMetadata.newBuilder()
                .setIncomplete(true)
                .setIncompleteReason("The supplied opinion ends before its holding.")
                .build();

        assertThat(validation(result).valid()).isTrue();
        assertThat(route(result, SOURCE)).isEqualTo(Route.REVIEW);
    }

    @Test
    void incompleteWithoutReasonIsInvalid() {
        OpinionMetadata result = OpinionMetadata.newBuilder().setIncomplete(true).build();

        assertThat(validation(result).valid()).isFalse();
        assertThat(validation(result).violations())
                .anyMatch(violation -> violation.ruleId().contains("incomplete-needs-reason"));
        assertThat(route(result, SOURCE)).isEqualTo(Route.INVALID);
    }

    @Test
    void proseDocketIsRejectedByTheActualSchema() {
        OpinionMetadata result = complete().setDocketNumber("not specified").build();

        assertThat(validation(result).valid()).isFalse();
        assertThat(validation(result).violations())
                .anyMatch(violation -> violation.ruleId().contains("no-prose-values"));
        assertThat(route(result, SOURCE)).isEqualTo(Route.INVALID);
    }

    @Test
    void inventedDocketPassesShapeButLacksSourceSupport() {
        OpinionMetadata invented = complete().setDocketNumber("No. 99-0000").build();
        OpinionMetadata printed = complete().setDocketNumber("No. 23-1042").build();

        assertThat(validation(invented).valid()).isTrue();
        assertThat(SOURCE).doesNotContain(invented.getDocketNumber());
        assertThat(route(invented, SOURCE)).isEqualTo(Route.REVIEW);
        assertThat(route(printed, SOURCE)).isEqualTo(Route.ACCEPTED);
    }

    @Test
    void copiedExemplarDocketDoesNotBecomeSourceEvidence() {
        OpinionMetadata copied = complete().setDocketNumber("No. 12-3456").build();

        assertThat(validation(copied).valid()).isTrue();
        assertThat(route(copied, SOURCE)).isEqualTo(Route.REVIEW);
    }

    @Test
    void unsupportedCitationPassesShapeButRoutesToReview() {
        OpinionMetadata cited = complete().setDocketNumber("No. 23-1042")
                .addLeadingAuthorities("Roe v. Wade, 410 U.S. 113 (1973)").build();

        assertThat(validation(cited).valid()).isTrue();
        assertThat(route(cited, SOURCE)).isEqualTo(Route.REVIEW);
    }

    private static OpinionMetadata.Builder complete() {
        return OpinionMetadata.newBuilder()
                .setCourt("UNITED STATES COURT OF APPEALS")
                .setCaption("Parker v. City of Northhaven")
                .setYear(2024)
                .addTopics("municipal law");
    }

    private static ai.protomolt.proto.validate.ValidationResult validation(
            OpinionMetadata result) {
        return ProtoValidator.forMessageType(OpinionMetadata.getDescriptor()).validate(result);
    }

    // A small fixture policy, not a general legal-source entailment check.
    private static Route route(OpinionMetadata result, String source) {
        if (!validation(result).valid()) return Route.INVALID;
        if (result.getIncomplete()) return Route.REVIEW;
        if (!result.getDocketNumber().isEmpty()
                && !source.contains(result.getDocketNumber())) return Route.REVIEW;
        if (result.getLeadingAuthoritiesList().stream()
                .anyMatch(citation -> !source.contains(citation))) return Route.REVIEW;
        return Route.ACCEPTED;
    }

    private enum Route { INVALID, REVIEW, ACCEPTED }
}
