package ai.pipestream.proto.rest;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ApiTokenRequirementTest {

    @ProtoApiToken(name = "tok", in = ProtoApiToken.In.QUERY, scheme = ProtoApiToken.Scheme.HTTP,
            httpScheme = "basic", required = false, description = "query token")
    private static final class FullySpecified {
    }

    @ProtoApiToken
    private static final class Defaults {
    }

    @Test
    void fromAnnotationMapsEveryAttribute() {
        ApiTokenRequirement requirement = ApiTokenRequirement.from(
                FullySpecified.class.getAnnotation(ProtoApiToken.class));

        assertThat(requirement.name()).isEqualTo("tok");
        assertThat(requirement.in()).isEqualTo(ProtoApiToken.In.QUERY);
        assertThat(requirement.scheme()).isEqualTo(ProtoApiToken.Scheme.HTTP);
        assertThat(requirement.httpScheme()).isEqualTo("basic");
        assertThat(requirement.required()).isFalse();
        assertThat(requirement.description()).isEqualTo("query token");
    }

    @Test
    void annotationDefaultsMatchTheRecordDefaults() {
        ApiTokenRequirement requirement = ApiTokenRequirement.from(
                Defaults.class.getAnnotation(ProtoApiToken.class));

        assertThat(requirement.name()).isEqualTo("api_token");
        assertThat(requirement.in()).isEqualTo(ProtoApiToken.In.HEADER);
        assertThat(requirement.scheme()).isEqualTo(ProtoApiToken.Scheme.API_KEY);
        assertThat(requirement.httpScheme()).isEqualTo("bearer");
        assertThat(requirement.required()).isTrue();
        assertThat(requirement.description()).isEqualTo("API access token");
    }

    @Test
    void blankOrNullHttpSchemeFallsBackToBearer() {
        assertThat(new ApiTokenRequirement(
                "tok", ProtoApiToken.In.HEADER, ProtoApiToken.Scheme.HTTP, "  ", true, "d")
                .httpScheme()).isEqualTo("bearer");
        assertThat(new ApiTokenRequirement(
                "tok", ProtoApiToken.In.HEADER, ProtoApiToken.Scheme.HTTP, null, true, "d")
                .httpScheme()).isEqualTo("bearer");
    }

    @Test
    void nullDescriptionFallsBackToEmpty() {
        ApiTokenRequirement requirement = new ApiTokenRequirement(
                "tok", ProtoApiToken.In.HEADER, ProtoApiToken.Scheme.API_KEY, "bearer", true, null);

        assertThat(requirement.description()).isEmpty();
    }

    @Test
    void nullMandatoryComponentsAreRejected() {
        assertThatThrownBy(() -> new ApiTokenRequirement(
                null, ProtoApiToken.In.HEADER, ProtoApiToken.Scheme.API_KEY, "bearer", true, ""))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new ApiTokenRequirement(
                "tok", null, ProtoApiToken.Scheme.API_KEY, "bearer", true, ""))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new ApiTokenRequirement(
                "tok", ProtoApiToken.In.HEADER, null, "bearer", true, ""))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> ApiTokenRequirement.from(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void apiKeyHeaderFactoryBuildsARequiredHeaderApiKey() {
        ApiTokenRequirement requirement = ApiTokenRequirement.apiKeyHeader("x-api-key");

        assertThat(requirement.name()).isEqualTo("x-api-key");
        assertThat(requirement.in()).isEqualTo(ProtoApiToken.In.HEADER);
        assertThat(requirement.scheme()).isEqualTo(ProtoApiToken.Scheme.API_KEY);
        assertThat(requirement.httpScheme()).isEqualTo("bearer");
        assertThat(requirement.required()).isTrue();
    }

    @Test
    void bearerFactoryBuildsAnAuthorizationHeaderHttpScheme() {
        ApiTokenRequirement requirement = ApiTokenRequirement.bearer();

        assertThat(requirement.name()).isEqualTo("Authorization");
        assertThat(requirement.in()).isEqualTo(ProtoApiToken.In.HEADER);
        assertThat(requirement.scheme()).isEqualTo(ProtoApiToken.Scheme.HTTP);
        assertThat(requirement.httpScheme()).isEqualTo("bearer");
        assertThat(requirement.required()).isTrue();
    }
}
