package ai.pipestream.proto.rest;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ProtoApiTokenValidatorTest {

    private static ApiTokenRequirement cookieToken(String name) {
        return new ApiTokenRequirement(
                name,
                ProtoApiToken.In.COOKIE,
                ProtoApiToken.Scheme.API_KEY,
                "bearer",
                true,
                "API access token");
    }

    @Test
    void sharedSecretMatchesNamedCookieInMultiCookieHeader() {
        ProtoApiTokenValidator validator = ProtoApiTokenValidator.sharedSecret("s3cret");
        Map<String, String> headers = Map.of("cookie", "session=abc; api_token=s3cret; theme=dark");

        assertThat(validator.validate(cookieToken("api_token"), headers, Map.of())).isEmpty();
    }

    @Test
    void sharedSecretRejectsWrongOrMissingCookie() {
        ProtoApiTokenValidator validator = ProtoApiTokenValidator.sharedSecret("s3cret");

        assertThat(validator.validate(
                cookieToken("api_token"),
                Map.of("cookie", "session=abc; api_token=wrong"),
                Map.of())).contains("Invalid API token");
        assertThat(validator.validate(
                cookieToken("api_token"),
                Map.of("cookie", "session=abc; theme=dark"),
                Map.of())).isPresent();
        assertThat(validator.validate(cookieToken("api_token"), Map.of(), Map.of())).isPresent();
    }

    @Test
    void denyAllRejectsEvenValidLookingTokens() {
        ProtoApiTokenValidator validator = ProtoApiTokenValidator.denyAll();

        assertThat(validator.validate(
                ApiTokenRequirement.apiKeyHeader("api_token"),
                Map.of("api_token", "some-real-looking-token"),
                Map.of())).isPresent();
        assertThat(validator.validate(ApiTokenRequirement.apiKeyHeader("api_token"), Map.of(), Map.of()))
                .isPresent();
    }

    @Test
    void acceptNonBlankRequiresTheNamedCookie() {
        ProtoApiTokenValidator validator = ProtoApiTokenValidator.acceptNonBlank();
        Map<String, String> headers = Map.of("cookie", "session=abc; theme=dark");

        assertThat(validator.validate(cookieToken("api_token"), headers, Map.of())).isPresent();
        assertThat(validator.validate(cookieToken("session"), headers, Map.of())).isEmpty();
    }
}
