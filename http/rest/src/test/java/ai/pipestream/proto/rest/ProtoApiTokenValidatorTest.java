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

    private static ApiTokenRequirement queryToken(String name) {
        return new ApiTokenRequirement(
                name,
                ProtoApiToken.In.QUERY,
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

    /**
     * The gateway lowercases incoming header names before handing them to the validator, so
     * lookup must use the lowercased token name — a mixed-case {@code name()} must still match.
     */
    @Test
    void sharedSecretReadsTheHeaderLocationCaseInsensitively() {
        ProtoApiTokenValidator validator = ProtoApiTokenValidator.sharedSecret("s3cret");

        assertThat(validator.validate(
                ApiTokenRequirement.apiKeyHeader("api_token"),
                Map.of("api_token", "s3cret"),
                Map.of())).isEmpty();
        assertThat(validator.validate(
                ApiTokenRequirement.apiKeyHeader("X-Api-Token"),
                Map.of("x-api-token", "s3cret"),
                Map.of())).isEmpty();
        assertThat(validator.validate(
                ApiTokenRequirement.apiKeyHeader("api_token"),
                Map.of("api_token", "wrong"),
                Map.of())).contains("Invalid API token");
        assertThat(validator.validate(
                ApiTokenRequirement.apiKeyHeader("api_token"),
                Map.of(),
                Map.of())).contains("Missing API token 'api_token'");
    }

    @Test
    void sharedSecretReadsTheQueryLocationWithTheExactName() {
        ProtoApiTokenValidator validator = ProtoApiTokenValidator.sharedSecret("s3cret");

        assertThat(validator.validate(
                queryToken("access_token"),
                Map.of(),
                Map.of("access_token", "s3cret"))).isEmpty();
        assertThat(validator.validate(
                queryToken("access_token"),
                Map.of(),
                Map.of("access_token", "wrong"))).contains("Invalid API token");
        // Query names are not lowercased: a case mismatch reads as absent, not as invalid.
        assertThat(validator.validate(
                queryToken("access_token"),
                Map.of(),
                Map.of("Access_Token", "s3cret"))).contains("Missing API token 'access_token'");
        // A header of the same name is not consulted for a QUERY token.
        assertThat(validator.validate(
                queryToken("access_token"),
                Map.of("access_token", "s3cret"),
                Map.of())).contains("Missing API token 'access_token'");
    }

    /**
     * Only the HTTP scheme strips {@code Bearer }; an API_KEY token is compared verbatim, so a
     * client that sends the prefix against an API_KEY requirement is rejected rather than
     * silently accepted.
     */
    @Test
    void sharedSecretStripsBearerPrefixOnlyForTheHttpScheme() {
        ProtoApiTokenValidator validator = ProtoApiTokenValidator.sharedSecret("s3cret");

        assertThat(validator.validate(
                ApiTokenRequirement.bearer(),
                Map.of("authorization", "Bearer s3cret"),
                Map.of())).isEmpty();
        assertThat(validator.validate(
                ApiTokenRequirement.bearer(),
                Map.of("authorization", "bearer   s3cret"),
                Map.of())).isEmpty();
        assertThat(validator.validate(
                ApiTokenRequirement.bearer(),
                Map.of("authorization", "s3cret"),
                Map.of())).isEmpty();
        assertThat(validator.validate(
                ApiTokenRequirement.bearer(),
                Map.of("authorization", "Bearer wrong"),
                Map.of())).contains("Invalid API token");

        assertThat(validator.validate(
                ApiTokenRequirement.apiKeyHeader("api_token"),
                Map.of("api_token", "Bearer s3cret"),
                Map.of())).contains("Invalid API token");
    }

    @Test
    void acceptNonBlankRequiresTheNamedCookie() {
        ProtoApiTokenValidator validator = ProtoApiTokenValidator.acceptNonBlank();
        Map<String, String> headers = Map.of("cookie", "session=abc; theme=dark");

        assertThat(validator.validate(cookieToken("api_token"), headers, Map.of())).isPresent();
        assertThat(validator.validate(cookieToken("session"), headers, Map.of())).isEmpty();
    }
}
