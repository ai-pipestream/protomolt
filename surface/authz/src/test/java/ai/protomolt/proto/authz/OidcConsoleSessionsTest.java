package ai.protomolt.proto.authz;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.authz.OidcConsoleSessions.LoginResult;
import ai.protomolt.proto.authz.OidcConsoleSessions.LoginStart;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The OIDC console session store against the fake provider: discovery at construction, the
 * PKCE code flow end to end, every refusal on the callback, the session's caller through the
 * request seam, and expiry and revocation on a hand-moved clock.
 */
class OidcConsoleSessionsTest {

    private static final URI REDIRECT = URI.create("http://console.test/console/session/callback");
    private static final String RETURN = "/console/repository/accounts";

    private final MutableClock clock = new MutableClock(Instant.parse("2026-10-21T10:00:00Z"));
    private FakeOpenIdProvider provider;
    private OidcConsoleSessions sessions;

    @BeforeEach
    void boot() throws IOException {
        provider = new FakeOpenIdProvider("protomolt-console", "console-secret", clock);
        sessions = new OidcConsoleSessions(provider.settings(REDIRECT), HttpClient.newHttpClient(), clock);
    }

    @AfterEach
    void shutdown() {
        provider.close();
    }

    /** Follows the provider's immediate redirect and returns the callback's query. */
    private Map<String, String> callbackQuery(LoginStart start) throws Exception {
        var response = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build()
                .send(java.net.http.HttpRequest.newBuilder(start.authorizationUri()).GET().build(),
                        java.net.http.HttpResponse.BodyHandlers.discarding());
        assertThat(response.statusCode()).isEqualTo(302);
        URI location = URI.create(response.headers().firstValue("Location").orElseThrow());
        assertThat(location.toString()).startsWith(REDIRECT.toString());
        return FakeOpenIdProvider.form(location.getRawQuery());
    }

    private static RequestHeaders cookie(String token) {
        return RequestHeaders.of(Map.of("Cookie", List.of("other=1; " + OidcConsoleSessions.COOKIE + "=" + token)));
    }

    @Test
    void discoveryReadsTheProviderAndRefusesAnotherIssuer() {
        assertThat(sessions.provider().authorizationEndpoint()).isEqualTo(URI.create(provider.issuer() + "/authorize"));
        assertThat(sessions.provider().tokenEndpoint()).isEqualTo(URI.create(provider.issuer() + "/token"));
        assertThat(sessions.provider().introspectionEndpoint()).isEqualTo(URI.create(provider.issuer() + "/introspect"));

        var other = new OidcConsoleSettings(URI.create("http://127.0.0.1:1/realm"), "c", "s", REDIRECT,
                Duration.ofHours(1));
        assertThatThrownBy(() -> new OidcConsoleSessions(other, HttpClient.newHttpClient(), clock))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("OIDC discovery at http://127.0.0.1:1/realm/.well-known/openid-configuration");
    }

    @Test
    void theLoginSendsThePkceCodeRequestAndTheCallbackIssuesASessionForTheIntrospectedCaller()
            throws Exception {
        LoginStart start = sessions.beginLogin(RETURN);
        Map<String, String> request = FakeOpenIdProvider.form(start.authorizationUri().getRawQuery());
        assertThat(request).containsEntry("response_type", "code")
                .containsEntry("client_id", "protomolt-console")
                .containsEntry("redirect_uri", REDIRECT.toString())
                .containsEntry("scope", "openid")
                .containsEntry("state", start.state())
                .containsEntry("code_challenge_method", "S256");
        assertThat(request.get("nonce")).isNotBlank();
        assertThat(request.get("code_challenge")).isNotBlank();

        LoginResult result = sessions.completeLogin(start.state(), callbackQuery(start));
        assertThat(result.succeeded()).as(result.failure().name()).isTrue();
        assertThat(result.returnPath()).isEqualTo(RETURN);
        assertThat(provider.lastVerifier).isNotBlank();

        ConsoleSession session = sessions.session(cookie(result.sessionToken())).orElseThrow();
        assertThat(session.getPrincipal()).isEqualTo("alice");
        assertThat(session.getScopesList()).containsExactly(Scopes.SEARCH_QUERY);
        assertThat(session.getIssuer()).isEqualTo(provider.issuer().toString());
        assertThat(session.getSubject()).isEqualTo("alice-subject");
        assertThat(session.getExpiresAt().getSeconds() - session.getIssuedAt().getSeconds()).isEqualTo(3600);

        AuthenticatedCaller caller = sessions.resolve(cookie(result.sessionToken())).orElseThrow();
        assertThat(caller.caller().name()).isEqualTo("alice");
        assertThat(caller.caller().holds(Scopes.SEARCH_QUERY)).isTrue();
        assertThat(caller.caller().unrestricted()).isFalse();
        assertThat(caller.binding()).isEmpty();
    }

    @Test
    void aReturnPathOutsideTheConsoleIsRefused() {
        for (String path : List.of("/", "/console", "/admin", "//evil.test/console/", "/console/x#frag",
                "/console/../v1", "/console/a b", "http://evil.test/console/")) {
            assertThat(OidcConsoleSessions.acceptableReturnPath(path)).as(path).isFalse();
            assertThatThrownBy(() -> sessions.beginLogin(path)).isInstanceOf(IllegalArgumentException.class);
        }
        assertThat(OidcConsoleSessions.acceptableReturnPath("/console/repository?account=a&x=1")).isTrue();
    }

    @Test
    void aCallbackWhoseStateIsNotThisBrowsersLoginIsRefused() throws Exception {
        LoginStart start = sessions.beginLogin(RETURN);
        Map<String, String> query = callbackQuery(start);
        assertThat(sessions.completeLogin(null, query).failure())
                .isEqualTo(SignInFailure.SIGN_IN_FAILURE_STATE_MISMATCH);
        assertThat(sessions.completeLogin("another-state", query).failure())
                .isEqualTo(SignInFailure.SIGN_IN_FAILURE_STATE_MISMATCH);
        assertThat(sessions.completeLogin(start.state(), Map.of("code", query.get("code"))).failure())
                .isEqualTo(SignInFailure.SIGN_IN_FAILURE_STATE_MISMATCH);
        // The pending login is still there: the refusals above consumed nothing.
        assertThat(sessions.completeLogin(start.state(), query).succeeded()).isTrue();
    }

    @Test
    void aPendingLoginExpiresAndACallbackReplaysNothing() throws Exception {
        LoginStart start = sessions.beginLogin(RETURN);
        Map<String, String> query = callbackQuery(start);
        clock.advance(OidcConsoleSessions.LOGIN_WINDOW.plusSeconds(1));
        assertThat(sessions.completeLogin(start.state(), query).failure())
                .isEqualTo(SignInFailure.SIGN_IN_FAILURE_LOGIN_EXPIRED);

        LoginStart again = sessions.beginLogin(RETURN);
        Map<String, String> fresh = callbackQuery(again);
        assertThat(sessions.completeLogin(again.state(), fresh).succeeded()).isTrue();
        assertThat(sessions.completeLogin(again.state(), fresh).failure())
                .isEqualTo(SignInFailure.SIGN_IN_FAILURE_LOGIN_EXPIRED);
    }

    @Test
    void theProvidersDenialAndATokenRefusalFailTheLoginWithoutASession() throws Exception {
        provider.denyWith = "access_denied";
        LoginStart denied = sessions.beginLogin(RETURN);
        assertThat(sessions.completeLogin(denied.state(), callbackQuery(denied)).failure())
                .isEqualTo(SignInFailure.SIGN_IN_FAILURE_PROVIDER_DENIED);
        provider.denyWith = null;

        LoginStart start = sessions.beginLogin(RETURN);
        Map<String, String> query = callbackQuery(start);
        Map<String, String> wrongCode = Map.of("state", query.get("state"), "code", "not-issued");
        assertThat(sessions.completeLogin(start.state(), wrongCode).failure())
                .isEqualTo(SignInFailure.SIGN_IN_FAILURE_TOKEN_EXCHANGE_FAILED);
        assertThat(sessions.liveSessions()).isZero();
    }

    @Test
    void aTokenEndpointOutageIsAStoreFailureNotARefusal() throws Exception {
        provider.tokenEndpointStatus = 500;
        LoginStart start = sessions.beginLogin(RETURN);
        Map<String, String> query = callbackQuery(start);
        assertThatThrownBy(() -> sessions.completeLogin(start.state(), query))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("token endpoint answered HTTP 500");
    }

    @Test
    void anIdTokenForAnotherIssuerAudienceNonceOrTimeIsRefused() throws Exception {
        provider.idTokenIssuer = "http://other.test/realm";
        expectIdTokenInvalid();
        provider.idTokenIssuer = null;

        provider.idTokenAudience = "another-client";
        expectIdTokenInvalid();
        provider.idTokenAudience = null;

        provider.idTokenNonce = "stale-nonce";
        expectIdTokenInvalid();
        provider.idTokenNonce = null;

        provider.idTokenLifetime = Duration.ofSeconds(-1);
        expectIdTokenInvalid();
        provider.idTokenLifetime = Duration.ofMinutes(5);

        LoginStart start = sessions.beginLogin(RETURN);
        assertThat(sessions.completeLogin(start.state(), callbackQuery(start)).succeeded()).isTrue();
    }

    private void expectIdTokenInvalid() throws Exception {
        LoginStart start = sessions.beginLogin(RETURN);
        assertThat(sessions.completeLogin(start.state(), callbackQuery(start)).failure())
                .isEqualTo(SignInFailure.SIGN_IN_FAILURE_ID_TOKEN_INVALID);
    }

    @Test
    void aUserTheCallerStoreDoesNotResolveGetsNoSession() throws Exception {
        provider.nextIntrospection = "{\"active\": true, \"username\": \"nobody-with-scopes\"}";
        LoginStart start = sessions.beginLogin(RETURN);
        assertThat(sessions.completeLogin(start.state(), callbackQuery(start)).failure())
                .isEqualTo(SignInFailure.SIGN_IN_FAILURE_UNKNOWN_PRINCIPAL);
        assertThat(sessions.liveSessions()).isZero();
    }

    @Test
    void aSessionExpiresAfterItsLifetimeAndRevocationEndsItAtOnce() throws Exception {
        LoginStart first = sessions.beginLogin(RETURN);
        String token = sessions.completeLogin(first.state(), callbackQuery(first)).sessionToken();
        clock.advance(Duration.ofMinutes(59));
        assertThat(sessions.session(cookie(token))).isPresent();
        clock.advance(Duration.ofMinutes(1));
        assertThat(sessions.session(cookie(token))).isEmpty();
        assertThat(sessions.resolve(cookie(token))).isEmpty();

        LoginStart second = sessions.beginLogin(RETURN);
        String live = sessions.completeLogin(second.state(), callbackQuery(second)).sessionToken();
        assertThat(sessions.revoke(cookie(live))).contains("alice");
        assertThat(sessions.session(cookie(live))).isEmpty();
        assertThat(sessions.revoke(cookie(live))).isEmpty();
        assertThat(sessions.revoke(RequestHeaders.of(Map.of()))).isEmpty();
    }

    @Test
    void anUnknownOrAbsentCookieIsNoSession() {
        assertThat(sessions.session(RequestHeaders.of(Map.of()))).isEmpty();
        assertThat(sessions.session(cookie("never-issued"))).isEmpty();
        assertThat(OidcConsoleSessions.cookie(RequestHeaders.of(Map.of("cookie",
                List.of("a=1", OidcConsoleSessions.LOGIN_COOKIE + "=st; b=2"))), OidcConsoleSessions.LOGIN_COOKIE))
                .isEqualTo("st");
    }
}
