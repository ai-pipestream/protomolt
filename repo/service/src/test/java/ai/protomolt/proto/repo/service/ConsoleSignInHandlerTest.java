package ai.protomolt.proto.repo.service;

import static org.assertj.core.api.Assertions.assertThat;

import ai.protomolt.proto.authz.AuthenticatedCaller;
import ai.protomolt.proto.authz.ConsoleSessionStatus;
import ai.protomolt.proto.authz.FakeOpenIdProvider;
import ai.protomolt.proto.authz.MutableClock;
import ai.protomolt.proto.authz.OidcConsoleSessions;
import ai.protomolt.proto.authz.OidcConsoleSettings;
import ai.protomolt.proto.authz.SignInFailure;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.List;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The sign-in routes on a plain JDK server against the fake provider: the login redirect
 * and its Lax cookie, the callback's Strict session cookie and return redirect, the status
 * answer as binary protobuf with the bound accounts, logout, the failure redirect, and the
 * not-configured host.
 */
class ConsoleSignInHandlerTest {

    private static final String RETURN = "/console/repository";
    private static final Function<AuthenticatedCaller, List<String>> ACCOUNTS =
            caller -> "alice".equals(caller.caller().name()) ? List.of("acct-a", "acct-b") : List.of();

    private final MutableClock clock = new MutableClock(Instant.parse("2026-10-21T10:00:00Z"));
    private final HttpClient client = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build();
    private FakeOpenIdProvider provider;
    private HttpServer host;
    private String base;
    private OidcConsoleSessions sessions;

    @BeforeEach
    void boot() throws IOException {
        provider = new FakeOpenIdProvider("protomolt-console", "console-secret", clock);
        host = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        base = "http://127.0.0.1:" + host.getAddress().getPort();
        sessions = new OidcConsoleSessions(provider.settings(URI.create(base + ConsoleSignInHandler.PATH + "/callback")),
                HttpClient.newHttpClient(), clock);
        host.createContext(ConsoleSignInHandler.PATH, ConsoleSignInHandler.over(sessions, ACCOUNTS));
        host.start();
    }

    @AfterEach
    void shutdown() {
        host.stop(0);
        provider.close();
    }

    private HttpResponse<byte[]> get(String path, String cookie) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(base + path)).GET();
        if (cookie != null) {
            request.header("Cookie", cookie);
        }
        return client.send(request.build(), HttpResponse.BodyHandlers.ofByteArray());
    }

    private static String cookieValue(HttpResponse<?> response, String name) {
        return response.headers().allValues("Set-Cookie").stream()
                .filter(header -> header.startsWith(name + "="))
                .map(header -> header.substring(name.length() + 1, header.indexOf(';')))
                .findFirst().orElseThrow();
    }

    private static String cookieHeader(HttpResponse<?> response, String name) {
        return response.headers().allValues("Set-Cookie").stream()
                .filter(header -> header.startsWith(name + "=")).findFirst().orElseThrow();
    }

    /** Signs in through the provider and returns the session cookie value. */
    private String signIn() throws Exception {
        HttpResponse<byte[]> login = get(ConsoleSignInHandler.PATH + "/login?return=" + RETURN, null);
        assertThat(login.statusCode()).isEqualTo(302);
        String state = cookieValue(login, OidcConsoleSessions.LOGIN_COOKIE);
        assertThat(cookieHeader(login, OidcConsoleSessions.LOGIN_COOKIE))
                .contains("HttpOnly", "Secure", "SameSite=Lax", "Path=/", "Max-Age=300");
        URI authorize = URI.create(login.headers().firstValue("Location").orElseThrow());
        assertThat(authorize.toString()).startsWith(provider.issuer() + "/authorize?");

        HttpResponse<byte[]> provided = client.send(HttpRequest.newBuilder(authorize).GET().build(),
                HttpResponse.BodyHandlers.ofByteArray());
        String callbackUrl = provided.headers().firstValue("Location").orElseThrow();
        assertThat(callbackUrl).startsWith(base + ConsoleSignInHandler.PATH + "/callback?");

        HttpResponse<byte[]> callback = get(callbackUrl.substring(base.length()),
                OidcConsoleSessions.LOGIN_COOKIE + "=" + state);
        assertThat(callback.statusCode()).isEqualTo(302);
        assertThat(callback.headers().firstValue("Location")).contains(RETURN);
        assertThat(cookieHeader(callback, OidcConsoleSessions.COOKIE))
                .contains("HttpOnly", "Secure", "SameSite=Strict", "Path=/", "Max-Age=3600");
        assertThat(cookieHeader(callback, OidcConsoleSessions.LOGIN_COOKIE)).contains("Max-Age=0");
        return cookieValue(callback, OidcConsoleSessions.COOKIE);
    }

    @Test
    void loginCallbackStatusAndLogoutRoundTrip() throws Exception {
        HttpResponse<byte[]> anonymous = get(ConsoleSignInHandler.PATH, null);
        assertThat(anonymous.statusCode()).isEqualTo(401);
        assertThat(anonymous.headers().firstValue("Content-Type")).contains("application/proto");
        assertThat(ConsoleSessionStatus.parseFrom(anonymous.body()).getAuthenticated()).isFalse();

        String session = signIn();
        HttpResponse<byte[]> status = get(ConsoleSignInHandler.PATH, OidcConsoleSessions.COOKIE + "=" + session);
        assertThat(status.statusCode()).isEqualTo(200);
        assertThat(status.headers().firstValue("Cache-Control")).contains("no-store");
        ConsoleSessionStatus parsed = ConsoleSessionStatus.parseFrom(status.body());
        assertThat(parsed.getAuthenticated()).isTrue();
        assertThat(parsed.getPrincipal()).isEqualTo("alice");
        assertThat(parsed.getAccountIdsList()).containsExactly("acct-a", "acct-b");
        assertThat(parsed.getExpiresAt().getSeconds()).isEqualTo(clock.instant().getEpochSecond() + 3600);

        HttpResponse<byte[]> logout = client.send(HttpRequest.newBuilder(URI.create(base + ConsoleSignInHandler.PATH + "/logout"))
                .header("Cookie", OidcConsoleSessions.COOKIE + "=" + session)
                .POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofByteArray());
        assertThat(logout.statusCode()).isEqualTo(204);
        assertThat(cookieHeader(logout, OidcConsoleSessions.COOKIE)).contains("Max-Age=0");
        assertThat(get(ConsoleSignInHandler.PATH, OidcConsoleSessions.COOKIE + "=" + session).statusCode())
                .isEqualTo(401);
    }

    @Test
    void anExpiredSessionIsUnauthenticated() throws Exception {
        String session = signIn();
        clock.advance(java.time.Duration.ofHours(1));
        assertThat(get(ConsoleSignInHandler.PATH, OidcConsoleSessions.COOKIE + "=" + session).statusCode())
                .isEqualTo(401);
    }

    @Test
    void aFailedLoginLandsOnTheSignInPageNamingTheReason() throws Exception {
        HttpResponse<byte[]> login = get(ConsoleSignInHandler.PATH + "/login?return=" + RETURN, null);
        URI authorize = URI.create(login.headers().firstValue("Location").orElseThrow());
        String callbackUrl = client.send(HttpRequest.newBuilder(authorize).GET().build(),
                HttpResponse.BodyHandlers.discarding()).headers().firstValue("Location").orElseThrow();
        // The callback arrives without this browser's login cookie.
        HttpResponse<byte[]> callback = get(callbackUrl.substring(base.length()), null);
        assertThat(callback.statusCode()).isEqualTo(302);
        assertThat(callback.headers().firstValue("Location")).contains(ConsoleSignInHandler.SIGN_IN_PAGE
                + "?error=" + SignInFailure.SIGN_IN_FAILURE_STATE_MISMATCH.name());
        assertThat(callback.headers().allValues("Set-Cookie"))
                .noneMatch(header -> header.startsWith(OidcConsoleSessions.COOKIE + "="));
    }

    @Test
    void aReturnPathOutsideTheConsoleAndWrongMethodsAreRefused() throws Exception {
        HttpResponse<byte[]> outside = get(ConsoleSignInHandler.PATH + "/login?return=/v1/documents", null);
        assertThat(outside.statusCode()).isEqualTo(400);
        assertThat(new String(outside.body())).contains("/console/");
        assertThat(get(ConsoleSignInHandler.PATH + "/login", null).statusCode()).isEqualTo(400);
        assertThat(get(ConsoleSignInHandler.PATH + "/logout", null).statusCode()).isEqualTo(405);
        assertThat(get(ConsoleSignInHandler.PATH + "/nowhere", null).statusCode()).isEqualTo(404);
    }

    @Test
    void aProviderOutageDuringTheCallbackIsUnavailableNotARefusal() throws Exception {
        HttpResponse<byte[]> login = get(ConsoleSignInHandler.PATH + "/login?return=" + RETURN, null);
        String state = cookieValue(login, OidcConsoleSessions.LOGIN_COOKIE);
        URI authorize = URI.create(login.headers().firstValue("Location").orElseThrow());
        String callbackUrl = client.send(HttpRequest.newBuilder(authorize).GET().build(),
                HttpResponse.BodyHandlers.discarding()).headers().firstValue("Location").orElseThrow();
        provider.tokenEndpointStatus = 503;
        HttpResponse<byte[]> callback = get(callbackUrl.substring(base.length()),
                OidcConsoleSessions.LOGIN_COOKIE + "=" + state);
        assertThat(callback.statusCode()).isEqualTo(503);
        assertThat(new String(callback.body())).contains("token endpoint answered HTTP 503");
    }

    @Test
    void aHostWithoutSignInAnswersEveryRouteNamingTheVariables() throws Exception {
        HttpServer bare = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        bare.createContext(ConsoleSignInHandler.PATH, ConsoleSignInHandler.notConfigured());
        bare.start();
        try {
            String bareBase = "http://127.0.0.1:" + bare.getAddress().getPort();
            for (String path : List.of(ConsoleSignInHandler.PATH, ConsoleSignInHandler.PATH + "/login?return=/console/x")) {
                HttpResponse<String> response = client.send(HttpRequest.newBuilder(URI.create(bareBase + path)).GET().build(),
                        HttpResponse.BodyHandlers.ofString());
                assertThat(response.statusCode()).isEqualTo(503);
                assertThat(response.body()).contains(OidcConsoleSettings.ENV_ISSUER, OidcConsoleSettings.ENV_CLIENT_ID,
                        OidcConsoleSettings.ENV_CLIENT_SECRET, OidcConsoleSettings.ENV_REDIRECT_URI,
                        OidcConsoleSettings.ENV_SESSION_TTL);
            }
        } finally {
            bare.stop(0);
        }
        assertThat(ConsoleSignInHandler.notConfigured().configured()).isFalse();
        assertThat(ConsoleSignInHandler.over(sessions, ACCOUNTS).sessions()).contains(sessions);
    }
}
