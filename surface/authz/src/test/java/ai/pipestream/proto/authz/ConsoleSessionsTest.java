package ai.pipestream.proto.authz;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.pipestream.proto.actions.Caller;
import ai.pipestream.proto.actions.Scopes;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpPrincipal;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * The console-session mechanism: the open form is process authority without cookies, the
 * secured form binds sessions to access-policy principals or a console login token's
 * bounded identity (never the operator), retains the presenting credential for proxying
 * onto guarded peers, and expires and revokes sessions.
 */
class ConsoleSessionsTest {

    private static final String COOKIE = "__Host-protomolt_test_session";
    private static final Caller QUERIER =
            Caller.scoped("querier", Set.of(Scopes.SEARCH_QUERY));
    private static final CallerResolver RESOLVER = credential ->
            "querier-credential".equals(credential)
                    ? Optional.of(QUERIER) : Optional.empty();

    @Test
    void theOpenConsoleRunsWithProcessAuthorityAndIssuesNothing() {
        ConsoleSessions sessions = ConsoleSessions.open(COOKIE);
        assertThat(sessions.requiresLogin()).isFalse();
        assertThat(sessions.caller(exchange(null)).orElseThrow().unrestricted()).isTrue();
        assertThat(sessions.credential(exchange(null))).isEmpty();
        assertThat(sessions.loginCaller("anything")).isNull();
        assertThatThrownBy(() -> sessions.issue(QUERIER, "anything"))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void aPolicyPrincipalLogsInAndItsSessionCarriesCallerAndCredential() {
        ConsoleSessions sessions =
                ConsoleSessions.secured(COOKIE, Duration.ofHours(1), RESOLVER);
        assertThat(sessions.requiresLogin()).isTrue();
        Caller caller = sessions.loginCaller("querier-credential");
        assertThat(caller).isEqualTo(QUERIER);
        String token = sessions.issue(caller, "querier-credential");
        HttpExchange exchange = exchange(COOKIE + "=" + token);
        assertThat(sessions.caller(exchange)).contains(QUERIER);
        assertThat(sessions.credential(exchange)).contains("querier-credential");
    }

    @Test
    void unknownCredentialsAndMissingCookiesNeverResolve() {
        ConsoleSessions sessions =
                ConsoleSessions.secured(COOKIE, Duration.ofHours(1), RESOLVER);
        assertThat(sessions.loginCaller("guessed")).isNull();
        assertThat(sessions.loginCaller("")).isNull();
        assertThat(sessions.loginCaller(null)).isNull();
        assertThat(sessions.caller(exchange(null))).isEmpty();
        assertThat(sessions.caller(exchange("other=value; " + COOKIE + "x=nope"))).isEmpty();
        assertThat(sessions.caller(exchange(COOKIE + "=forged"))).isEmpty();
    }

    @Test
    void revokingASessionDropsIt() {
        ConsoleSessions sessions =
                ConsoleSessions.secured(COOKIE, Duration.ofHours(1), RESOLVER);
        String token = sessions.issue(QUERIER, "querier-credential");
        HttpExchange exchange = exchange("first=1; " + COOKIE + "=" + token);
        assertThat(sessions.authorized(exchange)).isTrue();
        sessions.revoke(exchange);
        assertThat(sessions.authorized(exchange)).isFalse();
    }

    @Test
    void sessionsExpireAfterTheirTtl() {
        ConsoleSessions sessions =
                ConsoleSessions.secured(COOKIE, Duration.ofMillis(200), RESOLVER);
        String token = sessions.issue(QUERIER, "querier-credential");
        HttpExchange exchange = exchange(COOKIE + "=" + token);
        assertThat(sessions.authorized(exchange)).isTrue();
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (sessions.authorized(exchange)) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("the session outlived its ttl");
            }
            Thread.onSpinWait();
        }
        assertThat(sessions.caller(exchange)).isEmpty();
        assertThat(sessions.credential(exchange)).isEmpty();
    }

    @Test
    void aLoginTokenBindsToItsBoundedIdentityBesideThePolicy() {
        ConsoleSessions sessions = ConsoleSessions.secured(COOKIE, Duration.ofHours(1),
                RESOLVER, "console-token-with-at-least-32-characters",
                Caller.scoped("console", Set.of(Scopes.WORKER_COORDINATE)));
        assertThat(sessions.loginCaller("console-token-with-at-least-32-characters").name())
                .isEqualTo("console");
        assertThat(sessions.loginCaller("querier-credential")).isEqualTo(QUERIER);
        assertThat(sessions.loginCaller("guessed")).isNull();
    }

    @Test
    void aLoginTokenMustNeverBindToTheOperator() {
        assertThatThrownBy(() -> ConsoleSessions.secured(COOKIE, Duration.ofHours(1),
                RESOLVER, "console-token-with-at-least-32-characters", Caller.operator()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("never bind to the operator");
    }

    @Test
    void settingsAreValidatedByName() {
        assertThatThrownBy(() -> ConsoleSessions.validateSettings("short",
                Duration.ofHours(1)))
                .hasMessageContaining("32 to 1024 characters");
        assertThatThrownBy(() -> ConsoleSessions.validateSettings(
                "console-token-with-at-least-32-characters", Duration.ZERO))
                .hasMessageContaining("between one second and seven days");
        assertThatThrownBy(() -> ConsoleSessions.validateSettings(
                "console-token-with-at-least-32-characters", Duration.ofDays(8)))
                .hasMessageContaining("between one second and seven days");
        assertThatThrownBy(() -> ConsoleSessions.open(" "))
                .hasMessageContaining("cookie name");
    }

    private static HttpExchange exchange(String cookieHeader) {
        Headers request = new Headers();
        if (cookieHeader != null) {
            request.put("Cookie", List.of(cookieHeader));
        }
        return new FakeExchange(request);
    }

    /** Just enough of an exchange to carry request headers. */
    private static final class FakeExchange extends HttpExchange {

        private final Headers request;
        private final Headers response = new Headers();

        private FakeExchange(Headers request) {
            this.request = request;
        }

        @Override
        public Headers getRequestHeaders() {
            return request;
        }

        @Override
        public Headers getResponseHeaders() {
            return response;
        }

        @Override
        public URI getRequestURI() {
            return URI.create("/");
        }

        @Override
        public String getRequestMethod() {
            return "GET";
        }

        @Override
        public HttpContext getHttpContext() {
            return null;
        }

        @Override
        public void close() {
        }

        @Override
        public InputStream getRequestBody() {
            return InputStream.nullInputStream();
        }

        @Override
        public OutputStream getResponseBody() {
            return OutputStream.nullOutputStream();
        }

        @Override
        public void sendResponseHeaders(int code, long length) {
        }

        @Override
        public InetSocketAddress getRemoteAddress() {
            return new InetSocketAddress(0);
        }

        @Override
        public int getResponseCode() {
            return 0;
        }

        @Override
        public InetSocketAddress getLocalAddress() {
            return new InetSocketAddress(0);
        }

        @Override
        public String getProtocol() {
            return "HTTP/1.1";
        }

        @Override
        public Object getAttribute(String name) {
            return null;
        }

        @Override
        public void setAttribute(String name, Object value) {
        }

        @Override
        public void setStreams(InputStream in, OutputStream out) {
        }

        @Override
        public HttpPrincipal getPrincipal() {
            return null;
        }
    }
}
