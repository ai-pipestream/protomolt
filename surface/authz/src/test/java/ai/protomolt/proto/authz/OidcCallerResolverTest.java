package ai.protomolt.proto.authz;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.actions.Scopes;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The IdP-backed caller store against a fake RFC 7662 introspection endpoint (plain JDK
 * HttpServer): active tokens map claims onto a bounded caller, misconfigured grants
 * refuse loudly, and an endpoint failure is a store failure rather than a bad-credential
 * verdict.
 */
class OidcCallerResolverTest {

    static HttpServer idp;
    static OidcCallerResolver resolver;

    /** token → canned JSON answer. */
    static final Map<String, String> answers = new ConcurrentHashMap<>();

    static final AtomicReference<String> lastAuthorization = new AtomicReference<>();

    @BeforeAll
    static void boot() throws IOException {
        idp = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        idp.createContext("/introspect", exchange -> {
            lastAuthorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
            String body = new String(
                    exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            String token = URLDecoder.decode(
                    body.replaceFirst("^token=", ""), StandardCharsets.UTF_8);
            byte[] payload;
            int status;
            if ("boom".equals(token)) {
                status = 500;
                payload = "{}".getBytes(StandardCharsets.UTF_8);
            } else {
                status = 200;
                String answer = answers.get(token);
                payload = (answer == null ? "{\"active\": false}" : answer)
                        .getBytes(StandardCharsets.UTF_8);
            }
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, payload.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(payload);
            }
        });
        idp.start();
        resolver = new OidcCallerResolver(
                URI.create("http://127.0.0.1:" + idp.getAddress().getPort() + "/introspect"),
                "protomolt-resolver", "resolver-secret");
    }

    @AfterAll
    static void shutdown() {
        idp.stop(0);
    }

    @Test
    void anActiveTokenMapsClaimsOntoABoundedCaller() {
        answers.put("good-token", """
                {"active": true, "username": "ci-reader",
                 "protomolt_scopes": ["schema-read", "search-query"]}""");
        Caller caller = resolver.resolve("good-token").orElseThrow();
        assertThat(caller.name()).isEqualTo("ci-reader");
        assertThat(caller.unrestricted()).isFalse();
        assertThat(caller.holds(Scopes.SCHEMA_READ)).isTrue();
        assertThat(caller.holds(Scopes.SEARCH_QUERY)).isTrue();
        assertThat(caller.holds(Scopes.SCHEMA_WRITE)).isFalse();
        assertThat(lastAuthorization.get()).startsWith("Basic ");
    }

    @Test
    void theSubClaimAndSpaceDelimitedScopesAlsoResolve() {
        answers.put("sub-token", """
                {"active": true, "sub": "svc-4711",
                 "protomolt_scopes": "metrics-query metrics-rebuild"}""");
        Caller caller = resolver.resolve("sub-token").orElseThrow();
        assertThat(caller.name()).isEqualTo("svc-4711");
        assertThat(caller.scopes())
                .containsExactlyInAnyOrder(Scopes.METRICS_QUERY, Scopes.METRICS_REBUILD);
    }

    @Test
    void inactiveAndUnknownTokensResolveEmpty() {
        answers.put("stale-token", "{\"active\": false}");
        assertThat(resolver.resolve("stale-token")).isEmpty();
        assertThat(resolver.resolve("never-seen")).isEmpty();
    }

    @Test
    void misconfiguredGrantsRefuseInsteadOfAuthenticating() {
        answers.put("nameless", """
                {"active": true, "protomolt_scopes": ["schema-read"]}""");
        assertThat(resolver.resolve("nameless")).isEmpty();

        answers.put("scopeless", "{\"active\": true, \"username\": \"who\"}");
        assertThat(resolver.resolve("scopeless")).isEmpty();

        answers.put("misscoped", """
                {"active": true, "username": "who",
                 "protomolt_scopes": ["rule-the-world"]}""");
        assertThat(resolver.resolve("misscoped")).isEmpty();
    }

    @Test
    void anEndpointFailureIsAStoreFailureNotABadCredential() {
        assertThatThrownBy(() -> resolver.resolve("boom"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("HTTP 500");
    }

    @Test
    void environmentConstructionRequiresTheClientPair() {
        assertThat(OidcCallerResolver.fromEnvironmentMap(Map.of())).isNull();
        assertThatThrownBy(() -> OidcCallerResolver.fromEnvironmentMap(Map.of(
                OidcCallerResolver.ENV_INTROSPECTION_URL, "http://idp/introspect")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(OidcCallerResolver.ENV_CLIENT_ID)
                .hasMessageContaining(OidcCallerResolver.ENV_CLIENT_SECRET);
        assertThat(OidcCallerResolver.fromEnvironmentMap(Map.of(
                OidcCallerResolver.ENV_INTROSPECTION_URL, "http://idp/introspect",
                OidcCallerResolver.ENV_CLIENT_ID, "id",
                OidcCallerResolver.ENV_CLIENT_SECRET, "secret"))).isNotNull();
    }

    @Test
    void aChainAnswersTheFirstMatchAndPropagatesStoreFailures() {
        CallerResolver first = credential -> "one".equals(credential)
                ? Optional.of(Caller.scoped("first", Set.of(Scopes.SCHEMA_READ)))
                : Optional.empty();
        CallerResolver second = credential -> "two".equals(credential)
                ? Optional.of(Caller.scoped("second", Set.of(Scopes.SCHEMA_READ)))
                : Optional.empty();
        CallerResolver chain = CallerResolver.chain(List.of(first, second));
        assertThat(chain.resolve("one").orElseThrow().name()).isEqualTo("first");
        assertThat(chain.resolve("two").orElseThrow().name()).isEqualTo("second");
        assertThat(chain.resolve("three")).isEmpty();
        assertThatThrownBy(() -> CallerResolver.chain(List.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
