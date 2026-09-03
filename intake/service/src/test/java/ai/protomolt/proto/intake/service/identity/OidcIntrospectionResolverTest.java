package ai.protomolt.proto.intake.service.identity;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Exercises the IdP-backed key store against a fake RFC 7662 introspection
 * endpoint (plain JDK HttpServer). No live IdP is required: the contract
 * under test is what the resolver sends and how it maps the answer.
 */
class OidcIntrospectionResolverTest {

    static HttpServer idp;
    static URI endpoint;
    static OidcIntrospectionResolver resolver;

    /** token → canned JSON answer. */
    static final Map<String, String> answers = new ConcurrentHashMap<>();

    static final AtomicReference<String> lastAuthorization = new AtomicReference<>();
    static final AtomicReference<String> lastBody = new AtomicReference<>();

    @BeforeAll
    static void boot() throws IOException {
        idp = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        idp.createContext(
                "/introspect",
                exchange -> {
                    lastAuthorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
                    String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
                    lastBody.set(body);
                    String token =
                            URLDecoder.decode(body.replaceFirst("^token=", ""), StandardCharsets.UTF_8);
                    String answer = answers.get(token);
                    byte[] payload;
                    int status;
                    if ("boom".equals(token)) {
                        status = 500;
                        payload = "{}".getBytes(StandardCharsets.UTF_8);
                    } else {
                        status = 200;
                        payload =
                                (answer == null ? "{\"active\": false}" : answer)
                                        .getBytes(StandardCharsets.UTF_8);
                    }
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(status, payload.length);
                    try (OutputStream out = exchange.getResponseBody()) {
                        out.write(payload);
                    }
                });
        idp.start();
        endpoint = URI.create("http://127.0.0.1" + ":" + idp.getAddress().getPort() + "/introspect");
        resolver = new OidcIntrospectionResolver(endpoint, "intake-resolver", "resolver-secret");
    }

    @AfterAll
    static void shutdown() {
        idp.stop(0);
    }

    @Test
    void activeTokenMapsClaimsToTheScope() {
        answers.put(
                "good-key",
                """
                {"active": true, "account_id": "acct-oidc",
                 "datasource_ids": ["ds-1", "ds-2"],
                 "drives": "intake pipeline",
                 "mime_types": ["text/plain"],
                 "max_payload_bytes": 4096}
                """);
        IntakeScope scope = resolver.resolve("good-key").orElseThrow();
        assertThat(scope.accountId()).isEqualTo("acct-oidc");
        assertThat(scope.datasourceIds()).containsExactlyInAnyOrder("ds-1", "ds-2");
        // Space-delimited string form parses too (IdPs often flatten lists).
        assertThat(scope.drives()).containsExactlyInAnyOrder("intake", "pipeline");
        assertThat(scope.mimeTypes()).containsExactly("text/plain");
        assertThat(scope.maxPayloadBytes()).isEqualTo(4096L);

        // The resolver authenticated with basic client credentials and
        // form-encoded the token per RFC 7662.
        assertThat(lastAuthorization.get())
                .isEqualTo(
                        "Basic "
                                + Base64.getEncoder()
                                        .encodeToString(
                                                "intake-resolver:resolver-secret"
                                                        .getBytes(StandardCharsets.UTF_8)));
        assertThat(lastBody.get()).isEqualTo("token=good-key");
    }

    @Test
    void minimalActiveTokenIsUnrestrictedWithinItsAccount() {
        answers.put("minimal", "{\"active\": true, \"account_id\": \"acct-min\"}");
        IntakeScope scope = resolver.resolve("minimal").orElseThrow();
        assertThat(scope.accountId()).isEqualTo("acct-min");
        assertThat(scope.datasourceIds()).isEmpty();
        assertThat(scope.drives()).isEmpty();
        assertThat(scope.mimeTypes()).isEmpty();
        assertThat(scope.maxPayloadBytes()).isZero();
    }

    @Test
    void inactiveAndUnknownTokensResolveEmpty() {
        answers.put("revoked", "{\"active\": false, \"account_id\": \"acct-x\"}");
        assertThat(resolver.resolve("revoked")).isEmpty();
        assertThat(resolver.resolve("never-issued")).isEmpty();
    }

    @Test
    void activeTokenWithoutAccountIdIsRefused() {
        answers.put("ownerless", "{\"active\": true, \"datasource_ids\": [\"ds-1\"]}");
        assertThat(resolver.resolve("ownerless")).isEmpty();
    }

    @Test
    void endpointFailureThrowsInsteadOfDenying() {
        // A failing key store must surface as INTERNAL upstream, never as a
        // silent UNAUTHENTICATED that looks like a bad key.
        assertThatThrownBy(() -> resolver.resolve("boom"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("500");
    }

    @Test
    void constructorRejectsBlankClientCredentials() {
        assertThatThrownBy(() -> new OidcIntrospectionResolver(endpoint, " ", "secret"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new OidcIntrospectionResolver(endpoint, "id", ""))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
