package ai.pipestream.proto.registry.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.Caller;
import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.authz.CallerResolver;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The registry's route table under an access policy: reads require schema-read (the
 * POST-shaped content lookup included), every mutation requires schema-write, refusals name
 * the caller and the scope, the action endpoint serves each caller its filtered catalog and
 * dispatches under the action's own declaration, and the operator token keeps everything.
 */
class ScopedRegistryTest {

    private static final String TOKEN = "reg-operator";
    private static final String READER = "reg-reader";
    private static final String WRITER = "reg-writer";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static SchemaRegistryServer server;
    private static HttpClient http;
    private static String base;

    private static final CallerResolver RESOLVER = credential -> switch (credential) {
        case READER -> Optional.of(Caller.scoped("ci-reader", Set.of(Scopes.SCHEMA_READ)));
        case WRITER -> Optional.of(Caller.scoped("publisher", Set.of(Scopes.SCHEMA_WRITE)));
        default -> Optional.empty();
    };

    @BeforeAll
    static void start() {
        server = new SchemaRegistryServer(
                SchemaRegistryServerConfig.defaults()
                        .withHost("127.0.0.1")
                        .withPort(0)
                        .withApiToken(TOKEN),
                new EmptyStore(),
                ActionCatalog.defaults(ActionContext.create()),
                RESOLVER);
        base = "http://127.0.0.1:" + server.start();
        http = HttpClient.newHttpClient();
    }

    @AfterAll
    static void stop() {
        server.close();
        http.close();
    }

    private static HttpResponse<String> send(String method, String path, String body,
                                             String credential) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(base + path))
                .method(method, body == null
                        ? HttpRequest.BodyPublishers.noBody()
                        : HttpRequest.BodyPublishers.ofString(body))
                .header("content-type", "application/json");
        if (credential != null) {
            request.header("api_token", credential);
        }
        return http.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void aReaderReadsAndIsRefusedWritesByName() throws Exception {
        assertThat(send("GET", "/subjects", null, READER).statusCode()).isEqualTo(200);

        HttpResponse<String> denied = send("POST", "/subjects/orders/versions",
                "{\"schema\":\"syntax = \\\"proto3\\\";\"}", READER);
        assertThat(denied.statusCode()).isEqualTo(403);
        assertThat(denied.body()).contains("ci-reader").contains(Scopes.SCHEMA_WRITE);

        assertThat(send("PUT", "/config", "{\"compatibility\":\"BACKWARD\"}", READER)
                .statusCode()).isEqualTo(403);
    }

    @Test
    void aWriterWritesAndIsRefusedReadsByName() throws Exception {
        assertThat(send("PUT", "/config", "{\"compatibility\":\"BACKWARD\"}", WRITER)
                .statusCode()).isEqualTo(200);

        HttpResponse<String> denied = send("GET", "/subjects", null, WRITER);
        assertThat(denied.statusCode()).isEqualTo(403);
        assertThat(denied.body()).contains("publisher").contains(Scopes.SCHEMA_READ);
    }

    @Test
    void theContentLookupIsAReadDespiteItsVerb() throws Exception {
        HttpResponse<String> lookup = send("POST", "/subjects/orders",
                "{\"schema\":\"syntax = \\\"proto3\\\";\"}", READER);
        assertThat(lookup.statusCode()).isNotEqualTo(403);
        assertThat(lookup.statusCode()).isNotEqualTo(401);
    }

    @Test
    void theActionEndpointServesEachCallerItsOwnCatalog() throws Exception {
        HttpResponse<String> readerList = send("GET", "/protomolt/actions", null, READER);
        assertThat(MAPPER.readTree(readerList.body())).hasSize(17);

        HttpResponse<String> writerList = send("GET", "/protomolt/actions", null, WRITER);
        assertThat(MAPPER.readTree(writerList.body())).isEmpty();

        HttpResponse<String> denied = send("POST", "/protomolt/actions/list-types",
                "{}", WRITER);
        assertThat(denied.statusCode()).isEqualTo(403);
        assertThat(denied.body()).contains("permission-denied").contains("publisher");

        assertThat(send("POST", "/protomolt/actions/list-types", "{}", READER)
                .statusCode()).isNotEqualTo(403);
    }

    @Test
    void theOperatorTokenKeepsEverything() throws Exception {
        assertThat(send("GET", "/subjects", null, TOKEN).statusCode()).isEqualTo(200);
        assertThat(send("PUT", "/config", "{\"compatibility\":\"BACKWARD\"}", TOKEN)
                .statusCode()).isEqualTo(200);
    }

    @Test
    void anUnknownCredentialStaysUnauthenticated() throws Exception {
        assertThat(send("GET", "/subjects", null, "guessed").statusCode()).isEqualTo(401);
    }

    @Test
    void aResolverWithoutTheOperatorTokenRefusesAtConstruction() {
        assertThatThrownBy(() -> new SchemaRegistryServer(
                SchemaRegistryServerConfig.defaults().withHost("127.0.0.1").withPort(0),
                new EmptyStore(),
                ActionCatalog.defaults(ActionContext.create()),
                RESOLVER))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("operator api token");
    }
}
