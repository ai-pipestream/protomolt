package ai.pipestream.proto.platform;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import ai.pipestream.proto.authz.AccessPolicyCallers;
import ai.pipestream.proto.metric.DescribeMappingRequest;
import ai.pipestream.proto.metric.MetricServiceGrpc;
import ai.pipestream.proto.search.service.RepoDocumentMapping;
import ai.pipestream.proto.search.v1.SearchLane;
import ai.pipestream.proto.search.v1.SearchRequest;
import ai.pipestream.proto.search.v1.SearchServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The access policy wired through the platform's roles: with the operator
 * token set, every network surface the node serves demands a credential,
 * a policy principal holds exactly its scopes with the named refusal on
 * everything else, the boot publish and the config lane authenticate as
 * the node itself, and a policy document arriving on the lane re-scopes
 * the running node without a restart.
 */
class PlatformAccessPolicyTest {

    private static final String OPERATOR = "operator-secret";
    private static final String QUERIER = "querier-credential";
    private static final String REBUILDER = "rebuilder-credential";
    private static final String LANE_READER = "lane-reader-credential";

    @TempDir
    Path work;

    private DocumentPlatformConfig config(Map<String, String> extra) {
        Map<String, String> environment = new HashMap<>();
        environment.put("PROTOMOLT_REPO_TARGET", "127.0.0.1:1");
        environment.put(DocumentPlatformConfig.ENV_API_TOKEN, OPERATOR);
        environment.putAll(extra);
        return new DocumentPlatformConfig(
                null, null, work.resolve("registry-git"), 0, 0, 0, 0,
                null, null, null,
                60L, 1, 0, work.resolve("search-index"), 0, 0,
                List.of("registry", "search", "metric"), environment);
    }

    private Path policyFile() throws Exception {
        Path policy = work.resolve("access-policy.json");
        Files.writeString(policy, """
                {"principals": [
                  {"name": "querier",
                   "credentialSha256": ["%s"],
                   "scopes": ["search-query", "metrics-query", "schema-read"]},
                  {"name": "rebuilder",
                   "credentialSha256": ["%s"],
                   "scopes": ["metrics-rebuild"]}
                ]}""".formatted(
                AccessPolicyCallers.sha256Hex(QUERIER),
                AccessPolicyCallers.sha256Hex(REBUILDER)));
        return policy;
    }

    private static <T> T withChannel(int port, String credential,
            Function<ManagedChannel, T> body) {
        NettyChannelBuilder builder = NettyChannelBuilder
                .forAddress("127.0.0.1", port)
                .usePlaintext();
        if (credential != null) {
            Metadata metadata = new Metadata();
            metadata.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER),
                    credential);
            builder.intercept(MetadataUtils.newAttachHeadersInterceptor(metadata));
        }
        ManagedChannel channel = builder.build();
        try {
            return body.apply(channel);
        } finally {
            channel.shutdownNow();
        }
    }

    private static SearchRequest anyQuery() {
        return SearchRequest.newBuilder()
                .setMappingSubject(RepoDocumentMapping.SUBJECT)
                .setQuery("anything")
                .setK(1)
                .setLane(SearchLane.SEARCH_LANE_LEXICAL)
                .build();
    }

    private static HttpResponse<String> registryGet(int port, String path,
            String credential) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(
                        URI.create("http://127.0.0.1:" + port + path))
                .GET();
        if (credential != null) {
            request.header("api_token", credential);
        }
        return HttpClient.newHttpClient()
                .send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    private static HttpResponse<String> putConfig(int port, String subject,
            String credential, String envelope) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(
                        URI.create("http://127.0.0.1:" + port + "/protomolt/configs/"
                                + subject))
                .header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString(envelope));
        if (credential != null) {
            request.header("api_token", credential);
        }
        return HttpClient.newHttpClient()
                .send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void anAccessPolicyWithoutTheOperatorTokenRefusesAtBoot() throws Exception {
        Map<String, String> environment = new HashMap<>();
        environment.put("PROTOMOLT_REPO_TARGET", "127.0.0.1:1");
        environment.put(DocumentPlatformConfig.ENV_ACCESS_POLICY,
                policyFile().toString());
        DocumentPlatformConfig config = new DocumentPlatformConfig(
                null, null, work.resolve("registry-git"), 0, 0, 0, 0,
                null, null, null,
                60L, 1, 0, work.resolve("search-index"), 0, 0,
                List.of("registry", "search", "metric"), environment);
        assertThatThrownBy(() -> DocumentPlatform.start(config, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(DocumentPlatformConfig.ENV_ACCESS_POLICY)
                .hasMessageContaining(DocumentPlatformConfig.ENV_API_TOKEN);
    }

    @Test
    void aGuardedNodeServesTheConsoleThroughPrincipalSessions() throws Exception {
        Map<String, String> environment = new HashMap<>();
        environment.put("PROTOMOLT_REPO_TARGET", "127.0.0.1:1");
        environment.put(DocumentPlatformConfig.ENV_API_TOKEN, OPERATOR);
        environment.put(DocumentPlatformConfig.ENV_ACCESS_POLICY,
                policyFile().toString());
        DocumentPlatformConfig config = new DocumentPlatformConfig(
                null, null, work.resolve("registry-git"), 0, 0, 0, 0,
                null, null, null,
                60L, 1, 0, work.resolve("search-index"), 0, 0,
                List.of("registry", "search", "metric", "search-console"),
                environment);
        try (DocumentPlatform platform = DocumentPlatform.start(config, null)) {
            int consolePort = platform.searchConsolePort();
            HttpClient http = HttpClient.newHttpClient();

            // The page serves, the bridges demand a session.
            assertThat(consoleGet(http, consolePort, "/", null).statusCode())
                    .isEqualTo(200);
            assertThat(consoleGet(http, consolePort, "/subjects", null).statusCode())
                    .isEqualTo(401);

            // The operator token is not a browser login; sessions bind to
            // access-policy principals only.
            assertThat(consoleLoginStatus(http, consolePort, OPERATOR)).isEqualTo(401);
            assertThat(consoleLoginStatus(http, consolePort, "guessed")).isEqualTo(401);

            // A querier signs in and searches through the console's bridge.
            String querier = consoleLogin(http, consolePort, QUERIER);
            assertThat(consoleGet(http, consolePort, "/subjects", querier).statusCode())
                    .isEqualTo(200);
            HttpResponse<String> hits = consolePost(http, consolePort, "/search", """
                    {"mappingSubject": "%s", "query": "anything", "k": 1,
                     "lane": "SEARCH_LANE_LEXICAL"}"""
                    .formatted(RepoDocumentMapping.SUBJECT), querier);
            assertThat(hits.statusCode()).isEqualTo(200);

            // The operations proxy presents the session's own credential, so
            // the guarded registry answers the principal, not the console.
            assertThat(consoleGet(http, consolePort, "/actions", querier).statusCode())
                    .isEqualTo(200);

            // A principal without search-query is refused by name at the console.
            String rebuilder = consoleLogin(http, consolePort, REBUILDER);
            HttpResponse<String> refused =
                    consoleGet(http, consolePort, "/subjects", rebuilder);
            assertThat(refused.statusCode()).isEqualTo(403);
            assertThat(refused.body()).contains("rebuilder").contains("search-query");
        }
    }

    private static HttpResponse<String> consoleGet(HttpClient http, int port,
            String path, String cookie) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(
                URI.create("http://127.0.0.1:" + port + path)).GET();
        if (cookie != null) {
            request.header("Cookie", cookie);
        }
        return http.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    private static HttpResponse<String> consolePost(HttpClient http, int port,
            String path, String body, String cookie) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(
                        URI.create("http://127.0.0.1:" + port + path))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body));
        if (cookie != null) {
            request.header("Cookie", cookie);
        }
        return http.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    private static int consoleLoginStatus(HttpClient http, int port, String credential)
            throws Exception {
        return http.send(HttpRequest.newBuilder(
                        URI.create("http://127.0.0.1:" + port + "/session"))
                        .POST(HttpRequest.BodyPublishers.ofString(credential)).build(),
                HttpResponse.BodyHandlers.ofString()).statusCode();
    }

    private static String consoleLogin(HttpClient http, int port, String credential)
            throws Exception {
        HttpResponse<String> login = http.send(HttpRequest.newBuilder(
                        URI.create("http://127.0.0.1:" + port + "/session"))
                        .POST(HttpRequest.BodyPublishers.ofString(credential)).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(login.statusCode()).isEqualTo(200);
        String setCookie = login.headers().firstValue("set-cookie").orElseThrow();
        return setCookie.substring(0, setCookie.indexOf(';'));
    }

    @Test
    void theBootPolicyScopesEveryServingRole() throws Exception {
        Map<String, String> extra = Map.of(
                DocumentPlatformConfig.ENV_ACCESS_POLICY, policyFile().toString());
        try (DocumentPlatform platform = DocumentPlatform.start(config(extra), null)) {
            int searchPort = platform.searchPort();
            int metricsPort = platform.metricsPort();
            int registryPort = platform.registryPort();

            // No credential: unauthenticated, on gRPC and HTTP alike.
            StatusRuntimeException anonymous = withChannel(searchPort, null,
                    channel -> catchThrowableOfType(StatusRuntimeException.class,
                            () -> SearchServiceGrpc.newBlockingStub(channel)
                                    .search(anyQuery())));
            assertThat(anonymous.getStatus().getCode())
                    .isEqualTo(Code.UNAUTHENTICATED);
            assertThat(registryGet(registryPort, "/subjects", null)
                    .statusCode()).isEqualTo(401);

            // The querier holds search-query and metrics-query: both serve.
            assertThat(withChannel(searchPort, QUERIER,
                    channel -> SearchServiceGrpc.newBlockingStub(channel)
                            .search(anyQuery()))
                    .getHitsList()).isEmpty();
            assertThat(withChannel(metricsPort, QUERIER,
                    channel -> MetricServiceGrpc.newBlockingStub(channel)
                            .describeMapping(DescribeMappingRequest.newBuilder()
                                    .setMappingSubject(RepoDocumentMapping.SUBJECT)
                                    .build()))
                    .getMembersCount()).isPositive();

            // The rebuilder holds metrics-rebuild alone: a query is refused
            // by principal, scope, and method name.
            StatusRuntimeException denied = withChannel(searchPort, REBUILDER,
                    channel -> catchThrowableOfType(StatusRuntimeException.class,
                            () -> SearchServiceGrpc.newBlockingStub(channel)
                                    .search(anyQuery())));
            assertThat(denied.getStatus().getCode()).isEqualTo(Code.PERMISSION_DENIED);
            assertThat(denied.getStatus().getDescription())
                    .contains("rebuilder")
                    .contains("search-query");

            // An unknown credential is not a principal.
            StatusRuntimeException unknown = withChannel(searchPort, "who-is-this",
                    channel -> catchThrowableOfType(StatusRuntimeException.class,
                            () -> SearchServiceGrpc.newBlockingStub(channel)
                                    .search(anyQuery())));
            assertThat(unknown.getStatus().getCode()).isEqualTo(Code.UNAUTHENTICATED);

            // The operator token keeps every scope.
            assertThat(withChannel(searchPort, OPERATOR,
                    channel -> SearchServiceGrpc.newBlockingStub(channel)
                            .search(anyQuery()))
                    .getHitsList()).isEmpty();

            // Registry HTTP: schema-read serves the read; the boot publish
            // (authenticated as the node) registered the platform contracts.
            HttpResponse<String> subjects =
                    registryGet(registryPort, "/subjects", QUERIER);
            assertThat(subjects.statusCode()).isEqualTo(200);
            assertThat(subjects.body())
                    .contains(DocumentPlatform.DOCUMENT_SUBJECT)
                    .contains(DocumentPlatform.ACCESS_POLICY_SUBJECT);

            // schema-read does not write: publishing config is refused.
            assertThat(putConfig(registryPort, "some-subject", QUERIER, """
                    {"messageType": "ai.pipestream.proto.authz.v1.AccessPolicy",
                     "config": {}}""").statusCode()).isEqualTo(403);
        }
    }

    @Test
    void aPolicyDocumentOnTheConfigLaneRescopesTheRunningNode() throws Exception {
        Map<String, String> extra = Map.of(
                DocumentPlatformConfig.ENV_CONFIG_REFRESH_SECONDS, "1");
        try (DocumentPlatform platform = DocumentPlatform.start(config(extra), null)) {
            int searchPort = platform.searchPort();

            // No policy mounted: only the operator token authenticates.
            StatusRuntimeException before = withChannel(searchPort, LANE_READER,
                    channel -> catchThrowableOfType(StatusRuntimeException.class,
                            () -> SearchServiceGrpc.newBlockingStub(channel)
                                    .search(anyQuery())));
            assertThat(before.getStatus().getCode()).isEqualTo(Code.UNAUTHENTICATED);

            // The operator publishes a policy on the lane; the running node
            // follows on its refresh interval, no restart.
            HttpResponse<String> accepted = putConfig(platform.registryPort(),
                    "access-policy", OPERATOR, """
                    {"messageType": "ai.pipestream.proto.authz.v1.AccessPolicy",
                     "config": {"principals": [{
                         "name": "lane-reader",
                         "credentialSha256": ["%s"],
                         "scopes": ["search-query"]}]}}"""
                    .formatted(AccessPolicyCallers.sha256Hex(LANE_READER)));
            assertThat(accepted.statusCode()).isEqualTo(200);

            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            boolean scoped = false;
            while (System.nanoTime() < deadline && !scoped) {
                try {
                    withChannel(searchPort, LANE_READER,
                            channel -> SearchServiceGrpc.newBlockingStub(channel)
                                    .search(anyQuery()));
                    scoped = true;
                } catch (StatusRuntimeException e) {
                    Thread.sleep(200);
                }
            }
            assertThat(scoped)
                    .as("the lane policy re-scoped the running node")
                    .isTrue();
        }
    }
}
