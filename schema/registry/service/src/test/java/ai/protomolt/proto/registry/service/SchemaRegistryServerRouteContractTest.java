package ai.protomolt.proto.registry.service;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.descriptors.StabilityTier;
import ai.protomolt.proto.registry.InMemorySchemaRegistryStore;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SchemaRegistryServerRouteContractTest {

    @Test
    void routeContractsReflectDefaultAndCustomServerConfiguration() {
        List<SchemaRegistryServer.RouteContract> withoutActions =
                SchemaRegistryServer.routeContracts(SchemaRegistryServerConfig.defaults(), false);
        List<SchemaRegistryServer.RouteContract> withActions =
                SchemaRegistryServer.routeContracts(SchemaRegistryServerConfig.defaults(), true);

        assertThat(withoutActions).hasSize(14);
        assertThat(withActions).hasSize(16);
        assertThat(withActions)
                .extracting(SchemaRegistryServer.RouteContract::pathTemplate)
                .containsExactly(
                        "/health",
                        "/subjects",
                        "/subjects/{subject}",
                        "/subjects/{subject}/versions",
                        "/subjects/{subject}/versions/{version}",
                        "/schemas/ids/{id}",
                        "/config",
                        "/config/{subject}",
                        "/protomolt/subjects/{subject}/descriptor-set",
                        "/protomolt/subjects/{subject}/parquet-schema",
                        "/protomolt/configs",
                        "/protomolt/configs/{name}",
                        "/protomolt/workflows",
                        "/protomolt/workflows/{name}",
                        "/protomolt/actions",
                        "/protomolt/actions/{name}");
        assertThat(withActions)
                .allMatch(r -> r.stability() == StabilityTier.STABLE);

        SchemaRegistryServerConfig custom = new SchemaRegistryServerConfig(
                "127.0.0.1",
                0,
                "/readyz",
                "/v2-native",
                SchemaRegistryServerConfig.DEFAULT_MAX_REQUEST_BYTES,
                "secret-token");
        List<SchemaRegistryServer.RouteContract> customContracts =
                SchemaRegistryServer.routeContracts(custom, true);
        assertThat(customContracts.get(0).pathTemplate()).isEqualTo("/readyz");
        assertThat(customContracts.get(0).authRequired()).isFalse();
        assertThat(customContracts)
                .filteredOn(r -> !r.routeId().equals(SchemaRegistryServer.ROUTE_HEALTH))
                .allMatch(SchemaRegistryServer.RouteContract::authRequired);
        assertThat(customContracts)
                .extracting(SchemaRegistryServer.RouteContract::pathTemplate)
                .contains("/v2-native/subjects/{subject}/descriptor-set", "/v2-native/actions/{name}");
    }

    @Test
    void liveDispatchHonorsDeclaredRouteContractsAndMethodAllowlists() throws Exception {
        try (InMemorySchemaRegistryStore store = new InMemorySchemaRegistryStore();
             SchemaRegistryServer server = new SchemaRegistryServer(
                     SchemaRegistryServerConfig.defaults().withPort(0),
                     store,
                     ActionCatalog.defaults(ActionContext.create()))) {
            assertThat(server.routeContracts())
                    .isEqualTo(SchemaRegistryServer.routeContracts(
                            SchemaRegistryServerConfig.defaults().withPort(0), true));

            int port = server.start();
            HttpClient client = HttpClient.newHttpClient();
            String base = "http://127.0.0.1:" + port;

            HttpResponse<String> healthGet = client.send(
                    HttpRequest.newBuilder(URI.create(base + "/health")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(healthGet.statusCode()).isEqualTo(200);

            HttpResponse<String> healthPost = client.send(
                    HttpRequest.newBuilder(URI.create(base + "/health"))
                            .POST(HttpRequest.BodyPublishers.noBody())
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(healthPost.statusCode()).isEqualTo(405);
            assertThat(healthPost.headers().firstValue("Allow")).contains("GET");

            HttpResponse<String> subjectsPost = client.send(
                    HttpRequest.newBuilder(URI.create(base + "/subjects"))
                            .POST(HttpRequest.BodyPublishers.noBody())
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(subjectsPost.statusCode()).isEqualTo(405);
            assertThat(subjectsPost.headers().firstValue("Allow")).contains("GET");

            HttpResponse<String> actionsGet = client.send(
                    HttpRequest.newBuilder(URI.create(base + "/protomolt/actions")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(actionsGet.statusCode()).isEqualTo(200);
        }
    }
}
