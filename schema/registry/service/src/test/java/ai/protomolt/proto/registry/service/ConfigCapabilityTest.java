package ai.protomolt.proto.registry.service;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.registry.ConfigDocumentStore;
import ai.protomolt.proto.registry.InMemorySchemaRegistryStore;
import ai.protomolt.proto.registry.RegistryStoreException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConfigCapabilityTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String ENVELOPE = """
            {"messageType":"test.Config","config":{"limit":5}}
            """;

    @Test
    void nonGitCapabilityServesTypedConfigsThroughHttpAndActions() throws Exception {
        try (var schemas = schemas(); var client = HttpClient.newHttpClient()) {
            ConfigDocumentStore store = capability(schemas, null);
            try (var server = new SchemaRegistryServer(
                    SchemaRegistryServerConfig.defaults().withHost("127.0.0.1").withPort(0), store)) {
                String base = "http://127.0.0.1:" + server.start() + "/protomolt/configs";
                var put = client.send(HttpRequest.newBuilder(URI.create(base + "/limits"))
                        .PUT(HttpRequest.BodyPublishers.ofString(ENVELOPE)).build(),
                        HttpResponse.BodyHandlers.ofString());
                assertThat(put.statusCode()).isEqualTo(200);
                var get = client.send(HttpRequest.newBuilder(URI.create(base + "/limits")).build(),
                        HttpResponse.BodyHandlers.ofString());
                assertThat(get.statusCode()).isEqualTo(200);
                assertThat(JSON.readTree(get.body()).path("config").path("limit").asInt()).isEqualTo(5);
                assertThat(JSON.readTree(get.body()).path("version").asText()).isEqualTo("revision-1");
                var list = client.send(HttpRequest.newBuilder(URI.create(base)).build(),
                        HttpResponse.BodyHandlers.ofString());
                assertThat(list.statusCode()).isEqualTo(200);
                assertThat(JSON.readTree(list.body())).isEqualTo(JSON.readTree("[\"limits\"]"));
                var result = catalog(store).execute("publish-config", input());
                assertThat(result.path("version").asText()).isEqualTo("revision-2");
            }
        }
    }

    @Test
    void publicationFailureRetainsTheBackendCause() throws Exception {
        var failure = new RegistryStoreException("storage unavailable");
        try (var schemas = schemas()) {
            assertThatThrownBy(() -> catalog(capability(schemas, failure)).execute("publish-config", input()))
                    .isInstanceOfSatisfying(ActionException.class,
                            e -> assertThat(e.code()).isEqualTo("store-error"))
                    .hasCause(failure);
        }
    }

    private static ObjectNode input() throws Exception {
        return ((ObjectNode) JSON.readTree(ENVELOPE)).put("name", "limits");
    }

    private static ActionCatalog catalog(ConfigDocumentStore store) {
        return ActionCatalog.empty(ActionContext.create()).register(new PublishConfigAction(store));
    }

    private static InMemorySchemaRegistryStore schemas() {
        var schemas = new InMemorySchemaRegistryStore();
        schemas.register("config.proto", "syntax = \"proto3\"; package test; message Config { int32 limit = 1; }", List.of());
        return schemas;
    }

    private static ConfigDocumentStore capability(InMemorySchemaRegistryStore schemas, RegistryStoreException failure) {
        var documents = new TreeMap<String, String>();
        var versions = new TreeMap<String, String>();
        return (ConfigDocumentStore) Proxy.newProxyInstance(ConfigCapabilityTest.class.getClassLoader(),
                new Class<?>[] {ConfigDocumentStore.class}, (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "putConfig":
                            if (failure != null) throw failure;
                            String name = (String) args[0];
                            documents.put(name, (String) args[1]);
                            String version = "revision-" + (versions.containsKey(name) ? 2 : 1);
                            versions.put(name, version);
                            return version;
                        case "config": return Optional.ofNullable(documents.get(args[0]));
                        case "configVersion": return Optional.ofNullable(versions.get(args[0]));
                        case "configs": return List.copyOf(documents.keySet());
                        default:
                            try { return method.invoke(schemas, args); }
                            catch (InvocationTargetException e) { throw e.getCause(); }
                    }
                });
    }
}
