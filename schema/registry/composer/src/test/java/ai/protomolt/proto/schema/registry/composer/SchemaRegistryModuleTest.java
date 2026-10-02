package ai.protomolt.proto.schema.registry.composer;

import ai.protomolt.proto.composer.Composer;
import ai.protomolt.proto.registry.InMemorySchemaRegistryStore;
import ai.protomolt.proto.registry.SchemaRegistryStore;
import ai.protomolt.proto.registry.SchemaRegistryStoreProvider;
import ai.protomolt.proto.registry.SchemaRegistryStores;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SchemaRegistryModuleTest {
    @Test
    void spiDiscoveryDoesNotRequireRegistryConfiguration() {
        assertThat(Composer.builder().build().knownRoles()).contains("registry");
        try (var ignored = Composer.builder().environment(Map.of()).build().boot(List.of())) {
            // An unselected role must not parse configuration or open storage.
        }
    }

    @Test
    void servesAuthenticatedMemoryRegistryAndReleasesThePort() throws Exception {
        int port;
        try (var socket = new ServerSocket(0)) { port = socket.getLocalPort(); }
        var env = environment(port);
        env.put("PROTOMOLT_REGISTRY_AUTH", "token");
        env.put("PROTOMOLT_REGISTRY_TOKEN", "test-token");
        try (var client = HttpClient.newHttpClient();
             var node = Composer.builder().module(probeModule()).environment(env).build().boot(List.of("registry", "probe"))) {
            var store = node.context().contributions().all(SchemaRegistryStore.class).getFirst();
            store.register("test.proto", "syntax = \"proto3\"; message Test {}", List.of());
            URI uri = URI.create("http://127.0.0.1:" + port + "/subjects");
            assertThat(client.send(HttpRequest.newBuilder(uri).build(), HttpResponse.BodyHandlers.ofString())
                    .statusCode()).isEqualTo(401);
            var response = client.send(HttpRequest.newBuilder(uri).header("Authorization", "Bearer test-token").build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(response.body()).isEqualTo("[\"test.proto\"]");
            var action = client.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/protomolt/actions/probe"))
                    .header("Authorization", "Bearer test-token")
                    .POST(HttpRequest.BodyPublishers.ofString("{}")).build(), HttpResponse.BodyHandlers.ofString());
            assertThat(action.statusCode()).isEqualTo(200);
            assertThat(action.body()).contains("contributed");
        }
        try (var rebound = new ServerSocket(port)) { assertThat(rebound.isBound()).isTrue(); }
    }

    @Test
    void invalidConfigurationDoesNotOpenStorage() {
        var opened = new AtomicInteger();
        var closed = new AtomicInteger();
        var env = environment(0);
        env.put("PROTOMOLT_REGISTRY_STORE", "test");
        env.put("PROTOMOLT_REGISTRY_AUTH", "typo");
        var module = new SchemaRegistryModule(() -> providers(opened, closed));
        assertThatThrownBy(() -> Composer.emptyBuilder().module(module).environment(env).build().boot(List.of("registry")))
                .hasMessageContaining("AUTH");
        assertThat(opened).hasValue(0);
        env.put("PROTOMOLT_REGISTRY_AUTH", "none");
        env.put("PROTOMOLT_REGISTRY_TYPO", "unexpected");
        assertThatThrownBy(() -> Composer.emptyBuilder().module(module).environment(env).build().boot(List.of("registry")))
                .hasMessageContaining("Unknown registry setting");
        assertThat(opened).hasValue(0);
    }

    @Test
    void bindFailureClosesTheOpenedStore() throws Exception {
        var opened = new AtomicInteger();
        var closed = new AtomicInteger();
        try (var occupied = new ServerSocket(0)) {
            var env = environment(occupied.getLocalPort());
            env.put("PROTOMOLT_REGISTRY_STORE", "test");
            var module = new SchemaRegistryModule(() -> providers(opened, closed));
            assertThatThrownBy(() -> Composer.emptyBuilder().module(module).environment(env).build().boot(List.of("registry")))
                    .hasMessageContaining("Failed to start schema registry server");
        }
        assertThat(opened).hasValue(1);
        assertThat(closed).hasValue(1);
    }

    private static ai.protomolt.proto.composer.ServiceModule probeModule() {
        return new ai.protomolt.proto.composer.ServiceModule() {
            public String role() { return "probe"; }
            public ai.protomolt.proto.composer.ServiceMount wire(ai.protomolt.proto.composer.NodeContext context) {
                context.contributions().contribute(ai.protomolt.proto.actions.ProtoAction.class,
                        new ai.protomolt.proto.actions.ProtoAction() {
                            public String name() { return "probe"; }
                            public String description() { return "Verify contributed actions"; }
                            public com.google.protobuf.Descriptors.Descriptor requestType() { return com.google.protobuf.Struct.getDescriptor(); }
                            public com.google.protobuf.Descriptors.Descriptor responseType() { return com.google.protobuf.Struct.getDescriptor(); }
                            public com.google.protobuf.Message execute(com.google.protobuf.Message request,
                                    ai.protomolt.proto.actions.ActionContext context) {
                                return com.google.protobuf.Struct.newBuilder().putFields("result",
                                        com.google.protobuf.Value.newBuilder().setStringValue("contributed").build()).build();
                            }
                        });
                return ai.protomolt.proto.composer.ServiceMount.inert(() -> {});
            }
        };
    }

    private static Map<String, String> environment(int port) {
        return new HashMap<>(Map.of("PROTOMOLT_REGISTRY_STORE", "memory", "PROTOMOLT_REGISTRY_HOST", "127.0.0.1",
                "PROTOMOLT_REGISTRY_PORT", Integer.toString(port), "PROTOMOLT_REGISTRY_AUTH", "none",
                "PROTOMOLT_REGISTRY_COMPATIBILITY", "wire"));
    }

    private static SchemaRegistryStores providers(AtomicInteger opened, AtomicInteger closed) {
        return SchemaRegistryStores.of(List.of(new SchemaRegistryStoreProvider() {
            public String id() { return "test"; }
            public SchemaRegistryStore open(Map<String, String> options, SchemaRegistryStore.WriteGate gate) {
                opened.incrementAndGet();
                var store = new InMemorySchemaRegistryStore(gate);
                return (SchemaRegistryStore) Proxy.newProxyInstance(getClass().getClassLoader(),
                        new Class<?>[] {SchemaRegistryStore.class}, (proxy, method, args) -> {
                            if (method.getName().equals("close")) closed.incrementAndGet();
                            try { return method.invoke(store, args); }
                            catch (InvocationTargetException e) { throw e.getCause(); }
                        });
            }
        }));
    }
}
