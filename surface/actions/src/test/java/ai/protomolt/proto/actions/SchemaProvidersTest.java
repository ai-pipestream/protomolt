package ai.protomolt.proto.actions;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Struct;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Optional;
import java.util.ServiceConfigurationError;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SchemaProvidersTest {
    @TempDir Path temporary;

    @Test
    void schemaResolutionRequiresAnInstalledProvider() throws Exception {
        withProviders(SchemaResolverProvider.class, null, () ->
                assertThatThrownBy(SchemaProvidersTest::resolve)
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("exactly one SchemaResolverProvider; found 0"));
    }

    @Test
    void ambiguousSchemaProvidersFailBeforeEitherRuns() throws Exception {
        withProviders(SchemaResolverProvider.class, services(FirstResolver.class, SecondResolver.class), () ->
                assertThatThrownBy(SchemaProvidersTest::resolve)
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("found 2"));
    }

    @Test
    void brokenSchemaProviderIsNotReplacedByAnotherImplementation() throws Exception {
        Path broken = temporary.resolve("broken");
        Files.writeString(broken, "missing.SchemaProvider");
        withProviders(SchemaResolverProvider.class, broken.toUri().toURL(), () ->
                assertThatThrownBy(SchemaProvidersTest::resolve)
                        .isInstanceOf(ServiceConfigurationError.class));
    }

    @Test
    void namedContractsRequireExactlyOneDefinition() throws Exception {
        withProviders(ActionContractProvider.class, null, () ->
                assertThatThrownBy(() -> CatalogContract.request("fixture"))
                        .isInstanceOf(IllegalStateException.class).hasMessageContaining("found 0"));
        withProviders(ActionContractProvider.class, services(FirstContract.class), () ->
                assertThat(CatalogContract.request("fixture")).isSameAs(Struct.getDescriptor()));
        withProviders(ActionContractProvider.class, services(FirstContract.class, SecondContract.class), () ->
                assertThatThrownBy(() -> CatalogContract.request("fixture"))
                        .isInstanceOf(IllegalStateException.class).hasMessageContaining("found 2"));
    }

    @Test
    void providerFailureKeepsItsOriginalException() throws Exception {
        withProviders(SchemaResolverProvider.class, services(FirstResolver.class), () ->
                assertThatThrownBy(SchemaProvidersTest::resolve).isSameAs(FAILURE));
    }

    private static final IllegalStateException FAILURE = new IllegalStateException("provider failed");

    public static class FirstResolver implements SchemaResolverProvider {
        @Override public SchemaResolver.ResolvedSchema resolve(ObjectNode input, String field,
                ActionContext context) { throw FAILURE; }
        @Override public SchemaResolver.ResolvedSchema resolveSource(Message source, String pointer,
                ActionContext context) { throw FAILURE; }
    }

    public static final class SecondResolver extends FirstResolver {}

    public static class FirstContract implements ActionContractProvider {
        @Override public Optional<Descriptor> find(String name) {
            return name.equals("fixture") ? Optional.of(Struct.getDescriptor()) : Optional.empty();
        }
    }

    public static final class SecondContract extends FirstContract {}

    private URL services(Class<?>... providers) throws Exception {
        Path file = Files.createTempFile(temporary, "providers", ".txt");
        Files.writeString(file, String.join("\n", java.util.Arrays.stream(providers)
                .map(Class::getName).toList()));
        return file.toUri().toURL();
    }

    private static void resolve() throws ActionException {
        SchemaResolver.resolve(JsonNodeFactory.instance.objectNode(), "schema", ActionContext.create());
    }

    private static void withProviders(Class<?> spi, URL resource, Checked action) throws Exception {
        String service = "META-INF/services/" + spi.getName();
        Thread thread = Thread.currentThread();
        ClassLoader previous = thread.getContextClassLoader();
        try {
            thread.setContextClassLoader(new ClassLoader(previous) {
                @Override public Enumeration<URL> getResources(String name) throws java.io.IOException {
                    return name.equals(service)
                            ? Collections.enumeration(resource == null ? List.of() : List.of(resource))
                            : super.getResources(name);
                }
            });
            action.run();
        } finally {
            thread.setContextClassLoader(previous);
        }
    }

    @FunctionalInterface private interface Checked { void run() throws Exception; }
}
