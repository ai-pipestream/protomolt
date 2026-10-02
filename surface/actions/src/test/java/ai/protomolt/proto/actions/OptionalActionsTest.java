package ai.protomolt.proto.actions;

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
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OptionalActionsTest {
    private static final String SERVICES = "META-INF/services/" + ActionProvider.class.getName();
    @TempDir Path temporary;

    @Test
    void absentProvidersAreNotAdvertisedAndExplicitActionsStillWork() throws Exception {
        withProviders(null, () -> {
            ActionCatalog catalog = ActionCatalog.defaults(TestFixtures.personContext());
            assertThat(catalog.names()).isEmpty();
            catalog.register(new FakeAction("explicit-action", new AtomicBoolean()));
            assertThat(catalog.execute("explicit-action", Struct.getDefaultInstance())).isNotNull();
            assertThatThrownBy(() -> catalog.get("render-index-mappings"))
                    .isInstanceOf(ActionException.class).hasMessageContaining("Unknown action");
        });
    }

    @Test
    void installedProvidersHaveDeterministicClassAndActionOrder() {
        assertThat(ActionCatalog.defaults(ActionContext.create()).names())
                .startsWith("render-index-mappings", "compile", "validate-message")
                .containsSubsequence("render-prompt", "eval-cel");
    }

    @Test
    void duplicateProviderActionsCannotShadowEachOther() throws Exception {
        Path file = temporary.resolve("duplicate");
        Files.writeString(file, DuplicateProvider.class.getName());
        withProviders(file.toUri().toURL(), () ->
                assertThatThrownBy(() -> ActionCatalog.defaults(ActionContext.create()))
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("already registered"));
    }

    @Test
    void brokenProviderFailsConstructionInsteadOfHidingTheCapability() throws Exception {
        Path file = temporary.resolve("broken");
        Files.writeString(file, "example.missing.Provider");
        withProviders(file.toUri().toURL(), () ->
                assertThatThrownBy(() -> ActionCatalog.defaults(ActionContext.create()))
                        .isInstanceOf(java.util.ServiceConfigurationError.class));
    }

    @Test
    void emptyCatalogStillEnforcesAuthorizationAndRequestContracts() throws Exception {
        AtomicBoolean executed = new AtomicBoolean();
        ScopeBudgets budgets = new ScopeBudgets();
        ActionCatalog catalog = ActionCatalog.empty(ActionContext.create(), budgets)
                .register(new FakeAction("test-action", executed));
        assertThat(catalog.budgets()).isSameAs(budgets);
        assertThatThrownBy(() -> catalog.execute("test-action", Struct.getDefaultInstance(),
                Caller.scoped("denied", Set.of())))
                .isInstanceOf(ActionException.class).hasMessageContaining("does not hold");
        assertThat(executed).isFalse();
        assertThatThrownBy(() -> catalog.execute("test-action", com.google.protobuf.Empty.getDefaultInstance()))
                .isInstanceOf(ActionException.class);
        assertThat(executed).isFalse();
        assertThat(catalog.execute("test-action", Struct.getDefaultInstance()))
                .isEqualTo(Struct.getDefaultInstance());
        assertThat(executed).isTrue();
    }

    public static final class DuplicateProvider implements ActionProvider {
        @Override public List<? extends ProtoAction> actions() {
            return List.of(new FakeAction("compile", new AtomicBoolean()),
                    new FakeAction("compile", new AtomicBoolean()));
        }
    }

    private record FakeAction(String name, AtomicBoolean executed) implements ProtoAction {
        @Override public String description() { return "Provider fixture"; }
        @Override public String requiredScope() { return Scopes.SCHEMA_READ; }
        @Override public Descriptor requestType() { return Struct.getDescriptor(); }
        @Override public Descriptor responseType() { return Struct.getDescriptor(); }
        @Override public Message execute(Message request, ActionContext context) {
            executed.set(true);
            return Struct.getDefaultInstance();
        }
    }

    private static void withProviders(URL resource, Checked action) throws Exception {
        Thread thread = Thread.currentThread();
        ClassLoader previous = thread.getContextClassLoader();
        ClassLoader isolated = new ClassLoader(previous) {
            @Override public Enumeration<URL> getResources(String name) throws java.io.IOException {
                return name.equals(SERVICES)
                        ? Collections.enumeration(resource == null ? List.of() : List.of(resource))
                        : super.getResources(name);
            }
        };
        try {
            thread.setContextClassLoader(isolated);
            action.run();
        } finally {
            thread.setContextClassLoader(previous);
        }
    }
    @FunctionalInterface private interface Checked { void run() throws Exception; }
}
