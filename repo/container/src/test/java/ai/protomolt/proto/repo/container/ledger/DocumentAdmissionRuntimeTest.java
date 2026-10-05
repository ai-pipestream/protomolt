package ai.protomolt.proto.repo.container.ledger;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.*;

/** Run by admissionRuntimeTest with a freshly generated production artifact inventory. */
class DocumentAdmissionRuntimeTest {
    @TempDir Path directory;
    private record Component(String name, Path jar) {}
    private Path bundle() {
        String path = System.getProperty("protomolt.test.admissionRuntimeBundle");
        assertThat(path).as("Run :protomolt-repo-container:admissionRuntimeTest").isNotBlank();
        return Path.of(path);
    }
    private List<Component> components(Path root, DocumentRuntimeInventory inventory) {
        return inventory.identities().stream().map(identity -> new Component(identity.getName(),
                root.resolve("artifacts/" + identity.getArtifactSha256() + ".jar"))).toList();
    }
    private URLClassLoader loader(List<Component> components, Path probe) throws Exception {
        var urls = new ArrayList<URL>();
        if (probe != null) urls.add(probe.toUri().toURL());
        for (var component : components) urls.add(component.jar().toUri().toURL());
        return new URLClassLoader(urls.toArray(URL[]::new), ClassLoader.getPlatformClassLoader());
    }

    private Path compileProbe(List<Component> components) throws Exception {
        var source = directory.resolve("AdmissionProbe.java");
        try (var input = getClass().getResourceAsStream("/runtime-inventory/AdmissionProbe.java")) {
            assertThat(input).isNotNull(); Files.copy(input, source);
        }
        var output = Files.createDirectory(directory.resolve("classes"));
        String classpath = String.join(java.io.File.pathSeparator, components.stream().map(c -> c.jar().toString()).toList());
        var compiler = javax.tools.ToolProvider.getSystemJavaCompiler();
        assertThat(compiler).isNotNull();
        assertThat(compiler.run(null, null, null, "-proc:none", "-classpath", classpath, "-d", output.toString(), source.toString())).isZero();
        return output;
    }

    @Test void runsExactAdmissionValidatorUsingOnlyProductionJars() throws Exception {
        var root = bundle();
        var inventory = DocumentRuntimeInventory.read(root, () -> {});
        var components = components(root, inventory);
        assertThat(DocumentRuntimeClasspath.verify(inventory, components.stream().map(Component::jar).toList(), () -> {}))
                .hasSize(components.size());
        var output = compileProbe(components);
        try (var loader = loader(components, output)) {
            assertThat(invokeProbe(loader)).isEqualTo(5);
            var anchors = new ArrayList<DocumentRuntimeInventory.Anchor>();
            anchor(anchors, components, loader, "project :protomolt-repo-admission ::", "ai.protomolt.proto.repo.admission.DocumentSchemaAdmission");
            anchor(anchors, components, loader, "project ':protomolt-protobuf-validation' ::", "ai.protomolt.proto.validate.ProtoValidator");
            anchor(anchors, components, loader, "project ':protomolt-protobuf-validation' ::", "ai.protomolt.proto.validate.source.ProtomoltRuleSource");
            anchor(anchors, components, loader, "project ':protomolt-protobuf-validation-protovalidate' ::", "ai.protomolt.proto.validate.protovalidate.ProtovalidateRuleSource");
            anchor(anchors, components, loader, "project ':protomolt-cel' ::", "ai.protomolt.proto.cel.CelEvaluator");
            anchor(anchors, components, loader, "dev.cel:compiler:", "dev.cel.compiler.CelCompilerFactory");
            anchor(anchors, components, loader, "dev.cel:runtime:", "dev.cel.runtime.CelRuntimeFactory");
            anchor(anchors, components, loader, "project ':protomolt-formats' ::", "ai.protomolt.proto.formats.Identifiers");
            anchor(anchors, components, loader, "com.google.protobuf:protobuf-java:", "com.google.protobuf.DynamicMessage");
            inventory.verifyAnchors(anchors, () -> {});
            assertThatThrownBy(() -> Class.forName("org.junit.jupiter.api.Test", false, loader)).isInstanceOf(ClassNotFoundException.class);
        }
    }

    @Test void missingRuleAdapterCannotFallThroughToTheTestClasspath() throws Exception {
        var root = bundle();
        var inventory = DocumentRuntimeInventory.read(root, () -> {});
        var components = components(root, inventory);
        var output = compileProbe(components);
        var missing = components.stream().filter(c -> !c.name().startsWith("project ':protomolt-protobuf-validation-protovalidate' ::")).toList();
        assertThat(missing).hasSize(components.size() - 1);
        // The same compiled probe passes with the complete bundle first.
        try (var complete = loader(components, output)) { assertThat(invokeProbe(complete)).isEqualTo(5); }
        assertThat(Class.forName("ai.protomolt.proto.validate.protovalidate.ProtovalidateRuleSource")).isNotNull();
        try (var loader = loader(missing, output)) {
            assertThatThrownBy(() -> invokeProbe(loader)).isInstanceOf(java.lang.reflect.InvocationTargetException.class)
                    .cause().isInstanceOf(NoClassDefFoundError.class).hasMessageContaining("ProtovalidateRuleSource");
        }
    }

    private static Object invokeProbe(ClassLoader loader) throws Exception {
        var thread = Thread.currentThread();
        var previous = thread.getContextClassLoader();
        try {
            thread.setContextClassLoader(loader);
            var probe = Class.forName("ai.protomolt.proto.repo.admission.AdmissionProbe", true, loader);
            assertThat(probe.getClassLoader()).isSameAs(loader);
            return probe.getMethod("run").invoke(null);
        } finally { thread.setContextClassLoader(previous); }
    }

    private static void anchor(List<DocumentRuntimeInventory.Anchor> anchors, List<Component> components,
                               ClassLoader loader, String prefix, String className) throws Exception {
        var matching = components.stream().filter(c -> c.name().startsWith(prefix)).toList();
        assertThat(matching).hasSize(1);
        var type = Class.forName(className, false, loader);
        assertThat(type.getClassLoader()).isSameAs(loader);
        anchors.add(new DocumentRuntimeInventory.Anchor(matching.getFirst().name(), type));
    }
}
