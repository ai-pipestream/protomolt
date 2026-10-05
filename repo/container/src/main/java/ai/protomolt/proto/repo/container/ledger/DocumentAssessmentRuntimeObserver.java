package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.v1.DocumentAssessmentRuntime;
import ai.protomolt.proto.repo.v1.SchemaToolIdentity;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Host initialization evidence for a trusted immutable OpenJDK standard JAR classpath. */
final class DocumentAssessmentRuntimeObserver {
    private static final String CATALOG = "empty-taxonomy-and-postal/v1";
    private record Required(String component, String type) {}
    private static final List<Required> REQUIRED = List.of(
            new Required("project :protomolt-repo-admission ::", "ai.protomolt.proto.repo.admission.DocumentSchemaAdmission"),
            project("protomolt-protobuf-validation", "ai.protomolt.proto.validate.ProtoValidator"),
            project("protomolt-protobuf-validation", "ai.protomolt.proto.validate.source.ProtomoltRuleSource"),
            project("protomolt-protobuf-validation-protovalidate", "ai.protomolt.proto.validate.protovalidate.ProtovalidateRuleSource"),
            project("protomolt-cel", "ai.protomolt.proto.cel.CelEvaluator"),
            new Required("dev.cel:compiler:", "dev.cel.compiler.CelCompilerFactory"),
            new Required("dev.cel:runtime:", "dev.cel.runtime.CelRuntimeFactory"),
            project("protomolt-formats", "ai.protomolt.proto.formats.Identifiers"),
            project("protomolt-descriptors", "ai.protomolt.proto.descriptors.ClosedDescriptorSet"),
            project("protomolt-repo-codec", "ai.protomolt.proto.repo.codec.DocumentRevisionAssembly"),
            project("protomolt-repo-proto", "ai.protomolt.proto.repo.v1.DocumentPublicationMember"),
            new Required("com.google.protobuf:protobuf-java:", "com.google.protobuf.DynamicMessage"));
    private DocumentAssessmentRuntimeObserver() {}
    private static Required project(String name, String type) { return new Required("project ':" + name + "' ::", type); }
    private record Context(ClassLoader loader, String classpath, SchemaToolIdentity jvm) {}

    /** Only this observer can construct an observation; it is not a caller-authored runtime declaration. */
    static final class Observation {
        private final Context context;
        private final DocumentAssessmentRuntime identity;
        private Observation(Context context, DocumentAssessmentRuntime identity) { this.context = context; this.identity = identity; }
        DocumentAssessmentRuntime identity(Runnable control) {
            active(control);
            if (!context.equals(context())) throw new IllegalStateException("Admission runtime context differs from its observation");
            active(control);
            return identity;
        }
    }

    /**
     * Observe once at host initialization, outside SQL locks and request latency paths.
     * The host owns trusted build provenance and must keep classes/resources immutable.
     * This does not detect dynamic instrumentation or attest JVM loaded bytes.
     */
    static Observation observe(Path bundle, Runnable control) throws IOException {
        active(control);
        var context = context();
        var inventory = DocumentRuntimeInventory.read(bundle, control);
        var entries = context.classpath().split(java.util.regex.Pattern.quote(java.io.File.pathSeparator), -1);
        if (entries.length > 256) throw new IOException("Runtime classpath artifact count exceeds bounds");
        var paths = new ArrayList<Path>();
        for (var entry : entries) {
            if (entry.isEmpty()) throw new IOException("Implicit current-directory classpath is unsupported");
            paths.add(Path.of(entry));
        }
        DocumentRuntimeClasspath.verify(inventory, paths, control);
        var anchors = new ArrayList<DocumentRuntimeInventory.Anchor>();
        for (var required : REQUIRED) {
            active(control);
            var matching = inventory.identities().stream().filter(identity -> identity.getName().startsWith(required.component())).toList();
            if (matching.size() != 1) throw new IOException("Missing or ambiguous required admission runtime component: " + required.component());
            final Class<?> type;
            try { type = Class.forName(required.type(), true, context.loader()); }
            catch (ClassNotFoundException missing) { throw new IOException("Missing admission runtime class: " + required.type(), missing); }
            if (type.getClassLoader() != context.loader() || type.getModule().isNamed())
                throw new IOException("Admission runtime anchor has an unsupported loader or module");
            anchors.add(new DocumentRuntimeInventory.Anchor(matching.getFirst().getName(), type));
        }
        inventory.verifyAnchors(anchors, control);
        var profile = DocumentSchemaAdmission.runtimeProfile();
        if (!profile.validationProfile().equals("protomolt-retained-schema-admission/v1") || !profile.catalogConfiguration().equals(CATALOG))
            throw new IOException("Unsupported loaded admission runtime profile");
        var identity = DocumentAssessmentRuntime.newBuilder().setValidationProfile(profile.validationProfile())
                .setCatalogConfiguration(profile.catalogConfiguration()).setJvm(context.jvm()).addAllImplementationArtifacts(inventory.identities()).build();
        var observation = new Observation(context, identity);
        observation.identity(control);
        return observation;
    }

    private static Context context() {
        var loader = ClassLoader.getSystemClassLoader();
        if (!loader.getClass().getName().equals("jdk.internal.loader.ClassLoaders$AppClassLoader")
                || loader.getParent() != ClassLoader.getPlatformClassLoader()
                || DocumentSchemaAdmission.class.getClassLoader() != loader
                || DocumentSchemaAdmission.class.getModule().isNamed()
                || Thread.currentThread().getContextClassLoader() != loader
                || System.getProperty("java.system.class.loader") != null
                || System.getProperty("jdk.module.main") != null || System.getProperty("jdk.module.path") != null)
            throw new IllegalStateException("Unsupported admission runtime loader topology");
        var vm = ManagementFactory.getRuntimeMXBean();
        var arguments = vm.getInputArguments();
        if (arguments.size() > 256) throw new IllegalStateException("Runtime VM argument count exceeds bound");
        for (var argument : arguments) {
            if (argument.startsWith("-javaagent:") || argument.startsWith("-agentlib:") || argument.startsWith("-agentpath:")
                    || argument.startsWith("-Xrun")
                    || argument.startsWith("-Xbootclasspath") || argument.startsWith("--patch-module")
                    || argument.startsWith("--upgrade-module-path") || argument.startsWith("--module-path"))
                throw new IllegalStateException("Unsupported admission runtime instrumentation or module options");
        }
        String classpath = System.getProperty("java.class.path");
        if (classpath == null || classpath.isBlank() || classpath.length() > 128 * 1024)
            throw new IllegalStateException("Missing or oversized runtime classpath");
        var jvm = SchemaToolIdentity.newBuilder().setName(label(vm.getVmVendor() + " / " + vm.getVmName()))
                .setVersion(label(Runtime.version() + " / " + vm.getVmVersion())).build();
        return new Context(loader, classpath, jvm);
    }
    private static String label(String value) {
        if (value.isBlank() || value.length() > 200 || value.chars().anyMatch(Character::isISOControl))
            throw new IllegalStateException("Invalid observed JVM identity");
        return value;
    }
    private static void active(Runnable control) {
        if (Thread.currentThread().isInterrupted()) throw new java.util.concurrent.CancellationException("Runtime observation interrupted");
        Objects.requireNonNull(control).run();
    }
}
