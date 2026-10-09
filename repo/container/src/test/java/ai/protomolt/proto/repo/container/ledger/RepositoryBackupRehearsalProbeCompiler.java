package ai.protomolt.proto.repo.container.ledger;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Compiles the rehearsal hosts from {@code backup-recovery/} against the observed production
 * JAR set, without ambient test classes. Same discipline as {@link StorageRuntimeProbeCompiler},
 * which is a shared helper this assignment does not edit; this copy only lists its own sources.
 */
final class RepositoryBackupRehearsalProbeCompiler {
    static final List<String> SOURCES = List.of("RepositoryBackupRehearsalFailure", "RepositoryBackupRehearsalChecks",
            "RepositoryBackupRehearsalJson", "RepositoryBackupRehearsalFixture", "RepositoryBackupRehearsalLedger",
            "RepositoryBackupRehearsalPendingOperation", "RepositoryBackupRehearsalContentChecks",
            "RepositoryBackupRehearsalSeedHost", "RepositoryBackupRehearsalRecoveredHost");

    record Compiled(Path bundle, String classpath, Path probe, List<String> sourceDigests) {}

    static Compiled compile(Path directory) throws Exception {
        String bundleProperty = System.getProperty("protomolt.test.admissionRuntimeBundle");
        // rehearsal.init.gradle supplies the production host classpath; the module's own storage tasks use the other name.
        String hostProperty = System.getProperty("protomolt.test.backupHostClasspath", System.getProperty("protomolt.test.storageHostClasspath"));
        assertThat(bundleProperty).as("Run the suite with -I repo/container/src/test/resources/backup-recovery/rehearsal.init.gradle").isNotBlank();
        assertThat(hostProperty).as("Production host classpath from rehearsal.init.gradle").isNotBlank();
        var bundle = Path.of(bundleProperty);
        var inventory = DocumentRuntimeInventory.read(bundle, () -> {});
        var jars = new LinkedHashMap<String, Path>();
        inventory.identities().forEach(artifact -> jars.put(artifact.getArtifactSha256(),
                bundle.resolve("artifacts/" + artifact.getArtifactSha256() + ".jar")));
        // The production host shares admission dependencies. Include each exact artifact once, by bytes.
        for (String entry : hostProperty.split(java.util.regex.Pattern.quote(java.io.File.pathSeparator))) {
            var path = Path.of(entry);
            var identity = DocumentRuntimeArtifact.observe("storage-host", path,
                    new DocumentRuntimeArtifact.Limits(Files.size(path), 65536, 64), () -> {});
            jars.putIfAbsent(identity.getArtifactSha256(), path);
        }
        String classpath = String.join(java.io.File.pathSeparator, jars.values().stream().map(Path::toString).toList());
        var classes = Files.createDirectories(directory.resolve("classes"));
        var sources = new ArrayList<String>();
        var digests = new ArrayList<String>();
        for (String name : SOURCES) {
            var source = directory.resolve(name + ".java");
            try (var input = RepositoryBackupRehearsalProbeCompiler.class.getResourceAsStream("/backup-recovery/" + name + ".java")) {
                assertThat(input).as(name).isNotNull();
                Files.copy(input, source, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            }
            digests.add(name + ".java\t" + RepositoryBackupRehearsalBackupSet.sha256(source));
            sources.add(source.toString());
        }
        var arguments = new ArrayList<>(List.of("-proc:none", "-classpath", classpath, "-d", classes.toString()));
        arguments.addAll(sources);
        var diagnostics = new java.io.ByteArrayOutputStream();
        int status = javax.tools.ToolProvider.getSystemJavaCompiler().run(null, diagnostics, diagnostics, arguments.toArray(String[]::new));
        assertThat(status).as("Host sources compile against production JARs:\n" + diagnostics).isZero();
        var probe = directory.resolve("rehearsal-hosts.jar");
        try (var output = new java.util.jar.JarOutputStream(Files.newOutputStream(probe)); var paths = Files.walk(classes)) {
            for (var file : paths.filter(Files::isRegularFile).sorted().toList()) {
                output.putNextEntry(new java.util.jar.JarEntry(classes.relativize(file).toString().replace(java.io.File.separatorChar, '/')));
                Files.copy(file, output); output.closeEntry();
            }
        }
        return new Compiled(bundle, classpath, probe, List.copyOf(digests));
    }

    /** Every JAR the hosts run on, by digest: build evidence for the exact candidate. */
    static String runtimeInventory(Compiled compiled) {
        var text = new StringBuilder("protomolt-repository-backup-rehearsal-runtime/v1\n");
        for (String entry : compiled.classpath().split(java.util.regex.Pattern.quote(java.io.File.pathSeparator))) {
            var path = Path.of(entry);
            text.append(path.getFileName()).append('\t').append(RepositoryBackupRehearsalBackupSet.sha256(path)).append('\n');
        }
        text.append(compiled.probe().getFileName()).append('\t').append(RepositoryBackupRehearsalBackupSet.sha256(compiled.probe())).append('\n');
        return text.toString();
    }
}
