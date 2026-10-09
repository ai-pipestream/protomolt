package ai.protomolt.proto.repo.container.ledger;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.regex.Pattern;
import javax.tools.ToolProvider;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Compiles the archive qualification hosts against the observed production JAR set, without
 * ambient test classes: the suite's own sources from {@code archive-backup-qualification/} and
 * the document rehearsal's reusable helpers (failure, checks, JSON, fixture, ledger) from
 * {@code backup-recovery/}, read as resources and never edited. Same discipline as
 * {@link RepositoryBackupRehearsalProbeCompiler}, whose source list is fixed to its own hosts.
 */
final class ArchiveBackupQualificationProbeCompiler {
    static final List<String> REUSED = List.of("RepositoryBackupRehearsalFailure", "RepositoryBackupRehearsalChecks",
            "RepositoryBackupRehearsalJson", "RepositoryBackupRehearsalFixture", "RepositoryBackupRehearsalLedger");
    static final List<String> OWN = List.of("ArchiveBackupQualificationFixture", "ArchiveBackupQualificationLedger",
            "ArchiveBackupQualificationContentChecks", "ArchiveBackupQualificationSeedHost", "ArchiveBackupQualificationRecoveredHost");

    record Compiled(Path bundle, String classpath, Path probe, List<String> sourceDigests) {}

    static Compiled compile(Path directory) throws Exception {
        String bundleProperty = System.getProperty("protomolt.test.admissionRuntimeBundle");
        String hostProperty = System.getProperty("protomolt.test.backupHostClasspath");
        assertThat(bundleProperty).as("Run the suite with -I repo/container/src/test/resources/archive-backup-qualification/qualification.init.gradle").isNotBlank();
        assertThat(hostProperty).as("Production host classpath from qualification.init.gradle").isNotBlank();
        var bundle = Path.of(bundleProperty);
        var inventory = DocumentRuntimeInventory.read(bundle, () -> {});
        var jars = new LinkedHashMap<String, Path>();
        inventory.identities().forEach(artifact -> jars.put(artifact.getArtifactSha256(),
                bundle.resolve("artifacts/" + artifact.getArtifactSha256() + ".jar")));
        // The production host shares admission dependencies. Include each exact artifact once, by bytes.
        for (String entry : hostProperty.split(Pattern.quote(File.pathSeparator))) {
            var path = Path.of(entry);
            var identity = DocumentRuntimeArtifact.observe("storage-host", path,
                    new DocumentRuntimeArtifact.Limits(Files.size(path), 65536, 64), () -> {});
            jars.putIfAbsent(identity.getArtifactSha256(), path);
        }
        String classpath = String.join(File.pathSeparator, jars.values().stream().map(Path::toString).toList());
        var classes = Files.createDirectories(directory.resolve("classes"));
        var sources = new ArrayList<String>();
        var digests = new ArrayList<String>();
        copy(directory, "/backup-recovery/", REUSED, sources, digests);
        copy(directory, "/archive-backup-qualification/", OWN, sources, digests);
        var arguments = new ArrayList<>(List.of("-proc:none", "-classpath", classpath, "-d", classes.toString()));
        arguments.addAll(sources);
        var diagnostics = new ByteArrayOutputStream();
        int status = ToolProvider.getSystemJavaCompiler().run(null, diagnostics, diagnostics, arguments.toArray(String[]::new));
        assertThat(status).as("Host sources compile against production JARs:\n" + diagnostics).isZero();
        var probe = directory.resolve("qualification-hosts.jar");
        try (var output = new JarOutputStream(Files.newOutputStream(probe)); var paths = Files.walk(classes)) {
            for (var file : paths.filter(Files::isRegularFile).sorted().toList()) {
                output.putNextEntry(new JarEntry(classes.relativize(file).toString().replace(File.separatorChar, '/')));
                Files.copy(file, output); output.closeEntry();
            }
        }
        return new Compiled(bundle, classpath, probe, List.copyOf(digests));
    }

    private static void copy(Path directory, String resources, List<String> names, List<String> sources, List<String> digests) throws Exception {
        for (String name : names) {
            var source = directory.resolve(name + ".java");
            try (var input = ArchiveBackupQualificationProbeCompiler.class.getResourceAsStream(resources + name + ".java")) {
                assertThat(input).as(resources + name).isNotNull();
                Files.copy(input, source, StandardCopyOption.REPLACE_EXISTING);
            }
            digests.add(resources.substring(1) + name + ".java\t" + RepositoryBackupRehearsalBackupSet.sha256(source));
            sources.add(source.toString());
        }
    }

    /** Every JAR the hosts run on, by digest: build evidence for the exact candidate. */
    static String runtimeInventory(Compiled compiled) {
        var text = new StringBuilder("protomolt-archive-backup-qualification-runtime/v1\n");
        for (String entry : compiled.classpath().split(Pattern.quote(File.pathSeparator))) {
            var path = Path.of(entry);
            text.append(path.getFileName()).append('\t').append(RepositoryBackupRehearsalBackupSet.sha256(path)).append('\n');
        }
        text.append(compiled.probe().getFileName()).append('\t').append(RepositoryBackupRehearsalBackupSet.sha256(compiled.probe())).append('\n');
        return text.toString();
    }
}
