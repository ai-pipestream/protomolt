package ai.pipestream.proto.gather;

import ai.pipestream.proto.sources.ProtoSourceSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JarProtoGathererTest {

    @TempDir
    Path tempDir;

    private static final String COMMON_PROTO = """
            syntax = "proto3";
            package common.v1;
            message Id { string value = 1; }
            """;

    private static final String APP_PROTO = """
            syntax = "proto3";
            package app;
            message Doc { string id = 1; }
            """;

    private Path writeJar(String name, Map<String, String> entries) throws IOException {
        Path jar = tempDir.resolve(name);
        try (OutputStream out = Files.newOutputStream(jar);
                JarOutputStream jarOut = new JarOutputStream(out)) {
            jarOut.putNextEntry(new ZipEntry("some/dir/"));
            jarOut.closeEntry();
            for (Map.Entry<String, String> entry : entries.entrySet()) {
                jarOut.putNextEntry(new ZipEntry(entry.getKey()));
                jarOut.write(entry.getValue().getBytes(StandardCharsets.UTF_8));
                jarOut.closeEntry();
            }
        }
        return jar;
    }

    @Test
    void extractsEntriesPreservingNestedPaths() throws Exception {
        Path jar = writeJar("protos.jar", Map.of(
                "common/v1/id.proto", COMMON_PROTO,
                "app/doc.proto", APP_PROTO,
                "META-INF/MANIFEST.MF", "Manifest-Version: 1.0"));

        ProtoSourceSet set = JarProtoGatherer.builder().jar(jar).build().gather();

        assertThat(set.paths()).containsExactlyInAnyOrder("common/v1/id.proto", "app/doc.proto");
        assertThat(set.get("common/v1/id.proto").orElseThrow().content()).isEqualTo(COMMON_PROTO);
        assertThat(set.get("app/doc.proto").orElseThrow().origin()).isEqualTo("jar:protos.jar");
    }

    @Test
    void skipsGoogleWellKnownTypesByDefault() throws Exception {
        Path jar = writeJar("protos.jar", Map.of(
                "app/doc.proto", APP_PROTO,
                "google/protobuf/timestamp.proto", "syntax = \"proto3\";"));

        ProtoSourceSet set = JarProtoGatherer.builder().jar(jar).build().gather();

        assertThat(set.paths()).containsExactly("app/doc.proto");
    }

    @Test
    void includesGoogleWellKnownTypesWhenAsked() throws Exception {
        Path jar = writeJar("protos.jar", Map.of(
                "app/doc.proto", APP_PROTO,
                "google/protobuf/timestamp.proto", "syntax = \"proto3\";"));

        ProtoSourceSet set = JarProtoGatherer.builder()
                .jar(jar)
                .includeGoogleWellKnownTypes(true)
                .build()
                .gather();

        assertThat(set.paths())
                .containsExactlyInAnyOrder("app/doc.proto", "google/protobuf/timestamp.proto");
    }

    @Test
    void includeGlobsRestrictExtractedEntries() throws Exception {
        Path jar = writeJar("protos.jar", Map.of(
                "common/v1/id.proto", COMMON_PROTO,
                "app/doc.proto", APP_PROTO));

        ProtoSourceSet set = JarProtoGatherer.builder()
                .jar(jar)
                .includeEntries("common/**")
                .build()
                .gather();

        assertThat(set.paths()).containsExactly("common/v1/id.proto");
    }

    @Test
    void excludeGlobsSkipMatchingEntries() throws Exception {
        Path jar = writeJar("protos.jar", Map.of(
                "common/v1/id.proto", COMMON_PROTO,
                "common/v1/internal/secret.proto", APP_PROTO));

        ProtoSourceSet set = JarProtoGatherer.builder()
                .jar(jar)
                .excludeEntries("**/internal/**")
                .build()
                .gather();

        assertThat(set.paths()).containsExactly("common/v1/id.proto");
    }

    @Test
    void ignoresNonProtoAndDirectoryEntries() throws Exception {
        Path jar = writeJar("protos.jar", Map.of(
                "app/doc.proto", APP_PROTO,
                "app/data.json", "{}"));

        ProtoSourceSet set = JarProtoGatherer.builder().jar(jar).build().gather();

        assertThat(set.paths()).containsExactly("app/doc.proto");
    }

    @Test
    void identicalDuplicateEntriesAcrossJarsAreTolerated() throws Exception {
        Path first = writeJar("first.jar", Map.of("common/v1/id.proto", COMMON_PROTO));
        Path second = writeJar("second.jar", Map.of(
                "common/v1/id.proto", COMMON_PROTO,
                "app/doc.proto", APP_PROTO));

        ProtoSourceSet set = JarProtoGatherer.builder().jar(first).jar(second).build().gather();

        assertThat(set.paths()).containsExactlyInAnyOrder("common/v1/id.proto", "app/doc.proto");
        // First origin wins for the shared entry.
        assertThat(set.get("common/v1/id.proto").orElseThrow().origin()).isEqualTo("jar:first.jar");
    }

    @Test
    void conflictingEntriesAcrossJarsFailNamingBothJars() throws Exception {
        Path first = writeJar("first.jar", Map.of("common/v1/id.proto", COMMON_PROTO));
        Path second = writeJar("second.jar", Map.of("common/v1/id.proto", APP_PROTO));

        JarProtoGatherer gatherer = JarProtoGatherer.builder().jar(first).jar(second).build();

        assertThatThrownBy(gatherer::gather)
                .isInstanceOf(GatherException.class)
                .hasMessageContaining("common/v1/id.proto")
                .hasMessageContaining("jar:first.jar")
                .hasMessageContaining("jar:second.jar");
    }

    @Test
    void missingJarFails() {
        JarProtoGatherer gatherer = JarProtoGatherer.builder()
                .jar(tempDir.resolve("nope.jar"))
                .build();

        assertThatThrownBy(gatherer::gather)
                .isInstanceOf(GatherException.class)
                .hasMessageContaining("nope.jar");
    }

    @Test
    void builderRequiresAJar() {
        assertThatThrownBy(() -> JarProtoGatherer.builder().build())
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void originNamesTheJars() throws Exception {
        Path first = writeJar("first.jar", Map.of());
        Path second = writeJar("second.jar", Map.of());

        JarProtoGatherer gatherer = JarProtoGatherer.builder().jar(first).jar(second).build();

        assertThat(gatherer.origin()).isEqualTo("jar:[first.jar,second.jar]");
    }
}
