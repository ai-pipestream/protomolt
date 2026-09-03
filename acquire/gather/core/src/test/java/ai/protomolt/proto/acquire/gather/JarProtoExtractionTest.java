package ai.protomolt.proto.acquire.gather;

import ai.protomolt.proto.sources.ProtoSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JarProtoExtractionTest {

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
            for (Map.Entry<String, String> entry : entries.entrySet()) {
                jarOut.putNextEntry(new ZipEntry(entry.getKey()));
                jarOut.write(entry.getValue().getBytes(StandardCharsets.UTF_8));
                jarOut.closeEntry();
            }
        }
        return jar;
    }

    @Test
    void defaultOptionsSkipWellKnownTypesAndFilterNothing() {
        JarProtoExtraction.Options options = JarProtoExtraction.Options.defaults();

        assertThat(options.includeGoogleWellKnownTypes()).isFalse();
        assertThat(options.includeGlobs()).isEmpty();
        assertThat(options.excludeGlobs()).isEmpty();
    }

    @Test
    void optionsDefensivelyCopyTheGlobLists() {
        List<String> includes = new ArrayList<>(List.of("common/**"));
        List<String> excludes = new ArrayList<>(List.of("**/internal/**"));

        JarProtoExtraction.Options options = new JarProtoExtraction.Options(true, includes, excludes);
        includes.add("mutated/**");
        excludes.add("mutated/**");

        assertThat(options.includeGlobs()).containsExactly("common/**");
        assertThat(options.excludeGlobs()).containsExactly("**/internal/**");
        assertThat(options.includeGoogleWellKnownTypes()).isTrue();
    }

    @Test
    void rejectsNullArguments() throws IOException {
        Path jar = writeJar("protos.jar", Map.of("app/doc.proto", APP_PROTO));

        assertThatNullPointerException()
                .isThrownBy(() -> JarProtoExtraction.extract(null, "jar:x", JarProtoExtraction.Options.defaults()));
        assertThatNullPointerException()
                .isThrownBy(() -> JarProtoExtraction.extract(jar, null, JarProtoExtraction.Options.defaults()));
        assertThatNullPointerException()
                .isThrownBy(() -> JarProtoExtraction.extract(jar, "jar:x", null));
    }

    @Test
    void corruptJarFailsWithIoException() throws IOException {
        Path notAZip = tempDir.resolve("corrupt.jar");
        Files.writeString(notAZip, "this is not a zip archive");

        assertThatThrownBy(() -> JarProtoExtraction.extract(
                notAZip, "jar:corrupt.jar", JarProtoExtraction.Options.defaults()))
                .isInstanceOf(IOException.class);
    }

    @Test
    void jarWithoutProtoEntriesExtractsNothing() throws Exception {
        Path jar = writeJar("empty.jar", Map.of(
                "META-INF/MANIFEST.MF", "Manifest-Version: 1.0",
                "app/data.json", "{}"));

        List<ProtoSource> sources = JarProtoExtraction.extract(
                jar, "jar:empty.jar", JarProtoExtraction.Options.defaults());

        assertThat(sources).isEmpty();
    }

    @Test
    void everyExtractedSourceCarriesTheGivenOrigin() throws Exception {
        Path jar = writeJar("protos.jar", Map.of(
                "common/v1/id.proto", COMMON_PROTO,
                "app/doc.proto", APP_PROTO));

        List<ProtoSource> sources = JarProtoExtraction.extract(
                jar, "maven:com.example:protos:1.0", JarProtoExtraction.Options.defaults());

        assertThat(sources).hasSize(2);
        assertThat(sources).allSatisfy(source ->
                assertThat(source.origin()).isEqualTo("maven:com.example:protos:1.0"));
    }

    @Test
    void entryNamesWithBackslashesAreNormalizedToImportPaths() throws Exception {
        Path jar = writeJar("protos.jar", Map.of("app\\doc.proto", APP_PROTO));

        List<ProtoSource> sources = JarProtoExtraction.extract(
                jar, "jar:protos.jar", JarProtoExtraction.Options.defaults());

        assertThat(sources).extracting(ProtoSource::path).containsExactly("app/doc.proto");
    }

    @Test
    void includeAndExcludeGlobsCompose() throws Exception {
        Path jar = writeJar("protos.jar", Map.of(
                "common/v1/id.proto", COMMON_PROTO,
                "common/v1/internal/secret.proto", APP_PROTO,
                "app/doc.proto", APP_PROTO));

        List<ProtoSource> sources = JarProtoExtraction.extract(jar, "jar:protos.jar",
                new JarProtoExtraction.Options(false,
                        List.of("common/**"), List.of("**/internal/**")));

        assertThat(sources).extracting(ProtoSource::path).containsExactly("common/v1/id.proto");
    }

    @Test
    void multipleIncludeGlobsMatchAny() throws Exception {
        Path jar = writeJar("protos.jar", Map.of(
                "common/v1/id.proto", COMMON_PROTO,
                "app/doc.proto", APP_PROTO,
                "other/other.proto", COMMON_PROTO));

        List<ProtoSource> sources = JarProtoExtraction.extract(jar, "jar:protos.jar",
                new JarProtoExtraction.Options(false,
                        List.of("common/**", "app/**"), List.of()));

        assertThat(sources).extracting(ProtoSource::path)
                .containsExactlyInAnyOrder("common/v1/id.proto", "app/doc.proto");
    }

    @Test
    void googleWellKnownTypesStillSkipWhenGlobsWouldMatch() throws Exception {
        Path jar = writeJar("protos.jar", Map.of(
                "google/protobuf/timestamp.proto", "syntax = \"proto3\";",
                "app/doc.proto", APP_PROTO));

        List<ProtoSource> sources = JarProtoExtraction.extract(jar, "jar:protos.jar",
                new JarProtoExtraction.Options(false, List.of("**"), List.of()));

        assertThat(sources).extracting(ProtoSource::path).containsExactly("app/doc.proto");
    }
}
