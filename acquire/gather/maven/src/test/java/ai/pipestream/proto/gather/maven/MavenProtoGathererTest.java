package ai.pipestream.proto.gather.maven;

import ai.pipestream.proto.gather.GatherException;
import ai.pipestream.proto.sources.ProtoSourceSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Exercises {@link MavenProtoGatherer} against a {@code file://} repository laid out in a
 * temp directory — no network access.
 */
class MavenProtoGathererTest {

    @TempDir
    Path tempDir;

    private Path repoDir;
    private Path localRepo;

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

    @BeforeEach
    void setUp() throws IOException {
        repoDir = Files.createDirectories(tempDir.resolve("remote-repo"));
        localRepo = Files.createDirectories(tempDir.resolve("local-repo"));
    }

    private MavenProtoGatherer.Builder gatherer() {
        return MavenProtoGatherer.builder()
                .repositories(List.of(repoDir.toUri().toString()))
                .localRepository(localRepo);
    }

    /** Writes {@code <group>/<artifact>/<version>/} pom + jar into the file repository. */
    private void deploy(String group, String artifact, String version, String classifier,
                        Map<String, String> protoEntries, List<String[]> dependencies) throws IOException {
        Path dir = Files.createDirectories(repoDir
                .resolve(group.replace('.', '/')).resolve(artifact).resolve(version));

        StringBuilder pom = new StringBuilder("""
                <?xml version="1.0" encoding="UTF-8"?>
                <project xmlns="http://maven.apache.org/POM/4.0.0">
                  <modelVersion>4.0.0</modelVersion>
                """);
        pom.append("  <groupId>").append(group).append("</groupId>\n");
        pom.append("  <artifactId>").append(artifact).append("</artifactId>\n");
        pom.append("  <version>").append(version).append("</version>\n");
        pom.append("  <packaging>jar</packaging>\n");
        if (!dependencies.isEmpty()) {
            pom.append("  <dependencies>\n");
            for (String[] gav : dependencies) {
                pom.append("    <dependency>\n");
                pom.append("      <groupId>").append(gav[0]).append("</groupId>\n");
                pom.append("      <artifactId>").append(gav[1]).append("</artifactId>\n");
                pom.append("      <version>").append(gav[2]).append("</version>\n");
                pom.append("    </dependency>\n");
            }
            pom.append("  </dependencies>\n");
        }
        pom.append("</project>\n");
        Files.writeString(dir.resolve(artifact + "-" + version + ".pom"), pom.toString());

        String jarName = artifact + "-" + version
                + (classifier.isEmpty() ? "" : "-" + classifier) + ".jar";
        try (OutputStream out = Files.newOutputStream(dir.resolve(jarName));
                JarOutputStream jarOut = new JarOutputStream(out)) {
            for (Map.Entry<String, String> entry : protoEntries.entrySet()) {
                jarOut.putNextEntry(new ZipEntry(entry.getKey()));
                jarOut.write(entry.getValue().getBytes(StandardCharsets.UTF_8));
                jarOut.closeEntry();
            }
        }
    }

    @Test
    void resolvesACoordinateAndExtractsItsProtos() throws Exception {
        deploy("com.example", "protos-a", "1.0.0", "",
                Map.of("common/v1/id.proto", COMMON_PROTO, "not-a-proto.txt", "ignored"),
                List.of());

        ProtoSourceSet set = gatherer()
                .coordinates(List.of("com.example:protos-a:1.0.0"))
                .build()
                .gather();

        assertThat(set.paths()).containsExactly("common/v1/id.proto");
        assertThat(set.get("common/v1/id.proto").orElseThrow().origin())
                .isEqualTo("maven:com.example:protos-a:1.0.0");
        // Resolution went through the configured local repository cache.
        assertThat(localRepo.resolve("com/example/protos-a/1.0.0/protos-a-1.0.0.jar")).exists();
    }

    @Test
    void resolvesMultipleCoordinates() throws Exception {
        deploy("com.example", "protos-a", "1.0.0", "",
                Map.of("common/v1/id.proto", COMMON_PROTO), List.of());
        deploy("com.example", "protos-b", "2.0.0", "",
                Map.of("app/doc.proto", APP_PROTO), List.of());

        ProtoSourceSet set = gatherer()
                .coordinates(List.of("com.example:protos-a:1.0.0", "com.example:protos-b:2.0.0"))
                .build()
                .gather();

        assertThat(set.paths()).containsExactlyInAnyOrder("common/v1/id.proto", "app/doc.proto");
        assertThat(set.get("app/doc.proto").orElseThrow().origin())
                .isEqualTo("maven:com.example:protos-b:2.0.0");
    }

    @Test
    void resolvesAClassifierCoordinate() throws Exception {
        deploy("com.example", "protos-c", "1.0.0", "schemas",
                Map.of("common/v1/id.proto", COMMON_PROTO), List.of());

        ProtoSourceSet set = gatherer()
                .coordinates(List.of("com.example:protos-c:1.0.0:schemas"))
                .build()
                .gather();

        assertThat(set.paths()).containsExactly("common/v1/id.proto");
        assertThat(set.get("common/v1/id.proto").orElseThrow().origin())
                .isEqualTo("maven:com.example:protos-c:1.0.0:schemas");
    }

    @Test
    void nonTransitiveResolutionIgnoresDependencies() throws Exception {
        deploy("com.example", "protos-b", "2.0.0", "",
                Map.of("app/doc.proto", APP_PROTO), List.of());
        deploy("com.example", "protos-a", "1.0.0", "",
                Map.of("common/v1/id.proto", COMMON_PROTO),
                List.<String[]>of(new String[]{"com.example", "protos-b", "2.0.0"}));

        ProtoSourceSet set = gatherer()
                .coordinates(List.of("com.example:protos-a:1.0.0"))
                .build()
                .gather();

        assertThat(set.paths()).containsExactly("common/v1/id.proto");
    }

    @Test
    void transitiveResolutionPicksUpDependencyProtos() throws Exception {
        deploy("com.example", "protos-b", "2.0.0", "",
                Map.of("app/doc.proto", APP_PROTO), List.of());
        deploy("com.example", "protos-a", "1.0.0", "",
                Map.of("common/v1/id.proto", COMMON_PROTO),
                List.<String[]>of(new String[]{"com.example", "protos-b", "2.0.0"}));

        ProtoSourceSet set = gatherer()
                .coordinates(List.of("com.example:protos-a:1.0.0"))
                .transitive(true)
                .build()
                .gather();

        assertThat(set.paths()).containsExactlyInAnyOrder("common/v1/id.proto", "app/doc.proto");
        assertThat(set.get("app/doc.proto").orElseThrow().origin())
                .isEqualTo("maven:com.example:protos-b:2.0.0");
    }

    @Test
    void skipsGoogleWellKnownTypesLikeTheJarGatherer() throws Exception {
        deploy("com.example", "protos-a", "1.0.0", "",
                Map.of("common/v1/id.proto", COMMON_PROTO,
                        "google/protobuf/timestamp.proto", "syntax = \"proto3\";"),
                List.of());

        ProtoSourceSet skipped = gatherer()
                .coordinates(List.of("com.example:protos-a:1.0.0"))
                .build()
                .gather();
        ProtoSourceSet included = gatherer()
                .coordinates(List.of("com.example:protos-a:1.0.0"))
                .includeGoogleWellKnownTypes(true)
                .build()
                .gather();

        assertThat(skipped.paths()).containsExactly("common/v1/id.proto");
        assertThat(included.paths()).containsExactlyInAnyOrder(
                "common/v1/id.proto", "google/protobuf/timestamp.proto");
    }

    @Test
    void missingArtifactFails() {
        MavenProtoGatherer gatherer = gatherer()
                .coordinates(List.of("com.example:no-such-artifact:9.9.9"))
                .build();

        assertThatThrownBy(gatherer::gather)
                .isInstanceOf(GatherException.class)
                .hasMessageContaining("com.example:no-such-artifact:9.9.9");
    }

    @Test
    void invalidCoordinateFails() {
        MavenProtoGatherer gatherer = gatherer()
                .coordinates(List.of("not-a-coordinate"))
                .build();

        assertThatThrownBy(gatherer::gather)
                .isInstanceOf(GatherException.class)
                .hasMessageContaining("not-a-coordinate");
    }

    @Test
    void builderRequiresACoordinate() {
        assertThatThrownBy(() -> MavenProtoGatherer.builder().build())
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void originNamesTheCoordinates() {
        MavenProtoGatherer gatherer = gatherer()
                .coordinates(List.of("com.example:protos-a:1.0.0", "com.example:protos-b:2.0.0"))
                .build();

        assertThat(gatherer.origin())
                .isEqualTo("maven:com.example:protos-a:1.0.0,com.example:protos-b:2.0.0");
        assertThat(gatherer.isAvailable()).isTrue();
    }
}
