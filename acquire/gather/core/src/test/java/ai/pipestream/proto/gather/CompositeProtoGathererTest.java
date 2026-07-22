package ai.pipestream.proto.gather;

import ai.pipestream.proto.sources.ProtoSourceSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CompositeProtoGathererTest {

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

    private static ProtoGatherer fixed(ProtoSourceSet set, String origin, boolean available) {
        return new ProtoGatherer() {
            @Override
            public ProtoSourceSet gather() {
                return set;
            }

            @Override
            public String origin() {
                return origin;
            }

            @Override
            public boolean isAvailable() {
                return available;
            }
        };
    }

    @Test
    void mergesAcrossGathererTypes() throws Exception {
        Path root = tempDir.resolve("fs/common/v1");
        Files.createDirectories(root);
        Files.writeString(root.resolve("id.proto"), COMMON_PROTO);

        Path jar = tempDir.resolve("app.jar");
        try (OutputStream out = Files.newOutputStream(jar);
                JarOutputStream jarOut = new JarOutputStream(out)) {
            jarOut.putNextEntry(new ZipEntry("app/doc.proto"));
            jarOut.write(APP_PROTO.getBytes(StandardCharsets.UTF_8));
            jarOut.closeEntry();
        }

        CompositeProtoGatherer composite = CompositeProtoGatherer.of(
                FilesystemProtoGatherer.builder().root(tempDir.resolve("fs")).build(),
                JarProtoGatherer.builder().jar(jar).build());

        ProtoSourceSet set = composite.gather();

        assertThat(set.paths()).containsExactlyInAnyOrder("common/v1/id.proto", "app/doc.proto");
        assertThat(set.get("app/doc.proto").orElseThrow().origin()).isEqualTo("jar:app.jar");
    }

    @Test
    void conflictAcrossGatherersNamesBothOrigins() {
        ProtoSourceSet first = ProtoSourceSet.builder()
                .add("shared/x.proto", COMMON_PROTO, "git:repo-a@main")
                .build();
        ProtoSourceSet second = ProtoSourceSet.builder()
                .add("shared/x.proto", APP_PROTO, "jar:other.jar")
                .build();

        CompositeProtoGatherer composite = CompositeProtoGatherer.of(
                fixed(first, "git:repo-a@main", true),
                fixed(second, "jar:other.jar", true));

        assertThatThrownBy(composite::gather)
                .isInstanceOf(GatherException.class)
                .hasMessageContaining("shared/x.proto")
                .hasMessageContaining("git:repo-a@main")
                .hasMessageContaining("jar:other.jar")
                .hasCauseInstanceOf(IllegalStateException.class);
    }

    @Test
    void identicalDuplicatesAcrossGatherersAreToleratedFirstOriginWins() throws Exception {
        ProtoSourceSet first = ProtoSourceSet.builder()
                .add("shared/x.proto", COMMON_PROTO, "first")
                .build();
        ProtoSourceSet second = ProtoSourceSet.builder()
                .add("shared/x.proto", COMMON_PROTO, "second")
                .build();

        ProtoSourceSet merged = CompositeProtoGatherer.of(
                fixed(first, "first", true),
                fixed(second, "second", true)).gather();

        assertThat(merged.size()).isEqualTo(1);
        assertThat(merged.get("shared/x.proto").orElseThrow().origin()).isEqualTo("first");
    }

    @Test
    void originListsChildOrigins() {
        CompositeProtoGatherer composite = CompositeProtoGatherer.of(
                fixed(ProtoSourceSet.empty(), "git:a@main", true),
                fixed(ProtoSourceSet.empty(), "jar:[b.jar]", true));

        assertThat(composite.origin()).isEqualTo("composite[git:a@main, jar:[b.jar]]");
    }

    @Test
    void availableOnlyWhenEveryChildIs() {
        ProtoGatherer up = fixed(ProtoSourceSet.empty(), "up", true);
        ProtoGatherer down = fixed(ProtoSourceSet.empty(), "down", false);

        assertThat(CompositeProtoGatherer.of(up).isAvailable()).isTrue();
        assertThat(CompositeProtoGatherer.of(up, down).isAvailable()).isFalse();
    }

    @Test
    void requiresAtLeastOneGatherer() {
        assertThatThrownBy(CompositeProtoGatherer::of)
                .isInstanceOf(IllegalArgumentException.class);
    }
}
