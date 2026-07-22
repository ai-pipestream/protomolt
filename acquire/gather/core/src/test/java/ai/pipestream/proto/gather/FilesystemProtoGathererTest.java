package ai.pipestream.proto.gather;

import ai.pipestream.proto.sources.ProtoSourceSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FilesystemProtoGathererTest {

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

    private Path write(String relative, String content) throws IOException {
        Path file = tempDir.resolve(relative);
        Files.createDirectories(file.getParent());
        Files.writeString(file, content);
        return file;
    }

    @Test
    void stagesEachRootRelativeToItself() throws Exception {
        write("rootA/common/v1/id.proto", COMMON_PROTO);
        write("rootB/app/doc.proto", APP_PROTO);

        ProtoSourceSet set = FilesystemProtoGatherer.builder()
                .root(tempDir.resolve("rootA"))
                .root(tempDir.resolve("rootB"))
                .build()
                .gather();

        assertThat(set.paths()).containsExactlyInAnyOrder("common/v1/id.proto", "app/doc.proto");
        assertThat(set.get("common/v1/id.proto").orElseThrow().content()).isEqualTo(COMMON_PROTO);
    }

    @Test
    void nestedPackagePathsAreRelativeToTheRoot() throws Exception {
        Path file = write("root/deeply/nested/pkg/v2/thing.proto", COMMON_PROTO);

        ProtoSourceSet set = FilesystemProtoGatherer.builder()
                .root(tempDir.resolve("root"))
                .build()
                .gather();

        assertThat(set.paths()).containsExactly("deeply/nested/pkg/v2/thing.proto");
        assertThat(set.get("deeply/nested/pkg/v2/thing.proto").orElseThrow().origin())
                .isEqualTo("file:" + file.toAbsolutePath().normalize());
    }

    @Test
    void ignoresNonProtoFiles() throws Exception {
        write("root/a.proto", APP_PROTO);
        write("root/readme.md", "# not a proto");
        write("root/sub/notes.txt", "notes");

        ProtoSourceSet set = FilesystemProtoGatherer.builder()
                .root(tempDir.resolve("root"))
                .build()
                .gather();

        assertThat(set.paths()).containsExactly("a.proto");
    }

    @Test
    void missingRootFailsByDefault() {
        FilesystemProtoGatherer gatherer = FilesystemProtoGatherer.builder()
                .root(tempDir.resolve("does-not-exist"))
                .build();

        assertThatThrownBy(gatherer::gather)
                .isInstanceOf(GatherException.class)
                .hasMessageContaining("does-not-exist");
    }

    @Test
    void missingRootIsSkippedWhenNotFailingOnMissing() throws Exception {
        write("root/a.proto", APP_PROTO);

        ProtoSourceSet set = FilesystemProtoGatherer.builder()
                .root(tempDir.resolve("does-not-exist"))
                .root(tempDir.resolve("root"))
                .failIfMissing(false)
                .build()
                .gather();

        assertThat(set.paths()).containsExactly("a.proto");
    }

    @Test
    void missingScanRootFailsByDefault() {
        FilesystemProtoGatherer gatherer = FilesystemProtoGatherer.builder()
                .scanRoot(tempDir.resolve("no-such-tree"))
                .build();

        assertThatThrownBy(gatherer::gather)
                .isInstanceOf(GatherException.class)
                .hasMessageContaining("no-such-tree");
    }

    @Test
    void scanRootDiscoversNestedProtoTrees() throws Exception {
        write("checkout/moduleA/src/main/proto/x/a.proto", COMMON_PROTO);
        write("checkout/moduleB/src/main/proto/y/b.proto", APP_PROTO);
        write("checkout/moduleB/src/main/java/NotAProto.java", "class NotAProto {}");

        ProtoSourceSet set = FilesystemProtoGatherer.builder()
                .scanRoot(tempDir.resolve("checkout"))
                .build()
                .gather();

        assertThat(set.paths()).containsExactlyInAnyOrder("x/a.proto", "y/b.proto");
    }

    @Test
    void scanRootSkipsBuildAndHiddenDirectories() throws Exception {
        write("checkout/module/src/main/proto/real.proto", APP_PROTO);
        write("checkout/module/build/src/main/proto/stale.proto", COMMON_PROTO);
        write("checkout/.git/src/main/proto/vcs.proto", COMMON_PROTO);

        ProtoSourceSet set = FilesystemProtoGatherer.builder()
                .scanRoot(tempDir.resolve("checkout"))
                .build()
                .gather();

        assertThat(set.paths()).containsExactly("real.proto");
    }

    @Test
    void rootsAndScanRootCombine() throws Exception {
        write("root/direct.proto", APP_PROTO);
        write("checkout/module/src/main/proto/scanned.proto", COMMON_PROTO);

        ProtoSourceSet set = FilesystemProtoGatherer.builder()
                .root(tempDir.resolve("root"))
                .scanRoot(tempDir.resolve("checkout"))
                .build()
                .gather();

        assertThat(set.paths()).containsExactlyInAnyOrder("direct.proto", "scanned.proto");
    }

    @Test
    void identicalDuplicateAcrossRootsIsTolerated() throws Exception {
        write("rootA/shared/x.proto", COMMON_PROTO);
        write("rootB/shared/x.proto", COMMON_PROTO);

        ProtoSourceSet set = FilesystemProtoGatherer.builder()
                .root(tempDir.resolve("rootA"))
                .root(tempDir.resolve("rootB"))
                .build()
                .gather();

        assertThat(set.paths()).containsExactly("shared/x.proto");
    }

    @Test
    void conflictingContentAcrossRootsFailsNamingBothOrigins() throws Exception {
        write("rootA/shared/x.proto", COMMON_PROTO);
        write("rootB/shared/x.proto", APP_PROTO);

        FilesystemProtoGatherer gatherer = FilesystemProtoGatherer.builder()
                .root(tempDir.resolve("rootA"))
                .root(tempDir.resolve("rootB"))
                .build();

        assertThatThrownBy(gatherer::gather)
                .isInstanceOf(GatherException.class)
                .hasMessageContaining("shared/x.proto")
                .hasMessageContaining("rootA")
                .hasMessageContaining("rootB");
    }

    @Test
    void builderRequiresARootOrScanRoot() {
        assertThatThrownBy(() -> FilesystemProtoGatherer.builder().build())
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void originDescribesRootsAndScanRoot() throws Exception {
        write("root/a.proto", APP_PROTO);

        FilesystemProtoGatherer gatherer = FilesystemProtoGatherer.builder()
                .root(tempDir.resolve("root"))
                .scanRoot(tempDir.resolve("checkout"))
                .failIfMissing(false)
                .build();

        assertThat(gatherer.origin())
                .startsWith("filesystem[")
                .contains("root")
                .contains("checkout");
        assertThat(gatherer.isAvailable()).isTrue();
    }
}
