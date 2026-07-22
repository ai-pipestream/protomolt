package ai.pipestream.proto.gather;

import ai.pipestream.proto.descriptors.DescriptorLoader.DescriptorLoadException;
import ai.pipestream.proto.descriptors.DescriptorRegistry;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GatheringDescriptorLoaderTest {

    @TempDir
    Path tempDir;

    private static final String COMMON_PROTO = """
            syntax = "proto3";
            package common.v1;
            import "google/protobuf/timestamp.proto";
            message Audit {
              google.protobuf.Timestamp at = 1;
              message Entry { string detail = 1; }
            }
            """;

    private static final String APP_PROTO = """
            syntax = "proto3";
            package app;
            import "common/v1/audit.proto";
            message Doc { common.v1.Audit audit = 1; }
            """;

    private Path writeRoot() throws IOException {
        Path root = tempDir.resolve("protos");
        Files.createDirectories(root.resolve("common/v1"));
        Files.createDirectories(root.resolve("app"));
        Files.writeString(root.resolve("common/v1/audit.proto"), COMMON_PROTO);
        Files.writeString(root.resolve("app/doc.proto"), APP_PROTO);
        return root;
    }

    private GatheringDescriptorLoader loaderFor(Path root) {
        return new GatheringDescriptorLoader(
                FilesystemProtoGatherer.builder().root(root).build());
    }

    @Test
    void loadsDescriptorsForGatheredFilesOnly() throws Exception {
        try (GatheringDescriptorLoader loader = loaderFor(writeRoot())) {
            List<FileDescriptor> descriptors = loader.loadDescriptors();

            // The gathered two files only — not the Wire-supplied google/protobuf imports.
            assertThat(descriptors)
                    .extracting(FileDescriptor::getName)
                    .containsExactlyInAnyOrder("common/v1/audit.proto", "app/doc.proto");
        }
    }

    @Test
    void loadsDescriptorByImportPath() throws Exception {
        try (GatheringDescriptorLoader loader = loaderFor(writeRoot())) {
            FileDescriptor file = loader.loadDescriptor("app/doc.proto");

            assertThat(file).isNotNull();
            assertThat(file.findMessageTypeByName("Doc")
                    .findFieldByName("audit").getMessageType().getFullName())
                    .isEqualTo("common.v1.Audit");
            assertThat(loader.loadDescriptor("nope.proto")).isNull();
        }
    }

    @Test
    void findsTypeByFullSimpleAndNestedName() throws Exception {
        try (GatheringDescriptorLoader loader = loaderFor(writeRoot())) {
            FileDescriptor byFullName = loader.loadDescriptorForType("common.v1.Audit");
            assertThat(byFullName.getName()).isEqualTo("common/v1/audit.proto");

            FileDescriptor bySimpleName = loader.loadDescriptorForType("Doc");
            assertThat(bySimpleName.getName()).isEqualTo("app/doc.proto");

            FileDescriptor byNestedName = loader.loadDescriptorForType("common.v1.Audit.Entry");
            assertThat(byNestedName.getName()).isEqualTo("common/v1/audit.proto");

            assertThat(loader.loadDescriptorForType("no.such.Type")).isNull();
        }
    }

    @Test
    void refreshPicksUpChangedFiles() throws Exception {
        Path root = writeRoot();
        try (GatheringDescriptorLoader loader = loaderFor(root)) {
            assertThat(loader.loadDescriptorForType("app.Extra")).isNull();

            Files.writeString(root.resolve("app/extra.proto"), """
                    syntax = "proto3";
                    package app;
                    message Extra { string note = 1; }
                    """);

            // Cached until refresh().
            assertThat(loader.loadDescriptors()).hasSize(2);
            loader.refresh();

            assertThat(loader.loadDescriptors()).hasSize(3);
            assertThat(loader.loadDescriptorForType("app.Extra").getName())
                    .isEqualTo("app/extra.proto");
        }
    }

    @Test
    void resolvesTypesThroughADescriptorRegistry() throws Exception {
        try (GatheringDescriptorLoader loader = loaderFor(writeRoot())) {
            DescriptorRegistry registry = DescriptorRegistry.create();
            registry.addLoader(loader);

            Descriptor doc = registry.findDescriptorByFullName("app.Doc");
            assertThat(doc).isNotNull();
            assertThat(doc.findFieldByName("audit").getMessageType().getFullName())
                    .isEqualTo("common.v1.Audit");
            assertThat(registry.findDescriptor("Audit").getFullName()).isEqualTo("common.v1.Audit");
        }
    }

    @Test
    void gatherFailureSurfacesAsDescriptorLoadException() {
        try (GatheringDescriptorLoader loader = loaderFor(tempDir.resolve("missing"))) {
            assertThatThrownBy(loader::loadDescriptors)
                    .isInstanceOf(DescriptorLoadException.class)
                    .hasCauseInstanceOf(GatherException.class);
        }
    }

    @Test
    void compilationFailureSurfacesAsDescriptorLoadException() throws Exception {
        Path root = tempDir.resolve("broken");
        Files.createDirectories(root);
        Files.writeString(root.resolve("broken.proto"), "this is not proto");

        try (GatheringDescriptorLoader loader = loaderFor(root)) {
            assertThatThrownBy(loader::loadDescriptors)
                    .isInstanceOf(DescriptorLoadException.class)
                    .hasMessageContaining("compile");
        }
    }

    @Test
    void availabilityAndLoaderTypeReflectTheGatherer() throws Exception {
        FilesystemProtoGatherer gatherer = FilesystemProtoGatherer.builder()
                .root(writeRoot())
                .build();
        ProtoGatherer unavailable = new ProtoGatherer() {
            @Override
            public ai.pipestream.proto.sources.ProtoSourceSet gather() throws GatherException {
                throw new GatherException("unavailable");
            }

            @Override
            public String origin() {
                return "nowhere";
            }

            @Override
            public boolean isAvailable() {
                return false;
            }
        };

        try (GatheringDescriptorLoader loader = new GatheringDescriptorLoader(gatherer)) {
            assertThat(loader.isAvailable()).isTrue();
            assertThat(loader.getLoaderType()).isEqualTo("Proto Gatherer (" + gatherer.origin() + ")");
        }
        try (GatheringDescriptorLoader loader = new GatheringDescriptorLoader(unavailable)) {
            assertThat(loader.isAvailable()).isFalse();
            assertThat(loader.getLoaderType()).isEqualTo("Proto Gatherer (nowhere)");
        }
    }
}
