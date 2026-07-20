package ai.pipestream.proto.registry;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Subject names must not escape {@code subjects/}.
 *
 * <p>{@link java.net.URLEncoder} treats {@code .} as unreserved and leaves it alone, so the
 * subject {@code ".."} once encoded to itself and {@code subjects/..} resolved to the repository
 * root — {@code register} wrote its {@code .proto} and {@code .json} beside {@code registry.json}
 * and the index could not read them back. Tightening the encoder would rename every legitimate
 * dotted subject on disk, so the names are refused at the API boundary instead; both store
 * implementations must refuse them identically.
 */
class SubjectPathTraversalTest {

    @TempDir
    Path tempDir;

    private static final String SCHEMA = """
            syntax = "proto3";
            package common.v1;
            message Core {
              string id = 1;
            }
            """;

    private GitSchemaRegistryStore gitStore() {
        return GitSchemaRegistryStore.builder()
                .repositoryDir(tempDir.resolve("repo"))
                .build();
    }

    @Test
    void parentDirectorySubjectIsRejectedAndWritesNothing() throws Exception {
        Path dir = tempDir.resolve("repo");
        try (GitSchemaRegistryStore store = gitStore()) {
            assertThatThrownBy(() -> store.register("..", SCHEMA, List.of()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("..");

            assertThat(store.subjects()).isEmpty();
        }

        // Nothing leaked into the repository root.
        try (Stream<Path> rootFiles = Files.list(dir)) {
            assertThat(rootFiles.map(path -> path.getFileName().toString()))
                    .doesNotContain("v1.proto", "v1.json");
        }
    }

    @Test
    void currentDirectorySubjectIsRejected() throws Exception {
        try (GitSchemaRegistryStore store = gitStore()) {
            assertThatThrownBy(() -> store.register(".", SCHEMA, List.of()))
                    .isInstanceOf(IllegalArgumentException.class);

            assertThat(store.subjects()).isEmpty();
        }
    }

    @Test
    void setCompatibilityModeRejectsTheSameNames() throws Exception {
        try (GitSchemaRegistryStore store = gitStore()) {
            assertThatThrownBy(() -> store.setCompatibilityMode("..", "BACKWARD"))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> store.setCompatibilityMode(".", "BACKWARD"))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void inMemoryStoreRejectsTheSameNamesSoTheStoresAgree() {
        InMemorySchemaRegistryStore store = new InMemorySchemaRegistryStore();

        assertThatThrownBy(() -> store.register("..", SCHEMA, List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> store.register(".", SCHEMA, List.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * Dots are legal inside a subject name and must keep working — the common convention is
     * {@code <proto.package>.<Message>-value}, which is mostly dots.
     */
    @Test
    void ordinaryDottedSubjectsStillRegisterAndReadBack() throws Exception {
        try (GitSchemaRegistryStore store = gitStore()) {
            StoredSchema stored = store.register("common.v1.Core-value", SCHEMA, List.of());

            assertThat(stored.subject()).isEqualTo("common.v1.Core-value");
            assertThat(stored.version()).isEqualTo(1);
            assertThat(store.subjects()).containsExactly("common.v1.Core-value");
            assertThat(store.latest("common.v1.Core-value")).isPresent();
        }
    }

    @Test
    void subjectsBeginningWithDotsAreStillAllowed() throws Exception {
        try (GitSchemaRegistryStore store = gitStore()) {
            store.register("...", SCHEMA, List.of());
            store.register(".hidden", SCHEMA, List.of());

            assertThat(store.subjects()).containsExactlyInAnyOrder("...", ".hidden");
        }
    }

    @Test
    void registeredSubjectFilesStayUnderTheSubjectsDirectory() throws Exception {
        Path dir = tempDir.resolve("repo");
        try (GitSchemaRegistryStore store = gitStore()) {
            store.register("common.v1.Core-value", SCHEMA, List.of());
        }

        Path subjects = dir.resolve("subjects");
        assertThat(Files.isDirectory(subjects)).isTrue();
        try (Stream<Path> tree = Files.walk(subjects)) {
            assertThat(tree.filter(Files::isRegularFile))
                    .isNotEmpty()
                    .allSatisfy(path ->
                            assertThat(path.normalize()).startsWith(subjects.normalize()));
        }
    }
}
