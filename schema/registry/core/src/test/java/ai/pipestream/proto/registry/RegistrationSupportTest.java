package ai.pipestream.proto.registry;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link RegistrationSupport#requireSubject} for the absent and empty names.
 *
 * <p>The {@code "."} and {@code ".."} path-traversal names are covered by
 * {@link SubjectPathTraversalTest}; this covers the two cases that never reach a filesystem
 * path at all, and checks both store implementations refuse them the same way — a store that
 * accepted a blank subject would create an unnameable, unreadable subject directory.</p>
 */
class RegistrationSupportTest {

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
    void aNullSubjectIsRejectedAsANullPointer() {
        assertThatThrownBy(() -> RegistrationSupport.requireSubject(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("subject");
    }

    @Test
    void blankSubjectsAreRejectedWhateverWhitespaceTheyAreMadeOf() {
        for (String blank : List.of("", " ", "   ", "\t", "\n", " \t\n ")) {
            assertThatThrownBy(() -> RegistrationSupport.requireSubject(blank))
                    .as("requireSubject(%s)", blank.replace("\n", "\\n").replace("\t", "\\t"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("subject must not be blank");
        }
    }

    @Test
    void anAcceptedSubjectIsReturnedUnchanged() {
        assertThat(RegistrationSupport.requireSubject("common/v1/core.proto"))
                .isEqualTo("common/v1/core.proto");
        // Surrounding whitespace is not stripped; the subject is stored verbatim.
        assertThat(RegistrationSupport.requireSubject(" padded ")).isEqualTo(" padded ");
    }

    @Test
    void bothStoresRejectNullAndBlankSubjectsOnRegister() throws Exception {
        try (GitSchemaRegistryStore git = gitStore();
                InMemorySchemaRegistryStore memory = new InMemorySchemaRegistryStore()) {
            for (SchemaRegistryStore store : List.of(git, memory)) {
                assertThatThrownBy(() -> store.register(null, SCHEMA, List.of()))
                        .isInstanceOf(NullPointerException.class)
                        .hasMessage("subject");
                assertThatThrownBy(() -> store.register("   ", SCHEMA, List.of()))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessage("subject must not be blank");

                assertThat(store.subjects()).isEmpty();
            }
        }
    }

    @Test
    void bothStoresRejectNullAndBlankSubjectsOnSetCompatibilityMode() throws Exception {
        try (GitSchemaRegistryStore git = gitStore();
                InMemorySchemaRegistryStore memory = new InMemorySchemaRegistryStore()) {
            for (SchemaRegistryStore store : List.of(git, memory)) {
                assertThatThrownBy(() -> store.setCompatibilityMode(null, "BACKWARD"))
                        .isInstanceOf(NullPointerException.class)
                        .hasMessage("subject");
                assertThatThrownBy(() -> store.setCompatibilityMode("", "BACKWARD"))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessage("subject must not be blank");
            }
        }
    }
}
