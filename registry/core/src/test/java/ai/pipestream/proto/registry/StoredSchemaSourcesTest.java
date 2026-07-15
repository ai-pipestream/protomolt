package ai.pipestream.proto.registry;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Reference-graph resolution: import-path aliases are real (one stored schema referenced
 * under two names materializes under both), and one import path resolving to different
 * content across branches is a typed conflict — never a silent first-branch-wins.
 */
class StoredSchemaSourcesTest {

    private static final String COMMON = """
            syntax = "proto3";
            package deps;
            message Common { string id = 1; }
            """;

    private static final String OTHER = """
            syntax = "proto3";
            package deps;
            message Other { int64 n = 1; }
            """;

    private final InMemorySchemaRegistryStore store = new InMemorySchemaRegistryStore();

    private static String importer(String path) {
        return """
                syntax = "proto3";
                package deps;
                import "%s";
                message Wrapper { Common c = 1; }
                """.formatted(path);
    }

    @Test
    void oneSchemaReferencedUnderTwoNamesMaterializesBothAliases() {
        store.register("deps/common.proto", COMMON, List.of());
        store.register("mid-a", importer("a/common.proto"),
                List.of(new SchemaReference("a/common.proto", "deps/common.proto", 1)));
        store.register("mid-b", importer("b/common.proto"),
                List.of(new SchemaReference("b/common.proto", "deps/common.proto", 1)));

        // The root references both middlemen; each pulled the same stored schema under a
        // different import path. Both aliases must exist in the resolved set or the
        // second import cannot compile.
        StoredSchemaSources.Resolved resolved = StoredSchemaSources.resolve(store, "root", """
                        syntax = "proto3";
                        package deps;
                        import "mid-a.proto";
                        import "mid-b.proto";
                        message Root { }
                        """,
                List.of(new SchemaReference("mid-a.proto", "mid-a", 1),
                        new SchemaReference("mid-b.proto", "mid-b", 1)));

        assertThat(resolved.sources().sources())
                .extracting(s -> s.path())
                .contains("a/common.proto", "b/common.proto", "mid-a.proto", "mid-b.proto");
    }

    @Test
    void oneImportPathWithConflictingContentIsATypedError() {
        store.register("deps/common.proto", COMMON, List.of());
        store.register("deps/other.proto", OTHER, List.of());

        assertThatThrownBy(() -> StoredSchemaSources.resolve(store, "root", """
                        syntax = "proto3";
                        package deps;
                        import "shared.proto";
                        message Root { }
                        """,
                List.of(new SchemaReference("shared.proto", "deps/common.proto", 1),
                        new SchemaReference("shared.proto", "deps/other.proto", 1))))
                .isInstanceOf(ReferenceConflictException.class)
                .hasMessageContaining("shared.proto")
                .hasMessageContaining("conflicting");
    }

    @Test
    void sharedIdenticalDependenciesResolveOnce() {
        store.register("deps/common.proto", COMMON, List.of());
        // Two references to the same content under the same path: a legitimate diamond.
        StoredSchemaSources.Resolved resolved = StoredSchemaSources.resolve(store, "root", """
                        syntax = "proto3";
                        package deps;
                        import "deps/common.proto";
                        message Root { Common c = 1; }
                        """,
                List.of(new SchemaReference("deps/common.proto", "deps/common.proto", 1),
                        new SchemaReference("deps/common.proto", "deps/common.proto", 1)));
        assertThat(resolved.sources().sources())
                .extracting(s -> s.path())
                .containsOnlyOnce("deps/common.proto");
    }
}
