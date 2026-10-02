package ai.protomolt.proto.schema.registry.git;

import ai.protomolt.proto.registry.SchemaRegistryStores;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GitSchemaRegistryStoreProviderTest {
    @TempDir Path directory;

    @Test
    void installedGitProviderOpensOnlyWithCompleteConfiguration() {
        var stores = SchemaRegistryStores.discover();
        assertThat(stores.providerIds()).containsExactlyInAnyOrder("memory", "git");
        Path repo = directory.resolve("repo");
        Map<String, String> options = Map.of("directory", repo.toString(),
                "author-name", "Registry test", "author-email", "registry@example.test");
        var incomplete = new HashMap<>(options);
        incomplete.remove("author-email");
        assertThatThrownBy(() -> stores.open("git", incomplete, null))
                .isInstanceOf(IllegalArgumentException.class);
        var unknown = new HashMap<>(options);
        unknown.put("typo", "value");
        assertThatThrownBy(() -> stores.open("git", unknown, null))
                .isInstanceOf(IllegalArgumentException.class);
        var blank = new HashMap<>(options);
        blank.put("author-name", " ");
        assertThatThrownBy(() -> stores.open("git", blank, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(Files.exists(repo)).isFalse();
        try (var store = stores.open("git", options, null)) {
            store.register("test.proto", "syntax = \"proto3\"; message Test {}", List.of());
            assertThat(store.subjects()).containsExactly("test.proto");
        }
        try (var store = stores.open("git", options, null)) {
            assertThat(store.subjects()).containsExactly("test.proto");
        }
    }
}
