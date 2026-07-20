package ai.pipestream.proto.emit;

import org.eclipse.jgit.api.Git;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The emit SPI's guarantees: bundle paths cannot escape any destination, the directory sink
 * is faithful and write-only, the git sink commits exactly the bundle (idempotently), and
 * the in-memory zip is deterministic.
 */
class EmitSinksTest {

    private static Bundle sample() {
        return Bundle.builder()
                .add("index.md", "# root\n")
                .add("tables/orders.md", "orders")
                .add("tables/customers.md", "customers")
                .build();
    }

    @Test
    void bundleRejectsEscapingAndDuplicatePaths() {
        assertThatThrownBy(() -> Bundle.builder().add("../evil.md", "x"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Bundle.builder().add("/abs.md", "x"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Bundle.builder().add("a/./b.md", "x"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Bundle.builder().add("a.md", "x").add("a.md", "y"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Duplicate");
        // Insertion order is the bundle's order.
        assertThat(sample().paths()).containsExactly(
                "index.md", "tables/orders.md", "tables/customers.md");
    }

    @Test
    void directorySinkWritesEverythingAndOnlyWhatItIsGiven(@TempDir Path dir) throws Exception {
        Path stranger = dir.resolve("existing.txt");
        Files.writeString(stranger, "untouched");

        String receipt = new DirectorySink(dir).write(sample());
        assertThat(receipt).isEqualTo(dir.toAbsolutePath().normalize().toString());
        assertThat(Files.readString(dir.resolve("tables/orders.md"))).isEqualTo("orders");
        assertThat(Files.readString(dir.resolve("index.md"))).isEqualTo("# root\n");
        // Write-only: it never deletes what it did not write.
        assertThat(Files.readString(stranger)).isEqualTo("untouched");
    }

    @Test
    void gitSinkInitializesCommitsAndIsIdempotent(@TempDir Path dir) throws Exception {
        Path repo = dir.resolve("bundle-repo");
        GitSink sink = new GitSink(repo, "Publish knowledge bundle");

        String first = sink.write(sample());
        assertThat(first).hasSize(40); // a commit SHA

        try (Git git = Git.open(repo.toFile())) {
            assertThat(git.log().call().iterator().next().getFullMessage())
                    .isEqualTo("Publish knowledge bundle");
        }
        assertThat(Files.readString(repo.resolve("tables/customers.md")))
                .isEqualTo("customers");

        // The same bundle again: no second commit, the receipt says so.
        String second = sink.write(sample());
        assertThat(second).startsWith(first.substring(0, 40)).contains("unchanged");

        // A changed bundle commits again, under the configured prefix if one is set.
        GitSink prefixed = new GitSink(repo, "docs/okf", "Update bundle",
                "tester", "t@example.com");
        String third = prefixed.write(Bundle.builder().add("index.md", "v2").build());
        assertThat(third).hasSize(40).isNotEqualTo(first);
        assertThat(Files.readString(repo.resolve("docs/okf/index.md"))).isEqualTo("v2");
    }

    /**
     * The receipt and the commit are both scoped to the bundle's paths. An unrelated file staged
     * in the same repository must not make an unchanged bundle look changed, and must not ride
     * along in a commit the bundle does trigger.
     */
    @Test
    void unrelatedStagedChangesNeitherTriggerNorJoinTheCommit(@TempDir Path dir) throws Exception {
        Path repo = dir.resolve("mixed-repo");
        GitSink sink = new GitSink(repo, "Publish knowledge bundle");
        String first = sink.write(sample());

        Files.writeString(repo.resolve("unrelated.txt"), "not ours");
        try (Git git = Git.open(repo.toFile())) {
            git.add().addFilepattern("unrelated.txt").call();
        }

        // The bundle is byte-identical: nothing of ours changed, so no commit.
        assertThat(sink.write(sample())).isEqualTo(first + " (unchanged)");

        String second = sink.write(Bundle.builder().add("index.md", "v2").build());
        assertThat(second).hasSize(40).isNotEqualTo(first);
        try (Git git = Git.open(repo.toFile())) {
            // Still staged rather than committed: the commit carried only index.md.
            assertThat(git.status().call().getAdded()).contains("unrelated.txt");
        }
    }

    @Test
    void gitSinkPrefixIsNormalizedBeforeItBecomesAPath(@TempDir Path dir) throws Exception {
        Path repo = dir.resolve("prefix-repo");
        // Leading and trailing slashes and Windows separators all denote the same directory.
        new GitSink(repo, "/docs\\okf/", "Publish", "tester", "t@example.com").write(sample());

        assertThat(Files.readString(repo.resolve("docs/okf/index.md"))).isEqualTo("# root\n");
        assertThat(Files.readString(repo.resolve("docs/okf/tables/orders.md"))).isEqualTo("orders");
        // Only the prefixed copy exists; nothing landed at the root.
        assertThat(Files.exists(repo.resolve("index.md"))).isFalse();

        try (Git git = Git.open(repo.toFile())) {
            assertThat(git.log().call().iterator().next().getAuthorIdent().getName())
                    .isEqualTo("tester");
        }
    }

    @Test
    void gitSinkRejectsAPrefixThatCouldLeaveTheRepository(@TempDir Path dir) {
        Path repo = dir.resolve("bad-prefix-repo");
        for (String prefix : List.of("..", "../escape", "docs/../..", ".", "docs/./okf",
                "docs//okf", "   ")) {
            assertThatThrownBy(() -> new GitSink(repo, prefix, "Publish", "t", "t@example.com"))
                    .as("prefix %s", prefix)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Invalid prefix segment: " + prefix);
        }
        assertThatThrownBy(() -> new GitSink(repo, null, "Publish", "t", "t@example.com"))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("prefix");
        // The rejection happens in the constructor, before any repository is created.
        assertThat(Files.exists(repo)).isFalse();
    }

    /**
     * {@link Bundle} already refuses escaping paths, so the sink's own check is a second line.
     * It is kept because the sink joins prefix and path itself; this drives it with a bundle
     * built past the builder's validation to prove the guard still throws rather than writing
     * outside the work tree.
     */
    @Test
    void gitSinkRefusesToWriteOutsideTheWorkTree(@TempDir Path dir) throws Exception {
        Path repo = dir.resolve("guard-repo");
        Bundle escaping = bundleBypassingValidation("../escape.md", "owned");

        assertThatThrownBy(() -> new GitSink(repo, "Publish").write(escaping))
                .isInstanceOf(IOException.class)
                .hasMessage("Bundle path escapes the repository: ../escape.md");

        assertThat(Files.exists(dir.resolve("escape.md"))).isFalse();
        assertThat(Files.exists(repo.resolve("escape.md"))).isFalse();
    }

    /**
     * Builds a {@link Bundle} directly, skipping the builder's path validation. Uses the
     * package-private constructor rather than reflection, so a change to Bundle's shape breaks
     * this at compile time instead of at run time.
     */
    private static Bundle bundleBypassingValidation(String path, String content) {
        return new Bundle(Map.of(path, content.getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void zipIsCompleteOrderedAndDeterministic() throws Exception {
        byte[] once = Bundles.zip(sample());
        byte[] twice = Bundles.zip(sample());
        assertThat(once).isEqualTo(twice);

        List<String> names = new ArrayList<>();
        try (ZipInputStream zip = new ZipInputStream(new ByteArrayInputStream(once))) {
            for (ZipEntry entry = zip.getNextEntry(); entry != null; entry = zip.getNextEntry()) {
                names.add(entry.getName());
                if (entry.getName().equals("tables/orders.md")) {
                    assertThat(new String(zip.readAllBytes())).isEqualTo("orders");
                }
            }
        }
        assertThat(names).containsExactly("index.md", "tables/orders.md", "tables/customers.md");
    }
}
