package ai.pipestream.proto.emit;

import org.eclipse.jgit.api.Git;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
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

    @Test
    void zipIsCompleteOrderedAndDeterministic() throws Exception {
        byte[] once = Bundles.zip(sample());
        byte[] twice = Bundles.zip(sample());
        assertThat(once).isEqualTo(twice);

        List<String> names = new java.util.ArrayList<>();
        try (ZipInputStream zip = new ZipInputStream(new java.io.ByteArrayInputStream(once))) {
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
