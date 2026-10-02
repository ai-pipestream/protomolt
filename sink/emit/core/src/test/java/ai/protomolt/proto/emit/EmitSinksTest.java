package ai.protomolt.proto.emit;

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

/** Bundle paths, directory delivery and deterministic ZIP rendering. */
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
