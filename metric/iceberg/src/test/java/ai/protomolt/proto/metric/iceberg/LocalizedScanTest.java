package ai.protomolt.proto.metric.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Localizing a scan: local paths pass through untouched, object-store
 * locations materialize byte-identically through the table's FileIO into
 * scratch that closing removes, and the plan note says exactly what
 * moved — nothing when nothing did.
 */
class LocalizedScanTest {

    @TempDir
    Path work;

    @Test
    void localPathsPassThroughAndNothingIsNoted() throws Exception {
        Path parquet = work.resolve("part-0.parquet");
        Files.writeString(parquet, "local");
        try (LocalizedScan scan = LocalizedScan.of(
                List.of(parquet.toString()), new InMemoryFileIO())) {
            assertThat(scan.localPaths()).containsExactly(parquet.toString());
            assertThat(scan.note()).isEmpty();
        }
    }

    @Test
    void objectStoreLocationsMaterializeAndCloseCleansUp() throws Exception {
        InMemoryFileIO io = new InMemoryFileIO();
        byte[] bytes = "remote-bytes".getBytes(StandardCharsets.UTF_8);
        io.addFile("s3://lake/orders/part-1.parquet", bytes);
        Path local = work.resolve("part-0.parquet");
        Files.writeString(local, "local");

        Path materialized;
        try (LocalizedScan scan = LocalizedScan.of(
                List.of(local.toString(), "s3://lake/orders/part-1.parquet"), io)) {
            assertThat(scan.localPaths()).hasSize(2);
            assertThat(scan.localPaths().get(0)).isEqualTo(local.toString());
            materialized = Path.of(scan.localPaths().get(1));
            assertThat(materialized).exists();
            assertThat(Files.readAllBytes(materialized)).isEqualTo(bytes);
            assertThat(scan.note())
                    .isEqualTo(" -- 1 of 2 data files materialized from the object store");
        }
        assertThat(materialized).as("closing removed the scratch").doesNotExist();
        assertThat(local).as("the local file is not the scan's to remove").exists();
    }
}
