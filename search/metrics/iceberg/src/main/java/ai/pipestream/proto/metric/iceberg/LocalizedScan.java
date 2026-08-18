package ai.pipestream.proto.metric.iceberg;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.SeekableInputStream;

/**
 * The scan's files as local paths DuckDB can read. Local locations pass
 * through untouched; object-store locations ({@code s3://...} and every
 * other non-local scheme) materialize through the table's own
 * {@link FileIO} into a temporary directory for the query's duration —
 * the reader reaches exactly what the catalog reaches, no second
 * credential path and no DuckDB extension, at the cost of moving the
 * scanned bytes once. Closing removes everything materialized.
 */
final class LocalizedScan implements AutoCloseable {

    private final List<String> localPaths;
    private final Path scratch;
    private final int materialized;

    private LocalizedScan(List<String> localPaths, Path scratch, int materialized) {
        this.localPaths = localPaths;
        this.scratch = scratch;
        this.materialized = materialized;
    }

    /**
     * Localizes the scan.
     *
     * @param locations the data files' locations, as the table records them
     * @param io the table's file plane, used for non-local locations
     * @return the localized scan; close it after the query
     */
    static LocalizedScan of(List<String> locations, FileIO io) {
        List<String> localPaths = new ArrayList<>(locations.size());
        Path scratch = null;
        int materialized = 0;
        try {
            for (String location : locations) {
                if (location.startsWith("/")) {
                    localPaths.add(location);
                    continue;
                }
                if (location.startsWith("file:")) {
                    localPaths.add(Path.of(java.net.URI.create(location)).toString());
                    continue;
                }
                if (scratch == null) {
                    scratch = Files.createTempDirectory("protomolt-metric-scan");
                }
                String name = location.substring(location.lastIndexOf('/') + 1);
                Path target = scratch.resolve("%05d-%s".formatted(materialized, name));
                try (SeekableInputStream in = io.newInputFile(location).newStream()) {
                    Files.copy(in, target);
                }
                localPaths.add(target.toString());
                materialized++;
            }
        } catch (IOException | RuntimeException e) {
            remove(scratch);
            if (e instanceof IOException io_) {
                throw new UncheckedIOException(
                        "cannot materialize the scan's object-store files", io_);
            }
            throw (RuntimeException) e;
        }
        return new LocalizedScan(List.copyOf(localPaths), scratch, materialized);
    }

    /** The scan's files as local filesystem paths. */
    List<String> localPaths() {
        return localPaths;
    }

    /** Plan evidence: what was moved, or empty when everything was local. */
    String note() {
        return materialized == 0
                ? ""
                : " -- " + materialized + " of " + localPaths.size()
                        + " data files materialized from the object store";
    }

    @Override
    public void close() {
        remove(scratch);
    }

    private static void remove(Path scratch) {
        if (scratch == null) {
            return;
        }
        try (Stream<Path> entries = Files.walk(scratch)) {
            entries.sorted(Comparator.reverseOrder()).forEach(entry -> {
                try {
                    Files.deleteIfExists(entry);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        } catch (IOException | UncheckedIOException e) {
            // Scratch cleanup is best effort; the OS temp dir is the backstop.
        }
    }
}
