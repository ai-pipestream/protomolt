package ai.pipestream.proto.iceberg;

import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * The Hadoop-free local {@code FileIO}: plain paths and both {@code file://} URI spellings
 * resolve to the same file, output files create their parent directories, content round-trips
 * through the input side, and deletes are idempotent. This is the IO a {@code file://}
 * warehouse catalog gets with {@code io-impl=...LocalFileIO}, so the path tolerance the REST
 * catalog's metadata relies on is pinned here.
 */
class LocalFileIOTest {

    @TempDir
    Path dir;

    @Test
    void outputFileCreatesMissingParentDirectories() throws Exception {
        Path file = dir.resolve("deeply/nested/data.parquet");
        LocalFileIO io = new LocalFileIO();

        OutputFile out = io.newOutputFile(file.toString());
        try (PositionOutputStream stream = out.create()) {
            stream.write("payload".getBytes(StandardCharsets.UTF_8));
        }

        assertThat(file).hasContent("payload");
    }

    @Test
    void whatTheOutputFileWroteTheInputFileReadsBack() throws Exception {
        Path file = dir.resolve("roundtrip.bin");
        byte[] bytes = {0, 1, 2, 3, -1, -128, 127};
        LocalFileIO io = new LocalFileIO();

        try (PositionOutputStream stream = io.newOutputFile(file.toString()).create()) {
            stream.write(bytes);
        }
        InputFile in = io.newInputFile(file.toString());
        byte[] read;
        try (InputStream stream = in.newStream()) {
            read = stream.readAllBytes();
        }

        assertThat(read).isEqualTo(bytes);
        assertThat(in.getLength()).isEqualTo(bytes.length);
        assertThat(in.location()).isEqualTo(file.toString());
    }

    @Test
    void fileUriSpellingsResolveToTheSameLocalFile() throws Exception {
        Path file = dir.resolve("uri.parquet");
        Files.writeString(file, "via-uri");
        LocalFileIO io = new LocalFileIO();

        // Both the canonical file:///abs and the single-slash file:/abs form metadata carries.
        for (String location : new String[]{"file://" + file, "file:" + file}) {
            try (InputStream stream = io.newInputFile(location).newStream()) {
                assertThat(new String(stream.readAllBytes(), StandardCharsets.UTF_8))
                        .as("location %s", location).isEqualTo("via-uri");
            }
        }
        // And an output file addressed by URI lands on the same local path. The file already
        // exists, so overwrite it (create() refuses to clobber, per Iceberg's contract).
        try (PositionOutputStream stream = io.newOutputFile("file://" + file).createOrOverwrite()) {
            stream.write("rewritten".getBytes(StandardCharsets.UTF_8));
        }
        assertThat(file).hasContent("rewritten");
    }

    @Test
    void deleteFileRemovesAndToleratesTheFileAlreadyBeingGone() throws Exception {
        Path file = dir.resolve("gone.parquet");
        Files.writeString(file, "x");
        LocalFileIO io = new LocalFileIO();

        io.deleteFile(file.toString());
        assertThat(file).doesNotExist();
        // A second delete of the same path is a no-op, not an error.
        assertThatCode(() -> io.deleteFile(file.toString())).doesNotThrowAnyException();
        assertThatCode(() -> io.deleteFile("file://" + file)).doesNotThrowAnyException();
    }

    @Test
    void initializeAcceptsAnyPropertiesWithoutEffect() {
        LocalFileIO io = new LocalFileIO();
        assertThatCode(() -> io.initialize(java.util.Map.of("warehouse", "/somewhere")))
                .doesNotThrowAnyException();
        // Still usable afterwards: catalogs call initialize before the IO serves files.
        assertThat(io.newInputFile(dir.resolve("f").toString())).isNotNull();
    }
}
