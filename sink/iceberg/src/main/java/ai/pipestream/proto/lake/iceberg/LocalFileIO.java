package ai.pipestream.proto.lake.iceberg;

import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/**
 * A {@code FileIO} for {@code file://} warehouses with no Hadoop anywhere: Iceberg's default
 * resolver hands local paths to {@code HadoopFileIO}, whose filesystem cache calls
 * {@code Subject.getSubject} — gone since JDK 24 (JEP 486). This one is plain {@code java.nio}.
 * Point a catalog at it with {@code io-impl=ai.pipestream.proto.lake.iceberg.LocalFileIO};
 * useful for local warehouses, shared-volume test rigs, and anywhere else the data lives on
 * a real filesystem.
 */
public final class LocalFileIO implements FileIO {

    /** Catalogs instantiate FileIO reflectively; both the no-arg ctor and initialize run. */
    public LocalFileIO() {
    }

    @Override
    public void initialize(Map<String, String> properties) {
        // nothing to configure: the path is the whole story
    }

    @Override
    public InputFile newInputFile(String path) {
        return org.apache.iceberg.Files.localInput(toFile(path));
    }

    @Override
    public OutputFile newOutputFile(String path) {
        File file = toFile(path);
        File parent = file.getParentFile();
        if (parent != null) {
            try {
                Files.createDirectories(parent.toPath());
            } catch (IOException e) {
                throw new UncheckedIOException("Cannot create " + parent, e);
            }
        }
        return org.apache.iceberg.Files.localOutput(file);
    }

    @Override
    public void deleteFile(String path) {
        try {
            Files.deleteIfExists(toFile(path).toPath());
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot delete " + path, e);
        }
    }

    private static File toFile(String location) {
        if (location.startsWith("file:")) {
            // Tolerate both file:///abs and the single-slash form metadata sometimes carries.
            URI uri = URI.create(location.replaceFirst("^file:/+", "file:///"));
            return Path.of(uri).toFile();
        }
        return new File(location);
    }
}
