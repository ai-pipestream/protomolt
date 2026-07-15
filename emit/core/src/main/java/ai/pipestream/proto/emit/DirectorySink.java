package ai.pipestream.proto.emit;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

/**
 * Writes a bundle under a root directory, creating parents as needed and overwriting
 * existing files. Write-only: files already present under the root that the bundle does not
 * name are left alone. {@link Bundle} path validation plus a resolved-path containment check
 * guarantee nothing lands outside the root.
 */
public final class DirectorySink implements BundleSink {

    private final Path root;

    public DirectorySink(Path root) {
        this.root = Objects.requireNonNull(root, "root").toAbsolutePath().normalize();
    }

    @Override
    public String write(Bundle bundle) throws IOException {
        Files.createDirectories(root);
        for (String path : bundle.paths()) {
            Path target = root.resolve(path).normalize();
            if (!target.startsWith(root)) {
                throw new IOException("Bundle path escapes the sink root: " + path);
            }
            Files.createDirectories(target.getParent());
            Files.write(target, bundle.file(path));
        }
        return root.toString();
    }
}
