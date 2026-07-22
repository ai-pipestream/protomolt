package ai.pipestream.proto.gather;

import ai.pipestream.proto.sources.ProtoSourceSet;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Gathers {@code .proto} files from local directories.
 *
 * <p>Two configuration styles, combinable:</p>
 * <ul>
 *   <li><b>Explicit roots</b> ({@link Builder#root(Path)}): every {@code *.proto} under a root
 *       is gathered with its import path relative to that root, so
 *       {@code <root>/common/v1/a.proto} becomes {@code common/v1/a.proto}.</li>
 *   <li><b>Scan root</b> ({@link Builder#scanRoot(Path)}): the tree under the scan root is
 *       searched for nested {@code src/main/proto} directories (the conventional layout of a
 *       multi-module checkout) and each discovered directory is treated as a proto root.
 *       Hidden directories ({@code .git}, {@code .idea}, …) and {@code build} directories are
 *       not descended into, so stale build output cannot shadow real sources.</li>
 * </ul>
 *
 * <p>Non-{@code .proto} files are ignored. The same import path may be produced by more than
 * one root only with identical content; differing content is a {@link GatherException} naming
 * both origins. A missing root fails by default; {@link Builder#failIfMissing(boolean)
 * failIfMissing(false)} skips missing roots instead.</p>
 */
public final class FilesystemProtoGatherer implements ProtoGatherer {

    private final List<Path> roots;
    private final Path scanRoot;
    private final boolean failIfMissing;

    private FilesystemProtoGatherer(Builder builder) {
        this.roots = List.copyOf(builder.roots);
        this.scanRoot = builder.scanRoot;
        this.failIfMissing = builder.failIfMissing;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public ProtoSourceSet gather() throws GatherException {
        ProtoSourceSet.Builder builder = ProtoSourceSet.builder();
        try {
            for (Path root : roots) {
                Path normalized = root.toAbsolutePath().normalize();
                if (!Files.isDirectory(normalized)) {
                    if (failIfMissing) {
                        throw new GatherException("Proto root directory does not exist: " + normalized);
                    }
                    continue;
                }
                gatherTree(normalized, builder);
            }
            if (scanRoot != null) {
                Path normalized = scanRoot.toAbsolutePath().normalize();
                if (!Files.isDirectory(normalized)) {
                    if (failIfMissing) {
                        throw new GatherException("Proto scan root does not exist: " + normalized);
                    }
                } else {
                    for (Path discovered : discoverProtoRoots(normalized)) {
                        gatherTree(discovered, builder);
                    }
                }
            }
        } catch (IOException e) {
            throw new GatherException("Failed reading proto sources from the filesystem", e);
        } catch (IllegalStateException e) {
            throw new GatherException("Conflicting proto sources: " + e.getMessage(), e);
        }
        return builder.build();
    }

    private static void gatherTree(Path root, ProtoSourceSet.Builder builder) throws IOException {
        List<Path> protos;
        try (Stream<Path> files = Files.walk(root)) {
            protos = files
                    .filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().endsWith(".proto"))
                    .sorted()
                    .toList();
        }
        for (Path proto : protos) {
            String importPath = toImportPath(root.relativize(proto));
            builder.add(importPath, Files.readString(proto), "file:" + proto);
        }
    }

    /** Directories ending in {@code src/main/proto}, skipping hidden and {@code build} dirs. */
    private static List<Path> discoverProtoRoots(Path scanRoot) throws IOException {
        List<Path> discovered = new ArrayList<>();
        Files.walkFileTree(scanRoot, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                String name = dir.getFileName() == null ? "" : dir.getFileName().toString();
                if (!dir.equals(scanRoot) && (name.startsWith(".") || name.equals("build"))) {
                    return FileVisitResult.SKIP_SUBTREE;
                }
                if (dir.endsWith(Path.of("src", "main", "proto"))) {
                    discovered.add(dir);
                    return FileVisitResult.SKIP_SUBTREE;
                }
                return FileVisitResult.CONTINUE;
            }
        });
        discovered.sort(Path::compareTo);
        return discovered;
    }

    private static String toImportPath(Path relative) {
        return relative.toString().replace('\\', '/');
    }

    @Override
    public String origin() {
        Stream<String> rootOrigins = roots.stream().map(root -> "file:" + root.toAbsolutePath().normalize());
        Stream<String> scanOrigin = scanRoot == null
                ? Stream.empty()
                : Stream.of("scan:" + scanRoot.toAbsolutePath().normalize());
        return Stream.concat(rootOrigins, scanOrigin).collect(Collectors.joining(",", "filesystem[", "]"));
    }

    /** Builder for {@link FilesystemProtoGatherer}; at least one root or a scan root is required. */
    public static final class Builder {

        private final List<Path> roots = new ArrayList<>();
        private Path scanRoot;
        private boolean failIfMissing = true;

        private Builder() {
        }

        /** Adds a proto root directory; import paths are relative to it. */
        public Builder root(Path root) {
            roots.add(Objects.requireNonNull(root, "root"));
            return this;
        }

        /** Adds proto root directories; import paths are relative to each. */
        public Builder roots(Collection<Path> roots) {
            Objects.requireNonNull(roots, "roots").forEach(this::root);
            return this;
        }

        /** Discovers nested {@code src/main/proto} trees under the given root. */
        public Builder scanRoot(Path scanRoot) {
            this.scanRoot = Objects.requireNonNull(scanRoot, "scanRoot");
            return this;
        }

        /**
         * Whether a missing root (or scan root) fails the gather ({@code true}, the default)
         * or is silently skipped ({@code false}).
         */
        public Builder failIfMissing(boolean failIfMissing) {
            this.failIfMissing = failIfMissing;
            return this;
        }

        public FilesystemProtoGatherer build() {
            if (roots.isEmpty() && scanRoot == null) {
                throw new IllegalStateException("At least one root or a scanRoot is required");
            }
            return new FilesystemProtoGatherer(this);
        }
    }
}
