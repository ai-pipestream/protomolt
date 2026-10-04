package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.v1.SchemaToolIdentity;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;

/** Observes immutable local build content. This is not loaded-byte attestation or dependency discovery. */
final class DocumentRuntimeArtifact {
    private DocumentRuntimeArtifact() {}
    record Limits(long maxBytes, int maxEntries, int maxDepth) {
        Limits {
            if (maxBytes < 1 || maxBytes > 1024L * 1024 * 1024 || maxEntries < 1 || maxEntries > 65536 || maxDepth < 1 || maxDepth > 64)
                throw new IllegalArgumentException("Invalid runtime artifact bounds");
        }
    }
    private record Entry(Path path, String relative, long size, java.nio.file.attribute.FileTime modified, Object key) {}

    /**
     * Attributed origin must be a local regular artifact or exploded directory. Hosts keep
     * code immutable after observation and separately enumerate resources/dependencies.
     * No URL fetch, class loading, executable transfer or unknown-version fallback.
     */
    static SchemaToolIdentity observe(String name, Class<?> anchor, Limits limits, Runnable control) throws IOException {
        Objects.requireNonNull(anchor); active(control);
        var domain = anchor.getProtectionDomain();
        var source = domain == null ? null : domain.getCodeSource();
        if (source == null || source.getLocation() == null || !source.getLocation().getProtocol().equals("file"))
            throw new IllegalArgumentException("Runtime class has no local code origin");
        final Path path;
        try { path = Path.of(source.getLocation().toURI()); }
        catch (java.net.URISyntaxException invalid) { throw new IllegalArgumentException("Invalid runtime code origin", invalid); }
        return observe(name, path, limits, control);
    }

    /**
     * Content-derived build identity is explicit and never presented as a release
     * version. Tree/v1 covers regular files and names, not empty-directory topology.
     */
    static SchemaToolIdentity observe(String name, Path input, Limits limits, Runnable control) throws IOException {
        Objects.requireNonNull(input); Objects.requireNonNull(limits); active(control);
        if (name == null || name.isBlank() || name.length() > 200) throw new IllegalArgumentException("Invalid runtime artifact name");
        StandardCharsets.UTF_8.newEncoder().encode(java.nio.CharBuffer.wrap(name));
        var root = input.toAbsolutePath().normalize();
        var attributes = Files.readAttributes(root, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        var digest = sha256();
        byte[] buffer = new byte[8192];
        String kind;
        if (attributes.isRegularFile()) {
            if (attributes.size() > limits.maxBytes()) throw new IllegalArgumentException("Runtime artifact byte bound exceeded");
            read(entry(root, "", attributes), digest, buffer, limits.maxBytes(), control);
            kind = "file-sha256:";
        } else if (attributes.isDirectory()) {
            var entries = inventory(root, limits, control);
            if (entries.isEmpty()) throw new IllegalArgumentException("Empty runtime artifact directory");
            digest.update("protomolt-runtime-tree/v1\0".getBytes(StandardCharsets.UTF_8));
            for (var file : entries) {
                active(control);
                byte[] path = file.relative().getBytes(StandardCharsets.UTF_8);
                digest.update(ByteBuffer.allocate(4).putInt(path.length).array()); digest.update(path);
                digest.update(ByteBuffer.allocate(8).putLong(file.size()).array());
                read(file, digest, buffer, file.size(), control);
            }
            if (!entries.equals(inventory(root, limits, control))) throw new IOException("Runtime artifact changed during observation");
            kind = "tree-v1-sha256:";
        } else throw new IllegalArgumentException("Runtime artifact must be regular; links and special files are unsupported");
        active(control);
        String hash = HexFormat.of().formatHex(digest.digest());
        return SchemaToolIdentity.newBuilder().setName(name).setVersion(kind + hash).setArtifactSha256(hash).build();
    }

    private static List<Entry> inventory(Path root, Limits limits, Runnable control) throws IOException {
        var entries = new ArrayList<Entry>();
        long bytes = 0;
        int visited = 0;
        // Depth + 1 exposes a directory at the boundary so truncation cannot silently omit descendants.
        try (var paths = Files.walk(root, limits.maxDepth() + 1)) {
            var iterator = paths.iterator();
            while (iterator.hasNext()) {
                active(control);
                var path = iterator.next();
                if (path.equals(root)) continue;
                if (++visited > limits.maxEntries()) throw new IllegalArgumentException("Runtime artifact entry bound exceeded");
                var relative = root.relativize(path);
                if (!Path.of(relative.toString()).equals(relative)) throw new IOException("Runtime artifact path is not lossless text");
                if (relative.getNameCount() > limits.maxDepth()) throw new IllegalArgumentException("Runtime artifact depth bound exceeded");
                var attributes = Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
                if (attributes.isDirectory()) continue;
                if (!attributes.isRegularFile()) throw new IllegalArgumentException("Runtime artifact contains a link or special file");
                if (attributes.size() > limits.maxBytes() - bytes) throw new IllegalArgumentException("Runtime artifact byte bound exceeded");
                bytes += attributes.size();
                entries.add(entry(path, relative.toString().replace(java.io.File.separatorChar, '/'), attributes));
            }
        } catch (java.io.UncheckedIOException failed) { throw failed.getCause(); }
        // Java UTF-16 ordering is part of tree/v1; names are length-prefixed UTF-8.
        entries.sort(Comparator.comparing(Entry::relative));
        return List.copyOf(entries);
    }
    private static Entry entry(Path path, String relative, BasicFileAttributes attributes) {
        return new Entry(path, relative, attributes.size(), attributes.lastModifiedTime(), attributes.fileKey());
    }
    private static void read(Entry expected, MessageDigest digest, byte[] buffer, long limit, Runnable control) throws IOException {
        active(control);
        if (!expected.equals(entry(expected.path(), expected.relative(),
                Files.readAttributes(expected.path(), BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS))))
            throw new IOException("Runtime artifact changed before read");
        long count = 0;
        try (var input = Files.newInputStream(expected.path(), LinkOption.NOFOLLOW_LINKS)) {
            for (int n; (n = input.read(buffer)) != -1;) {
                active(control);
                if (n > limit - count) throw new IOException("Runtime artifact grew during observation");
                digest.update(buffer, 0, n); count += n;
            }
        }
        if (count != expected.size() || !expected.equals(entry(expected.path(), expected.relative(),
                Files.readAttributes(expected.path(), BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS))))
            throw new IOException("Runtime artifact changed during read");
        active(control);
    }
    private static MessageDigest sha256() {
        try { return MessageDigest.getInstance("SHA-256"); }
        catch (NoSuchAlgorithmException unavailable) { throw new IllegalStateException("SHA-256 unavailable", unavailable); }
    }
    private static void active(Runnable control) {
        if (Thread.currentThread().isInterrupted()) throw new java.util.concurrent.CancellationException("Runtime observation interrupted");
        Objects.requireNonNull(control).run();
    }
}
