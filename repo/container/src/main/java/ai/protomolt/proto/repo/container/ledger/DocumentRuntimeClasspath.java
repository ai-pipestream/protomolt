package ai.protomolt.proto.repo.container.ledger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.zip.ZipFile;

/** Qualifies an explicitly enumerated immutable standard JAR classpath, without loading code. */
final class DocumentRuntimeClasspath {
    private static final long MAX_BYTES = 1024L * 1024 * 1024;
    private static final int MAX_ENTRIES = 250_000;
    private static final int MAX_ENTRY_NAME = 4096;
    private record Artifact(Path path, String hash, long size, java.nio.file.attribute.FileTime modified, Object key) {}
    private DocumentRuntimeClasspath() {}

    /**
     * Every inventory blob must be present exactly once by content. Other host JARs may
     * coexist but cannot provide any of the same classes. The caller separately verifies
     * the actual loader topology and anchor origins; this method accepts paths, not authority.
     */
    static Map<String, Path> verify(DocumentRuntimeInventory inventory, List<Path> classpath, Runnable control) throws IOException {
        active(control);
        if (classpath.isEmpty() || classpath.size() > 256) throw new IOException("Runtime classpath artifact count exceeds bounds");
        var expected = new HashSet<String>();
        inventory.identities().forEach(identity -> expected.add(identity.getArtifactSha256()));
        var matched = new HashMap<String, Path>();
        var artifacts = new ArrayList<Artifact>();
        var paths = new HashSet<Path>();
        long total = 0;
        for (var input : List.copyOf(classpath)) {
            active(control);
            var path = input.toAbsolutePath().normalize();
            if (!paths.add(path)) throw new IOException("Duplicate runtime classpath path");
            var attributes = Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            if (!attributes.isRegularFile() || !path.getFileName().toString().endsWith(".jar"))
                throw new IOException("Runtime classpath requires regular JAR files without links");
            long size = attributes.size();
            if (size < 1 || size > MAX_BYTES - total) throw new IOException("Runtime classpath exceeds aggregate byte bound");
            total += size;
            var identity = DocumentRuntimeArtifact.observe("runtime-classpath", path,
                    new DocumentRuntimeArtifact.Limits(size, 65536, 64), control);
            var hash = identity.getArtifactSha256();
            if (expected.contains(hash) && matched.putIfAbsent(hash, path) != null)
                throw new IOException("Duplicate runtime provider for inventory artifact");
            artifacts.add(new Artifact(path, hash, size, attributes.lastModifiedTime(), attributes.fileKey()));
        }
        if (!matched.keySet().equals(expected)) throw new IOException("Runtime classpath is missing inventory artifacts");

        // Inventory providers first, so additional host classes cannot hide duplicates by order.
        artifacts.sort(java.util.Comparator.comparing(artifact -> !expected.contains(artifact.hash())));
        var inventoryClasses = new HashMap<String, Path>();
        int visited = 0;
        long nameBytes = 0;
        for (var artifact : artifacts) {
            active(control);
            unchanged(artifact);
            boolean selected = expected.contains(artifact.hash());
            // Multi-release entries participate conservatively, including releases not active on this JVM.
            // Manifest Class-Path would extend the enumerated closure and is deliberately unsupported.
            try (var jar = new ZipFile(artifact.path().toFile())) {
                var entries = jar.entries();
                var localNames = new HashSet<String>();
                boolean seenManifest = false;
                while (entries.hasMoreElements()) {
                    active(control);
                    var entry = entries.nextElement();
                    if (++visited > MAX_ENTRIES) throw new IOException("Runtime classpath ZIP entry bound exceeded");
                    String name = entry.getName();
                    if (name.length() > MAX_ENTRY_NAME || (nameBytes += name.length()) > 16 * 1024 * 1024)
                        throw new IOException("Runtime classpath ZIP name bound exceeded");
                    if (!localNames.add(name)) throw new IOException("Duplicate runtime JAR entry");
                    if (name.equalsIgnoreCase("META-INF/MANIFEST.MF")) {
                        if (seenManifest) throw new IOException("Ambiguous runtime JAR manifest");
                        seenManifest = true;
                        try { checkManifest(jar, entry, control); }
                        catch (IOException invalid) {
                            throw new IOException(invalid.getMessage() + " in " + artifact.path(), invalid);
                        }
                    }
                    if (entry.isDirectory() || !name.endsWith(".class")) continue;
                    String className = className(name);
                    if (className.equals("module-info.class")) continue;
                    var prior = inventoryClasses.get(className);
                    if (prior != null && !prior.equals(artifact.path()))
                        throw new IOException("Runtime classpath shadows inventory class: " + className);
                    if (selected) inventoryClasses.put(className, artifact.path());
                }
            }
            unchanged(artifact);
        }
        active(control);
        return Map.copyOf(matched);
    }

    private static void unchanged(Artifact artifact) throws IOException {
        var current = Files.readAttributes(artifact.path(), BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (!current.isRegularFile() || current.size() != artifact.size() || !current.lastModifiedTime().equals(artifact.modified())
                || !Objects.equals(current.fileKey(), artifact.key())) throw new IOException("Runtime classpath artifact changed during observation");
    }

    private static void checkManifest(ZipFile jar, java.util.zip.ZipEntry entry, Runnable control) throws IOException {
        // Hibernate's production OSGi metadata currently occupies 171,463 bytes.
        // This bounded startup parse still inspects the complete manifest, so a
        // Class-Path attribute cannot hide beyond the former 64 KiB boundary.
        final int limit = 256 * 1024;
        if (entry.getSize() > limit) throw new IOException("Runtime JAR manifest exceeds byte bound");
        var bytes = new java.io.ByteArrayOutputStream();
        byte[] buffer = new byte[8192];
        try (var input = jar.getInputStream(entry)) {
            for (int n; (n = input.read(buffer)) != -1;) {
                active(control);
                if (n > limit - bytes.size()) throw new IOException("Runtime JAR manifest exceeds byte bound");
                bytes.write(buffer, 0, n);
            }
        }
        var manifest = new java.util.jar.Manifest(new java.io.ByteArrayInputStream(bytes.toByteArray()));
        if (manifest.getMainAttributes().getValue("Class-Path") != null)
            throw new IOException("Manifest Class-Path is unsupported for admission runtime evidence");
    }

    private static String className(String name) throws IOException {
        String prefix = "META-INF/versions/";
        if (!name.startsWith(prefix)) return name;
        int separator = name.indexOf('/', prefix.length());
        if (separator < 0 || !name.substring(prefix.length(), separator).matches("[1-9][0-9]{0,8}"))
            throw new IOException("Malformed multi-release class entry");
        return name.substring(separator + 1);
    }
    private static void active(Runnable control) {
        if (Thread.currentThread().isInterrupted()) throw new java.util.concurrent.CancellationException("Runtime classpath observation interrupted");
        Objects.requireNonNull(control).run();
    }
}
