package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.v1.SchemaToolIdentity;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

/** Reads local build evidence; neither discovers a dependency closure nor loads executable code. */
final class DocumentRuntimeInventory {
    private static final String HEADER = "protomolt-admission-runtime-inventory/v1";
    private static final int MAX_MANIFEST_BYTES = 128 * 1024;
    private static final long MAX_ARTIFACT_BYTES = 1024L * 1024 * 1024;
    private final List<Entry> entries;
    private record Entry(String name, String hash, long size) {}
    record Anchor(String artifact, Class<?> type) {
        Anchor { Objects.requireNonNull(artifact); Objects.requireNonNull(type); }
    }
    private DocumentRuntimeInventory(List<Entry> entries) { this.entries = List.copyOf(entries); }

    /** Caller requires successful build generation and keeps bundle/origins immutable during use. */
    static DocumentRuntimeInventory read(Path bundle, Runnable control) throws IOException {
        active(control);
        var root = Objects.requireNonNull(bundle).toAbsolutePath().normalize();
        directory(root); directory(root.resolve("artifacts"));
        var manifest = root.resolve("inventory.tsv");
        var before = regular(manifest);
        if (before.size() > MAX_MANIFEST_BYTES) throw new IOException("Runtime inventory exceeds manifest byte bound");
        var bytes = new ByteArrayOutputStream();
        byte[] buffer = new byte[8192];
        try (var input = Files.newInputStream(manifest, LinkOption.NOFOLLOW_LINKS)) {
            for (int n; (n = input.read(buffer)) != -1;) {
                active(control);
                if (n > MAX_MANIFEST_BYTES - bytes.size()) throw new IOException("Runtime inventory exceeds manifest byte bound");
                bytes.write(buffer, 0, n);
            }
        }
        var after = regular(manifest);
        if (bytes.size() != before.size() || after.size() != before.size()
                || !after.lastModifiedTime().equals(before.lastModifiedTime()) || !Objects.equals(after.fileKey(), before.fileKey()))
            throw new IOException("Runtime inventory changed during read");
        String text = StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(bytes.toByteArray())).toString();
        String[] lines = text.split("\n", -1);
        if (lines.length < 3 || lines.length > 66 || !lines[0].equals(HEADER) || !lines[lines.length - 1].isEmpty())
            throw new IOException("Invalid runtime inventory header, count or final newline");
        var entries = new ArrayList<Entry>();
        long total = 0;
        String previous = null;
        for (int i = 1; i < lines.length - 1; i++) {
            active(control);
            var fields = lines[i].split("\t", -1);
            if (fields.length != 4 || fields[0].isBlank() || fields[0].length() > 200 || fields[0].chars().anyMatch(Character::isISOControl)
                    || (previous != null && previous.compareTo(fields[0]) >= 0)
                    || !fields[1].matches("[0-9a-f]{64}") || !fields[2].matches("[1-9][0-9]{0,9}")
                    || !fields[3].equals("artifacts/" + fields[1] + ".jar"))
                throw new IOException("Invalid or noncanonical runtime inventory row");
            long size = Long.parseLong(fields[2]);
            if (size > MAX_ARTIFACT_BYTES - total) throw new IOException("Runtime inventory exceeds aggregate byte bound");
            total += size;
            entries.add(new Entry(fields[0], fields[1], size));
            previous = fields[0];
        }
        // Validate the entire bounded manifest before opening its artifact files.
        for (var entry : entries) {
            active(control);
            var file = root.resolve("artifacts/" + entry.hash() + ".jar");
            if (regular(file).size() != entry.size()) throw new IOException("Runtime artifact size does not match inventory");
            var observed = DocumentRuntimeArtifact.observe(entry.name(), file, limits(entry), control);
            if (!observed.getArtifactSha256().equals(entry.hash())) throw new IOException("Runtime artifact hash does not match inventory");
            try (var ignored = new java.util.zip.ZipFile(file.toFile())) { /* Check archive structure without extraction. */ }
        }
        active(control);
        return new DocumentRuntimeInventory(entries);
    }

    /**
     * Checks caller-selected loaded classes against observed bundle identities. This does not
     * prove that unselected classes/resources came from this closure or attest JVM loaded bytes.
     */
    void verifyAnchors(List<Anchor> anchors, Runnable control) throws IOException {
        active(control);
        if (anchors.isEmpty() || anchors.size() > 256) throw new IllegalArgumentException("Invalid runtime anchor count");
        var seen = new HashSet<Class<?>>();
        for (var anchor : List.copyOf(anchors)) {
            active(control);
            if (!seen.add(anchor.type())) throw new IllegalArgumentException("Duplicate runtime anchor class");
            var entry = entries.stream().filter(e -> e.name().equals(anchor.artifact())).findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Runtime anchor artifact is absent from inventory"));
            var observed = DocumentRuntimeArtifact.observe(entry.name(), anchor.type(), limits(entry), control);
            if (!observed.getVersion().equals("file-sha256:" + entry.hash()))
                throw new IOException("Runtime anchor origin does not match inventory");
        }
        active(control);
    }

    List<SchemaToolIdentity> identities() {
        return entries.stream().map(entry -> SchemaToolIdentity.newBuilder().setName(entry.name())
                .setVersion("file-sha256:" + entry.hash()).setArtifactSha256(entry.hash()).build()).toList();
    }
    private static DocumentRuntimeArtifact.Limits limits(Entry entry) {
        return new DocumentRuntimeArtifact.Limits(entry.size(), 65536, 64);
    }
    private static BasicFileAttributes regular(Path path) throws IOException {
        var attributes = Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (!attributes.isRegularFile()) throw new IOException("Runtime inventory requires regular files without links");
        return attributes;
    }
    private static void directory(Path path) throws IOException {
        if (!Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) throw new IOException("Runtime inventory requires directories without links");
    }
    private static void active(Runnable control) {
        if (Thread.currentThread().isInterrupted()) throw new java.util.concurrent.CancellationException("Runtime inventory interrupted");
        Objects.requireNonNull(control).run();
    }
}
