package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.spi.CallerIdentityExpansion;
import ai.protomolt.proto.repo.v1.IdentityLink;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Identity links read from a regular file of length-delimited {@link IdentityLink} messages,
 * reread under a single-flight lock when the file's canonical identity, size, or modification
 * timestamp changes. Every read enforces byte and link-count bounds and verifies that the file
 * stamp is stable before and after reading; an unreadable, oversized, malformed, or concurrently
 * mutating file fails closed without exposing host filesystem paths. Each load logs its link
 * count (and warns when the file holds none); a failed read logs once until the failure changes.
 * Mappings that produce {@code IdentityLinks} must be flattened to one {@link IdentityLink} per entry.
 */
final class IdentityLinkFile implements Supplier<CallerIdentityExpansion> {

    /** Environment variable naming the identity link file: {@value}. */
    static final String ENV_IDENTITY_LINKS = "PROTOMOLT_IDENTITY_LINKS";

    /** Maximum byte size accepted for the identity link file (64 MiB). */
    static final int DEFAULT_MAX_BYTES = 64 * 1024 * 1024;

    /** Maximum number of {@link IdentityLink} entries accepted from the file. */
    static final int DEFAULT_MAX_LINKS = 500_000;

    static final int MAX_STABLE_READ_ATTEMPTS = 3;

    private static final Logger LOG = LoggerFactory.getLogger(IdentityLinkFile.class);

    private record FileStamp(Path realPath, Object fileKey, FileTime lastModifiedTime, long size) {
    }

    private record Loaded(FileStamp stamp, CallerIdentityExpansion expansion) {
    }

    private final Path file;
    private final int maxBytes;
    private final int maxLinks;
    private final Object reloadLock = new Object();
    private volatile Loaded loaded;
    private volatile String lastFailure;

    IdentityLinkFile(Path file) {
        this(file, DEFAULT_MAX_BYTES, DEFAULT_MAX_LINKS);
    }

    IdentityLinkFile(Path file, int maxBytes, int maxLinks) {
        this.file = Objects.requireNonNull(file, "file");
        if (maxBytes < 1) {
            throw new IllegalArgumentException("maxBytes must be >= 1");
        }
        if (maxLinks < 1) {
            throw new IllegalArgumentException("maxLinks must be >= 1");
        }
        this.maxBytes = maxBytes;
        this.maxLinks = maxLinks;
        get();
    }

    /** The identity link file the environment names, read once now so a bad file fails startup. */
    static Optional<IdentityLinkFile> fromEnvironment(Map<String, String> environment) {
        Objects.requireNonNull(environment, "environment");
        String named = environment.get(ENV_IDENTITY_LINKS);
        if (named == null) {
            return Optional.empty();
        }
        String trimmed = named.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException(ENV_IDENTITY_LINKS
                    + " is set but blank: name an identity link file or unset it");
        }
        return Optional.of(new IdentityLinkFile(Path.of(trimmed)));
    }

    @Override
    public CallerIdentityExpansion get() {
        try {
            FileStamp currentStamp = stamp();
            Loaded fast = loaded;
            if (fast != null && fast.stamp().equals(currentStamp)) {
                return fast.expansion();
            }
            synchronized (reloadLock) {
                FileStamp rechecked = stamp();
                Loaded current = loaded;
                if (current != null && current.stamp().equals(rechecked)) {
                    return current.expansion();
                }
                Loaded reloaded = readStable();
                loaded = reloaded;
                lastFailure = null;
                return reloaded.expansion();
            }
        } catch (IOException | IllegalArgumentException e) {
            String failure = e.getClass().getSimpleName() + ": " + e.getMessage();
            if (!failure.equals(lastFailure)) {
                lastFailure = failure;
                LOG.error("Identity links named by {} are unusable; every bound call fails until fixed: {}",
                        ENV_IDENTITY_LINKS, failure);
            }
            throw new UncheckedIOException(
                    "failed to read the identity links named by " + ENV_IDENTITY_LINKS,
                    e instanceof IOException io ? io : new IOException("Invalid identity link entry", e));
        }
    }

    private Loaded readStable() throws IOException {
        for (int attempt = 1; attempt <= MAX_STABLE_READ_ATTEMPTS; attempt++) {
            FileStamp before = stamp();
            byte[] bytes = readBounded(before.realPath());
            FileStamp after = stamp();
            if (before.equals(after) && bytes.length == before.size()) {
                List<IdentityLink> links = parseLinks(bytes);
                if (links.isEmpty()) {
                    LOG.warn("Identity links named by {} hold no links; callers keep only their own identities",
                            ENV_IDENTITY_LINKS);
                } else {
                    LOG.info("Loaded {} identity links named by {}", links.size(), ENV_IDENTITY_LINKS);
                }
                return new Loaded(after, new CallerIdentityExpansion(links));
            }
        }
        throw new IOException("Identity link file changed concurrently during read");
    }

    private FileStamp stamp() throws IOException {
        Path real = file.toRealPath();
        BasicFileAttributes attributes = Files.readAttributes(real, BasicFileAttributes.class);
        if (!attributes.isRegularFile()) {
            throw new IOException("Identity link path is not a regular file");
        }
        if (attributes.size() > maxBytes) {
            throw new IOException("Identity link file exceeds maximum size of " + maxBytes + " bytes");
        }
        return new FileStamp(real, attributes.fileKey(), attributes.lastModifiedTime(), attributes.size());
    }

    private byte[] readBounded(Path realPath) throws IOException {
        try (InputStream in = Files.newInputStream(realPath)) {
            byte[] bytes = in.readNBytes(maxBytes + 1);
            if (bytes.length > maxBytes) {
                throw new IOException("Identity link file exceeds maximum size of " + maxBytes + " bytes");
            }
            return bytes;
        }
    }

    private List<IdentityLink> parseLinks(byte[] bytes) throws IOException {
        List<IdentityLink> links = new ArrayList<>();
        try (ByteArrayInputStream in = new ByteArrayInputStream(bytes)) {
            IdentityLink link;
            while ((link = IdentityLink.parseDelimitedFrom(in)) != null) {
                if (links.size() >= maxLinks) {
                    throw new IOException("Identity link file exceeds maximum count of " + maxLinks + " links");
                }
                links.add(link);
            }
        }
        return links;
    }
}
