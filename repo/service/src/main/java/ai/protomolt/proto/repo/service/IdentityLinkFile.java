package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.spi.CallerIdentityExpansion;
import ai.protomolt.proto.repo.v1.IdentityLink;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Identity links read from a file of length-delimited {@link IdentityLink} messages,
 * reread when the file's size or modification time changes. A file that cannot be read
 * fails the call that asked for it.
 */
final class IdentityLinkFile implements Supplier<CallerIdentityExpansion> {

    /** Environment variable naming the identity link file: {@value}. */
    static final String ENV_IDENTITY_LINKS = "PROTOMOLT_IDENTITY_LINKS";

    private record Loaded(Object fileKey, long size, long modified, CallerIdentityExpansion expansion) {
    }

    private final Path file;
    private volatile Loaded loaded;

    IdentityLinkFile(Path file) {
        this.file = Objects.requireNonNull(file, "file");
        get();
    }

    /** The identity link file the environment names, read once now so a bad file fails the boot. */
    static Optional<IdentityLinkFile> fromEnvironment(Map<String, String> environment) {
        String named = environment.get(ENV_IDENTITY_LINKS);
        if (named == null) return Optional.empty();
        if (named.isBlank()) {
            throw new IllegalArgumentException(ENV_IDENTITY_LINKS
                    + " is set but blank: name an identity link file or unset it");
        }
        return Optional.of(new IdentityLinkFile(Path.of(named)));
    }

    @Override
    public CallerIdentityExpansion get() {
        try {
            var attributes = Files.readAttributes(file, BasicFileAttributes.class);
            long modified = attributes.lastModifiedTime().toMillis();
            Loaded current = loaded;
            if (current != null && current.size() == attributes.size() && current.modified() == modified
                    && Objects.equals(current.fileKey(), attributes.fileKey())) {
                return current.expansion();
            }
            List<IdentityLink> links = new ArrayList<>();
            try (InputStream in = Files.newInputStream(file)) {
                IdentityLink link;
                while ((link = IdentityLink.parseDelimitedFrom(in)) != null) links.add(link);
            }
            var expansion = new CallerIdentityExpansion(links);
            loaded = new Loaded(attributes.fileKey(), attributes.size(), modified, expansion);
            return expansion;
        } catch (IOException e) {
            throw new UncheckedIOException("failed to read the identity links named by "
                    + ENV_IDENTITY_LINKS + " at " + file + ": " + e.getMessage(), e);
        }
    }
}
