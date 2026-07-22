package ai.pipestream.proto.gather;

import ai.pipestream.proto.sources.ProtoSource;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Objects;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Extracts {@code .proto} entries from a jar (or any zip) as {@link ProtoSource}s, preserving
 * each entry's in-jar path as its import path.
 *
 * <p>Shared by {@link JarProtoGatherer} and the Maven gatherer so a jar is read identically no
 * matter how it was obtained. {@code google/protobuf/**} entries are skipped by default: the
 * compiler ({@code ProtoSourceCompiler}) supplies the well-known types itself, and a jar's
 * bundled copies would conflict with (or needlessly shadow) the compiler-supplied ones.</p>
 */
public final class JarProtoExtraction {

    private static final String GOOGLE_WELL_KNOWN_PREFIX = "google/protobuf/";

    private JarProtoExtraction() {
    }

    /**
     * Extraction options.
     *
     * @param includeGoogleWellKnownTypes when {@code false} (the default),
     *        {@code google/protobuf/**} entries are skipped — the compiler supplies them
     * @param includeGlobs when non-empty, only entries matching at least one glob are extracted
     * @param excludeGlobs entries matching any glob are skipped
     */
    public record Options(boolean includeGoogleWellKnownTypes,
                          List<String> includeGlobs,
                          List<String> excludeGlobs) {

        public Options {
            includeGlobs = List.copyOf(includeGlobs);
            excludeGlobs = List.copyOf(excludeGlobs);
        }

        /** Well-known types skipped, no glob filters. */
        public static Options defaults() {
            return new Options(false, List.of(), List.of());
        }
    }

    /**
     * Extracts every matching {@code *.proto} entry of {@code jar}.
     *
     * @param jar    the jar file to read
     * @param origin origin recorded on each extracted source, e.g. {@code jar:common-1.2.jar}
     * @throws IOException when the jar cannot be read
     */
    public static List<ProtoSource> extract(Path jar, String origin, Options options) throws IOException {
        Objects.requireNonNull(jar, "jar");
        Objects.requireNonNull(origin, "origin");
        Objects.requireNonNull(options, "options");
        List<PathMatcher> includes = matchers(options.includeGlobs());
        List<PathMatcher> excludes = matchers(options.excludeGlobs());

        List<ProtoSource> sources = new ArrayList<>();
        try (ZipFile zip = new ZipFile(jar.toFile())) {
            Enumeration<? extends ZipEntry> entries = zip.entries();
            while (entries.hasMoreElements()) {
                ZipEntry entry = entries.nextElement();
                String name = entry.getName().replace('\\', '/');
                if (entry.isDirectory() || !name.endsWith(".proto")) {
                    continue;
                }
                if (!options.includeGoogleWellKnownTypes() && name.startsWith(GOOGLE_WELL_KNOWN_PREFIX)) {
                    continue;
                }
                Path asPath = Path.of(name);
                if (!includes.isEmpty() && includes.stream().noneMatch(m -> m.matches(asPath))) {
                    continue;
                }
                if (excludes.stream().anyMatch(m -> m.matches(asPath))) {
                    continue;
                }
                try (InputStream in = zip.getInputStream(entry)) {
                    sources.add(new ProtoSource(name, new String(in.readAllBytes(), StandardCharsets.UTF_8), origin));
                }
            }
        }
        return List.copyOf(sources);
    }

    private static List<PathMatcher> matchers(List<String> globs) {
        return globs.stream()
                .map(glob -> FileSystems.getDefault().getPathMatcher("glob:" + glob))
                .toList();
    }
}
