package ai.pipestream.proto.gather.git;

import ai.pipestream.proto.gather.GatherException;
import ai.pipestream.proto.gather.ProtoGatherer;
import ai.pipestream.proto.sources.ProtoSourceSet;

import org.eclipse.jgit.transport.CredentialsProvider;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Gathers {@code .proto} files from a git repository through a persistent clone cache
 * ({@link GitCloneCache}: clone on first use, fetch + hard reset to the ref on reuse).
 *
 * <p>Three layout modes, selected in priority order by what is configured:</p>
 * <ol>
 *   <li><b>Multi-module mode</b> — when {@link Builder#modules(List)} is non-empty, each
 *       module's {@code <module>/<subdir>} tree (or the module directory itself when the
 *       subdir does not exist) is gathered relative to that per-module root, flattening every
 *       module onto one shared import root so cross-module imports resolve. Flatten
 *       collisions with identical content are tolerated; differing content is an error.</li>
 *   <li><b>Explicit paths mode</b> — when {@link Builder#paths(List)} is set, each listed
 *       file or directory is gathered from {@code <subdir>/<path>}, import paths relative to
 *       {@code subdir}.</li>
 *   <li><b>Single-subdir mode</b> — otherwise, every {@code .proto} under {@code <subdir>}
 *       (default {@code "proto"}) is gathered, import paths relative to {@code subdir}.</li>
 * </ol>
 *
 * <p>The ref (default {@code "main"}) may be a branch, a tag, or a commit SHA. Everything
 * gathered gets origin {@code git:<repo>@<ref>}.</p>
 */
public final class GitProtoGatherer implements ProtoGatherer {

    private final String repo;
    private final String ref;
    private final String subdir;
    private final List<String> modules;
    private final List<String> paths;
    private final CredentialsProvider credentials;
    private final Path cacheDir;
    private final boolean offline;

    private GitProtoGatherer(Builder builder) {
        this.repo = builder.repo;
        this.ref = builder.ref;
        this.subdir = builder.subdir;
        this.modules = List.copyOf(builder.modules);
        this.paths = List.copyOf(builder.paths);
        this.credentials = credentials(builder.token, builder.username, builder.password);
        Path cacheDir = builder.cacheDir != null
                ? builder.cacheDir
                : builder.cacheRoot != null
                        ? builder.cacheRoot.resolve(sha256Prefix(builder.repo))
                        : defaultCacheDir(builder.repo);
        this.cacheDir = cacheDir.toAbsolutePath().normalize();
        this.offline = builder.offline;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** {@code ${user.home}/.cache/protomolt/gather/git/<sha256(repo)[:16]>}. */
    private static Path defaultCacheDir(String repo) {
        return Path.of(System.getProperty("user.home"),
                ".cache", "protomolt", "gather", "git", sha256Prefix(repo));
    }

    private static String sha256Prefix(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(value.getBytes(StandardCharsets.UTF_8)))
                    .substring(0, 16);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    private static CredentialsProvider credentials(String token, String username, String password) {
        String trimmedToken = token == null ? "" : token.trim();
        if (!trimmedToken.isEmpty()) {
            return new UsernamePasswordCredentialsProvider("x-access-token", trimmedToken);
        }
        String trimmedUsername = username == null ? "" : username.trim();
        if (trimmedUsername.isEmpty()) {
            return null;
        }
        return new UsernamePasswordCredentialsProvider(trimmedUsername, password == null ? "" : password);
    }

    @Override
    public ProtoSourceSet gather() throws GatherException {
        Path checkout = GitCloneCache.ensureCheckout(cacheDir, repo, ref, credentials, offline);
        ProtoSourceSet.Builder builder = ProtoSourceSet.builder();
        try {
            if (!modules.isEmpty()) {
                gatherModules(checkout, builder);
            } else {
                Path protoRoot = resolveInside(checkout, subdir, "git proto subdir");
                if (!Files.isDirectory(protoRoot)) {
                    throw new GatherException("Configured git proto subdir does not exist: "
                            + subdir + " (in " + origin() + ")");
                }
                if (!paths.isEmpty()) {
                    gatherPaths(protoRoot, builder);
                } else {
                    gatherTree(protoRoot, protoRoot, builder);
                }
            }
        } catch (IOException e) {
            throw new GatherException("Failed reading protos from " + origin(), e);
        } catch (IllegalStateException e) {
            throw new GatherException("Conflicting proto sources in " + origin() + ": "
                    + e.getMessage(), e);
        }
        return builder.build();
    }

    private void gatherModules(Path checkout, ProtoSourceSet.Builder builder)
            throws GatherException, IOException {
        for (String module : modules) {
            Path moduleDir = resolveInside(checkout, module, "git module");
            if (!Files.isDirectory(moduleDir)) {
                throw new GatherException("Git module directory does not exist in repo: "
                        + module + " (in " + origin() + ")");
            }
            Path protoRoot = resolveInside(moduleDir, subdir, "git proto subdir");
            if (!Files.isDirectory(protoRoot)) {
                protoRoot = moduleDir;
            }
            gatherTree(protoRoot, protoRoot, builder);
        }
    }

    private void gatherPaths(Path protoRoot, ProtoSourceSet.Builder builder)
            throws GatherException, IOException {
        for (String configured : paths) {
            Path resolved = resolveInside(protoRoot, configured, "git path");
            if (Files.isDirectory(resolved)) {
                gatherTree(protoRoot, resolved, builder);
                continue;
            }
            if (!Files.isRegularFile(resolved)) {
                throw new GatherException("Configured git path does not exist: " + configured
                        + " (in " + origin() + ")");
            }
            if (!resolved.getFileName().toString().endsWith(".proto")) {
                throw new GatherException("Configured git path is not a .proto file: " + configured
                        + " (in " + origin() + ")");
            }
            addFile(protoRoot, resolved, builder);
        }
    }

    private void gatherTree(Path importRoot, Path dir, ProtoSourceSet.Builder builder) throws IOException {
        List<Path> protos;
        try (Stream<Path> files = Files.walk(dir)) {
            protos = files
                    .filter(path -> !pathContainsGitDir(dir.relativize(path)))
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".proto"))
                    .sorted()
                    .toList();
        }
        for (Path proto : protos) {
            addFile(importRoot, proto, builder);
        }
    }

    private void addFile(Path importRoot, Path proto, ProtoSourceSet.Builder builder) throws IOException {
        String importPath = importRoot.relativize(proto).toString().replace('\\', '/');
        builder.add(importPath, Files.readString(proto), origin());
    }

    private static boolean pathContainsGitDir(Path relative) {
        for (Path element : relative) {
            if (element.toString().equals(".git")) {
                return true;
            }
        }
        return false;
    }

    private static Path resolveInside(Path root, String relative, String what) throws GatherException {
        Path resolved = root.resolve(relative).normalize();
        if (!resolved.startsWith(root)) {
            throw new GatherException("Configured " + what + " escapes its root: " + relative);
        }
        return resolved;
    }

    @Override
    public String origin() {
        return "git:" + repo + "@" + ref;
    }

    /** Offline gathering needs a warm cache; online gathering is always attemptable. */
    @Override
    public boolean isAvailable() {
        return !offline || Files.isDirectory(cacheDir);
    }

    /** Builder for {@link GitProtoGatherer}; {@link #repo(String)} is required. */
    public static final class Builder {

        private String repo;
        private String ref = "main";
        private String subdir = "proto";
        private final List<String> modules = new ArrayList<>();
        private final List<String> paths = new ArrayList<>();
        private String token;
        private String username;
        private String password;
        private Path cacheDir;
        private Path cacheRoot;
        private boolean offline = false;

        private Builder() {
        }

        /** Repository URL to clone from, e.g. {@code https://github.com/acme/protos.git}. */
        public Builder repo(String url) {
            this.repo = Objects.requireNonNull(url, "url");
            return this;
        }

        /** Branch, tag, or commit SHA to gather from; default {@code "main"}. */
        public Builder ref(String ref) {
            this.ref = Objects.requireNonNull(ref, "ref");
            return this;
        }

        /** Repository subdirectory holding the protos; default {@code "proto"}. */
        public Builder subdir(String subdir) {
            this.subdir = Objects.requireNonNull(subdir, "subdir");
            return this;
        }

        /** Module directories whose {@code <module>/<subdir>} trees are flattened together. */
        public Builder modules(List<String> modules) {
            this.modules.addAll(Objects.requireNonNull(modules, "modules"));
            return this;
        }

        /** Files or directories to gather, relative to {@code subdir}. */
        public Builder paths(List<String> paths) {
            this.paths.addAll(Objects.requireNonNull(paths, "paths"));
            return this;
        }

        /** Access token; sent as {@code x-access-token} basic credentials (GitHub-style). */
        public Builder token(String token) {
            this.token = token;
            return this;
        }

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        /** Overrides the persistent clone cache directory. */
        public Builder cacheDir(Path cacheDir) {
            this.cacheDir = Objects.requireNonNull(cacheDir, "cacheDir");
            return this;
        }

        /**
         * Overrides where per-repo clone caches live: this directory plus the standard
         * per-repo hash, instead of {@code ${user.home}/.cache/protomolt/gather/git}.
         * The operator-level knob; {@link #cacheDir} pins one exact directory instead.
         */
        public Builder cacheRoot(Path cacheRoot) {
            this.cacheRoot = Objects.requireNonNull(cacheRoot, "cacheRoot");
            return this;
        }

        /** When {@code true}, only the cached checkout is used and a cold cache fails. */
        public Builder offline(boolean offline) {
            this.offline = offline;
            return this;
        }

        public GitProtoGatherer build() {
            if (repo == null || repo.isBlank()) {
                throw new IllegalStateException("repo is required");
            }
            return new GitProtoGatherer(this);
        }
    }
}
