package ai.pipestream.proto.registry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.PersonIdent;

import java.io.IOException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * A git repository <em>is</em> the storage: every {@code register}/{@code setCompatibilityMode}
 * is one commit against a non-bare working tree, so the registry's full history is a plain git
 * log and can be pushed, pulled and reviewed like any other repo.
 *
 * <h2>Layout</h2>
 * <pre>
 * registry.json                              global compatibility mode + next globalId counter
 * subjects/&lt;url-encoded-subject&gt;/v&lt;N&gt;.proto  schema text
 * subjects/&lt;url-encoded-subject&gt;/v&lt;N&gt;.json   metadata: references, globalId, contentHash
 * subjects/&lt;url-encoded-subject&gt;/config.json per-subject compatibility mode, when set
 * </pre>
 *
 * <h2>Concurrency</h2>
 * <p>Writes are serialized with a JVM lock (shared per lock-file path, so two stores in one
 * JVM never race or hit {@code OverlappingFileLockException}) plus a file lock on
 * {@code registry.lock} — the same idiom as the gather module's {@code GitCloneCache} — so two
 * stores on one repository, in the same JVM or different processes, cannot corrupt it. Reads
 * are lock-free off the working tree through an in-memory index rebuilt on demand; the index
 * is invalidated after every write, and {@link #refresh()} exposes the invalidation for
 * commits made externally (e.g. an out-of-band {@code git pull}).</p>
 */
public final class GitSchemaRegistryStore implements SchemaRegistryStore {

    private static final ConcurrentMap<Path, ReentrantLock> JVM_LOCKS = new ConcurrentHashMap<>();

    private static final String SUBJECTS_DIR = "subjects";
    private static final String CHAINS_DIR = "chains";
    private static final String REGISTRY_FILE = "registry.json";
    private static final String LOCK_FILE = "registry.lock";
    private static final String CONFIG_FILE = "config.json";
    private static final Pattern VERSION_FILE = Pattern.compile("v(\\d+)\\.proto");

    private final Path repoDir;
    private final Path lockFile;
    private final ReentrantLock jvmLock;
    private final Git git;
    private final WriteGate writeGate;
    private final PersonIdent author;
    private final ObjectMapper json = new ObjectMapper();

    private volatile Index index;

    private record Index(Map<String, SubjectEntry> subjects,
                         Map<Integer, StoredSchema> byGlobalId,
                         String globalMode,
                         int nextGlobalId) {
    }

    private record SubjectEntry(Map<Integer, StoredSchema> versions, String mode) {
    }

    private GitSchemaRegistryStore(Builder builder) {
        this.repoDir = builder.repositoryDir.toAbsolutePath().normalize();
        this.lockFile = repoDir.resolve(LOCK_FILE);
        this.jvmLock = JVM_LOCKS.computeIfAbsent(lockFile, path -> new ReentrantLock());
        this.writeGate = builder.writeGate;
        this.author = new PersonIdent(builder.authorName, builder.authorEmail);
        this.git = openOrInit(repoDir);
    }

    public static Builder builder() {
        return new Builder();
    }

    private static Git openOrInit(Path repoDir) {
        try {
            Files.createDirectories(repoDir);
            if (Files.isDirectory(repoDir.resolve(".git"))) {
                return Git.open(repoDir.toFile());
            }
            return Git.init().setDirectory(repoDir.toFile()).call();
        } catch (Exception e) {
            throw new RegistryStoreException("Failed to open git registry at " + repoDir, e);
        }
    }

    // ---------------------------------------------------------------- reads (lock-free)

    @Override
    public List<String> subjects() {
        // Map.copyOf drops iteration order; sort on the way out.
        return index().subjects().entrySet().stream()
                .filter(entry -> !entry.getValue().versions().isEmpty())
                .map(Map.Entry::getKey)
                .sorted()
                .toList();
    }

    @Override
    public List<Integer> versions(String subject) {
        SubjectEntry entry = index().subjects().get(subject);
        return entry == null ? List.of() : entry.versions().keySet().stream().sorted().toList();
    }

    @Override
    public Optional<StoredSchema> version(String subject, int version) {
        SubjectEntry entry = index().subjects().get(subject);
        return entry == null ? Optional.empty() : Optional.ofNullable(entry.versions().get(version));
    }

    @Override
    public Optional<StoredSchema> latest(String subject) {
        SubjectEntry entry = index().subjects().get(subject);
        if (entry == null || entry.versions().isEmpty()) {
            return Optional.empty();
        }
        int highest = entry.versions().keySet().stream().mapToInt(Integer::intValue).max().orElseThrow();
        return Optional.of(entry.versions().get(highest));
    }

    @Override
    public Optional<StoredSchema> byGlobalId(int globalId) {
        return Optional.ofNullable(index().byGlobalId().get(globalId));
    }

    @Override
    public Optional<StoredSchema> findByContent(String subject, String schemaText,
                                                List<SchemaReference> references) {
        List<SchemaReference> refs = references == null ? List.of() : List.copyOf(references);
        SubjectEntry entry = index().subjects().get(subject);
        if (entry == null) {
            return Optional.empty();
        }
        return entry.versions().keySet().stream().sorted()
                .map(entry.versions()::get)
                .filter(stored -> SchemaContents.sameContent(stored, schemaText, refs))
                .findFirst();
    }

    @Override
    public Optional<String> compatibilityMode(String subject) {
        SubjectEntry entry = index().subjects().get(subject);
        return entry == null ? Optional.empty() : Optional.ofNullable(entry.mode());
    }

    @Override
    public String globalCompatibilityMode() {
        return index().globalMode();
    }

    /** Drops the in-memory index; the next read re-scans the working tree. */
    @Override
    public void refresh() {
        index = null;
    }

    // ---------------------------------------------------------------- writes (locked commits)

    @Override
    public StoredSchema register(String subject, String schemaText, List<SchemaReference> references)
            throws RegistryStoreException {
        RegistrationSupport.requireSubject(subject);
        Objects.requireNonNull(schemaText, "schemaText");
        List<SchemaReference> refs = references == null ? List.of() : List.copyOf(references);
        return locked(() -> {
            Index fresh = loadIndex(); // see commits other stores made since our last read
            index = fresh;
            RegistrationSupport.verifyReferences(this, refs);
            Optional<StoredSchema> existing = findByContent(subject, schemaText, refs);
            if (existing.isPresent()) {
                return existing.get();
            }
            List<StoredSchema> history = RegistrationSupport.history(this, subject);
            RegistrationSupport.enforceWriteGate(writeGate, this, subject, history, schemaText, refs);
            RegistrationSupport.compileCandidate(this, subject, schemaText, refs);

            int version = history.isEmpty() ? 1 : history.getLast().version() + 1;
            StoredSchema stored = new StoredSchema(subject, version, fresh.nextGlobalId(),
                    schemaText, refs, SchemaContents.contentHash(schemaText, refs));

            String subjectDir = subjectDir(subject);
            String protoPath = subjectDir + "/v" + version + ".proto";
            String metaPath = subjectDir + "/v" + version + ".json";
            Files.createDirectories(repoDir.resolve(subjectDir));
            Files.writeString(repoDir.resolve(protoPath), schemaText);
            Files.writeString(repoDir.resolve(metaPath), metadataJson(stored));
            writeRegistryFile(fresh.globalMode(), fresh.nextGlobalId() + 1);

            commit(List.of(protoPath, metaPath, REGISTRY_FILE),
                    "Register " + subject + " v" + version);
            index = null;
            return stored;
        });
    }

    /**
     * Stores a named chain definition (an opaque JSON document to this store), one commit
     * per put under {@code chains/<name>.json}. The registry server gates writes with
     * {@code check-chain}; the store only persists and versions via Git history.
     */
    public void putChain(String name, String chainJson) throws RegistryStoreException {
        requireChainName(name);
        Objects.requireNonNull(chainJson, "chainJson");
        locked(() -> {
            String path = CHAINS_DIR + "/" + encode(name) + ".json";
            Files.createDirectories(repoDir.resolve(CHAINS_DIR));
            Files.writeString(repoDir.resolve(path), chainJson);
            commit(List.of(path), "Put chain " + name);
            return null;
        });
    }

    /** The named chain's JSON document, when present. */
    public Optional<String> chain(String name) throws RegistryStoreException {
        requireChainName(name);
        Path path = repoDir.resolve(CHAINS_DIR).resolve(encode(name) + ".json");
        try {
            return Files.isRegularFile(path)
                    ? Optional.of(Files.readString(path))
                    : Optional.empty();
        } catch (IOException e) {
            throw new RegistryStoreException("Failed to read chain " + name, e);
        }
    }

    /** Every stored chain name, sorted. */
    public List<String> chains() throws RegistryStoreException {
        Path dir = repoDir.resolve(CHAINS_DIR);
        if (!Files.isDirectory(dir)) {
            return List.of();
        }
        try (var files = Files.list(dir)) {
            return files.map(path -> path.getFileName().toString())
                    .filter(fileName -> fileName.endsWith(".json"))
                    .map(fileName -> decode(fileName.substring(0, fileName.length() - 5)))
                    .sorted()
                    .toList();
        } catch (IOException e) {
            throw new RegistryStoreException("Failed to list chains", e);
        }
    }

    private static void requireChainName(String name) {
        if (name == null || name.isBlank() || !name.matches("[A-Za-z0-9._-]+")) {
            throw new IllegalArgumentException(
                    "Chain names use [A-Za-z0-9._-]; got '" + name + "'");
        }
    }

    @Override
    public void setCompatibilityMode(String subject, String mode) {
        RegistrationSupport.requireSubject(subject);
        CompatibilityModes.requireValid(mode);
        locked(() -> {
            index = loadIndex();
            String subjectDir = subjectDir(subject);
            String configPath = subjectDir + "/" + CONFIG_FILE;
            Files.createDirectories(repoDir.resolve(subjectDir));
            Files.writeString(repoDir.resolve(configPath),
                    json.createObjectNode().put("compatibility", mode).toPrettyString());
            commit(List.of(configPath), "Set compatibility " + subject + " " + mode);
            index = null;
            return null;
        });
    }

    @Override
    public void setGlobalCompatibilityMode(String mode) {
        CompatibilityModes.requireValid(mode);
        locked(() -> {
            Index fresh = loadIndex();
            writeRegistryFile(mode, fresh.nextGlobalId());
            commit(List.of(REGISTRY_FILE), "Set global compatibility " + mode);
            index = null;
            return null;
        });
    }

    @Override
    public void close() {
        git.close();
    }

    // ---------------------------------------------------------------- locking

    @FunctionalInterface
    private interface WriteAction<T> {
        T run() throws Exception;
    }

    /**
     * Serializes a write with the shared JVM lock plus a {@code registry.lock} file lock,
     * mirroring {@code GitCloneCache}: the JVM lock covers stores inside this process, the
     * file lock covers other processes on the same repository.
     */
    private <T> T locked(WriteAction<T> action) {
        jvmLock.lock();
        try (FileChannel channel = FileChannel.open(lockFile,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                FileLock ignored = channel.lock()) {
            return action.run();
        } catch (RegistryStoreException | IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new RegistryStoreException("Registry write failed at " + repoDir, e);
        } finally {
            jvmLock.unlock();
        }
    }

    // ---------------------------------------------------------------- git plumbing

    private void commit(List<String> paths, String message) throws Exception {
        var add = git.add();
        paths.forEach(add::addFilepattern);
        add.call();
        git.commit()
                .setMessage(message)
                .setAuthor(author)
                .setCommitter(author)
                .call();
    }

    // ---------------------------------------------------------------- files <-> model

    private void writeRegistryFile(String globalMode, int nextGlobalId) throws IOException {
        ObjectNode node = json.createObjectNode()
                .put("compatibility", globalMode)
                .put("nextGlobalId", nextGlobalId);
        Files.writeString(repoDir.resolve(REGISTRY_FILE), node.toPrettyString());
    }

    private String metadataJson(StoredSchema stored) {
        ObjectNode node = json.createObjectNode()
                .put("globalId", stored.globalId())
                .put("contentHash", stored.contentHash());
        ArrayNode array = node.putArray("references");
        for (SchemaReference reference : stored.references()) {
            array.addObject()
                    .put("name", reference.name())
                    .put("subject", reference.subject())
                    .put("version", reference.version());
        }
        return node.toPrettyString();
    }

    private Index index() {
        Index current = index;
        if (current == null) {
            current = locked(this::loadIndex);
            index = current;
        }
        return current;
    }

    /** Rebuilds the whole in-memory view from the working tree. */
    private Index loadIndex() throws IOException {
        String globalMode = CompatibilityModes.DEFAULT_GLOBAL;
        int nextGlobalId = 1;
        Path registryFile = repoDir.resolve(REGISTRY_FILE);
        if (Files.isRegularFile(registryFile)) {
            JsonNode node = json.readTree(Files.readString(registryFile));
            globalMode = node.path("compatibility").asText(CompatibilityModes.DEFAULT_GLOBAL);
            nextGlobalId = node.path("nextGlobalId").asInt(1);
        }

        Map<String, SubjectEntry> subjects = new TreeMap<>();
        Map<Integer, StoredSchema> byGlobalId = new TreeMap<>();
        Path subjectsDir = repoDir.resolve(SUBJECTS_DIR);
        if (Files.isDirectory(subjectsDir)) {
            try (Stream<Path> dirs = Files.list(subjectsDir)) {
                for (Path dir : dirs.filter(Files::isDirectory).sorted().toList()) {
                    String subject = URLDecoder.decode(dir.getFileName().toString(), StandardCharsets.UTF_8);
                    SubjectEntry entry = loadSubject(subject, dir);
                    subjects.put(subject, entry);
                    entry.versions().values().forEach(s -> byGlobalId.put(s.globalId(), s));
                }
            }
        }
        // Guard against a stale counter after external commits: never reuse a globalId.
        for (int globalId : byGlobalId.keySet()) {
            nextGlobalId = Math.max(nextGlobalId, globalId + 1);
        }
        return new Index(Map.copyOf(subjects), Map.copyOf(byGlobalId), globalMode, nextGlobalId);
    }

    private SubjectEntry loadSubject(String subject, Path dir) throws IOException {
        TreeMap<Integer, StoredSchema> versions = new TreeMap<>();
        String mode = null;
        Path configFile = dir.resolve(CONFIG_FILE);
        if (Files.isRegularFile(configFile)) {
            mode = json.readTree(Files.readString(configFile)).path("compatibility").asText(null);
        }
        try (Stream<Path> files = Files.list(dir)) {
            for (Path file : files.toList()) {
                Matcher matcher = VERSION_FILE.matcher(file.getFileName().toString());
                if (!matcher.matches()) {
                    continue;
                }
                int version = Integer.parseInt(matcher.group(1));
                String schemaText = Files.readString(file);
                JsonNode meta = json.readTree(
                        Files.readString(dir.resolve("v" + version + ".json")));
                List<SchemaReference> references = new ArrayList<>();
                for (JsonNode reference : meta.path("references")) {
                    references.add(new SchemaReference(
                            reference.path("name").asText(),
                            reference.path("subject").asText(),
                            reference.path("version").asInt()));
                }
                versions.put(version, new StoredSchema(subject, version,
                        meta.path("globalId").asInt(), schemaText, references,
                        meta.path("contentHash").asText()));
            }
        }
        return new SubjectEntry(versions, mode);
    }

    private static String encode(String subject) {
        return URLEncoder.encode(subject, StandardCharsets.UTF_8);
    }

    /**
     * The repo-relative {@code subjects/<encoded>} path for a subject, checked to stay inside
     * {@link #SUBJECTS_DIR}.
     *
     * <p>{@link RegistrationSupport#requireSubject} already refuses the names that can escape,
     * so this is a second line rather than the only one: it keeps a future change to
     * {@link #encode} from silently turning a subject name into a path traversal.
     */
    private String subjectDir(String subject) {
        String relative = SUBJECTS_DIR + "/" + encode(subject);
        Path resolved = repoDir.resolve(relative).normalize();
        if (!resolved.startsWith(repoDir.resolve(SUBJECTS_DIR).normalize())
                || resolved.equals(repoDir.resolve(SUBJECTS_DIR).normalize())) {
            throw new IllegalArgumentException(
                    "subject '" + subject + "' does not name a directory under " + SUBJECTS_DIR);
        }
        return relative;
    }

    private static String decode(String encoded) {
        return java.net.URLDecoder.decode(encoded, StandardCharsets.UTF_8);
    }

    // ---------------------------------------------------------------- builder

    /** Builder for {@link GitSchemaRegistryStore}. */
    public static final class Builder {

        private Path repositoryDir;
        private WriteGate writeGate;
        private String authorName = "protomolt-registry";
        private String authorEmail = "registry@localhost";

        private Builder() {
        }

        /** Working-tree directory of the registry repo; initialized (non-bare) when absent. */
        public Builder repositoryDir(Path repositoryDir) {
            this.repositoryDir = Objects.requireNonNull(repositoryDir, "repositoryDir");
            return this;
        }

        /** Optional compatibility write gate. */
        public Builder writeGate(WriteGate writeGate) {
            this.writeGate = writeGate;
            return this;
        }

        /** Author/committer identity for registry commits. */
        public Builder author(String name, String email) {
            this.authorName = Objects.requireNonNull(name, "name");
            this.authorEmail = Objects.requireNonNull(email, "email");
            return this;
        }

        public GitSchemaRegistryStore build() {
            Objects.requireNonNull(repositoryDir, "repositoryDir is required");
            return new GitSchemaRegistryStore(this);
        }
    }
}
