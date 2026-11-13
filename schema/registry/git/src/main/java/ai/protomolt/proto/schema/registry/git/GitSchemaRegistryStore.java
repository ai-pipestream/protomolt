package ai.protomolt.proto.schema.registry.git;

import ai.protomolt.proto.registry.ConfigDocumentStore;
import ai.protomolt.proto.registry.WorkflowDocumentStore;

import ai.protomolt.proto.registry.CompatibilityModes;
import ai.protomolt.proto.registry.DescriptorSetArtifacts;
import ai.protomolt.proto.registry.RegistrationSupport;
import ai.protomolt.proto.registry.RegistryStoreException;
import ai.protomolt.proto.registry.SchemaContents;
import ai.protomolt.proto.registry.SchemaReference;
import ai.protomolt.proto.registry.SchemaRegistryStore;
import ai.protomolt.proto.registry.StoredSchema;

import ai.protomolt.proto.grpc.workflow.WorkflowValidation;
import ai.protomolt.proto.grpc.workflow.v1.VersionedWorkflow;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.PersonIdent;
import org.eclipse.jgit.lib.Repository;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A git repository <em>is</em> the storage: every {@code register}/{@code setCompatibilityMode}
 * is one commit, so the registry's full history is a plain git log and can be pushed, pulled
 * and reviewed like any other repo.
 *
 * <h2>Object authority</h2>
 * <p>All production reads answer from the committed HEAD tree through
 * {@link RegistryGitObjects}; the working tree is never consulted, so stale, edited or
 * absent working-tree files change nothing. All writes insert blobs, trees and one commit
 * with {@code ObjectInserter} and advance HEAD only through a {@code RefUpdate}
 * compare-and-swap against the exact commit HEAD named when the write started. On non-bare
 * repositories, {@code .git/index} ({@code DirCache}) is aligned with the newly committed root
 * tree alongside HEAD so subsequent porcelain commits preserve registered files; bare
 * repositories have no index and skip {@code DirCache}. The working tree itself is never
 * written: a non-bare repository stays usable for {@code git log}, review and porcelain commits,
 * and its working tree simply does not reflect registry state (a manual {@code git reset --hard}
 * materializes HEAD into the working tree without affecting the store).</p>
 *
 * <h2>Layout</h2>
 * <pre>
 * registry.json                              global compatibility mode + next globalId counter
 * subjects/&lt;url-encoded-subject&gt;/v&lt;N&gt;.proto  schema text
 * subjects/&lt;url-encoded-subject&gt;/v&lt;N&gt;.json   metadata: references, globalId, contentHash
 * subjects/&lt;url-encoded-subject&gt;/config.json per-subject compatibility mode, when set
 * workflows/&lt;url-encoded-name&gt;.json         stored workflow definition
 * workflow-versions/&lt;url-encoded-name&gt;/&lt;url-encoded-version&gt;.pb  immutable promoted version
 * descriptors/sha256/&lt;fingerprint&gt;.pb         content-addressed descriptor set
 * </pre>
 *
 * <h2>Bare repositories</h2>
 * <p>An existing bare repository (a directory that already has the shape of a git directory)
 * is opened as-is and behaves identically to a non-bare one; {@link Builder#bareInit(boolean)
 * bareInit} initializes a new repository bare. Non-bare initialization remains the default,
 * and the {@code git} provider configuration is unchanged.</p>
 *
 * <h2>Concurrency and failures</h2>
 * <p>Writes and initial index loads are serialized with a JVM lock (shared per lock-file path,
 * so two stores in one JVM never race or hit {@code OverlappingFileLockException}) plus a file
 * lock on {@code registry.lock} — the same idiom as the gather module's {@code GitCloneCache} —
 * so two stores on one repository, in the same JVM or different processes, cannot corrupt it.
 * Cached reads are lock-free off an immutable in-memory index built from one pinned commit.
 * Validation and compilation during {@link #register} run against a private read-only view of
 * the pre-write HEAD snapshot without publishing it to concurrent readers; once a schema or
 * compatibility-mode commit lands, the store atomically publishes the committed post-write
 * index before releasing the lock. {@link #refresh()} acquires the JVM lock to drop the cached
 * index for commits made externally (e.g. an out-of-band {@code git pull}).</p>
 *
 * <p>The compare-and-swap rejects a write when anything advanced HEAD after the write read
 * it (an external commit, another registry not honoring the lock file): the write is refused,
 * the external commit is never overwritten, and the store drops its cached index under the lock
 * so the next read reloads HEAD. A failure before the ref update leaves HEAD untouched; the
 * unreachable objects a failed write inserted are harmless and left for normal Git GC. A failure
 * after the ref update is an ambiguous outcome — the commit may or may not have landed — so the
 * store drops its cached index and recovery is to {@link #refresh()} (re-read HEAD) before
 * retrying, never to force or reset.</p>
 */
public final class GitSchemaRegistryStore implements ConfigDocumentStore, WorkflowDocumentStore {

    private static final ConcurrentMap<Path, ReentrantLock> JVM_LOCKS = new ConcurrentHashMap<>();

    private final Path repoDir;
    private final RegistryLayout layout;
    private final Path lockFile;
    private final ReentrantLock jvmLock;
    private final Git git;
    private final Repository repository;
    private final WriteGate writeGate;
    private final PersonIdent author;
    private final ObjectMapper json = new ObjectMapper();

    private volatile Index index;
    private volatile RegistryGitObjects.WriteHook writeHook = RegistryGitObjects.WriteHook.NONE;

    private record Index(Map<String, SubjectEntry> subjects,
                         Map<Integer, StoredSchema> byGlobalId,
                         String globalMode,
                         int nextGlobalId) {
        private Index {
            subjects = Map.copyOf(subjects);
            byGlobalId = Map.copyOf(byGlobalId);
        }
    }

    private record SubjectEntry(Map<Integer, StoredSchema> versions, String mode) {
        private SubjectEntry {
            versions = Map.copyOf(versions);
        }
    }

    /**
     * Private read-only {@link SchemaRegistryStore} view over a single pinned {@link Index}
     * snapshot so {@link #register} can run reference checks, write-gate enforcement and
     * candidate compilation without publishing pre-commit state to {@link #index}.
     */
    private final class IndexView implements SchemaRegistryStore {

        private final Index snapshot;

        private IndexView(Index snapshot) {
            this.snapshot = Objects.requireNonNull(snapshot, "snapshot");
        }

        @Override
        public List<String> subjects() {
            return subjectsOf(snapshot);
        }

        @Override
        public List<Integer> versions(String subject) {
            return versionsOf(snapshot, subject);
        }

        @Override
        public Optional<StoredSchema> version(String subject, int version) {
            return versionOf(snapshot, subject, version);
        }

        @Override
        public Optional<StoredSchema> latest(String subject) {
            return latestOf(snapshot, subject);
        }

        @Override
        public Optional<StoredSchema> byGlobalId(int globalId) {
            return byGlobalIdOf(snapshot, globalId);
        }

        @Override
        public boolean supportsDescriptorSets() {
            return GitSchemaRegistryStore.this.supportsDescriptorSets();
        }

        @Override
        public Optional<ByteString> descriptorSet(String fingerprint) {
            return GitSchemaRegistryStore.this.descriptorSet(fingerprint);
        }

        @Override
        public Optional<StoredSchema> findByContent(
                String subject, String schemaText, List<SchemaReference> references) {
            return findByContentOf(snapshot, subject, schemaText, references);
        }

        @Override
        public Optional<String> compatibilityMode(String subject) {
            return compatibilityModeOf(snapshot, subject);
        }

        @Override
        public String globalCompatibilityMode() {
            return snapshot.globalMode();
        }

        @Override
        public StoredSchema register(
                String subject, String schemaText, List<SchemaReference> references) {
            throw new UnsupportedOperationException("IndexView is read-only");
        }

        @Override
        public void setCompatibilityMode(String subject, String mode) {
            throw new UnsupportedOperationException("IndexView is read-only");
        }

        @Override
        public void setGlobalCompatibilityMode(String mode) {
            throw new UnsupportedOperationException("IndexView is read-only");
        }

        @Override
        public void close() {
            // Read-only view over an in-memory snapshot.
        }
    }

    private GitSchemaRegistryStore(Builder builder) {
        this.repoDir = builder.repositoryDir.toAbsolutePath().normalize();
        this.layout = new RegistryLayout(repoDir);
        this.lockFile = repoDir.resolve(RegistryLayout.LOCK_FILE);
        this.jvmLock = JVM_LOCKS.computeIfAbsent(lockFile, path -> new ReentrantLock());
        this.writeGate = builder.writeGate;
        this.author = new PersonIdent(builder.authorName, builder.authorEmail);
        this.git = openOrInit(repoDir, builder.bareInit);
        this.repository = git.getRepository();
    }

    public static Builder builder() {
        return new Builder();
    }

    private static Git openOrInit(Path repoDir, boolean bareInit) {
        try {
            if (Files.isDirectory(repoDir.resolve(".git"))) {
                return Git.open(repoDir.toFile());
            }
            if (RegistryLayout.isGitDirectory(repoDir)) {
                return Git.open(repoDir.toFile()); // an existing bare repository
            }
            Files.createDirectories(repoDir);
            return Git.init().setDirectory(repoDir.toFile()).setBare(bareInit).call();
        } catch (Exception e) {
            throw new RegistryStoreException("Failed to open git registry at " + repoDir, e);
        }
    }

    /** Test-only fault injection between the stages of one write; production uses {@code NONE}. */
    GitSchemaRegistryStore writeHook(RegistryGitObjects.WriteHook hook) {
        this.writeHook = Objects.requireNonNull(hook, "hook");
        return this;
    }

    // ---------------------------------------------------------------- reads (lock-free)

    @Override
    public List<String> subjects() {
        return subjectsOf(index());
    }

    @Override
    public List<Integer> versions(String subject) {
        return versionsOf(index(), subject);
    }

    @Override
    public Optional<StoredSchema> version(String subject, int version) {
        return versionOf(index(), subject, version);
    }

    @Override
    public Optional<StoredSchema> latest(String subject) {
        return latestOf(index(), subject);
    }

    @Override
    public Optional<StoredSchema> byGlobalId(int globalId) {
        return byGlobalIdOf(index(), globalId);
    }

    private static List<String> subjectsOf(Index snapshot) {
        // Map.copyOf drops iteration order; sort on the way out.
        return snapshot.subjects().entrySet().stream()
                .filter(entry -> !entry.getValue().versions().isEmpty())
                .map(Map.Entry::getKey)
                .sorted()
                .toList();
    }

    private static List<Integer> versionsOf(Index snapshot, String subject) {
        SubjectEntry entry = snapshot.subjects().get(subject);
        return entry == null ? List.of() : entry.versions().keySet().stream().sorted().toList();
    }

    private static Optional<StoredSchema> versionOf(Index snapshot, String subject, int version) {
        SubjectEntry entry = snapshot.subjects().get(subject);
        return entry == null ? Optional.empty() : Optional.ofNullable(entry.versions().get(version));
    }

    private static Optional<StoredSchema> latestOf(Index snapshot, String subject) {
        SubjectEntry entry = snapshot.subjects().get(subject);
        if (entry == null || entry.versions().isEmpty()) {
            return Optional.empty();
        }
        int highest = entry.versions().keySet().stream().mapToInt(Integer::intValue).max().orElseThrow();
        return Optional.of(entry.versions().get(highest));
    }

    private static Optional<StoredSchema> byGlobalIdOf(Index snapshot, int globalId) {
        return Optional.ofNullable(snapshot.byGlobalId().get(globalId));
    }

    private static Optional<StoredSchema> findByContentOf(
            Index snapshot, String subject, String schemaText, List<SchemaReference> references) {
        List<SchemaReference> refs = references == null ? List.of() : List.copyOf(references);
        SubjectEntry entry = snapshot.subjects().get(subject);
        if (entry == null) {
            return Optional.empty();
        }
        return entry.versions().keySet().stream().sorted()
                .map(entry.versions()::get)
                .filter(stored -> SchemaContents.sameContent(stored, schemaText, refs))
                .findFirst();
    }

    private static Optional<String> compatibilityModeOf(Index snapshot, String subject) {
        SubjectEntry entry = snapshot.subjects().get(subject);
        return entry == null ? Optional.empty() : Optional.ofNullable(entry.mode());
    }

    @Override
    public boolean supportsDescriptorSets() {
        return true;
    }

    @Override
    public Optional<ByteString> descriptorSet(String fingerprint) {
        DescriptorSetArtifacts.requireFingerprint(fingerprint);
        String relative = layout.descriptorRelPath(fingerprint);
        byte[] bytes;
        try {
            bytes = RegistryGitObjects.headBlob(repository, relative, DescriptorSetArtifacts.MAX_BYTES);
        } catch (RegistryGitObjects.ObjectTooLargeException invalid) {
            throw new IllegalArgumentException("descriptor set exceeds the 16 MiB limit", invalid);
        } catch (RegistryGitObjects.CorruptTreeException corrupt) {
            throw new RegistryStoreException("Failed to read descriptor " + fingerprint, corrupt);
        } catch (IOException unavailable) {
            throw unavailableDescriptors(unavailable);
        }
        if (bytes == null) {
            // An absent artifact is meaningful only while the backing repository is readable:
            // a vanished object database (moved or deleted repository) is unavailability.
            if (!repository.getObjectDatabase().exists()) {
                throw unavailableDescriptors(new IOException("object database is not readable"));
            }
            return Optional.empty();
        }
        ByteString descriptorSet = ByteString.copyFrom(bytes);
        DescriptorSetArtifacts.validate(fingerprint, descriptorSet);
        return Optional.of(descriptorSet);
    }

    @Override
    public void putDescriptorSet(String fingerprint, ByteString descriptorSet) {
        DescriptorSetArtifacts.validate(fingerprint, descriptorSet);
        locked(() -> {
            ObjectId expectedHead = RegistryGitObjects.headCommit(repository);
            String relative = layout.descriptorRelPath(fingerprint);
            byte[] committed;
            try {
                committed = RegistryGitObjects.blob(
                        repository, expectedHead, relative, DescriptorSetArtifacts.MAX_BYTES);
            } catch (RegistryGitObjects.ObjectTooLargeException invalid) {
                throw new IllegalArgumentException("descriptor set exceeds the 16 MiB limit", invalid);
            } catch (IOException e) {
                throw new RegistryStoreException("Failed to read descriptor " + fingerprint, e);
            }
            if (committed != null) {
                DescriptorSetArtifacts.validate(fingerprint, ByteString.copyFrom(committed));
                return null;
            }
            commitChanges(expectedHead, "Store descriptor " + fingerprint,
                    Map.of(relative, Optional.of(descriptorSet.toByteArray())));
            return null;
        });
    }

    @Override
    public Optional<StoredSchema> findByContent(String subject, String schemaText,
                                                List<SchemaReference> references) {
        return findByContentOf(index(), subject, schemaText, references);
    }

    @Override
    public Optional<String> compatibilityMode(String subject) {
        return compatibilityModeOf(index(), subject);
    }

    @Override
    public String globalCompatibilityMode() {
        return index().globalMode();
    }

    /** Drops the in-memory index under the JVM lock; the next read re-scans the committed HEAD tree. */
    @Override
    public void refresh() {
        jvmLock.lock();
        try {
            index = null;
        } finally {
            jvmLock.unlock();
        }
    }

    // ---------------------------------------------------------------- writes (locked commits)

    @Override
    public StoredSchema register(String subject, String schemaText, List<SchemaReference> references)
            throws RegistryStoreException {
        RegistrationSupport.requireSubject(subject);
        Objects.requireNonNull(schemaText, "schemaText");
        List<SchemaReference> refs = references == null ? List.of() : List.copyOf(references);
        return locked(() -> {
            ObjectId expectedHead = RegistryGitObjects.headCommit(repository);
            Index fresh = loadIndex(expectedHead); // see commits other stores made since our last read
            SchemaRegistryStore view = new IndexView(fresh);
            RegistrationSupport.verifyReferences(view, refs);
            Optional<StoredSchema> existing = view.findByContent(subject, schemaText, refs);
            if (existing.isPresent()) {
                index = fresh;
                return existing.get();
            }
            List<StoredSchema> history = RegistrationSupport.history(view, subject);
            RegistrationSupport.enforceWriteGate(writeGate, view, subject, history, schemaText, refs);
            RegistrationSupport.compileCandidate(view, subject, schemaText, refs);

            int version = history.isEmpty() ? 1 : history.getLast().version() + 1;
            StoredSchema stored = new StoredSchema(subject, version, fresh.nextGlobalId(),
                    schemaText, refs, SchemaContents.contentHash(schemaText, refs));

            String subjectDir = layout.subjectDir(subject);
            String protoPath = subjectDir + "/v" + version + ".proto";
            String metaPath = subjectDir + "/v" + version + ".json";
            Map<String, Optional<byte[]>> changes = new TreeMap<>();
            changes.put(protoPath, Optional.of(schemaText.getBytes(StandardCharsets.UTF_8)));
            changes.put(metaPath, Optional.of(
                    RegistryMetadata.metadataJson(json, stored).getBytes(StandardCharsets.UTF_8)));
            changes.put(RegistryLayout.REGISTRY_FILE, Optional.of(RegistryMetadata.registryJson(
                    json, fresh.globalMode(), fresh.nextGlobalId() + 1)
                    .getBytes(StandardCharsets.UTF_8)));

            String commitId = commitChanges(expectedHead, "Register " + subject + " v" + version, changes);
            index = loadIndex(ObjectId.fromString(commitId));
            return stored;
        });
    }

    /**
     * Stores a named workflow definition (an opaque JSON document to this store), one commit
     * per put under {@code workflows/<name>.json}. The registry server gates writes with
     * {@code check-workflow}; the store only persists and versions via Git history.
     */
    public void putWorkflow(String name, String workflowJson) throws RegistryStoreException {
        RegistryLayout.requireWorkflowName(name);
        Objects.requireNonNull(workflowJson, "workflowJson");
        locked(() -> {
            ObjectId expectedHead = RegistryGitObjects.headCommit(repository);
            String path = RegistryLayout.WORKFLOWS_DIR + "/" + RegistryLayout.encode(name) + ".json";
            commitChanges(expectedHead, "Put workflow " + name,
                    Map.of(path, Optional.of(workflowJson.getBytes(StandardCharsets.UTF_8))));
            return null;
        });
    }

    /** The named workflow's JSON document from HEAD, when present. */
    public Optional<String> workflow(String name) throws RegistryStoreException {
        RegistryLayout.requireWorkflowName(name);
        String path = RegistryLayout.WORKFLOWS_DIR + "/" + RegistryLayout.encode(name) + ".json";
        byte[] bytes = headBlobOrThrow("Failed to read workflow " + name, path);
        return bytes == null
                ? Optional.empty()
                : Optional.of(new String(bytes, StandardCharsets.UTF_8));
    }

    /** Every stored workflow name from HEAD, sorted. */
    public List<String> workflows() throws RegistryStoreException {
        return documentNames(RegistryLayout.WORKFLOWS_DIR, "Failed to list workflows");
    }

    // ---------------------------------------------------------------- promoted workflows

    /**
     * Stores one immutable promoted workflow version, one commit per version under
     * {@code workflow-versions/<name>/<version>.pb} (serialized {@link VersionedWorkflow};
     * binary, never JSON). The candidate is fully validated first: malformed identities,
     * fingerprints, or a content fingerprint that does not match the workflow bytes are
     * rejected before anything touches the repository.
     *
     * <p>Promotion is immutable: saving a version that already exists is a no-op when the
     * stored bytes are identical (idempotent re-promotion) and refused otherwise. The
     * working tree is never consulted, so a working-tree file at the version path neither
     * blocks nor alters a promotion; only HEAD is authoritative.</p>
     *
     * @throws IllegalArgumentException when the workflow violates the contract or the version
     *         already exists with different content
     */
    public void putWorkflow(VersionedWorkflow workflow) throws RegistryStoreException {
        Objects.requireNonNull(workflow, "workflow");
        WorkflowValidation.validate(workflow);
        locked(() -> {
            ObjectId expectedHead = RegistryGitObjects.headCommit(repository);
            String path = layout.workflowPath(workflow.getWorkflow().getName(), workflow.getVersion());
            byte[] bytes = workflow.toByteArray();
            byte[] committed = committedWorkflowBytes(expectedHead, path);
            if (committed != null) {
                if (Arrays.equals(committed, bytes)) {
                    return null; // idempotent re-promotion of identical content
                }
                throw new IllegalArgumentException("workflow " + workflow.getWorkflow().getName()
                        + " version " + workflow.getVersion()
                        + " is immutable: the stored content differs");
            }
            commitChanges(expectedHead, "Promote workflow " + workflow.getWorkflow().getName()
                    + " " + workflow.getVersion(), Map.of(path, Optional.of(bytes)));
            return null;
        });
    }

    /** One exact promoted workflow version, when present; stored bytes are re-validated. */
    public Optional<VersionedWorkflow> workflow(String name, String version)
            throws RegistryStoreException {
        WorkflowValidation.validateName(name, "workflow.name");
        WorkflowValidation.validateName(version, "versioned_workflow.version");
        String relative = layout.workflowPath(name, version);
        byte[] committed = committedWorkflowBytes(relative);
        if (committed == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(readWorkflow(committed));
        } catch (IOException e) {
            throw new RegistryStoreException("Failed to read workflow " + name, e);
        }
    }

    /** All promoted versions of one workflow, ascending by version name. */
    public List<VersionedWorkflow> workflowVersions(String name) throws RegistryStoreException {
        WorkflowValidation.validateName(name, "workflow.name");
        try {
            return workflowVersions(RegistryGitObjects.headCommit(repository), name);
        } catch (IOException e) {
            throw new RegistryStoreException("Failed to list workflow versions for " + name, e);
        }
    }

    private List<VersionedWorkflow> workflowVersions(ObjectId head, String name) throws IOException {
        String dir = RegistryLayout.WORKFLOW_VERSIONS_DIR + "/" + RegistryLayout.encode(name);
        List<VersionedWorkflow> versions = new ArrayList<>();
        for (RegistryGitObjects.TreeChild child
                : RegistryGitObjects.children(repository, head, dir)) {
            if (!child.name().endsWith(".pb") || !child.isBlob()) {
                continue;
            }
            String relative = dir + "/" + child.name();
            byte[] committed = committedWorkflowBytes(head, relative);
            if (committed == null) {
                continue;
            }
            versions.add(readWorkflow(committed));
        }
        return List.copyOf(versions);
    }

    /** Every workflow holding at least one promoted version, sorted. */
    public List<String> workflowNames() throws RegistryStoreException {
        try {
            ObjectId head = RegistryGitObjects.headCommit(repository);
            List<String> names = new ArrayList<>();
            for (RegistryGitObjects.TreeChild child
                    : RegistryGitObjects.children(repository, head, RegistryLayout.WORKFLOW_VERSIONS_DIR)) {
                if (!child.isTree()) {
                    continue;
                }
                String name = RegistryLayout.decode(child.name());
                if (!workflowVersions(head, name).isEmpty()) {
                    names.add(name);
                }
            }
            return names.stream().sorted().toList();
        } catch (IOException e) {
            throw new RegistryStoreException("Failed to list workflows", e);
        }
    }

    private static VersionedWorkflow readWorkflow(byte[] bytes) throws IOException {
        VersionedWorkflow workflow = VersionedWorkflow.parseFrom(bytes);
        // Stored bytes answer to the same contract as fresh promotions: a corrupted or
        // hand-edited commit fails loudly here instead of serving an invalid workflow.
        WorkflowValidation.validate(workflow);
        return workflow;
    }

    /** HEAD, rather than a working-tree file, is the durable promotion boundary. */
    private byte[] committedWorkflowBytes(String relative) {
        try {
            return committedWorkflowBytes(RegistryGitObjects.headCommit(repository), relative);
        } catch (IOException e) {
            throw new RegistryStoreException("Failed to verify committed workflow " + relative, e);
        }
    }

    private byte[] committedWorkflowBytes(ObjectId commit, String relative) {
        try {
            return RegistryGitObjects.blob(
                    repository, commit, relative, WorkflowValidation.MAX_WORKFLOW_BYTES);
        } catch (IOException e) {
            throw new RegistryStoreException("Failed to verify committed workflow " + relative, e);
        }
    }

    /**
     * Stores one config document envelope per put under
     * {@code configs/<name>.json}. The registry server gates writes (type
     * resolution against the registered schemas, strict parse, and the
     * type's own declared rules); the store only persists and versions via
     * Git history.
     *
     * @param name the config subject
     * @param envelopeJson the envelope document
     * @return the commit id: the document's version
     */
    public String putConfig(String name, String envelopeJson) throws RegistryStoreException {
        RegistryLayout.requireDocumentName(name, "Config");
        Objects.requireNonNull(envelopeJson, "envelopeJson");
        return locked(() -> {
            ObjectId expectedHead = RegistryGitObjects.headCommit(repository);
            String path = RegistryLayout.CONFIGS_DIR + "/" + RegistryLayout.encode(name) + ".json";
            return commitChanges(expectedHead, "Put config " + name,
                    Map.of(path, Optional.of(envelopeJson.getBytes(StandardCharsets.UTF_8))));
        });
    }

    /** The named config document's envelope JSON from HEAD, when present. */
    public Optional<String> config(String name) throws RegistryStoreException {
        RegistryLayout.requireDocumentName(name, "Config");
        String path = RegistryLayout.CONFIGS_DIR + "/" + RegistryLayout.encode(name) + ".json";
        byte[] bytes = headBlobOrThrow("Failed to read config " + name, path);
        return bytes == null
                ? Optional.empty()
                : Optional.of(new String(bytes, StandardCharsets.UTF_8));
    }

    /**
     * The named config document's version: the last commit that touched
     * it, or empty when the document does not exist.
     */
    public Optional<String> configVersion(String name) throws RegistryStoreException {
        RegistryLayout.requireDocumentName(name, "Config");
        String path = RegistryLayout.CONFIGS_DIR + "/" + RegistryLayout.encode(name) + ".json";
        if (headBlobOrThrow("Failed to read config version " + name, path) == null) {
            return Optional.empty();
        }
        try {
            for (var commit : git.log().addPath(path).setMaxCount(1).call()) {
                return Optional.of(commit.getName());
            }
            return Optional.empty();
        } catch (Exception e) {
            throw new RegistryStoreException("Failed to read config version " + name, e);
        }
    }

    /** Every stored config subject from HEAD, sorted. */
    public List<String> configs() throws RegistryStoreException {
        return documentNames(RegistryLayout.CONFIGS_DIR, "Failed to list configs");
    }

    @Override
    public void setCompatibilityMode(String subject, String mode) {
        RegistrationSupport.requireSubject(subject);
        CompatibilityModes.requireValid(mode);
        locked(() -> {
            ObjectId expectedHead = RegistryGitObjects.headCommit(repository);
            loadIndex(expectedHead);
            String subjectDir = layout.subjectDir(subject);
            String configPath = subjectDir + "/" + RegistryLayout.CONFIG_FILE;
            String commitId = commitChanges(expectedHead,
                    "Set compatibility " + subject + " " + mode, Map.of(configPath,
                            Optional.of(json.createObjectNode().put("compatibility", mode)
                                    .toPrettyString().getBytes(StandardCharsets.UTF_8))));
            index = loadIndex(ObjectId.fromString(commitId));
            return null;
        });
    }

    @Override
    public void setGlobalCompatibilityMode(String mode) {
        CompatibilityModes.requireValid(mode);
        locked(() -> {
            ObjectId expectedHead = RegistryGitObjects.headCommit(repository);
            Index fresh = loadIndex(expectedHead);
            String commitId = commitChanges(expectedHead, "Set global compatibility " + mode,
                    Map.of(RegistryLayout.REGISTRY_FILE, Optional.of(RegistryMetadata.registryJson(
                            json, mode, fresh.nextGlobalId()).getBytes(StandardCharsets.UTF_8))));
            index = loadIndex(ObjectId.fromString(commitId));
            return null;
        });
    }

    @Override
    public void close() {
        git.close();
    }

    /** The underlying git handle, for same-package collaborators ({@link RegistryFederation}). */
    Git git() {
        return git;
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

    /**
     * One atomic write: insert the changed blobs, rebuild the touched trees from
     * {@code expectedHead}, insert one commit, and advance HEAD by compare-and-swap against
     * {@code expectedHead}. Pre-ref failures leave HEAD untouched; any failure clears the
     * cached index under the lock so the next read reloads HEAD.
     */
    private String commitChanges(
            ObjectId expectedHead, String message, Map<String, Optional<byte[]>> changes) {
        try {
            return RegistryGitObjects.commit(
                    repository, expectedHead, author, message, changes, writeHook);
        } catch (RegistryGitObjects.CorruptTreeException e) {
            index = null;
            throw new RegistryStoreException("Registry tree is corrupt at " + repoDir, e);
        } catch (RegistryGitObjects.HeadChangedException e) {
            index = null; // HEAD moved externally: the next read must reload it
            throw new RegistryStoreException("Registry write refused at " + repoDir, e);
        } catch (IOException e) {
            index = null;
            throw new RegistryStoreException("Registry write failed at " + repoDir, e);
        }
    }

    private byte[] headBlobOrThrow(String message, String path) throws RegistryStoreException {
        try {
            return RegistryGitObjects.headBlob(repository, path);
        } catch (IOException e) {
            throw new RegistryStoreException(message, e);
        }
    }

    private static RegistryStoreException unavailableDescriptors(IOException cause) {
        return new RegistryStoreException("Descriptor repository is unavailable", cause);
    }

    private List<String> documentNames(String dir, String failureMessage) {
        try {
            return RegistryGitObjects.headChildren(repository, dir).stream()
                    .filter(child -> child.name().endsWith(".json") && child.isBlob())
                    .map(child -> RegistryLayout.decode(
                            child.name().substring(0, child.name().length() - ".json".length())))
                    .sorted()
                    .toList();
        } catch (IOException e) {
            throw new RegistryStoreException(failureMessage, e);
        }
    }

    // ---------------------------------------------------------------- files <-> model

    private Index index() {
        Index current = index;
        if (current != null) {
            return current;
        }
        return locked(() -> {
            Index cached = index;
            if (cached != null) {
                return cached;
            }
            Index loaded = loadIndex(RegistryGitObjects.headCommit(repository));
            index = loaded;
            return loaded;
        });
    }

    /** Rebuilds the whole in-memory view from one pinned commit (or empty defaults when unborn). */
    private Index loadIndex(ObjectId head) throws IOException {
        String globalMode = CompatibilityModes.DEFAULT_GLOBAL;
        int nextGlobalId = 1;
        byte[] registryBytes = RegistryGitObjects.blob(repository, head, RegistryLayout.REGISTRY_FILE);
        if (registryBytes != null) {
            JsonNode node = RegistryMetadata.readMetadata(json, RegistryLayout.REGISTRY_FILE,
                    registryBytes);
            globalMode = RegistryMetadata.readCompatibility(node, RegistryLayout.REGISTRY_FILE);
            JsonNode counter = node.path("nextGlobalId");
            if (!counter.isIntegralNumber() || !counter.canConvertToInt() || counter.intValue() < 1) {
                throw new RegistryStoreException(
                        "Invalid nextGlobalId in " + RegistryLayout.REGISTRY_FILE);
            }
            nextGlobalId = counter.intValue();
        }

        // Subject files of the whole tree in one walk: subjects/<encoded>/<file> -> bytes.
        Map<String, byte[]> files =
                RegistryGitObjects.blobsUnder(repository, head, RegistryLayout.SUBJECTS_DIR);
        Map<String, Map<String, byte[]>> bySubjectDir = new TreeMap<>();
        for (Map.Entry<String, byte[]> file : files.entrySet()) {
            List<String> parts = splitOnSlash(file.getKey());
            if (parts.size() != 3 || !parts.get(0).equals(RegistryLayout.SUBJECTS_DIR)) {
                continue; // the layout is subjects/<encoded>/<file>; anything else is noise
            }
            bySubjectDir.computeIfAbsent(parts.get(1), key -> new TreeMap<>())
                    .put(parts.get(2), file.getValue());
        }

        Map<String, SubjectEntry> subjects = new TreeMap<>();
        Map<Integer, StoredSchema> byGlobalId = new TreeMap<>();
        for (Map.Entry<String, Map<String, byte[]>> subjectDir : bySubjectDir.entrySet()) {
            String subject = RegistryLayout.decode(subjectDir.getKey());
            SubjectEntry entry = loadSubject(subject, subjectDir.getKey(), subjectDir.getValue());
            subjects.put(subject, entry);
            entry.versions().values().forEach(s -> byGlobalId.put(s.globalId(), s));
        }
        // Guard against a stale counter after external commits: never reuse a globalId.
        for (int globalId : byGlobalId.keySet()) {
            nextGlobalId = Math.max(nextGlobalId, globalId + 1);
        }
        return new Index(subjects, byGlobalId, globalMode, nextGlobalId);
    }

    private SubjectEntry loadSubject(String subject, String encodedDir, Map<String, byte[]> files)
            throws IOException {
        TreeMap<Integer, StoredSchema> versions = new TreeMap<>();
        String mode = null;
        byte[] config = files.get(RegistryLayout.CONFIG_FILE);
        if (config != null) {
            String source = RegistryLayout.SUBJECTS_DIR + "/" + encodedDir + "/"
                    + RegistryLayout.CONFIG_FILE;
            mode = RegistryMetadata.readCompatibility(
                    RegistryMetadata.readMetadata(json, source, config), source);
        }
        for (Map.Entry<String, byte[]> file : files.entrySet()) {
            int version = RegistryLayout.versionNumber(file.getKey());
            if (version < 0) {
                continue;
            }
            String schemaText = new String(file.getValue(), StandardCharsets.UTF_8);
            byte[] metaBytes = files.get("v" + version + ".json");
            if (metaBytes == null) {
                throw new RegistryStoreException("Missing metadata for " + subject + " v" + version);
            }
            JsonNode meta = json.readTree(new String(metaBytes, StandardCharsets.UTF_8));
            versions.put(version, RegistryMetadata.storedSchema(subject, version, schemaText, meta));
        }
        return new SubjectEntry(versions, mode);
    }

    /**
     * {@link String#split} on {@code '/'} without regex: one entry per segment, with
     * trailing empty segments dropped and all others retained.
     */
    private static List<String> splitOnSlash(String path) {
        List<String> parts = new ArrayList<>();
        int start = 0;
        int slash;
        while ((slash = path.indexOf('/', start)) >= 0) {
            parts.add(path.substring(start, slash));
            start = slash + 1;
        }
        parts.add(path.substring(start));
        int end = parts.size();
        while (end > 0 && parts.get(end - 1).isEmpty()) {
            end--;
        }
        return parts.subList(0, end);
    }

    // ---------------------------------------------------------------- builder

    /** Builder for {@link GitSchemaRegistryStore}. */
    public static final class Builder {

        private Path repositoryDir;
        private WriteGate writeGate;
        private boolean bareInit;
        private String authorName = "protomolt-registry";
        private String authorEmail = "registry@localhost";

        private Builder() {
        }

        /**
         * Directory of the registry repository; initialized when absent (non-bare by
         * default, bare with {@link #bareInit(boolean)}). An existing bare or non-bare
         * repository is opened as-is, whatever the flag says.
         */
        public Builder repositoryDir(Path repositoryDir) {
            this.repositoryDir = Objects.requireNonNull(repositoryDir, "repositoryDir");
            return this;
        }

        /** Optional compatibility write gate. */
        public Builder writeGate(WriteGate writeGate) {
            this.writeGate = writeGate;
            return this;
        }

        /** Initializes a new repository bare when the directory is absent; default non-bare. */
        public Builder bareInit(boolean bareInit) {
            this.bareInit = bareInit;
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
