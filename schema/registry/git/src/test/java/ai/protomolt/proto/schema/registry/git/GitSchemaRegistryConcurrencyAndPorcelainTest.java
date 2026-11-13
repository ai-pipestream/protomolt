package ai.protomolt.proto.schema.registry.git;

import ai.protomolt.proto.grpc.workflow.WorkflowValidation;
import ai.protomolt.proto.grpc.workflow.v1.ServiceDependency;
import ai.protomolt.proto.grpc.workflow.v1.StepCompletion;
import ai.protomolt.proto.grpc.workflow.v1.VersionedWorkflow;
import ai.protomolt.proto.grpc.workflow.v1.Workflow;
import ai.protomolt.proto.grpc.workflow.v1.WorkflowStep;
import ai.protomolt.proto.registry.CompatibilityWriteGate;
import ai.protomolt.proto.registry.RegistryStoreException;
import ai.protomolt.proto.registry.SchemaReference;
import ai.protomolt.proto.registry.SchemaRegistryStoreContractTest;
import ai.protomolt.proto.registry.StoredSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.Repository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression suite for issue #867:
 * <ul>
 *   <li>commit-pinned object reads ({@link RegistryGitObjects#blob} and
 *       {@link RegistryGitObjects#blobsUnder}),</li>
 *   <li>monotonic post-commit index publication under concurrent reads, writes and
 *       {@link GitSchemaRegistryStore#refresh()}, and</li>
 *   <li>non-bare {@code DirCache} ({@code .git/index}) alignment across porcelain commits and
 *       refused CAS writes.</li>
 * </ul>
 */
class GitSchemaRegistryConcurrencyAndPorcelainTest {

    private static final long TIMEOUT_SECONDS = 15L;
    private static final String CORE_SUBJECT = "common/v1/core.proto";
    private static final String USER_SUBJECT = "common/v1/user.proto";
    private static final String WORKFLOW_FINGERPRINT = "a".repeat(64);

    private static final String CORE_V1 = """
            syntax = "proto3";
            package common.v1;
            message Core {
              string id = 1;
            }
            """;

    private static final String CORE_V2 = """
            syntax = "proto3";
            package common.v1;
            message Core {
              string id = 1;
              string name = 2;
            }
            """;

    private static final String USER_V1 = """
            syntax = "proto3";
            package common.v1;
            import "common/v1/core.proto";
            message User {
              Core core = 1;
            }
            """;

    @TempDir
    Path tempDir;

    private static String versionedSchema(String pkg, int version) {
        StringBuilder builder = new StringBuilder();
        builder.append("syntax = \"proto3\";\n");
        builder.append("package ").append(pkg).append(";\n");
        builder.append("message Record {\n");
        for (int field = 1; field <= version; field++) {
            builder.append("  string f").append(field).append(" = ").append(field).append(";\n");
        }
        builder.append("}\n");
        return builder.toString();
    }

    private static VersionedWorkflow promotedWorkflow(String version) {
        Workflow workflow = Workflow.newBuilder()
                .setName("analyze-document")
                .setInputType("example.v1.Document")
                .addDependencies(ServiceDependency.newBuilder()
                        .setAlias("nlp")
                        .setServiceProfile("tokenizer")
                        .setEndpoint("local")
                        .setDescriptorFingerprint(WORKFLOW_FINGERPRINT)
                        .build())
                .addSteps(WorkflowStep.newBuilder()
                        .setName("tokenize")
                        .setDependency("nlp")
                        .setMethod("example.v1.Tokenizer/Tokenize")
                        .setCompletion(StepCompletion.STEP_COMPLETION_LIVE)
                        .build())
                .setDeadline(Duration.newBuilder().setSeconds(30).build())
                .build();
        return VersionedWorkflow.newBuilder()
                .setWorkflow(workflow)
                .setVersion(version)
                .setWorkflowFingerprint(WorkflowValidation.fingerprint(workflow))
                .setCreatedAt(Timestamp.newBuilder().setSeconds(10).build())
                .build();
    }

    @Test
    void commitPinnedObjectHelpersReadExactCommitSnapshotEvenAfterHeadAdvances() throws Exception {
        Path dir = tempDir.resolve("pinned-reads");
        try (GitSchemaRegistryStore store = GitSchemaRegistryStore.builder()
                .repositoryDir(dir).build()) {
            Repository repo = store.git().getRepository();
            assertThat(RegistryGitObjects.blob(repo, null, RegistryLayout.REGISTRY_FILE)).isNull();
            assertThat(RegistryGitObjects.blobsUnder(repo, null, RegistryLayout.SUBJECTS_DIR))
                    .isEmpty();

            store.register(CORE_SUBJECT, CORE_V1, List.of());
            ObjectId firstCommit = RegistryGitObjects.headCommit(repo);
            assertThat(firstCommit).isNotNull();

            store.register(CORE_SUBJECT, CORE_V2, List.of());
            store.setGlobalCompatibilityMode("FULL");
            ObjectId thirdCommit = RegistryGitObjects.headCommit(repo);
            assertThat(thirdCommit).isNotNull().isNotEqualTo(firstCommit);

            byte[] firstRegistry = RegistryGitObjects.blob(
                    repo, firstCommit, RegistryLayout.REGISTRY_FILE);
            Map<String, byte[]> firstSubjects = RegistryGitObjects.blobsUnder(
                    repo, firstCommit, RegistryLayout.SUBJECTS_DIR);
            Map<String, byte[]> thirdSubjects = RegistryGitObjects.blobsUnder(
                    repo, thirdCommit, RegistryLayout.SUBJECTS_DIR);

            assertThat(new String(firstRegistry, StandardCharsets.UTF_8))
                    .contains("\"BACKWARD\"");
            assertThat(firstSubjects.keySet()).anyMatch(path -> path.endsWith("/v1.proto"))
                    .noneMatch(path -> path.endsWith("/v2.proto"));
            assertThat(thirdSubjects.keySet()).anyMatch(path -> path.endsWith("/v1.proto"))
                    .anyMatch(path -> path.endsWith("/v2.proto"));
        }
    }

    @Test
    void writeStagesNeverLeakUncommittedDataOrRevertToPreWriteIndexAfterCommit() throws Exception {
        Path dir = tempDir.resolve("deterministic-interleaving");
        try (GitSchemaRegistryStore store = GitSchemaRegistryStore.builder()
                .repositoryDir(dir)
                .writeGate(new CompatibilityWriteGate())
                .build();
                ExecutorService pool = Executors.newFixedThreadPool(3)) {
            StoredSchema v1 = store.register(CORE_SUBJECT, CORE_V1, List.of());
            // Prime the cached index on v1 before overlapping v2 registration.
            assertThat(store.versions(CORE_SUBJECT)).containsExactly(1);

            CountDownLatch atAfterCommit = new CountDownLatch(1);
            CountDownLatch releaseAfterCommit = new CountDownLatch(1);
            CountDownLatch atAfterRefUpdate = new CountDownLatch(1);
            CountDownLatch releaseAfterRefUpdate = new CountDownLatch(1);

            store.writeHook(stage -> {
                try {
                    if (stage == RegistryGitObjects.WriteStage.AFTER_COMMIT) {
                        atAfterCommit.countDown();
                        if (!releaseAfterCommit.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                            throw new IOException("timed out waiting at AFTER_COMMIT");
                        }
                    } else if (stage == RegistryGitObjects.WriteStage.AFTER_REF_UPDATE) {
                        atAfterRefUpdate.countDown();
                        if (!releaseAfterRefUpdate.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                            throw new IOException("timed out waiting at AFTER_REF_UPDATE");
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted in writeHook", e);
                }
            });

            Future<StoredSchema> writeV2 = pool.submit(
                    () -> store.register(CORE_SUBJECT, CORE_V2, List.of()));
            assertThat(atAfterCommit.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();

            // Before HEAD advances, lock-free readers see only committed v1, never uncommitted v2.
            assertThat(store.subjects()).containsExactly(CORE_SUBJECT);
            assertThat(store.versions(CORE_SUBJECT)).containsExactly(1);
            assertThat(store.latest(CORE_SUBJECT)).contains(v1);
            assertThat(store.byGlobalId(v1.globalId() + 1)).isEmpty();

            releaseAfterCommit.countDown();
            assertThat(atAfterRefUpdate.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
            releaseAfterRefUpdate.countDown();

            StoredSchema v2 = writeV2.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            assertThat(v2.version()).isEqualTo(2);

            // Immediately upon return, the post-commit index is published; no reader sees stale v1.
            assertThat(store.versions(CORE_SUBJECT)).containsExactly(1, 2);
            assertThat(store.latest(CORE_SUBJECT)).contains(v2);
            assertThat(store.byGlobalId(v2.globalId())).contains(v2);

            // Second check: start from a cleared cache (refresh()) so index is null when a write
            // enters locked(). A reader arriving at AFTER_REF_UPDATE must wait for the post-write
            // index rather than observing a pre-commit index published early during validation.
            store.refresh();
            CountDownLatch userAtAfterRefUpdate = new CountDownLatch(1);
            CountDownLatch releaseUserAfterRefUpdate = new CountDownLatch(1);
            store.writeHook(stage -> {
                if (stage == RegistryGitObjects.WriteStage.AFTER_REF_UPDATE) {
                    userAtAfterRefUpdate.countDown();
                    try {
                        if (!releaseUserAfterRefUpdate.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                            throw new IOException("timed out waiting at AFTER_REF_UPDATE");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException("interrupted in writeHook", e);
                    }
                }
            });

            List<SchemaReference> refs = List.of(new SchemaReference(CORE_SUBJECT, CORE_SUBJECT, 2));
            Future<StoredSchema> writeUser = pool.submit(
                    () -> store.register(USER_SUBJECT, USER_V1, refs));
            assertThat(userAtAfterRefUpdate.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();

            // Submit a concurrent reader while index is still null and HEAD has just advanced.
            Future<List<String>> concurrentSubjects = pool.submit(store::subjects);
            Future<Void> concurrentRefresh = pool.submit(() -> {
                store.refresh();
                return null;
            });

            releaseUserAfterRefUpdate.countDown();
            StoredSchema userV1 = writeUser.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            assertThat(concurrentSubjects.get(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                    .containsExactly(CORE_SUBJECT, USER_SUBJECT);
            concurrentRefresh.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            assertThat(store.subjects()).containsExactly(CORE_SUBJECT, USER_SUBJECT);
            assertThat(store.latest(USER_SUBJECT)).contains(userV1);
        }
    }

    @Test
    void concurrentReadsWritesModeUpdatesAndRefreshesObserveStrictMonotonicity() throws Exception {
        Path dir = tempDir.resolve("concurrent-monotonicity");
        int subjectCount = 4;
        int versionsPerSubject = 6;
        List<String> subjects = new ArrayList<>();
        Map<String, AtomicInteger> committedVersions = new ConcurrentHashMap<>();
        for (int i = 0; i < subjectCount; i++) {
            String subject = "svc" + i + "/v1/record.proto";
            subjects.add(subject);
            committedVersions.put(subject, new AtomicInteger(0));
        }
        Queue<StoredSchema> committedSchemas = new ConcurrentLinkedQueue<>();

        try (GitSchemaRegistryStore store = GitSchemaRegistryStore.builder()
                .repositoryDir(dir)
                .writeGate(new CompatibilityWriteGate())
                .build();
                ExecutorService pool = Executors.newFixedThreadPool(8)) {
            CountDownLatch start = new CountDownLatch(1);
            AtomicBoolean writersDone = new AtomicBoolean(false);
            List<Future<?>> tasks = new ArrayList<>();

            // Two writer threads registering versions across disjoint subjects + updating modes.
            for (int writer = 0; writer < 2; writer++) {
                final int writerId = writer;
                tasks.add(pool.submit(() -> {
                    assertThat(start.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
                    for (int v = 1; v <= versionsPerSubject; v++) {
                        for (int s = writerId; s < subjectCount; s += 2) {
                            String subject = subjects.get(s);
                            String pkg = "svc" + s + ".v1";
                            StoredSchema stored = store.register(
                                    subject, versionedSchema(pkg, v), List.of());
                            assertThat(stored.version()).isEqualTo(v);
                            committedSchemas.add(stored);
                            committedVersions.get(subject).accumulateAndGet(v, Math::max);
                            if (v % 2 == 0) {
                                store.setCompatibilityMode(subject, "BACKWARD");
                            }
                        }
                        if (writerId == 0 && v % 3 == 0) {
                            store.setGlobalCompatibilityMode("BACKWARD");
                        }
                    }
                    return null;
                }));
            }

            // One invalidator thread repeatedly calling refresh().
            tasks.add(pool.submit(() -> {
                assertThat(start.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
                while (!writersDone.get()) {
                    store.refresh();
                    Thread.onSpinWait();
                }
                return null;
            }));

            // Five reader threads checking linearizable monotonicity against committed versions.
            List<Future<?>> readerTasks = new ArrayList<>();
            for (int reader = 0; reader < 5; reader++) {
                Future<?> readerFuture = pool.submit(() -> {
                    assertThat(start.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
                    while (!writersDone.get()) {
                        assertMonotonicView(store, subjects, committedVersions, committedSchemas);
                    }
                    // Final pass after writers finish.
                    assertMonotonicView(store, subjects, committedVersions, committedSchemas);
                    return null;
                });
                readerTasks.add(readerFuture);
            }

            start.countDown();
            // Wait for writers first, then stop reader/refresh loops and propagate any failure.
            tasks.get(0).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            tasks.get(1).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            writersDone.set(true);
            tasks.get(2).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            for (Future<?> readerFuture : readerTasks) {
                readerFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }

            assertThat(store.subjects()).containsExactlyElementsOf(subjects.stream().sorted().toList());
            for (String subject : subjects) {
                assertThat(store.versions(subject)).hasSize(versionsPerSubject);
                assertThat(store.latest(subject))
                        .hasValueSatisfying(s -> assertThat(s.version()).isEqualTo(versionsPerSubject));
            }
        }
    }

    private static void assertMonotonicView(
            GitSchemaRegistryStore store,
            List<String> subjects,
            Map<String, AtomicInteger> committedVersions,
            Queue<StoredSchema> committedSchemas) {
        for (String subject : subjects) {
            // Read the committed floor BEFORE querying the store so linearizability holds.
            int floor = committedVersions.get(subject).get();
            if (floor == 0) {
                continue;
            }
            List<Integer> versions = store.versions(subject);
            assertThat(versions).as("versions(%s) with floor %d", subject, floor)
                    .isNotEmpty()
                    .contains(floor);
            assertThat(store.latest(subject)).as("latest(%s) with floor %d", subject, floor)
                    .hasValueSatisfying(latest -> assertThat(latest.version())
                            .isGreaterThanOrEqualTo(floor));
            assertThat(store.subjects()).contains(subject);
        }
        for (StoredSchema schema : committedSchemas) {
            assertThat(store.byGlobalId(schema.globalId())).contains(schema);
        }
        assertThat(store.globalCompatibilityMode()).isEqualTo("BACKWARD");
    }

    @Test
    void nonBarePorcelainCommitsPreserveSchemasWorkflowsConfigsAndDescriptors() throws Exception {
        Path dir = tempDir.resolve("non-bare-porcelain");
        ByteString descriptorBytes = SchemaRegistryStoreContractTest.descriptorSet();
        String fingerprint = HexFormat.of().formatHex(
                MessageDigest.getInstance("SHA-256").digest(descriptorBytes.toByteArray()));
        VersionedWorkflow promotedV1 = promotedWorkflow("v1");

        StoredSchema coreV1;
        StoredSchema coreV2;
        StoredSchema userV1;
        try (GitSchemaRegistryStore store = GitSchemaRegistryStore.builder()
                .repositoryDir(dir).build()) {
            assertThat(store.git().getRepository().isBare()).isFalse();

            coreV1 = store.register(CORE_SUBJECT, CORE_V1, List.of());
            coreV2 = store.register(CORE_SUBJECT, CORE_V2, List.of());
            userV1 = store.register(USER_SUBJECT, USER_V1,
                    List.of(new SchemaReference(CORE_SUBJECT, CORE_SUBJECT, coreV1.version())));
            store.setCompatibilityMode(USER_SUBJECT, "FULL");
            store.setGlobalCompatibilityMode("FORWARD");
            store.putWorkflow("ingest.v1", "{\"steps\":[\"parse\"]}");
            store.putWorkflow(promotedV1);
            store.putConfig("limits", "{\"maxBatch\":32}");
            store.putDescriptorSet(fingerprint, descriptorBytes);

            // Working tree itself stays clean of registry paths.
            try (var files = Files.list(dir)) {
                assertThat(files.map(p -> p.getFileName().toString()))
                        .containsExactlyInAnyOrder(".git", RegistryLayout.LOCK_FILE);
            }

            Git git = Git.wrap(store.git().getRepository());

            // 1. Porcelain empty commit (without git reset) must keep the aligned DirCache tree.
            git.commit()
                    .setAllowEmpty(true)
                    .setMessage("porcelain empty commit")
                    .call();

            assertAllRegistryStatePresent(
                    store, coreV1, coreV2, userV1, promotedV1, fingerprint, descriptorBytes);
            store.refresh();
            assertAllRegistryStatePresent(
                    store, coreV1, coreV2, userV1, promotedV1, fingerprint, descriptorBytes);

            // 2. Staging a new working-tree file via git add + git commit (without git reset)
            //    must add README.md while preserving every registry entry on HEAD.
            Files.writeString(dir.resolve("README.md"), "# Registry Repository\n");
            git.add().addFilepattern("README.md").call();
            git.commit().setMessage("add readme").call();

            assertThat(RegistryGitTestSupport.headBlob(dir, "README.md"))
                    .isEqualTo("# Registry Repository\n".getBytes(StandardCharsets.UTF_8));
            assertAllRegistryStatePresent(
                    store, coreV1, coreV2, userV1, promotedV1, fingerprint, descriptorBytes);
            store.refresh();
            assertAllRegistryStatePresent(
                    store, coreV1, coreV2, userV1, promotedV1, fingerprint, descriptorBytes);

            // 3. Subsequent store writes preserve README.md AND keep DirCache aligned again.
            StoredSchema extra = store.register("extra/v1/item.proto", CORE_V1, List.of());
            git.commit()
                    .setAllowEmpty(true)
                    .setMessage("porcelain commit after extra schema")
                    .call();
            store.refresh();
            assertThat(store.latest("extra/v1/item.proto")).contains(extra);
            assertThat(RegistryGitTestSupport.headBlob(dir, "README.md"))
                    .isEqualTo("# Registry Repository\n".getBytes(StandardCharsets.UTF_8));
        }

        // 4. Reopening a fresh store instance on the repository sees everything intact.
        try (GitSchemaRegistryStore reopened = GitSchemaRegistryStore.builder()
                .repositoryDir(dir).build()) {
            assertAllRegistryStatePresent(
                    reopened, coreV1, coreV2, userV1, promotedV1, fingerprint, descriptorBytes);
            assertThat(reopened.subjects())
                    .containsExactly(CORE_SUBJECT, USER_SUBJECT, "extra/v1/item.proto");
        }
    }

    private static void assertAllRegistryStatePresent(
            GitSchemaRegistryStore store,
            StoredSchema coreV1,
            StoredSchema coreV2,
            StoredSchema userV1,
            VersionedWorkflow promotedV1,
            String fingerprint,
            ByteString descriptorBytes) {
        assertThat(store.subjects()).contains(CORE_SUBJECT, USER_SUBJECT);
        assertThat(store.versions(CORE_SUBJECT)).containsExactly(1, 2);
        assertThat(store.version(CORE_SUBJECT, 1)).contains(coreV1);
        assertThat(store.latest(CORE_SUBJECT)).contains(coreV2);
        assertThat(store.latest(USER_SUBJECT)).contains(userV1);
        assertThat(store.byGlobalId(coreV1.globalId())).contains(coreV1);
        assertThat(store.byGlobalId(coreV2.globalId())).contains(coreV2);
        assertThat(store.byGlobalId(userV1.globalId())).contains(userV1);
        assertThat(store.compatibilityMode(USER_SUBJECT)).contains("FULL");
        assertThat(store.globalCompatibilityMode()).isEqualTo("FORWARD");
        assertThat(store.workflow("ingest.v1")).contains("{\"steps\":[\"parse\"]}");
        assertThat(store.workflows()).containsExactly("ingest.v1");
        assertThat(store.workflow("analyze-document", "v1")).contains(promotedV1);
        assertThat(store.workflowNames()).containsExactly("analyze-document");
        assertThat(store.config("limits")).contains("{\"maxBatch\":32}");
        assertThat(store.configs()).containsExactly("limits");
        assertThat(store.descriptorSet(fingerprint)).contains(descriptorBytes);
    }

    @Test
    void refusedWriteOnNonBareRepoUnlocksDirCacheWithoutStagingRefusedEntries() throws Exception {
        Path dir = tempDir.resolve("refused-dircache-rollback");
        try (GitSchemaRegistryStore store = GitSchemaRegistryStore.builder()
                .repositoryDir(dir).build()) {
            store.register(CORE_SUBJECT, CORE_V1, List.of());

            store.writeHook(stage -> {
                if (stage == RegistryGitObjects.WriteStage.AFTER_COMMIT) {
                    try {
                        RegistryGitTestSupport.commitEmptyExternally(dir, "External advance");
                    } catch (Exception e) {
                        throw new IOException("external advance failed", e);
                    }
                }
            });

            assertThatThrownBy(() -> store.register("refused.proto", CORE_V1, List.of()))
                    .isInstanceOf(RegistryStoreException.class)
                    .hasMessageContaining("refused");
            store.writeHook(RegistryGitObjects.WriteHook.NONE);

            // .git/index.lock must not remain, and a subsequent porcelain commit must not pick up
            // the refused schema from .git/index.
            assertThat(Files.exists(dir.resolve(".git/index.lock"))).isFalse();
            Git.wrap(store.git().getRepository())
                    .commit()
                    .setAllowEmpty(true)
                    .setMessage("porcelain commit after refused write")
                    .call();

            store.refresh();
            assertThat(store.subjects()).containsExactly(CORE_SUBJECT);
        }
    }

    @Test
    void idempotentRegistrationAfterRefreshRepublishesCommittedIndexWithoutNewCommit() throws Exception {
        Path dir = tempDir.resolve("idempotent-republish");
        try (GitSchemaRegistryStore store = GitSchemaRegistryStore.builder()
                .repositoryDir(dir).build()) {
            StoredSchema v1 = store.register(CORE_SUBJECT, CORE_V1, List.of());
            int commitsBefore = RegistryGitTestSupport.commitMessages(dir).size();

            store.refresh();
            StoredSchema again = store.register(CORE_SUBJECT, CORE_V1, List.of());

            assertThat(again).isEqualTo(v1);
            assertThat(RegistryGitTestSupport.commitMessages(dir)).hasSize(commitsBefore);
            assertThat(store.subjects()).containsExactly(CORE_SUBJECT);
            assertThat(store.latest(CORE_SUBJECT)).contains(v1);
        }
    }
}
