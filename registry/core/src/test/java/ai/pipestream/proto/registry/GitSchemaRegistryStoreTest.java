package ai.pipestream.proto.registry;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.revwalk.RevCommit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs the store contract against {@link GitSchemaRegistryStore} plus the git-specific
 * behavior: one commit per write, persistence across reopen, {@link SchemaRegistryStore#refresh()}
 * after external commits, on-disk layout and two-store lock contention.
 */
class GitSchemaRegistryStoreTest extends SchemaRegistryStoreContractTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @TempDir
    Path tempDir;

    private int repos;

    @Override
    protected SchemaRegistryStore create(SchemaRegistryStore.WriteGate gate) {
        return GitSchemaRegistryStore.builder()
                .repositoryDir(tempDir.resolve("repo-" + repos++))
                .writeGate(gate)
                .build();
    }

    private GitSchemaRegistryStore storeAt(Path dir) {
        return GitSchemaRegistryStore.builder().repositoryDir(dir).build();
    }

    // ---------------------------------------------------------------- git behavior

    @Test
    void everyWriteIsExactlyOneCommitWithADescriptiveMessage() throws Exception {
        Path dir = tempDir.resolve("commits");
        try (GitSchemaRegistryStore store = storeAt(dir)) {
            store.register(CORE_SUBJECT, CORE_PROTO, List.of());
            store.register(CORE_SUBJECT, CORE_PROTO_V2, List.of());
            store.register(CORE_SUBJECT, CORE_PROTO_V2, List.of()); // idempotent: no commit
            store.setCompatibilityMode(CORE_SUBJECT, "FULL");
            store.setGlobalCompatibilityMode("NONE");
        }
        assertThat(commitMessages(dir)).containsExactly( // git log is newest-first
                "Set global compatibility NONE",
                "Set compatibility " + CORE_SUBJECT + " FULL",
                "Register " + CORE_SUBJECT + " v2",
                "Register " + CORE_SUBJECT + " v1");
    }

    @Test
    void commitsCarryTheConfiguredAuthor() throws Exception {
        Path dir = tempDir.resolve("author");
        try (GitSchemaRegistryStore store = GitSchemaRegistryStore.builder()
                .repositoryDir(dir)
                .author("Registry Bot", "bot@example.com")
                .build()) {
            store.register(CORE_SUBJECT, CORE_PROTO, List.of());
        }
        try (Git git = Git.open(dir.toFile())) {
            RevCommit head = git.log().setMaxCount(1).call().iterator().next();
            assertThat(head.getAuthorIdent().getName()).isEqualTo("Registry Bot");
            assertThat(head.getAuthorIdent().getEmailAddress()).isEqualTo("bot@example.com");
        }
    }

    @Test
    void layoutUsesUrlEncodedSubjectDirectories() throws Exception {
        Path dir = tempDir.resolve("layout");
        try (GitSchemaRegistryStore store = storeAt(dir)) {
            store.register(CORE_SUBJECT, CORE_PROTO, List.of());
            store.setCompatibilityMode(CORE_SUBJECT, "FULL");
        }
        Path subjectDir = dir.resolve("subjects")
                .resolve(URLEncoder.encode(CORE_SUBJECT, StandardCharsets.UTF_8));
        assertThat(subjectDir.resolve("v1.proto")).content().isEqualTo(CORE_PROTO);
        assertThat(subjectDir.resolve("v1.json")).exists();
        assertThat(subjectDir.resolve("config.json")).exists();
        assertThat(dir.resolve("registry.json")).exists();
    }

    @Test
    void stateSurvivesReopeningTheRepository() throws Exception {
        Path dir = tempDir.resolve("reopen");
        StoredSchema user;
        try (GitSchemaRegistryStore store = storeAt(dir)) {
            store.register(CORE_SUBJECT, CORE_PROTO, List.of());
            user = store.register(USER_SUBJECT, USER_PROTO,
                    List.of(new SchemaReference(CORE_SUBJECT, CORE_SUBJECT, 1)));
            store.setCompatibilityMode(USER_SUBJECT, "FULL");
            store.setGlobalCompatibilityMode("FORWARD");
        }
        try (GitSchemaRegistryStore reopened = storeAt(dir)) {
            assertThat(reopened.subjects()).containsExactly(CORE_SUBJECT, USER_SUBJECT);
            assertThat(reopened.latest(USER_SUBJECT)).contains(user);
            assertThat(reopened.byGlobalId(user.globalId())).contains(user);
            assertThat(reopened.compatibilityMode(USER_SUBJECT)).contains("FULL");
            assertThat(reopened.globalCompatibilityMode()).isEqualTo("FORWARD");
            // The globalId counter continues, never reusing ids.
            StoredSchema next = reopened.register("new.proto", CORE_PROTO, List.of());
            assertThat(next.globalId()).isGreaterThan(user.globalId());
        }
    }

    @Test
    void refreshSeesVersionsCommittedExternally() throws Exception {
        Path dir = tempDir.resolve("external");
        try (GitSchemaRegistryStore store = storeAt(dir)) {
            StoredSchema v1 = store.register(CORE_SUBJECT, CORE_PROTO, List.of());
            assertThat(store.latest(CORE_SUBJECT)).contains(v1); // populate the cached index

            commitVersionExternally(dir, CORE_SUBJECT, 2, CORE_PROTO_V2, v1.globalId() + 1);
            assertThat(store.versions(CORE_SUBJECT)).containsExactly(1); // still the cached view

            store.refresh();
            assertThat(store.versions(CORE_SUBJECT)).containsExactly(1, 2);
            StoredSchema v2 = store.latest(CORE_SUBJECT).orElseThrow();
            assertThat(v2.schemaText()).isEqualTo(CORE_PROTO_V2);
            assertThat(v2.globalId()).isEqualTo(v1.globalId() + 1);
        }
    }

    @Test
    void twoStoresOnOneRepositoryRegisterConcurrentlyWithoutCorruption() throws Exception {
        Path dir = tempDir.resolve("contention");
        int perStore = 4;
        try (GitSchemaRegistryStore storeA = storeAt(dir);
                GitSchemaRegistryStore storeB = storeAt(dir);
                ExecutorService pool = Executors.newFixedThreadPool(2)) {
            CountDownLatch start = new CountDownLatch(1);
            Future<List<StoredSchema>> a = pool.submit(() -> registerAll(storeA, "a", perStore, start));
            Future<List<StoredSchema>> b = pool.submit(() -> registerAll(storeB, "b", perStore, start));
            start.countDown();

            List<StoredSchema> all = new ArrayList<>(a.get());
            all.addAll(b.get());
            assertThat(all).hasSize(2 * perStore);
            assertThat(all.stream().map(StoredSchema::globalId).toList()).doesNotHaveDuplicates();

            storeA.refresh();
            assertThat(storeA.subjects()).hasSize(2 * perStore);
        }
        assertThat(commitMessages(dir)).hasSize(2 * perStore);
    }

    // ---------------------------------------------------------------- helpers

    private static List<StoredSchema> registerAll(SchemaRegistryStore store, String prefix,
                                                  int count, CountDownLatch start) throws Exception {
        start.await();
        List<StoredSchema> registered = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            registered.add(store.register(prefix + "/v1/s" + i + ".proto", CORE_PROTO, List.of()));
        }
        return registered;
    }

    /** Hand-crafts a new version + counter update and commits it with plain JGit. */
    private static void commitVersionExternally(Path repoDir, String subject, int version,
                                                String schemaText, int globalId) throws Exception {
        String encoded = URLEncoder.encode(subject, StandardCharsets.UTF_8);
        Path subjectDir = repoDir.resolve("subjects").resolve(encoded);
        Files.writeString(subjectDir.resolve("v" + version + ".proto"), schemaText);
        var meta = JSON.createObjectNode()
                .put("globalId", globalId)
                .put("contentHash", SchemaContents.contentHash(schemaText, List.of()));
        meta.putArray("references");
        Files.writeString(subjectDir.resolve("v" + version + ".json"), meta.toString());
        Files.writeString(repoDir.resolve("registry.json"), JSON.createObjectNode()
                .put("compatibility", "BACKWARD")
                .put("nextGlobalId", globalId + 1)
                .toString());
        try (Git git = Git.open(repoDir.toFile())) {
            git.add()
                    .addFilepattern("subjects/" + encoded + "/v" + version + ".proto")
                    .addFilepattern("subjects/" + encoded + "/v" + version + ".json")
                    .addFilepattern("registry.json")
                    .call();
            git.commit().setMessage("External register " + subject + " v" + version).call();
        }
    }

    private static List<String> commitMessages(Path repoDir) throws Exception {
        try (Git git = Git.open(repoDir.toFile())) {
            List<String> messages = new ArrayList<>();
            for (RevCommit commit : git.log().call()) {
                messages.add(commit.getFullMessage().strip());
            }
            return messages;
        }
    }
}
