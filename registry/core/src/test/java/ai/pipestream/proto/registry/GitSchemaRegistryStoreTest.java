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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Runs the store contract against {@link GitSchemaRegistryStore} plus the git-specific
 * behavior: one commit per write, persistence across reopen, {@link SchemaRegistryStore#refresh()}
 * after external commits, on-disk layout and two-store lock contention.
 */
class GitSchemaRegistryStoreTest extends SchemaRegistryStoreContractTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    /** A third revision of {@link #CORE_PROTO}, distinct in content from v1 and v2. */
    private static final String CORE_PROTO_V3 = """
            syntax = "proto3";
            package common.v1;
            message Core {
              string id = 1;
              string name = 2;
              string label = 3;
            }
            """;

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

    /**
     * The counter in {@code registry.json} is advisory: an external commit can add versions
     * without touching it. The index recomputes the floor from the ids actually present, so a
     * stale counter cannot hand out an id that is already in use.
     */
    @Test
    void aStaleCounterAfterAnExternalCommitDoesNotReissueAGlobalId() throws Exception {
        Path dir = tempDir.resolve("stale-counter");
        try (GitSchemaRegistryStore store = storeAt(dir)) {
            StoredSchema v1 = store.register(CORE_SUBJECT, CORE_PROTO, List.of());
            assertThat(v1.globalId()).isEqualTo(1);

            // v2 lands with globalId 5 while registry.json still claims the next id is 2.
            commitVersionExternally(dir, CORE_SUBJECT, 2, CORE_PROTO_V2, 5, 2);
            assertThat(nextGlobalIdOnDisk(dir)).isEqualTo(2);

            StoredSchema v3 = store.register(CORE_SUBJECT, CORE_PROTO_V3, List.of());

            assertThat(v3.version()).isEqualTo(3);
            assertThat(v3.globalId()).isEqualTo(6);
            // The externally committed schema is still reachable under its own id.
            assertThat(store.byGlobalId(5))
                    .map(StoredSchema::schemaText)
                    .contains(CORE_PROTO_V2);
            assertThat(store.byGlobalId(6)).contains(v3);
            assertThat(nextGlobalIdOnDisk(dir)).isEqualTo(7);
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

    // ---------------------------------------------------------------- chains

    @Test
    void aChainRoundTripsAndEachPutIsOneCommit() throws Exception {
        Path dir = tempDir.resolve("chain-roundtrip");
        String chainJson = """
                {"steps":[{"call":"enrich"},{"call":"index"}]}""";
        try (GitSchemaRegistryStore store = storeAt(dir)) {
            store.putChain("ingest.v1", chainJson);

            assertThat(store.chain("ingest.v1")).contains(chainJson);
        }
        assertThat(dir.resolve("chains").resolve("ingest.v1.json")).content().isEqualTo(chainJson);
        assertThat(commitMessages(dir)).containsExactly("Put chain ingest.v1");
    }

    @Test
    void puttingAnExistingNameReplacesTheDocumentAndKeepsTheOldOneInHistory() throws Exception {
        Path dir = tempDir.resolve("chain-overwrite");
        try (GitSchemaRegistryStore store = storeAt(dir)) {
            store.putChain("ingest", "{\"v\":1}");
            store.putChain("ingest", "{\"v\":2}");

            assertThat(store.chain("ingest")).contains("{\"v\":2}");
            assertThat(store.chains()).containsExactly("ingest");
        }
        // Overwriting is a second commit, not an edit in place: the v1 document stays in the log.
        assertThat(commitMessages(dir)).containsExactly("Put chain ingest", "Put chain ingest");
    }

    @Test
    void anUnknownChainNameReadsAsEmpty() throws Exception {
        try (GitSchemaRegistryStore store = storeAt(tempDir.resolve("chain-missing"))) {
            assertThat(store.chains()).isEmpty();
            assertThat(store.chain("never-stored")).isEmpty();

            store.putChain("stored", "{}");
            assertThat(store.chain("never-stored")).isEmpty();
        }
    }

    @Test
    void chainsAreListedSortedRegardlessOfWriteOrder() throws Exception {
        try (GitSchemaRegistryStore store = storeAt(tempDir.resolve("chain-order"))) {
            store.putChain("zeta", "{}");
            store.putChain("alpha", "{}");
            store.putChain("Beta_2", "{}");
            store.putChain("mid.chain-1", "{}");

            assertThat(store.chains())
                    .containsExactly("Beta_2", "alpha", "mid.chain-1", "zeta");
        }
    }

    @Test
    void chainNamesOutsideTheAllowedCharacterSetAreRejected() throws Exception {
        try (GitSchemaRegistryStore store = storeAt(tempDir.resolve("chain-names"))) {
            for (String rejected : List.of("", "   ", "has space", "a/b", "a\\b", "a:b", "a$b",
                    "chains/../escape", "naïve")) {
                assertThatThrownBy(() -> store.putChain(rejected, "{}"))
                        .as("putChain(%s)", rejected)
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessage("Chain names use [A-Za-z0-9._-]; got '" + rejected + "'");
                assertThatThrownBy(() -> store.chain(rejected))
                        .as("chain(%s)", rejected)
                        .isInstanceOf(IllegalArgumentException.class);
            }
            assertThatThrownBy(() -> store.putChain(null, "{}"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Chain names use [A-Za-z0-9._-]; got 'null'");

            // Rejected names are refused before anything is written.
            assertThat(store.chains()).isEmpty();
        }
    }

    @Test
    void putChainRejectsANullDocument() throws Exception {
        try (GitSchemaRegistryStore store = storeAt(tempDir.resolve("chain-null-doc"))) {
            assertThatThrownBy(() -> store.putChain("ingest", null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("chainJson");
        }
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
        commitVersionExternally(repoDir, subject, version, schemaText, globalId, globalId + 1);
    }

    /** As above, but writes an explicit {@code nextGlobalId} so it can be left stale. */
    private static void commitVersionExternally(Path repoDir, String subject, int version,
                                                String schemaText, int globalId,
                                                int nextGlobalId) throws Exception {
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
                .put("nextGlobalId", nextGlobalId)
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

    private static int nextGlobalIdOnDisk(Path repoDir) throws Exception {
        return JSON.readTree(Files.readString(repoDir.resolve("registry.json")))
                .path("nextGlobalId").asInt();
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
