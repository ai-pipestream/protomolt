package ai.protomolt.proto.schema.registry.git;

import ai.protomolt.proto.registry.RegistryStoreException;
import ai.protomolt.proto.registry.StoredSchema;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Fault injection across the stages of one atomic write, against a repository of real JGit
 * objects: a failure while inserting blobs, trees or the commit leaves HEAD untouched, an
 * external HEAD advance is refused without being overwritten, and a failure after the ref
 * update is recoverable by re-reading HEAD.
 */
class AtomicWriteFaultInjectionTest {

    private static final String PROTO = """
            syntax = "proto3";
            package test.v1;
            message A {
              string id = 1;
            }
            """;

    @TempDir
    Path tempDir;

    private static RegistryGitObjects.WriteHook faultAt(RegistryGitObjects.WriteStage stage) {
        return seen -> {
            if (seen == stage) {
                throw new IOException("injected fault at " + stage);
            }
        };
    }

    @Test
    void faultAfterBlobsLeavesHeadUnchanged() throws Exception {
        faultBeforeRefUpdateLeavesHeadUnchanged(RegistryGitObjects.WriteStage.AFTER_BLOBS);
    }

    @Test
    void faultAfterTreesLeavesHeadUnchanged() throws Exception {
        faultBeforeRefUpdateLeavesHeadUnchanged(RegistryGitObjects.WriteStage.AFTER_TREES);
    }

    @Test
    void faultAfterCommitLeavesHeadUnchanged() throws Exception {
        faultBeforeRefUpdateLeavesHeadUnchanged(RegistryGitObjects.WriteStage.AFTER_COMMIT);
    }

    private void faultBeforeRefUpdateLeavesHeadUnchanged(RegistryGitObjects.WriteStage stage)
            throws Exception {
        Path dir = tempDir.resolve("fault-" + stage.name().toLowerCase());
        try (GitSchemaRegistryStore store = GitSchemaRegistryStore.builder()
                .repositoryDir(dir).build()) {
            String headBefore = RegistryGitTestSupport.head(dir); // unborn branch: null
            store.writeHook(faultAt(stage));
            assertThatThrownBy(() -> store.register("test.proto", PROTO, List.of()))
                    .isInstanceOf(RegistryStoreException.class)
                    .hasRootCauseMessage("injected fault at " + stage);

            store.refresh();
            assertThat(store.subjects()).isEmpty();
            // Pre-ref failures never advanced HEAD (still unborn) and leaked only
            // unreachable objects.
            assertThat(RegistryGitTestSupport.head(dir)).isEqualTo(headBefore);
        }
    }

    @Test
    void externalHeadAdvanceIsRefusedWithoutOverwritingIt() throws Exception {
        Path dir = tempDir.resolve("external-advance");
        try (GitSchemaRegistryStore store = GitSchemaRegistryStore.builder()
                .repositoryDir(dir).build()) {
            store.register("first.proto", PROTO, List.of());
            String headBefore = RegistryGitTestSupport.head(dir);

            // While the store's write sits between commit insertion and the ref update,
            // an outside actor (one not honoring the lock file) advances HEAD.
            AtomicReference<String> external = new AtomicReference<>();
            store.writeHook(seen -> {
                if (seen == RegistryGitObjects.WriteStage.AFTER_COMMIT) {
                    try {
                        external.set(RegistryGitTestSupport.commitEmptyExternally(dir, "External advance"));
                    } catch (Exception e) {
                        throw new IOException("external advance failed", e);
                    }
                }
            });

            assertThatThrownBy(() -> store.register("second.proto", PROTO, List.of()))
                    .isInstanceOf(RegistryStoreException.class)
                    .hasMessageContaining("refused")
                    .satisfies(error -> assertThat(error.getCause().getMessage())
                            .contains("changed concurrently"));

            // The compare-and-swap lost and left the external commit in place.
            assertThat(RegistryGitTestSupport.head(dir))
                    .isEqualTo(external.get())
                    .isNotEqualTo(headBefore);
            assertThat(RegistryGitTestSupport.commitMessages(dir).getFirst())
                    .isEqualTo("External advance");

            // The refused write is not visible after rereading HEAD.
            store.refresh();
            assertThat(store.subjects()).containsExactly("first.proto");
        }
    }

    @Test
    void conflictingFileAndDescendantChangesFailBeforeWriting() throws Exception {
        Path dir = tempDir.resolve("conflicting-paths");
        try (var git = org.eclipse.jgit.api.Git.init().setDirectory(dir.toFile()).call()) {
            assertThatThrownBy(() -> RegistryGitObjects.commit(git.getRepository(),
                    new org.eclipse.jgit.lib.PersonIdent("Test", "test@example.test"), "Invalid tree",
                    java.util.Map.of("a", java.util.Optional.of(new byte[]{1}),
                            "a/b", java.util.Optional.of(new byte[]{2})),
                    RegistryGitObjects.WriteHook.NONE))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("file and its descendant");
            assertThat(RegistryGitTestSupport.head(dir)).isNull();
        }
    }

    @Test
    void faultAfterRefUpdateIsRecoverableByRereadingHead() throws Exception {
        Path dir = tempDir.resolve("post-ref-fault");
        try (GitSchemaRegistryStore store = GitSchemaRegistryStore.builder()
                .repositoryDir(dir).build()) {
            store.writeHook(faultAt(RegistryGitObjects.WriteStage.AFTER_REF_UPDATE));

            // An ambiguous outcome: the write reported failure, but the ref update landed.
            assertThatThrownBy(() -> store.register("test.proto", PROTO, List.of()))
                    .isInstanceOf(RegistryStoreException.class)
                    .hasRootCauseMessage("injected fault at AFTER_REF_UPDATE");

            // Recovery is refresh() (re-read HEAD), never a retry blind to the outcome.
            store.refresh();
            assertThat(store.subjects()).containsExactly("test.proto");

            // A retried identical write is idempotent against the landed commit.
            StoredSchema again = store.register("test.proto", PROTO, List.of());
            assertThat(again.version()).isEqualTo(1);
            assertThat(RegistryGitTestSupport.commitMessages(dir)).hasSize(1);
        }
    }
}
