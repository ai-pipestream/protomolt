package ai.pipestream.proto.gather.git;

import ai.pipestream.proto.gather.GatherException;
import ai.pipestream.proto.sources.ProtoSourceSet;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.ObjectId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GitProtoGathererTest {

    @TempDir
    Path tempDir;

    private static final String COMMON_PROTO = """
            syntax = "proto3";
            package common.v1;
            message Id { string value = 1; }
            """;

    private static final String SERVICE_PROTO = """
            syntax = "proto3";
            package svc.v1;
            import "common/v1/id.proto";
            message Request { common.v1.Id id = 1; }
            """;

    private static final String OTHER_PROTO = """
            syntax = "proto3";
            package other;
            message Other { string name = 1; }
            """;

    private Path upstream;
    private Git upstreamGit;

    private String initUpstream() throws Exception {
        upstream = tempDir.resolve("upstream");
        Files.createDirectories(upstream);
        upstreamGit = Git.init().setDirectory(upstream.toFile()).setInitialBranch("main").call();
        return upstream.toUri().toString();
    }

    private void writeUpstream(String relative, String content) throws IOException {
        Path file = upstream.resolve(relative);
        Files.createDirectories(file.getParent());
        Files.writeString(file, content);
    }

    private ObjectId commitAll(String message) throws Exception {
        upstreamGit.add().addFilepattern(".").call();
        return upstreamGit.commit()
                .setMessage(message)
                .setAuthor("Gather Test", "gather@test")
                .setCommitter("Gather Test", "gather@test")
                .setSign(false)
                .call()
                .getId();
    }

    private GitProtoGatherer.Builder gatherer(String url) {
        return GitProtoGatherer.builder()
                .repo(url)
                .cacheDir(tempDir.resolve("cache"));
    }

    @Test
    void singleSubdirModeGathersTheDefaultProtoTree() throws Exception {
        String url = initUpstream();
        writeUpstream("proto/common/v1/id.proto", COMMON_PROTO);
        writeUpstream("proto/svc/v1/request.proto", SERVICE_PROTO);
        writeUpstream("README.md", "not gathered");
        commitAll("initial");

        ProtoSourceSet set = gatherer(url).build().gather();

        assertThat(set.paths()).containsExactlyInAnyOrder(
                "common/v1/id.proto", "svc/v1/request.proto");
        assertThat(set.get("common/v1/id.proto").orElseThrow().origin())
                .isEqualTo("git:" + url + "@main");
    }

    @Test
    void customSubdirIsRespected() throws Exception {
        String url = initUpstream();
        writeUpstream("schemas/common/v1/id.proto", COMMON_PROTO);
        commitAll("initial");

        ProtoSourceSet set = gatherer(url).subdir("schemas").build().gather();

        assertThat(set.paths()).containsExactly("common/v1/id.proto");
    }

    @Test
    void missingSubdirFails() throws Exception {
        String url = initUpstream();
        writeUpstream("README.md", "no protos");
        commitAll("initial");

        assertThatThrownBy(gatherer(url).build()::gather)
                .isInstanceOf(GatherException.class)
                .hasMessageContaining("proto subdir does not exist");
    }

    @Test
    void pathsModeGathersOnlyTheListedFilesAndDirectories() throws Exception {
        String url = initUpstream();
        writeUpstream("proto/common/v1/id.proto", COMMON_PROTO);
        writeUpstream("proto/svc/v1/request.proto", SERVICE_PROTO);
        writeUpstream("proto/other/other.proto", OTHER_PROTO);
        commitAll("initial");

        ProtoSourceSet set = gatherer(url)
                .paths(List.of("common/v1/id.proto", "svc"))
                .build()
                .gather();

        assertThat(set.paths()).containsExactlyInAnyOrder(
                "common/v1/id.proto", "svc/v1/request.proto");
    }

    @Test
    void pathsModeFailsOnMissingOrEscapingPaths() throws Exception {
        String url = initUpstream();
        writeUpstream("proto/common/v1/id.proto", COMMON_PROTO);
        writeUpstream("secret.proto", OTHER_PROTO);
        commitAll("initial");

        assertThatThrownBy(gatherer(url).paths(List.of("nope.proto")).build()::gather)
                .isInstanceOf(GatherException.class)
                .hasMessageContaining("does not exist");

        assertThatThrownBy(gatherer(url).paths(List.of("../secret.proto")).build()::gather)
                .isInstanceOf(GatherException.class)
                .hasMessageContaining("escapes");
    }

    @Test
    void modulesModeFlattensPerModuleTreesOntoOneRoot() throws Exception {
        String url = initUpstream();
        writeUpstream("common/proto/common/v1/id.proto", COMMON_PROTO);
        writeUpstream("service/proto/svc/v1/request.proto", SERVICE_PROTO);
        // A "flat" module without the proto subdir: the module dir is the proto root.
        writeUpstream("flat/other/other.proto", OTHER_PROTO);
        commitAll("initial");

        ProtoSourceSet set = gatherer(url)
                .modules(List.of("common", "service", "flat"))
                .build()
                .gather();

        assertThat(set.paths()).containsExactlyInAnyOrder(
                "common/v1/id.proto", "svc/v1/request.proto", "other/other.proto");
    }

    @Test
    void modulesModeTakesPriorityOverPaths() throws Exception {
        String url = initUpstream();
        writeUpstream("common/proto/common/v1/id.proto", COMMON_PROTO);
        commitAll("initial");

        ProtoSourceSet set = gatherer(url)
                .modules(List.of("common"))
                .paths(List.of("ignored"))
                .build()
                .gather();

        assertThat(set.paths()).containsExactly("common/v1/id.proto");
    }

    @Test
    void moduleFlattenCollisionWithIdenticalContentIsTolerated() throws Exception {
        String url = initUpstream();
        writeUpstream("a/proto/common/v1/id.proto", COMMON_PROTO);
        writeUpstream("b/proto/common/v1/id.proto", COMMON_PROTO);
        commitAll("initial");

        ProtoSourceSet set = gatherer(url).modules(List.of("a", "b")).build().gather();

        assertThat(set.paths()).containsExactly("common/v1/id.proto");
    }

    @Test
    void moduleFlattenCollisionWithDifferentContentFails() throws Exception {
        String url = initUpstream();
        writeUpstream("a/proto/common/v1/id.proto", COMMON_PROTO);
        writeUpstream("b/proto/common/v1/id.proto", OTHER_PROTO);
        commitAll("initial");

        assertThatThrownBy(gatherer(url).modules(List.of("a", "b")).build()::gather)
                .isInstanceOf(GatherException.class)
                .hasMessageContaining("common/v1/id.proto");
    }

    @Test
    void missingModuleFails() throws Exception {
        String url = initUpstream();
        writeUpstream("common/proto/common/v1/id.proto", COMMON_PROTO);
        commitAll("initial");

        assertThatThrownBy(gatherer(url).modules(List.of("nope")).build()::gather)
                .isInstanceOf(GatherException.class)
                .hasMessageContaining("nope");
    }

    @Test
    void refCanBeABranch() throws Exception {
        String url = initUpstream();
        writeUpstream("proto/common/v1/id.proto", COMMON_PROTO);
        commitAll("initial");
        upstreamGit.checkout().setCreateBranch(true).setName("feature").call();
        writeUpstream("proto/other/other.proto", OTHER_PROTO);
        commitAll("feature work");

        ProtoSourceSet main = gatherer(url).ref("main").build().gather();
        ProtoSourceSet feature = gatherer(url).ref("feature")
                .cacheDir(tempDir.resolve("cache-feature")).build().gather();

        assertThat(main.paths()).containsExactly("common/v1/id.proto");
        assertThat(feature.paths()).containsExactlyInAnyOrder(
                "common/v1/id.proto", "other/other.proto");
        assertThat(feature.get("other/other.proto").orElseThrow().origin())
                .isEqualTo("git:" + url + "@feature");
    }

    @Test
    void refCanBeATag() throws Exception {
        String url = initUpstream();
        writeUpstream("proto/common/v1/id.proto", COMMON_PROTO);
        commitAll("v1 content");
        upstreamGit.tag().setName("v1").setMessage("release v1").setSigned(false).call();
        writeUpstream("proto/other/other.proto", OTHER_PROTO);
        commitAll("post-tag work");

        ProtoSourceSet set = gatherer(url).ref("v1").build().gather();

        assertThat(set.paths()).containsExactly("common/v1/id.proto");
    }

    @Test
    void refCanBeACommitSha() throws Exception {
        String url = initUpstream();
        writeUpstream("proto/common/v1/id.proto", COMMON_PROTO);
        ObjectId first = commitAll("first");
        writeUpstream("proto/other/other.proto", OTHER_PROTO);
        commitAll("second");

        ProtoSourceSet set = gatherer(url).ref(first.getName()).build().gather();

        assertThat(set.paths()).containsExactly("common/v1/id.proto");
    }

    @Test
    void cacheRootPlacesPerRepoCachesUnderTheOperatorsDirectory() throws Exception {
        String url = initUpstream();
        writeUpstream("proto/common/v1/id.proto", COMMON_PROTO);
        commitAll("initial");
        Path root = tempDir.resolve("operator-cache");

        ProtoSourceSet set = GitProtoGatherer.builder()
                .repo(url)
                .cacheRoot(root)
                .build()
                .gather();

        assertThat(set.paths()).containsExactly("common/v1/id.proto");
        try (var children = Files.list(root)) {
            // One per-repo hash directory under the operator's root; nothing under $HOME.
            assertThat(children.filter(Files::isDirectory).toList()).hasSize(1);
        }
    }

    @Test
    void cacheIsReusedAndFetchSeesUpstreamChanges() throws Exception {
        String url = initUpstream();
        writeUpstream("proto/common/v1/id.proto", COMMON_PROTO);
        commitAll("initial");
        Path cacheDir = tempDir.resolve("cache");

        ProtoSourceSet first = gatherer(url).build().gather();
        assertThat(first.paths()).containsExactly("common/v1/id.proto");
        assertThat(cacheDir).isDirectory();

        // Mutate upstream; the second gather must fetch + reset, not re-clone.
        writeUpstream("proto/other/other.proto", OTHER_PROTO);
        commitAll("added other");

        ProtoSourceSet second = gatherer(url).build().gather();
        assertThat(second.paths()).containsExactlyInAnyOrder(
                "common/v1/id.proto", "other/other.proto");
    }

    @Test
    void offlineWithWarmCacheGathersWithoutTouchingTheRemote() throws Exception {
        String url = initUpstream();
        writeUpstream("proto/common/v1/id.proto", COMMON_PROTO);
        commitAll("initial");

        gatherer(url).build().gather(); // warm the cache

        // Remove the upstream entirely: offline gathering must not need it.
        deleteRecursively(upstream);

        GitProtoGatherer offline = gatherer(url).offline(true).build();
        assertThat(offline.isAvailable()).isTrue();
        assertThat(offline.gather().paths()).containsExactly("common/v1/id.proto");
    }

    @Test
    void offlineWithColdCacheFails() throws Exception {
        String url = initUpstream();
        writeUpstream("proto/common/v1/id.proto", COMMON_PROTO);
        commitAll("initial");

        GitProtoGatherer offline = gatherer(url)
                .cacheDir(tempDir.resolve("cold-cache"))
                .offline(true)
                .build();

        assertThat(offline.isAvailable()).isFalse();
        assertThatThrownBy(offline::gather)
                .isInstanceOf(GatherException.class)
                .hasMessageContaining("Offline")
                .hasMessageContaining(url);
    }

    @Test
    void fetchFailureFallsBackToTheCachedCheckout() throws Exception {
        String url = initUpstream();
        writeUpstream("proto/common/v1/id.proto", COMMON_PROTO);
        commitAll("initial");

        gatherer(url).build().gather(); // warm the cache

        // Break the remote: the next (online) gather's fetch fails and falls back.
        deleteRecursively(upstream);

        ProtoSourceSet set = gatherer(url).build().gather();
        assertThat(set.paths()).containsExactly("common/v1/id.proto");
    }

    /**
     * Falling back to the cached clone is only sound while it holds the requested ref;
     * serving another ref's content under this one's origin would be silently wrong.
     */
    @Test
    void fetchFailureWithAnUncachedRefFails() throws Exception {
        String url = initUpstream();
        writeUpstream("proto/common/v1/id.proto", COMMON_PROTO);
        commitAll("initial");

        gatherer(url).build().gather(); // warm the cache on main

        // Break the remote: 'feature' was never fetched, so it cannot be served.
        deleteRecursively(upstream);

        assertThatThrownBy(gatherer(url).ref("feature").build()::gather)
                .isInstanceOf(GatherException.class)
                .hasMessageContaining("Could not resolve ref feature");
    }

    @Test
    void modulesModeRejectsASubdirThatEscapesTheModule() throws Exception {
        String url = initUpstream();
        writeUpstream("common/proto/common/v1/id.proto", COMMON_PROTO);
        commitAll("initial");
        Path outside = tempDir.resolve("outside");
        Files.createDirectories(outside);
        Files.writeString(outside.resolve("other.proto"), OTHER_PROTO);

        assertThatThrownBy(gatherer(url)
                .modules(List.of("common"))
                .subdir("../../outside")
                .build()::gather)
                .isInstanceOf(GatherException.class)
                .hasMessageContaining("escapes");
    }

    @Test
    void defaultsAndValidation() {
        assertThatThrownBy(() -> GitProtoGatherer.builder().build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("repo");

        GitProtoGatherer gatherer = GitProtoGatherer.builder()
                .repo("https://example.invalid/protos.git")
                .build();
        assertThat(gatherer.origin()).isEqualTo("git:https://example.invalid/protos.git@main");
        assertThat(gatherer.isAvailable()).isTrue();
    }

    private static void deleteRecursively(Path root) throws IOException {
        try (Stream<Path> paths = Files.walk(root)) {
            paths.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            });
        }
    }
}
