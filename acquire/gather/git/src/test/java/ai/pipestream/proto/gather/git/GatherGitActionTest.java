package ai.pipestream.proto.gather.git;

import ai.pipestream.proto.actions.ActionContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.eclipse.jgit.api.Git;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/** The gather-git verb end to end against a real local repository. */
class GatherGitActionTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void gathersCompilesAndReturnsSourcesPlusDescriptors(@TempDir Path tmp) throws Exception {
        Path upstream = tmp.resolve("upstream");
        Files.createDirectories(upstream.resolve("proto/shop/v1"));
        Files.writeString(upstream.resolve("proto/shop/v1/order.proto"), """
                syntax = "proto3";
                package shop.v1;
                message Order { string id = 1; }
                """);
        try (Git git = Git.init().setDirectory(upstream.toFile()).setInitialBranch("main").call()) {
            git.add().addFilepattern(".").call();
            git.commit().setMessage("initial").setSign(false)
                    .setAuthor("t", "t@example.com").setCommitter("t", "t@example.com").call();
        }

        ObjectNode input = MAPPER.createObjectNode();
        input.put("repo", upstream.toUri().toString());
        ObjectNode result = new GatherGitAction().execute(input, ActionContext.create());

        assertThat(result.path("ok").asBoolean()).as(result.toString()).isTrue();
        assertThat(result.path("files").findValuesAsText(null))
                .isNotNull();
        assertThat(result.path("sources").path("shop/v1/order.proto").asText())
                .contains("message Order");
        assertThat(result.path("descriptorSetBase64").asText()).isNotEmpty();
    }

    @Test
    void unreachableRepoIsAStructuredFailureNotAnError(@TempDir Path tmp) {
        ObjectNode input = MAPPER.createObjectNode();
        input.put("repo", tmp.resolve("does-not-exist").toUri().toString());
        ObjectNode result = new GatherGitAction().execute(input, ActionContext.create());
        assertThat(result.path("ok").asBoolean()).isFalse();
        assertThat(result.path("error").asText()).isNotEmpty();
    }
}
