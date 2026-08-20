package ai.pipestream.proto.registry.service;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.registry.GitSchemaRegistryStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The config lane's write verb: a valid document publishes through the
 * same gate the HTTP config gate mounts and the commit id comes back as
 * the version; an unresolvable type refuses and nothing lands; missing
 * inputs refuse by name.
 */
class PublishConfigActionTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    private static final String LIMITS_PROTO = """
            syntax = "proto3";
            package cfg.v1;
            message Limits {
              int32 max = 1;
            }
            """;

    @TempDir
    Path tempDir;

    private GitSchemaRegistryStore store;
    private ActionCatalog catalog;

    @BeforeEach
    void setUp() {
        store = GitSchemaRegistryStore.builder().repositoryDir(tempDir).build();
        store.register("cfg/v1/limits.proto", LIMITS_PROTO, List.of());
        catalog = ActionCatalog.defaults(ActionContext.create())
                .register(new PublishConfigAction(store));
    }

    @AfterEach
    void tearDown() {
        store.close();
    }

    private ObjectNode input(String json) throws Exception {
        return (ObjectNode) JSON.readTree(json);
    }

    @Test
    void aValidDocumentPublishesWithTheCommitAsTheVersion() throws Exception {
        ObjectNode result = catalog.execute(PublishConfigAction.NAME, input("""
                {"name": "limits", "messageType": "cfg.v1.Limits",
                 "config": {"max": 5}}"""));
        assertThat(result.get("name").asText()).isEqualTo("limits");
        assertThat(result.get("messageType").asText()).isEqualTo("cfg.v1.Limits");
        String version = result.get("version").asText();
        assertThat(version).isNotBlank();
        assertThat(store.configVersion("limits")).contains(version);
        assertThat(store.config("limits")).isPresent();

        // Every publish is a commit: the version moves, evidence of which
        // document a consumer runs.
        ObjectNode again = catalog.execute(PublishConfigAction.NAME, input("""
                {"name": "limits", "messageType": "cfg.v1.Limits",
                 "config": {"max": 9}}"""));
        assertThat(again.get("version").asText()).isNotEqualTo(version);
    }

    @Test
    void anUnresolvableTypeRefusesAndNothingLands() throws Exception {
        assertThatThrownBy(() -> catalog.execute(PublishConfigAction.NAME, input("""
                {"name": "orphan", "messageType": "cfg.v1.Nope",
                 "config": {}}""")))
                .isInstanceOfSatisfying(ActionException.class, e ->
                        assertThat(e.code()).isEqualTo("invalid-config"));
        assertThat(store.config("orphan")).isEmpty();
    }

    @Test
    void missingInputsRefuseByName() throws Exception {
        assertThatThrownBy(() -> catalog.execute(PublishConfigAction.NAME, input("{}")))
                .isInstanceOfSatisfying(ActionException.class, e -> {
                    assertThat(e.code()).isEqualTo("invalid-input");
                    assertThat(e.getMessage()).contains("name");
                });
        assertThatThrownBy(() -> catalog.execute(PublishConfigAction.NAME, input("""
                {"name": "limits", "messageType": "cfg.v1.Limits",
                 "config": "not-an-object"}""")))
                .isInstanceOfSatisfying(ActionException.class, e ->
                        assertThat(e.code()).isEqualTo("invalid-input"));
    }
}
