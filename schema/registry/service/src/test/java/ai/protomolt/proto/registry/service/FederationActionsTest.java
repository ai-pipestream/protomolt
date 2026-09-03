package ai.protomolt.proto.registry.service;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.registry.GitSchemaRegistryStore;
import ai.protomolt.proto.registry.RegistryFederation;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The federation verbs as mounted actions: JSON shapes, required fields refused by name, and a
 * whole remote round trip (add, sync, report) against a second real store.
 */
class FederationActionsTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    private static final String CORE_PROTO = """
            syntax = "proto3";
            package common.v1;
            message Core {
              string id = 1;
            }
            """;

    @TempDir
    Path tempDir;

    private GitSchemaRegistryStore local;
    private GitSchemaRegistryStore remote;
    private ActionCatalog catalog;

    @BeforeEach
    void setUp() {
        local = GitSchemaRegistryStore.builder().repositoryDir(tempDir.resolve("local")).build();
        remote = GitSchemaRegistryStore.builder().repositoryDir(tempDir.resolve("remote")).build();
        RegistryFederation federation = RegistryFederation.over(local);
        catalog = ActionCatalog.defaults(ActionContext.create())
                .register(new RegistryRemotesAction(federation))
                .register(new RegistrySyncAction(federation));
    }

    @AfterEach
    void tearDown() {
        local.close();
        remote.close();
    }

    private ObjectNode input(String json) throws Exception {
        return (ObjectNode) JSON.readTree(json);
    }

    @Test
    void remotesRoundTripThroughTheVerb() throws Exception {
        ObjectNode empty = catalog.execute("registry-remotes", input("{\"operation\":\"REMOTE_OPERATION_LIST\"}"));
        assertThat(empty.get("remotes")).isEmpty();

        String url = tempDir.resolve("remote").toUri().toString();
        ObjectNode added = catalog.execute("registry-remotes",
                input("{\"operation\":\"REMOTE_OPERATION_ADD\",\"name\":\"upstream\",\"url\":\"" + url + "\"}"));
        assertThat(added.get("remotes")).hasSize(1);
        assertThat(added.get("remotes").get(0).get("name").asText()).isEqualTo("upstream");

        ObjectNode removed = catalog.execute("registry-remotes",
                input("{\"operation\":\"REMOTE_OPERATION_REMOVE\",\"name\":\"upstream\"}"));
        assertThat(removed.get("remotes")).isEmpty();
    }

    @Test
    void missingFieldsAreRefusedByName() throws Exception {
        // Adding a remote needs both a name and a URL, and the message says so through two
        // cross-field rules rather than through checks in the verb.
        assertThatThrownBy(() -> catalog.execute("registry-remotes",
                input("{\"operation\":\"REMOTE_OPERATION_ADD\"}")))
                .isInstanceOf(ActionException.class)
                .hasMessageContaining("name");
        // An operation the enum does not declare cannot be parsed at all.
        assertThatThrownBy(() -> catalog.execute("registry-remotes",
                input("{\"operation\":\"REMOTE_OPERATION_BOOM\"}")))
                .isInstanceOf(ActionException.class)
                .hasMessageContaining("REMOTE_OPERATION_BOOM");
        assertThatThrownBy(() -> catalog.execute("registry-sync", input("{}")))
                .isInstanceOf(ActionException.class)
                .hasMessageContaining("remote field is required");
    }

    @Test
    void syncVerbImportsAndReports() throws Exception {
        remote.register("common/v1/core.proto", CORE_PROTO, List.of());
        String url = tempDir.resolve("remote").toUri().toString();
        catalog.execute("registry-remotes",
                input("{\"operation\":\"REMOTE_OPERATION_ADD\",\"name\":\"upstream\",\"url\":\"" + url + "\"}"));

        ObjectNode report = catalog.execute("registry-sync", input("{\"remote\":\"upstream\"}"));
        assertThat(report.get("remote").asText()).isEqualTo("upstream");
        assertThat(report.get("subjects")).hasSize(1);
        ObjectNode subject = (ObjectNode) report.get("subjects").get(0);
        assertThat(subject.get("localSubject").asText()).isEqualTo("upstream:common/v1/core.proto");
        assertThat(subject.get("imported").asInt()).isEqualTo(1);
        assertThat(subject.get("rejections")).isEmpty();
        assertThat(report.get("errors")).isEmpty();

        assertThat(local.subjects()).contains("upstream:common/v1/core.proto");
    }

    @Test
    void syncOfAnUnknownRemoteIsInvalidInput() throws Exception {
        assertThatThrownBy(() -> catalog.execute("registry-sync",
                input("{\"remote\":\"nowhere\"}")))
                .isInstanceOf(ActionException.class)
                .satisfies(e -> assertThat(((ActionException) e).code())
                        .isEqualTo("invalid-input"))
                .hasMessageContaining("unknown remote 'nowhere'");
    }
}
