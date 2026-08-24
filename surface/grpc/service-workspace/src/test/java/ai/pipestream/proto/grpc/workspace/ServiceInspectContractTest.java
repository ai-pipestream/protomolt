package ai.pipestream.proto.grpc.workspace;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.grpc.profile.ServiceProfileRepository;
import ai.pipestream.proto.grpc.profile.v1.DescriptorArtifact;
import ai.pipestream.proto.grpc.profile.v1.ServiceProfile;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The verb publishes the schema it enforces. Deriving the schema from the request message
 * is only worth anything if the two cannot drift, so this reads the bounds back out of the
 * published document and then proves the verb applies the same ones.
 */
class ServiceInspectContractTest {

    /**
     * An empty workspace. The verb checks that a workspace is configured before it parses,
     * so passing null here would short-circuit on availability and every contract assertion
     * below would pass without the contract being consulted.
     */
    private static final ServiceProfileRepository EMPTY = new ServiceProfileRepository() {
        @Override
        public Optional<ServiceProfile> find(String name) {
            return Optional.empty();
        }

        @Override
        public List<ServiceProfile> list() {
            return List.of();
        }

        @Override
        public void save(ServiceProfile profile) {
            throw new UnsupportedOperationException("read-only fixture");
        }

        @Override
        public Optional<DescriptorArtifact> findDescriptorArtifact(String fingerprint) {
            return Optional.empty();
        }

        @Override
        public void saveDescriptorArtifact(DescriptorArtifact artifact) {
            throw new UnsupportedOperationException("read-only fixture");
        }
    };

    private final ServiceInspectAction action = new ServiceInspectAction(EMPTY, null);

    /**
     * A verb manifest describes its root message in place, because a tool-calling client
     * reads the root's properties directly and cannot follow a reference to find them.
     */
    private ObjectNode properties() {
        ObjectNode schema = action.inputSchema();
        assertThat(schema.path("$ref").isMissingNode())
                .as("a manifest root is described in place, not referenced").isTrue();
        assertThat(schema.path("type").asText()).isEqualTo("object");
        return (ObjectNode) schema.path("properties");
    }

    private static ObjectNode envelope(String name) {
        ObjectNode input = JsonNodeFactory.instance.objectNode();
        input.put("name", name);
        return input;
    }

    @Test
    void thePublishedSchemaCarriesTheDeclaredBounds() {
        JsonNode name = properties().path("name");

        assertThat(name.path("maxLength").asInt()).isEqualTo(128);
        assertThat(name.path("minLength").asInt()).isEqualTo(1);
    }

    @Test
    void aNameTheContractRefusesNeverReachesTheStore() {
        // The workspace is empty, so a name that got past validation would come back as a
        // not-found instead. A path-shaped name is the case the format rule exists to refuse.
        assertThatThrownBy(() -> action.execute(envelope("billing/api"), ActionContext.create()))
                .isInstanceOf(ActionException.class)
                .hasMessageContaining("request contract");
    }

    @Test
    void aMissingNameIsRefusedByTheContract() {
        assertThatThrownBy(() -> action.execute(
                        JsonNodeFactory.instance.objectNode(), ActionContext.create()))
                .isInstanceOf(ActionException.class)
                .hasMessageContaining("request contract");
    }

    @Test
    void anUnknownMemberIsRefusedRatherThanIgnored() {
        ObjectNode input = envelope("billing-api");
        input.put("nmae", "typo");

        assertThatThrownBy(() -> action.execute(input, ActionContext.create()))
                .isInstanceOf(ActionException.class)
                .hasMessageContaining("ServiceInspectRequest");
    }
}
