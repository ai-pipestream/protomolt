package ai.pipestream.proto.actions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The catalog is an index over request messages, not a registry of hand-written schemas.
 *
 * <p>A verb names the message it accepts and nothing else. The catalog derives what it
 * publishes from that message and enforces it before dispatch, so a verb cannot advertise
 * one contract and apply another, and cannot forget to apply one at all.
 */
class CatalogIndexTest {

    /** A verb that names a message and does no checking of its own. */
    private static final class Echo implements ProtoAction {
        @Override
        public String name() {
            return "echo";
        }

        @Override
        public String description() {
            return "Answers with what it was given.";
        }

        @Override
        public Descriptor requestType() {
            return CatalogContract.request("ListTypesRequest");
        }

        @Override
        public Descriptor responseType() {
            return CatalogContract.request("ListTypesResponse");
        }

        @Override
        public Message execute(Message input, ActionContext context) {
            // Reports what it was given, but as its own response message: a verb answers
            // under the contract it names, so it cannot simply hand the request back.
            Reply output = Reply.of(responseType());
            output.append("types").set("fullName", Fields.string(input, "filter")).build();
            return output.build();
        }
    }

    private ActionCatalog catalog() {
        return ActionCatalog.defaults(ActionContext.create()).replace(new Echo());
    }

    @Test
    void theManifestIsDerivedFromTheRequestMessage() {
        JsonNode published = catalog().list().findValue("inputSchema");

        assertThat(published).isNotNull();
        // Same document the generator produces for that message: derived, not transcribed.
        assertThat(new Echo().inputSchema())
                .isEqualTo(CatalogContract.schemaFor(new Echo().requestType()));
    }

    @Test
    void theCatalogEnforcesTheContractTheVerbNames() {
        ObjectNode misspelled = JsonNodeFactory.instance.objectNode();
        misspelled.put("fliter", "x");

        // The verb itself checks nothing, so a refusal here can only come from the catalog.
        assertThatThrownBy(() -> catalog().execute("echo", misspelled))
                .isInstanceOfSatisfying(ActionException.class,
                        e -> assertThat(e.code()).isEqualTo("invalid-input"));
    }

    @Test
    void anEnvelopeTheMessageAcceptsReachesTheVerb() throws Exception {
        ObjectNode input = JsonNodeFactory.instance.objectNode();
        input.put("filter", "shop");

        assertThat(catalog().execute("echo", input)
                .path("types").path(0).path("fullName").asText()).isEqualTo("shop");
    }
}
