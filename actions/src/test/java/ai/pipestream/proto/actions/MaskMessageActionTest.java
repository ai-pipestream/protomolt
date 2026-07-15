package ai.pipestream.proto.actions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import static ai.pipestream.proto.actions.TestFixtures.obj;
import static org.assertj.core.api.Assertions.assertThat;

/** Schema-declared masking: the sensitivity option decides, the caller picks the policy. */
class MaskMessageActionTest {

    private static final String PROTO = """
            syntax = "proto3";
            package mask.test;
            import "ai/pipestream/proto/meta/v1/metadata.proto";
            message Customer {
              string id = 1;
              string email = 2 [(ai.pipestream.proto.meta.v1.field) = {sensitivity: "pii"}];
              string ssn = 3 [(ai.pipestream.proto.meta.v1.field) = {sensitivity: "secret"}];
              Contact contact = 4;
            }
            message Contact {
              string phone = 1 [(ai.pipestream.proto.meta.v1.field) = {sensitivity: "pii"}];
              string city = 2;
            }
            """;

    private final ActionCatalog catalog = ActionCatalog.defaults(TestFixtures.personContext());

    private ObjectNode maskInput(String extraJson) throws Exception {
        String metadataProto = new String(getClass().getClassLoader()
                .getResourceAsStream("ai/pipestream/proto/meta/v1/metadata.proto")
                .readAllBytes());
        ObjectNode input = obj("""
                {"schema": {"sources": {}}, "type": "mask.test.Customer",
                 "message": {"id": "c-1", "email": "pat@example.com", "ssn": "123",
                             "contact": {"phone": "555", "city": "Springfield"}}%s}
                """.formatted(extraJson.isEmpty() ? "" : ", " + extraJson));
        ObjectNode sources = (ObjectNode) input.get("schema").get("sources");
        sources.put("ai/pipestream/proto/meta/v1/metadata.proto", metadataProto);
        sources.put("mask/test/customer.proto", PROTO);
        return input;
    }

    @Test
    void removeClearsDeclaredClassesRecursively() throws Exception {
        ObjectNode result = catalog.execute("mask-message",
                maskInput("\"classes\": [\"pii\"]"));
        JsonNode message = result.get("message");
        // The transcoder prints proto3 defaults, so a REMOVEd string shows as "".
        assertThat(message.get("email").asText()).isEmpty();
        assertThat(message.get("ssn").asText()).isEqualTo("123");   // different class: kept
        assertThat(message.get("contact").get("phone").asText()).isEmpty(); // nested pii
        assertThat(message.get("contact").get("city").asText()).isEqualTo("Springfield");
        assertThat(result.get("maskedFields")).extracting(JsonNode::asText)
                .containsExactly("email", "contact.phone");
    }

    @Test
    void encryptRoundTripsWithTheKeyAndOnlyTheKey() throws Exception {
        String key = java.util.Base64.getEncoder().encodeToString(new byte[16]);
        ObjectNode sealed = catalog.execute("mask-message", maskInput(
                "\"classes\": [\"pii\"], \"strategy\": \"encrypt\", \"key\": \"" + key + "\""));
        String ciphertext = sealed.get("message").get("email").asText();
        assertThat(ciphertext).isNotEqualTo("pat@example.com").isNotEmpty();

        // Decrypt with the same key restores the original...
        ObjectNode input = maskInput(
                "\"classes\": [\"pii\"], \"strategy\": \"decrypt\", \"key\": \"" + key + "\"");
        ((ObjectNode) input.get("message")).setAll((ObjectNode) sealed.get("message"));
        ObjectNode opened = catalog.execute("mask-message", input);
        assertThat(opened.get("message").get("email").asText()).isEqualTo("pat@example.com");

        // ...and a wrong key fails loudly, never silently.
        String wrong = java.util.Base64.getEncoder().encodeToString(
                "0123456789abcdef".getBytes());
        ObjectNode tampered = maskInput(
                "\"classes\": [\"pii\"], \"strategy\": \"decrypt\", \"key\": \"" + wrong + "\"");
        ((ObjectNode) tampered.get("message")).setAll((ObjectNode) sealed.get("message"));
        org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                        catalog.execute("mask-message", tampered))
                .hasMessageContaining("wrong key, wrong field, or tampered");
    }

    @Test
    void redactMarksStringsVisibly() throws Exception {
        ObjectNode result = catalog.execute("mask-message",
                maskInput("\"classes\": [\"pii\", \"secret\"], \"strategy\": \"redact\""));
        JsonNode message = result.get("message");
        assertThat(message.get("email").asText()).isEqualTo("***");
        assertThat(message.get("ssn").asText()).isEqualTo("***");
        assertThat(message.get("contact").get("phone").asText()).isEqualTo("***");
        assertThat(result.get("maskedFields")).hasSize(3);
    }
}
