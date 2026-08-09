package ai.pipestream.proto.actions;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import static ai.pipestream.proto.actions.TestFixtures.MAPPER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

class ActionExceptionTest {

    @Test
    void carriesCodeMessageAndNoDetailsByDefault() {
        ActionException exception = new ActionException("unknown-type", "Unknown type 'x.Y'");
        assertThat(exception.code()).isEqualTo("unknown-type");
        assertThat(exception.getMessage()).isEqualTo("Unknown type 'x.Y'");
        assertThat(exception.details()).isEmpty();
    }

    @Test
    void carriesDetailsWhenGiven() {
        ObjectNode details = MAPPER.createObjectNode().put("pointer", "/schema");
        ActionException exception = new ActionException("invalid-input", "bad", details);
        assertThat(exception.details()).containsSame(details);
    }

    @Test
    void nullCodeIsRejected() {
        assertThatNullPointerException()
                .isThrownBy(() -> new ActionException(null, "message"));
    }

    @Test
    void toJsonWithoutDetailsOmitsTheDetailsKey() {
        ObjectNode json = new ActionException("compile-failed", "did not compile")
                .toJson(MAPPER);
        assertThat(json).isEqualTo(TestFixtures.obj(
                "{\"error\": \"compile-failed\", \"message\": \"did not compile\"}"));
    }

    @Test
    void toJsonWithDetailsEmbedsThem() {
        ObjectNode details = MAPPER.createObjectNode().put("pointer", "/message");
        ObjectNode json = new ActionException("invalid-message", "bad json", details)
                .toJson(MAPPER);
        assertThat(json.get("error").asText()).isEqualTo("invalid-message");
        assertThat(json.get("message").asText()).isEqualTo("bad json");
        assertThat(json.get("details").get("pointer").asText()).isEqualTo("/message");
    }
}
