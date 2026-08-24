package ai.pipestream.proto.grpc.invoke;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.grpc.inprocess.InProcessChannelBuilder;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/** The verbs' {@code tls} input reaches the channel factory; plaintext stays the default. */
class ChannelFactoryTlsTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void tlsInputReachesTheFactoryAndDefaultsToPlaintext() throws Exception {
        AtomicBoolean sawTls = new AtomicBoolean();
        ReflectAction action = new ReflectAction((target, tls) -> {
            sawTls.set(tls);
            return InProcessChannelBuilder.forName("nowhere-" + tls).build();
        });

        ObjectNode plain = MAPPER.createObjectNode().put("target", "x").put("deadlineMs", 200);
        dispatch(action, plain);
        assertThat(sawTls).isFalse();

        ObjectNode secure = MAPPER.createObjectNode()
                .put("target", "x").put("deadlineMs", 200).put("tls", true);
        dispatch(action, secure);
        assertThat(sawTls).isTrue();
    }

    /**
     * Dispatches the way every surface does: through a catalog holding the verb, which is
     * where the request contract is checked before the verb runs.
     */
    private static ObjectNode dispatch(ProtoAction verb, ObjectNode input)
            throws ActionException {
        return ActionCatalog.defaults(ActionContext.create())
                .replace(verb).execute(verb.name(), input);
    }

}
