package ai.pipestream.proto.acp.agent;

import ai.pipestream.proto.acp.PromptContext;
import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.actions.StreamEmitter;
import ai.pipestream.proto.actions.StreamingAction;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Drives {@link CatalogLineRunner} directly with a recording {@link PromptContext}: the console
 * line grammar (list/help, {@code <verb> <json>}), its error reporting, and per-emission
 * streaming, without the protocol transport.
 */
class CatalogLineRunnerTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private RecordingContext context;
    private CatalogLineRunner runner;

    /** The turn channel, captured: every sendMessage/sendThought lands in a list. */
    private static final class RecordingContext implements PromptContext {
        private final List<String> messages = new ArrayList<>();
        private final List<String> thoughts = new ArrayList<>();

        @Override
        public void sendMessage(String text) {
            messages.add(text);
        }

        @Override
        public void sendThought(String text) {
            thoughts.add(text);
        }

        private String allMessages() {
            return String.join("", messages);
        }
    }

    @BeforeEach
    void setUp() {
        ActionCatalog catalog = ActionCatalog.defaults(ActionContext.create())
                .register(new EchoAction())
                .register(new FailingAction())
                .register(new BoomAction())
                .register(new TickAction());
        runner = new CatalogLineRunner(catalog);
        context = new RecordingContext();
    }

    @Test
    void blankLinesProduceNoOutput() {
        runner.run("", context);
        runner.run("   ", context);
        runner.run("\t", context);
        assertThat(context.messages).isEmpty();
        assertThat(context.thoughts).isEmpty();
    }

    @Test
    void listAndHelpNameEveryVerbWithItsDescription() {
        runner.run("list", context);
        assertThat(context.allMessages()).contains("echo").contains("counts the input fields");

        context.messages.clear();
        runner.run("help", context);
        assertThat(context.allMessages()).contains("echo");
    }

    @Test
    void anUnknownVerbIsReportedAndPointsAtList() {
        runner.run("frobnicate {\"a\":1}", context);
        assertThat(context.allMessages())
                .contains("Unknown verb 'frobnicate'")
                .contains("list");
    }

    @Test
    void aVerbWithoutJsonRunsWithAnEmptyEnvelope() {
        runner.run("echo", context);
        assertThat(context.thoughts).containsExactly("running echo");
        assertThat(context.allMessages()).contains("\"fieldCount\" : 0");
    }

    @Test
    void aVerbWithJsonRunsWithThatEnvelope() {
        runner.run("echo {\"a\":1,\"b\":2}", context);
        assertThat(context.allMessages()).contains("\"fieldCount\" : 2");
    }

    @Test
    void inputThatIsNotJsonIsReportedAsSuch() {
        runner.run("echo {not json", context);
        assertThat(context.allMessages()).contains("error: input is not JSON:");
    }

    @Test
    void inputThatIsNotAnObjectIsNamedByItsShape() {
        runner.run("echo [1,2,3]", context);
        assertThat(context.allMessages())
                .contains("error: input must be a JSON object, got array")
                .doesNotContain("ClassCastException");

        context.messages.clear();
        runner.run("echo \"just a string\"", context);
        assertThat(context.allMessages()).contains("got string");
    }

    @Test
    void anActionExceptionPrintsItsCodeAndMessage() {
        runner.run("fail", context);
        assertThat(context.allMessages()).contains("bad-input: the envelope was rejected");
    }

    @Test
    void anUnexpectedFailureIsReportedAndTheRunnerKeepsGoing() {
        runner.run("boom", context);
        assertThat(context.allMessages()).contains("error: kapow");

        context.messages.clear();
        context.thoughts.clear();
        runner.run("echo", context);
        assertThat(context.allMessages()).contains("\"fieldCount\" : 0");
    }

    @Test
    void eachStreamingEmissionIsItsOwnMessageChunk() {
        runner.run("tick", context);
        assertThat(context.thoughts).containsExactly("running tick");
        assertThat(context.messages).hasSize(2);
        assertThat(context.messages.get(0)).contains("\"tick\" : 1");
        assertThat(context.messages.get(1)).contains("\"tick\" : 2");
    }

    /** Echoes the field count of its envelope; succeeds on any object, including empty. */
    private final class EchoAction implements ProtoAction {
        @Override
        public String name() {
            return "echo";
        }

        @Override
        public String description() {
            return "counts the input fields";
        }

        @Override
        public ObjectNode inputSchema() {
            ObjectNode schema = mapper.createObjectNode();
            schema.put("type", "object");
            return schema;
        }

        @Override
        public ObjectNode execute(ObjectNode input, ActionContext actionContext) {
            ObjectNode out = mapper.createObjectNode();
            out.put("fieldCount", input.size());
            return out;
        }
    }

    private final class FailingAction implements ProtoAction {
        @Override
        public String name() {
            return "fail";
        }

        @Override
        public String description() {
            return "always rejects its input";
        }

        @Override
        public ObjectNode inputSchema() {
            return mapper.createObjectNode();
        }

        @Override
        public ObjectNode execute(ObjectNode input, ActionContext actionContext) throws ActionException {
            throw new ActionException("bad-input", "the envelope was rejected");
        }
    }

    private final class BoomAction implements ProtoAction {
        @Override
        public String name() {
            return "boom";
        }

        @Override
        public String description() {
            return "always blows up";
        }

        @Override
        public ObjectNode inputSchema() {
            return mapper.createObjectNode();
        }

        @Override
        public ObjectNode execute(ObjectNode input, ActionContext actionContext) {
            throw new IllegalStateException("kapow");
        }
    }

    private final class TickAction implements StreamingAction {
        @Override
        public String name() {
            return "tick";
        }

        @Override
        public String description() {
            return "emits two ticks";
        }

        @Override
        public ObjectNode inputSchema() {
            return mapper.createObjectNode();
        }

        @Override
        public ObjectNode execute(ObjectNode input, ActionContext actionContext) {
            ObjectNode out = mapper.createObjectNode();
            out.put("ticks", 2);
            return out;
        }

        @Override
        public void executeStreaming(ObjectNode input, ActionContext actionContext,
                StreamEmitter emitter) {
            for (int i = 1; i <= 2; i++) {
                ObjectNode tick = mapper.createObjectNode();
                tick.put("tick", i);
                emitter.emit(tick);
            }
        }
    }
}
