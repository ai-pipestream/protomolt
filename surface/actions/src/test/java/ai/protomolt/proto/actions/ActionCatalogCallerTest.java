package ai.protomolt.proto.actions;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Struct;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/** The catalog carries the authenticated caller through every dispatch form. */
class ActionCatalogCallerTest {
    private final ActionCatalog catalog = ActionCatalog.defaults(ActionContext.create());
    private final Caller author = Caller.scoped("author-7", Set.of(Scopes.WORKER_COORDINATE));
    private final Caller denied = Caller.scoped("reader-8", Set.of(Scopes.SCHEMA_READ));

    @Test
    void typedJsonAndUnaryStreamingPassTheExactAuthorizedCaller() throws Exception {
        var action = new CallerAction();
        catalog.register(action);

        catalog.execute(action.name(), Struct.getDefaultInstance(), author);
        catalog.execute(action.name(), JsonNodeFactory.instance.objectNode(), author);
        List<Message> typed = new ArrayList<>();
        catalog.executeStreaming(action.name(), Struct.getDefaultInstance(), author, typed::add);
        var json = new ArrayList<com.fasterxml.jackson.databind.node.ObjectNode>();
        catalog.executeStreaming(action.name(), JsonNodeFactory.instance.objectNode(), author, json::add);

        assertThat(action.callers).containsExactly(author, author, author, author);
        assertThat(action.callers).allSatisfy(caller -> assertThat(caller).isSameAs(author));
        assertThat(typed).containsExactly(Struct.getDefaultInstance());
        assertThat(json).hasSize(1);
        assertThat(action.legacyCalls).isZero();

    }

    @Test
    void trueStreamingPassesTheExactAuthorizedCaller() throws Exception {
        var action = new CallerStreamAction();
        catalog.register(action);
        List<Message> typed = new ArrayList<>();
        catalog.executeStreaming(action.name(), Struct.getDefaultInstance(), author, typed::add);
        var json = new ArrayList<com.fasterxml.jackson.databind.node.ObjectNode>();
        catalog.executeStreaming(action.name(), JsonNodeFactory.instance.objectNode(), author, json::add);

        assertThat(action.callers).containsExactly(author, author);
        assertThat(action.callers).allSatisfy(caller -> assertThat(caller).isSameAs(author));
        assertThat(typed).containsExactly(Struct.getDefaultInstance());
        assertThat(json).hasSize(1);
        assertThat(action.legacyCalls).isZero();

        ActionException typedDenied = catchThrowableOfType(ActionException.class, () ->
                catalog.executeStreaming(action.name(), Struct.getDefaultInstance(), denied,
                        ignored -> {}));
        ActionException jsonDenied = catchThrowableOfType(ActionException.class, () ->
                catalog.executeStreaming(action.name(), JsonNodeFactory.instance.objectNode(),
                        denied, ignored -> {}));
        assertThat(typedDenied.code()).isEqualTo("permission-denied");
        assertThat(jsonDenied.code()).isEqualTo("permission-denied");
        assertThat(action.callers).hasSize(2);
    }

    @Test
    void legacyCoordinationActionStillRunsAndDenialPrecedesAnyHandler() throws Exception {
        var action = new LegacyCoordinationAction();
        catalog.register(action);

        assertThat(catalog.execute(action.name(), Struct.getDefaultInstance(), author))
                .isEqualTo(Struct.getDefaultInstance());
        List<Message> emitted = new ArrayList<>();
        catalog.executeStreaming(action.name(), Struct.getDefaultInstance(), author, emitted::add);
        assertThat(action.calls).isEqualTo(2);

        ActionException typedDenied = catchThrowableOfType(ActionException.class, () ->
                catalog.execute(action.name(), Struct.getDefaultInstance(), denied));
        ActionException jsonDenied = catchThrowableOfType(ActionException.class, () ->
                catalog.execute(action.name(), JsonNodeFactory.instance.objectNode(), denied));
        ActionException streamDenied = catchThrowableOfType(ActionException.class, () ->
                catalog.executeStreaming(action.name(), Struct.getDefaultInstance(), denied,
                        ignored -> {}));
        assertThat(typedDenied.code()).isEqualTo("permission-denied");
        assertThat(jsonDenied.code()).isEqualTo("permission-denied");
        assertThat(streamDenied.code()).isEqualTo("permission-denied");
        assertThat(action.calls).isEqualTo(2);
    }

    private abstract static class BaseAction implements ProtoAction {
        @Override public String description() { return "caller dispatch test"; }
        @Override public String requiredScope() { return Scopes.WORKER_COORDINATE; }
        @Override public Descriptor requestType() { return Struct.getDescriptor(); }
        @Override public Descriptor responseType() { return Struct.getDescriptor(); }
    }

    private static final class CallerAction extends BaseAction {
        private final List<Caller> callers = new ArrayList<>();
        private int legacyCalls;
        @Override public String name() { return "test-caller-unary"; }
        @Override public Message execute(Message request, ActionContext context) {
            legacyCalls++;
            return Struct.getDefaultInstance();
        }
        @Override public Message execute(Message request, ActionContext context, Caller caller) {
            callers.add(caller);
            return Struct.getDefaultInstance();
        }
    }

    private static final class CallerStreamAction extends BaseAction implements StreamingAction {
        private final List<Caller> callers = new ArrayList<>();
        private int legacyCalls;
        @Override public String name() { return "test-caller-stream"; }
        @Override public Message execute(Message request, ActionContext context) {
            throw new AssertionError("unary execution is not expected");
        }
        @Override public void executeStreaming(Message request, ActionContext context,
                StreamEmitter emitter) throws ActionException {
            legacyCalls++;
            emitter.emit(Struct.getDefaultInstance());
        }
        @Override public void executeStreaming(Message request, ActionContext context,
                Caller caller, StreamEmitter emitter) throws ActionException {
            callers.add(caller);
            emitter.emit(Struct.getDefaultInstance());
        }
    }

    private static final class LegacyCoordinationAction extends BaseAction {
        private int calls;
        @Override public String name() { return "test-legacy-coordination"; }
        @Override public Message execute(Message request, ActionContext context) {
            calls++;
            return Struct.getDefaultInstance();
        }
    }
}
