package ai.protomolt.proto.actions;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Empty;
import com.google.protobuf.Message;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ActionResponseContractTest {
    private final Descriptors.Descriptor response = TestFixtures.personFile().findMessageTypeByName("Person");

    @Test
    void unaryAndJsonRejectNullWrongTypeAndRuleViolations() {
        for (Message invalid : new Message[] {null, Empty.getDefaultInstance(), person("x")}) {
            var catalog = catalog(action(invalid));
            assertInvalid(() -> catalog.execute("probe", Empty.getDefaultInstance()));
            assertInvalid(() -> catalog.execute("probe", TestFixtures.obj("{}")));
            var emitted = new ArrayList<Message>();
            assertInvalid(() -> catalog.executeStreaming("probe", Empty.getDefaultInstance(), emitted::add));
            assertThat(emitted).isEmpty();
        }
    }

    @Test
    void declaredRulesCannotBeRemovedByASameNamedResponseDescriptor() throws Exception {
        var file = response.getFile();
        var weakened = file.toProto().toBuilder();
        var message = weakened.getMessageTypeBuilder(0);
        for (var field : message.getFieldBuilderList()) field.clearOptions();
        var descriptor = Descriptors.FileDescriptor.buildFrom(weakened.build(),
                file.getDependencies().toArray(Descriptors.FileDescriptor[]::new)).findMessageTypeByName("Person");
        var invalid = DynamicMessage.newBuilder(descriptor).setField(descriptor.findFieldByName("name"), "x").build();
        assertInvalid(() -> catalog(action(invalid)).execute("probe", Empty.getDefaultInstance()));
    }

    @Test
    void validResponsesPreserveMessagesAndEquivalentDescriptorsWork() throws Exception {
        var valid = person("Ada");
        assertThat(catalog(action(valid)).execute("probe", Empty.getDefaultInstance())).isSameAs(valid);
        var file = response.getFile();
        var copied = Descriptors.FileDescriptor.buildFrom(file.toProto(),
                file.getDependencies().toArray(Descriptors.FileDescriptor[]::new)).findMessageTypeByName("Person");
        var copy = DynamicMessage.parseFrom(copied, valid.toByteString());
        assertThat(catalog(action(copy)).execute("probe", Empty.getDefaultInstance()).toByteString())
                .isEqualTo(valid.toByteString());
    }

    @Test
    void aStreamCannotSuppressARejectedEmissionAndReportSuccess() {
        var emitted = new ArrayList<Message>();
        var valid = person("Ada");
        var streaming = new StreamingAction() {
            public String name() { return "probe"; }
            public String description() { return "response boundary test"; }
            public Descriptors.Descriptor requestType() { return Empty.getDescriptor(); }
            public Descriptors.Descriptor responseType() { return response; }
            public Message execute(Message request, ActionContext context) { return valid; }
            public void executeStreaming(Message request, ActionContext context, StreamEmitter sink) throws ActionException {
                sink.emit(valid);
                try { sink.emit(person("x")); } catch (ActionException expected) { /* Deliberately broken provider. */ }
                try { sink.emit(valid); } catch (ActionException expected) { /* Must remain terminal. */ }
            }
        };
        assertInvalid(() -> catalog(streaming).executeStreaming("probe", Empty.getDefaultInstance(), emitted::add));
        assertThat(emitted).containsExactly(valid);
    }

    private ProtoAction action(Message value) {
        return new ProtoAction() {
            public String name() { return "probe"; }
            public String description() { return "response boundary test"; }
            public Descriptors.Descriptor requestType() { return Empty.getDescriptor(); }
            public Descriptors.Descriptor responseType() { return response; }
            public Message execute(Message request, ActionContext context) { return value; }
        };
    }
    private Message person(String name) {
        return DynamicMessage.newBuilder(response).setField(response.findFieldByName("name"), name).build();
    }
    private static ActionCatalog catalog(ProtoAction action) {
        return ActionCatalog.empty(ActionContext.create()).register(action);
    }
    private static void assertInvalid(org.assertj.core.api.ThrowableAssert.ThrowingCallable call) {
        assertThatThrownBy(call).isInstanceOfSatisfying(ActionException.class,
                e -> assertThat(e.code()).isEqualTo("invalid-response"));
    }
}
