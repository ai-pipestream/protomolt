package ai.protomolt.proto.actions;

import ai.protomolt.proto.validate.CelRule;
import ai.protomolt.proto.validate.MessageRules;
import ai.protomolt.proto.validate.ValidateProto;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Empty;
import com.google.protobuf.Message;
import com.google.protobuf.Struct;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ActionResponseBoundaryTest {
    @ParameterizedTest @ValueSource(booleans = {false, true})
    void generatedResponseContractGuardsEverySurface(boolean defaults) throws Exception {
        for (Message invalid : new Message[] {null, Empty.getDefaultInstance()}) {
            var catalog = catalog(defaults, action(Struct.getDescriptor(), invalid));
            rejectsEverySurface(catalog);
        }
        var good = Struct.getDefaultInstance();
        var catalog = catalog(defaults, action(Struct.getDescriptor(), good));
        assertThat(catalog.execute("boundary-probe", Empty.getDefaultInstance())).isSameAs(good);
        assertThat(catalog.execute("boundary-probe", TestFixtures.obj("{}"))).isEqualTo(TestFixtures.obj("{}"));
        var typed = new ArrayList<Message>();
        catalog.executeStreaming("boundary-probe", Empty.getDefaultInstance(), typed::add);
        assertThat(typed).containsExactly(good);
        var json = new ArrayList<Object>();
        catalog.executeStreaming("boundary-probe", TestFixtures.obj("{}"), json::add);
        assertThat(json).containsExactly(TestFixtures.obj("{}"));
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void nestedConstraintsAndCelRulesGuardEverySurface(boolean defaults) throws Exception {
        var person = TestFixtures.personFile();
        var wrapper = DescriptorProtos.DescriptorProto.newBuilder().setName("Wrapper")
                .addField(DescriptorProtos.FieldDescriptorProto.newBuilder().setName("person").setNumber(1)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                        .setTypeName(".actions.test.Person"));
        var file = Descriptors.FileDescriptor.buildFrom(DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("response-wrapper.proto").setSyntax("proto3").addDependency(person.getName())
                .addMessageType(wrapper).build(), new Descriptors.FileDescriptor[] {person});
        var descriptor = file.findMessageTypeByName("Wrapper");
        var child = person.findMessageTypeByName("Person");
        var invalid = DynamicMessage.newBuilder(descriptor).setField(descriptor.findFieldByName("person"),
                DynamicMessage.newBuilder(child).setField(child.findFieldByName("name"), "x").build()).build();
        rejectsEverySurface(catalog(defaults, action(descriptor, invalid)));
        var valid = DynamicMessage.newBuilder(descriptor).setField(descriptor.findFieldByName("person"),
                DynamicMessage.newBuilder(child).setField(child.findFieldByName("name"), "Ada").build()).build();
        assertThat(catalog(defaults, action(descriptor, valid)).execute("boundary-probe", Empty.getDefaultInstance()))
                .isSameAs(valid);
        // false is a supported rule that rejects the value; an unknown function cannot compile.
        for (String expression : List.of("false", "missingResponseRule()")) {
            var ruleType = celType(expression);
            var catalog = catalog(defaults, action(ruleType, DynamicMessage.getDefaultInstance(ruleType)));
            rejectsEverySurface(catalog);
            if (expression.startsWith("missing")) {
                assertThatThrownBy(() -> catalog.execute("boundary-probe", Empty.getDefaultInstance()))
                        .isInstanceOf(ActionException.class).hasCauseInstanceOf(RuntimeException.class);
            }
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void streamFailureStaysTerminalAndProviderErrorsArePreserved(boolean defaults) throws Exception {
        var providerError = new ActionException("provider-failed", "provider failure");
        var action = new StreamingAction() {
            public String name() { return "boundary-probe"; }
            public String description() { return "test"; }
            public Descriptors.Descriptor requestType() { return Empty.getDescriptor(); }
            public Descriptors.Descriptor responseType() { return Struct.getDescriptor(); }
            public Message execute(Message request, ActionContext context) throws ActionException { throw providerError; }
            public void executeStreaming(Message request, ActionContext context, StreamEmitter emitter) throws ActionException {
                emitter.emit(Struct.getDefaultInstance());
                throw providerError;
            }
        };
        var catalog = catalog(defaults, action);
        assertThatThrownBy(() -> catalog.execute("boundary-probe", Empty.getDefaultInstance())).isSameAs(providerError);
        assertThatThrownBy(() -> catalog.execute("boundary-probe", TestFixtures.obj("{}"))).isSameAs(providerError);
        var emitted = new ArrayList<Message>();
        assertThatThrownBy(() -> catalog.executeStreaming("boundary-probe", Empty.getDefaultInstance(), emitted::add))
                .isSameAs(providerError);
        assertThat(emitted).containsExactly(Struct.getDefaultInstance());
        var cancelled = new java.util.concurrent.CancellationException("consumer cancelled");
        assertThatThrownBy(() -> catalog.executeStreaming("boundary-probe", Empty.getDefaultInstance(), message -> {
            throw cancelled;
        })).isSameAs(cancelled);
        assertThatThrownBy(() -> catalog.executeStreaming("boundary-probe", TestFixtures.obj("{}"), message -> {
            throw cancelled;
        })).isSameAs(cancelled);
    }

    private static Descriptors.Descriptor celType(String expression) throws Exception {
        return Descriptors.FileDescriptor.buildFrom(DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("response-cel-" + Integer.toUnsignedString(expression.hashCode()) + ".proto").setSyntax("proto3")
                .addDependency(ValidateProto.getDescriptor().getName())
                .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("Reply")
                        .setOptions(DescriptorProtos.MessageOptions.newBuilder().setExtension(ValidateProto.message,
                                MessageRules.newBuilder().addCel(CelRule.newBuilder().setId("reply.rule")
                                        .setExpression(expression).setMessage("response rule failed")).build())))
                .build(), new Descriptors.FileDescriptor[] {ValidateProto.getDescriptor()}).findMessageTypeByName("Reply");
    }

    private static StreamingAction action(Descriptors.Descriptor descriptor, Message result) {
        return new StreamingAction() {
            public String name() { return "boundary-probe"; }
            public String description() { return "test"; }
            public Descriptors.Descriptor requestType() { return Empty.getDescriptor(); }
            public Descriptors.Descriptor responseType() { return descriptor; }
            public Message execute(Message request, ActionContext context) { return result; }
            public void executeStreaming(Message request, ActionContext context, StreamEmitter emitter) throws ActionException {
                emitter.emit(result);
            }
        };
    }

    private static ActionCatalog catalog(boolean defaults, ProtoAction action) {
        var context = ActionContext.create();
        return (defaults ? ActionCatalog.defaults(context) : ActionCatalog.empty(context)).register(action);
    }

    private static void rejectsEverySurface(ActionCatalog catalog) {
        invalid(() -> catalog.execute("boundary-probe", Empty.getDefaultInstance()));
        invalid(() -> catalog.execute("boundary-probe", TestFixtures.obj("{}")));
        invalid(() -> catalog.executeStreaming("boundary-probe", Empty.getDefaultInstance(), message -> {
            throw new AssertionError("Invalid emission reached caller");
        }));
        invalid(() -> catalog.executeStreaming("boundary-probe", TestFixtures.obj("{}"), message -> {
            throw new AssertionError("Invalid emission reached JSON caller");
        }));
    }

    private static void invalid(org.assertj.core.api.ThrowableAssert.ThrowingCallable call) {
        assertThatThrownBy(call).isInstanceOfSatisfying(ActionException.class,
                failure -> assertThat(failure.code()).isEqualTo("invalid-response"));
    }
}
