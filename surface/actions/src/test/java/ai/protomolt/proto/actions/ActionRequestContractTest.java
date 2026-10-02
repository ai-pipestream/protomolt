package ai.protomolt.proto.actions;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Empty;
import com.google.protobuf.Message;
import java.util.ArrayList;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ActionRequestContractTest {
    private final Descriptors.Descriptor contract = TestFixtures.personFile().findMessageTypeByName("Person");

    @Test
    void callerCannotRemoveDeclaredRequestRules() throws Exception {
        var weakened = contract.getFile().toProto().toBuilder();
        for (var field : weakened.getMessageTypeBuilder(0).getFieldBuilderList()) field.clearOptions();
        var descriptor = Descriptors.FileDescriptor.buildFrom(weakened.build(),
                contract.getFile().getDependencies().toArray(Descriptors.FileDescriptor[]::new))
                .findMessageTypeByName("Person");
        var invalid = DynamicMessage.newBuilder(descriptor).setField(descriptor.findFieldByName("name"), "x").build();
        var executed = new ArrayList<Message>();
        var catalog = catalog(executed);
        assertInvalid(() -> catalog.execute("probe", invalid));
        assertInvalid(() -> catalog.executeStreaming("probe", invalid, message -> {}));
        assertInvalid(() -> CatalogContract.validate(invalid, contract, "probe"));
        assertThat(executed).isEmpty();
    }

    @Test
    void nullRequestsAreContractErrorsBeforeExecution() {
        var executed = new ArrayList<Message>();
        var catalog = catalog(executed);
        assertInvalid(() -> catalog.execute("probe", (Message) null));
        assertInvalid(() -> catalog.executeStreaming("probe", (Message) null, message -> {}));
        assertThat(executed).isEmpty();
    }

    @Test
    void equivalentDescriptorsAreCanonicalizedBeforeHandlersRun() throws Exception {
        var copied = Descriptors.FileDescriptor.buildFrom(contract.getFile().toProto(),
                contract.getFile().getDependencies().toArray(Descriptors.FileDescriptor[]::new))
                .findMessageTypeByName("Person");
        var valid = DynamicMessage.newBuilder(copied).setField(copied.findFieldByName("name"), "Ada").build();
        var executed = new ArrayList<Message>();
        var catalog = catalog(executed);
        catalog.execute("probe", valid);
        catalog.executeStreaming("probe", valid, message -> {});
        assertThat(executed).hasSize(2).allSatisfy(message -> {
            assertThat(message.getDescriptorForType()).isSameAs(contract);
            assertThat(message.toByteString()).isEqualTo(valid.toByteString());
        });
        var exact = DynamicMessage.parseFrom(contract, valid.toByteString());
        catalog.execute("probe", exact);
        assertThat(executed.getLast()).isSameAs(exact);
    }

    private ActionCatalog catalog(ArrayList<Message> executed) {
        return ActionCatalog.empty(ActionContext.create()).register(new ProtoAction() {
            public String name() { return "probe"; }
            public String description() { return "request boundary test"; }
            public Descriptors.Descriptor requestType() { return contract; }
            public Descriptors.Descriptor responseType() { return Empty.getDescriptor(); }
            public Message execute(Message request, ActionContext context) {
                executed.add(request);
                return Empty.getDefaultInstance();
            }
        });
    }

    private static void assertInvalid(org.assertj.core.api.ThrowableAssert.ThrowingCallable call) {
        assertThatThrownBy(call).isInstanceOfSatisfying(ActionException.class,
                e -> assertThat(e.code()).isEqualTo("invalid-input"));
    }
}
