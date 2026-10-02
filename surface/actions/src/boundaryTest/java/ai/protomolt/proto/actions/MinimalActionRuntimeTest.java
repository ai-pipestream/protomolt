package ai.protomolt.proto.actions;

import ai.protomolt.proto.validate.FieldRules;
import ai.protomolt.proto.validate.StringRules;
import ai.protomolt.proto.validate.ValidateProto;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.Struct;
import org.junit.jupiter.api.Test;

import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Runs with production core dependencies and no optional action/schema providers. */
class MinimalActionRuntimeTest {
    @Test
    void annotationViolationIsRefusedBeforeTheProviderRuns() throws Exception {
        var field = DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setName("name").setNumber(1)
                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
                .setOptions(DescriptorProtos.FieldOptions.newBuilder().setExtension(ValidateProto.field,
                        FieldRules.newBuilder().setString(StringRules.newBuilder().setMinLen(3)).build()));
        Descriptor type = FileDescriptor.buildFrom(DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("boundary.proto").setPackage("boundary").setSyntax("proto3")
                .addDependency(ValidateProto.getDescriptor().getName())
                .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("Request").addField(field))
                .build(), new FileDescriptor[]{ValidateProto.getDescriptor()}).findMessageTypeByName("Request");
        AtomicBoolean invoked = new AtomicBoolean();
        ActionCatalog catalog = ActionCatalog.empty(ActionContext.create()).register(new ProtoAction() {
            @Override public String name() { return "validated"; }
            @Override public String description() { return "Annotation validation fixture"; }
            @Override public Descriptor requestType() { return type; }
            @Override public Descriptor responseType() { return Struct.getDescriptor(); }
            @Override public Message execute(Message request, ActionContext context) {
                invoked.set(true);
                return Struct.getDefaultInstance();
            }
        });
        Message invalid = DynamicMessage.newBuilder(type).setField(type.findFieldByName("name"), "x").build();
        assertThatThrownBy(() -> catalog.execute("validated", invalid))
                .isInstanceOf(ActionException.class).hasMessageContaining("request contract");
        assertThat(invoked).isFalse();
        Message valid = DynamicMessage.newBuilder(type).setField(type.findFieldByName("name"), "Ada").build();
        catalog.execute("validated", valid);
        assertThat(invoked).isTrue();
    }

    @Test
    void descriptorNativeActionWorksWithAuthorizationAndValidation() throws Exception {
        assertThat(ServiceLoader.load(ActionProvider.class)).isEmpty();
        assertThat(ServiceLoader.load(SchemaResolverProvider.class)).isEmpty();
        assertThat(ServiceLoader.load(ActionContractProvider.class)).isEmpty();
        ActionCatalog catalog = ActionCatalog.empty(ActionContext.create()).register(new Echo());
        assertThat(catalog.list()).hasSize(1);
        assertThat(catalog.execute("echo", Struct.getDefaultInstance()))
                .isEqualTo(Struct.getDefaultInstance());
        assertThatThrownBy(() -> catalog.execute("echo", Struct.getDefaultInstance(),
                Caller.scoped("unauthorized", Set.of())))
                .isInstanceOf(ActionException.class).hasMessageContaining("does not hold");
        assertThatThrownBy(() -> catalog.execute("echo", com.google.protobuf.Empty.getDefaultInstance()))
                .isInstanceOf(ActionException.class);
        assertThatThrownBy(() -> CatalogContract.request("CompileRequest"))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("found 0");
    }

    private static final class Echo implements ProtoAction {
        @Override public String name() { return "echo"; }
        @Override public String description() { return "Echo a protobuf message"; }
        @Override public String requiredScope() { return Scopes.SCHEMA_READ; }
        @Override public Descriptor requestType() { return Struct.getDescriptor(); }
        @Override public Descriptor responseType() { return Struct.getDescriptor(); }
        @Override public Message execute(Message request, ActionContext context) { return request; }
    }
}
