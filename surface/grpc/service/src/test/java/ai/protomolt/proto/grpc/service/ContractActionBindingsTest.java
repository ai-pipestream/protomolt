package ai.protomolt.proto.grpc.service;

import ai.protomolt.proto.actions.*;
import com.google.protobuf.DescriptorProtos.*;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.DynamicMessage;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ContractActionBindingsTest {
    private final FileDescriptor file = descriptor();
    private final ServiceDescriptor author = file.findServiceByName("Author");
    private final ServiceDescriptor coordinator = file.findServiceByName("Coordinator");
    private final ActionCatalog catalog = ActionCatalog.defaults(ActionContext.create());

    @Test void explicitAuthorMethodsAndAutomaticCoordinatorMethodsUseSeparateVerbs() {
        register("coordinator-first", "RequestOne", "ResponseOne");
        register("coordinator-second", "RequestTwo", "ResponseTwo");
        register("author-first", "RequestOne", "ResponseOne");
        register("author-second", "RequestTwo", "ResponseTwo");
        var explicit = Map.of(author.getFullName(), Map.of(
                "First", "author-first", "Second", "author-second"));

        assertThat(ContractActionBindings.mounted(catalog, author, explicit).values())
                .containsExactly("author-first", "author-second");
        assertThat(ContractActionBindings.mounted(catalog, coordinator, explicit).values())
                .containsExactly("coordinator-first", "coordinator-second");
    }

    @Test void incompleteMissingAndMismatchedExplicitBindingsFailClosed() {
        register("author-first", "RequestOne", "ResponseOne");
        register("wrong-type", "RequestOne", "ResponseOne");
        assertThatThrownBy(() -> ContractActionBindings.mounted(catalog, author,
                Map.of(author.getFullName(), Map.of("First", "author-first"))))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("Incomplete");
        assertThatThrownBy(() -> ContractActionBindings.mounted(catalog, author,
                Map.of(author.getFullName(), Map.of("First", "author-first", "Second", "absent"))))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("absent");
        assertThatThrownBy(() -> ContractActionBindings.mounted(catalog, author,
                Map.of(author.getFullName(), Map.of("First", "author-first", "Second", "wrong-type"))))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("type differs");
    }

    @Test void explicitBindingCannotPointAtARestoredRemoteProxy() throws Exception {
        register("author-first", "RequestOne", "ResponseOne");
        register("author-second", "RequestTwo", "ResponseTwo");
        var proxyType = Class.forName("ai.protomolt.proto.grpc.workspace.ReflectedMethodAction");
        var constructor = java.util.Arrays.stream(proxyType.getDeclaredConstructors())
                .filter(value -> value.getParameterCount() == 7).findFirst().orElseThrow();
        constructor.setAccessible(true);
        catalog.replace((ProtoAction) constructor.newInstance("author-first", "remote",
                author.findMethodByName("First"), null, null, null, null));

        assertThatThrownBy(() -> ContractActionBindings.mounted(catalog, author,
                Map.of(author.getFullName(), Map.of(
                        "First", "author-first", "Second", "author-second"))))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("reflected proxy");
    }

    private void register(String name, String request, String response) {
        catalog.register(new ProtoAction() {
            @Override public String name() { return name; }
            @Override public String description() { return name; }
            @Override public com.google.protobuf.Descriptors.Descriptor requestType() {
                return file.findMessageTypeByName(request);
            }
            @Override public com.google.protobuf.Descriptors.Descriptor responseType() {
                return file.findMessageTypeByName(response);
            }
            @Override public Message execute(Message input, ActionContext context) {
                return DynamicMessage.getDefaultInstance(responseType());
            }
        });
    }

    private static FileDescriptor descriptor() {
        var first = MethodDescriptorProto.newBuilder().setName("First")
                .setInputType(".binding.v1.RequestOne").setOutputType(".binding.v1.ResponseOne");
        var second = MethodDescriptorProto.newBuilder().setName("Second")
                .setInputType(".binding.v1.RequestTwo").setOutputType(".binding.v1.ResponseTwo");
        var proto = FileDescriptorProto.newBuilder().setName("binding.proto")
                .setPackage("binding.v1").setSyntax("proto3")
                .addMessageType(DescriptorProto.newBuilder().setName("RequestOne"))
                .addMessageType(DescriptorProto.newBuilder().setName("ResponseOne"))
                .addMessageType(DescriptorProto.newBuilder().setName("RequestTwo"))
                .addMessageType(DescriptorProto.newBuilder().setName("ResponseTwo"))
                .addService(ServiceDescriptorProto.newBuilder().setName("Author")
                        .addMethod(first).addMethod(second))
                .addService(ServiceDescriptorProto.newBuilder().setName("Coordinator")
                        .addMethod(first).addMethod(second)).build();
        try { return FileDescriptor.buildFrom(proto, new FileDescriptor[0]); }
        catch (com.google.protobuf.Descriptors.DescriptorValidationException invalid) {
            throw new IllegalStateException(invalid);
        }
    }
}
