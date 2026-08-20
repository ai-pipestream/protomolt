package ai.pipestream.proto.http.rest;

import ai.pipestream.proto.http.json.MalformedProtobufJsonException;
import ai.pipestream.proto.http.json.ProtobufJsonTranscoder;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.MethodDescriptorProto;
import com.google.protobuf.DescriptorProtos.ServiceDescriptorProto;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Struct;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Dispatch plumbing around {@link ProtoRestGateway#invoke}: header/query normalization before
 * token validation, the descriptor-driven (dynamic) decode path, and the empty-response and
 * missing-type fallbacks.
 */
class ProtoRestGatewayDispatchTest {

    private ProtoRestMethodRegistry registry;
    private ProtobufJsonTranscoder transcoder;

    @BeforeEach
    void setUp() {
        registry = new ProtoRestMethodRegistry();
        transcoder = new ProtobufJsonTranscoder();
    }

    private ProtoRestGateway gatewayWithValidator(ProtoApiTokenValidator validator) {
        return new ProtoRestGateway(registry, transcoder, validator);
    }

    private void registerTokenProtected() {
        registry.register(ProtoRestMethod.builder("Secure", "Go", r -> Struct.getDefaultInstance())
                .requestType(Struct.class)
                .apiToken(ApiTokenRequirement.apiKeyHeader("x-token"))
                .build());
    }

    @Test
    void constructorRejectsNulls() {
        assertThatThrownBy(() -> new ProtoRestGateway(null, transcoder))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new ProtoRestGateway(registry, null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new ProtoRestGateway(registry, transcoder, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void invokeRejectsNullRouteAndBody() {
        ProtoRestGateway gateway = gatewayWithValidator(ProtoApiTokenValidator.acceptNonBlank());

        assertThatThrownBy(() -> gateway.invoke(null, "m", "{}"))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> gateway.invoke("s", null, "{}"))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> gateway.invoke("s", "m", null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void exposesTheWiredRegistryAndTranscoder() {
        ProtoRestGateway gateway = gatewayWithValidator(ProtoApiTokenValidator.acceptNonBlank());

        assertThat(gateway.getRegistry()).isSameAs(registry);
        assertThat(gateway.getTranscoder()).isSameAs(transcoder);
    }

    @Test
    void headersAreLowercasedAndNullEntriesDroppedBeforeValidation() {
        registerTokenProtected();
        AtomicReference<Map<String, String>> seenHeaders = new AtomicReference<>();
        AtomicReference<Map<String, String>> seenQuery = new AtomicReference<>();
        ProtoRestGateway gateway = gatewayWithValidator((cfg, headers, query) -> {
            seenHeaders.set(headers);
            seenQuery.set(query);
            return Optional.empty();
        });

        Map<String, String> headers = new HashMap<>();
        headers.put("X-Token", "abc");
        headers.put(null, "dropped-key");
        headers.put("X-Null-Value", null);

        String json = gateway.invoke("Secure", "Go", "{}", headers, null);

        assertThat(json).isNotBlank();
        assertThat(seenHeaders.get()).containsExactly(Map.entry("x-token", "abc"));
        // A null query map reaches the validator as an empty map, not null.
        assertThat(seenQuery.get()).isEmpty();
    }

    @Test
    void normalizedHeaderMapIsUnmodifiableForTheValidator() {
        registerTokenProtected();
        AtomicReference<Map<String, String>> seenHeaders = new AtomicReference<>();
        ProtoRestGateway gateway = gatewayWithValidator((cfg, headers, query) -> {
            seenHeaders.set(headers);
            return Optional.empty();
        });

        gateway.invoke("Secure", "Go", "{}", Map.of("x-token", "abc"), Map.of());

        assertThatThrownBy(() -> seenHeaders.get().put("injected", "nope"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    /**
     * Hosts forward headers with their original casing, so two entries can collide once
     * lowercased; the first one wins, matching the host contract for duplicate headers.
     */
    @Test
    void caseVariantDuplicateHeadersKeepTheFirstValue() {
        registerTokenProtected();
        AtomicReference<Map<String, String>> seenHeaders = new AtomicReference<>();
        ProtoRestGateway gateway = gatewayWithValidator((cfg, headers, query) -> {
            seenHeaders.set(headers);
            return Optional.empty();
        });

        // LinkedHashMap: insertion order fixes the stream encounter order the merge sees.
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("X-Token", "first");
        headers.put("x-token", "second");

        gateway.invoke("Secure", "Go", "{}", headers, Map.of());

        assertThat(seenHeaders.get()).containsExactly(Map.entry("x-token", "first"));
    }

    @Test
    void nullResponseFromTheInvokerBecomesAnEmptyJsonObject() {
        registry.register(ProtoRestMethod.builder("Null", "Go", r -> null)
                .requestType(Struct.class)
                .build());
        ProtoRestGateway gateway = gatewayWithValidator(ProtoApiTokenValidator.acceptNonBlank());

        assertThat(gateway.invoke("Null", "Go", "{}")).isEqualTo("{}");
    }

    @Test
    void methodWithoutRequestTypeOrDescriptorFailsAtDecode() {
        registry.register(ProtoRestMethod.builder("Bare", "Go", r -> r).build());
        ProtoRestGateway gateway = gatewayWithValidator(ProtoApiTokenValidator.acceptNonBlank());

        assertThatThrownBy(() -> gateway.invoke("Bare", "Go", "{}"))
                .isInstanceOf(ProtoRestInvocationException.class)
                .hasMessageContaining("Bare/Go")
                .hasMessageContaining("no request type or method descriptor");
    }

    private static FileDescriptor dynamicFile() throws Exception {
        FileDescriptorProto fileProto = FileDescriptorProto.newBuilder()
                .setName("gw_dispatch_test.proto")
                .setPackage("gwtest")
                .addMessageType(DescriptorProto.newBuilder().setName("EchoRequest")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("name").setNumber(1)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)))
                .addMessageType(DescriptorProto.newBuilder().setName("EchoResponse")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("message").setNumber(1)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)))
                .addService(ServiceDescriptorProto.newBuilder().setName("DynService")
                        .addMethod(MethodDescriptorProto.newBuilder()
                                .setName("Echo")
                                .setInputType(".gwtest.EchoRequest")
                                .setOutputType(".gwtest.EchoResponse")))
                .build();
        return FileDescriptor.buildFrom(fileProto, new FileDescriptor[0]);
    }

    @Test
    void invokesDescriptorRegisteredMethodThroughDynamicMessages() throws Exception {
        ServiceDescriptor service = dynamicFile().findServiceByName("DynService");
        MethodDescriptor method = service.findMethodByName("Echo");
        registry.register(service, method, request -> {
            String name = (String) request.getField(method.getInputType().findFieldByName("name"));
            return DynamicMessage.newBuilder(method.getOutputType())
                    .setField(method.getOutputType().findFieldByName("message"), "hello " + name)
                    .build();
        }, null);

        ProtoRestGateway gateway = gatewayWithValidator(ProtoApiTokenValidator.acceptNonBlank());
        String json = gateway.invoke("DynService", "Echo", "{\"name\":\"dyn\"}");

        assertThat(json).contains("hello dyn");
    }

    @Test
    void malformedJsonAgainstADynamicMethodNamesTheDescriptor() throws Exception {
        ServiceDescriptor service = dynamicFile().findServiceByName("DynService");
        MethodDescriptor method = service.findMethodByName("Echo");
        registry.register(service, method, r -> r, null);

        ProtoRestGateway gateway = gatewayWithValidator(ProtoApiTokenValidator.acceptNonBlank());

        assertThatThrownBy(() -> gateway.invoke("DynService", "Echo", "{nope"))
                .isInstanceOf(MalformedProtobufJsonException.class)
                .hasMessageContaining("gwtest.EchoRequest");
    }
}
