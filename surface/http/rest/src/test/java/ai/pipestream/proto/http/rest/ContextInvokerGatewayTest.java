package ai.pipestream.proto.http.rest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.Message;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.util.Map;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

/**
 * The optional context-invoker seam: an invoker implementing
 * {@link ProtoRestContextInvoker} receives the normalized headers, a plain function invoker
 * is called exactly as before, and a forbidden refusal carries its own exception type.
 */
class ContextInvokerGatewayTest {

    private static final class HeaderAware
            implements Function<Message, Message>, ProtoRestContextInvoker {

        Map<String, String> seenHeaders;

        @Override
        public Message apply(Message request) {
            throw new IllegalStateException("the plain path must not be used");
        }

        @Override
        public Message invoke(Message request, Map<String, String> headers,
                              Map<String, String> queryParams) {
            seenHeaders = headers;
            if (!"friend".equals(headers.get("x-caller"))) {
                throw new ForbiddenProtoRestException(
                        "permission-denied: caller does not hold 'schema-read'");
            }
            return Struct.newBuilder()
                    .putFields("via", Value.newBuilder().setStringValue("context").build())
                    .build();
        }
    }

    private static ProtoRestGateway gateway(HeaderAware invoker) {
        ProtoRestMethodRegistry registry = new ProtoRestMethodRegistry();
        registry.register(ProtoRestMethod.builder("CtxService", "Do", invoker)
                .requestType(Struct.class)
                .build());
        return new ProtoRestGateway(registry,
                new ai.pipestream.proto.http.json.ProtobufJsonTranscoder());
    }

    @Test
    void aContextInvokerReceivesTheNormalizedHeaders() {
        HeaderAware invoker = new HeaderAware();
        String response = gateway(invoker).invoke("CtxService", "Do", "{}",
                Map.of("X-Caller", "friend"), Map.of());
        assertThat(response).contains("context");
        assertThat(invoker.seenHeaders).containsEntry("x-caller", "friend");
    }

    @Test
    void aForbiddenRefusalRidesItsOwnExceptionType() {
        assertThatThrownBy(() -> gateway(new HeaderAware())
                .invoke("CtxService", "Do", "{}", Map.of(), Map.of()))
                .isInstanceOf(ForbiddenProtoRestException.class)
                .hasMessageContaining("permission-denied");
    }

    @Test
    void aPlainInvokerStaysOnThePlainPath() {
        ProtoRestMethodRegistry registry = new ProtoRestMethodRegistry();
        registry.register(ProtoRestMethod.builder("PlainService", "Do", request ->
                        Struct.newBuilder()
                                .putFields("via",
                                        Value.newBuilder().setStringValue("plain").build())
                                .build())
                .requestType(Struct.class)
                .build());
        ProtoRestGateway gateway = new ProtoRestGateway(registry,
                new ai.pipestream.proto.http.json.ProtobufJsonTranscoder());
        assertThat(gateway.invoke("PlainService", "Do", "{}",
                Map.of("x-caller", "friend"), Map.of())).contains("plain");
    }
}
