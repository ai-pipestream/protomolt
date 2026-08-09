package ai.pipestream.proto.server;

import ai.pipestream.proto.json.ProtobufJsonException;
import ai.pipestream.proto.rest.MalformedRequestException;
import ai.pipestream.proto.rest.ProtoRestException;
import ai.pipestream.proto.rest.ProtoRestInvocationException;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Edge cases for {@link ProtoRestHttpSupport} not covered by {@link ProtoRestHttpSupportTest}:
 * path-parse boundaries, query-decoding corners, wrapped-error mapping, and 5xx logging.
 */
class ProtoRestHttpSupportEdgeCasesTest {

    @Test
    void parseServiceMethodRejectsNullAndBarePrefixPaths() {
        assertThat(ProtoRestHttpSupport.parseServiceMethod(null, "/grpc-json")).isEmpty();
        assertThat(ProtoRestHttpSupport.parseServiceMethod("/grpc-json", "/grpc-json")).isEmpty();
        assertThat(ProtoRestHttpSupport.parseServiceMethod("/grpc-json/", "/grpc-json")).isEmpty();
    }

    @Test
    void parseServiceMethodRejectsExtraSegmentsAndBlankParts() {
        assertThat(ProtoRestHttpSupport.parseServiceMethod("/grpc-json/a/b/c", "/grpc-json")).isEmpty();
        assertThat(ProtoRestHttpSupport.parseServiceMethod("/grpc-json//ping", "/grpc-json")).isEmpty();
        assertThat(ProtoRestHttpSupport.parseServiceMethod("/grpc-json/ /ping", "/grpc-json")).isEmpty();
    }

    @Test
    void parseServiceMethodWithRootPrefix() {
        assertThat(ProtoRestHttpSupport.parseServiceMethod("/Echo/ping", "/"))
                .contains(new String[] {"Echo", "ping"});
        assertThat(ProtoRestHttpSupport.parseServiceMethod("/", "/")).isEmpty();
    }

    @Test
    void parseServiceMethodCaseSensitivePrefix() {
        assertThat(ProtoRestHttpSupport.parseServiceMethod("/GRPC-JSON/Echo/ping", "/grpc-json")).isEmpty();
    }

    @Test
    void parseQueryHandlesEmptyAndBlankQueries() {
        assertThat(ProtoRestHttpSupport.parseQuery("")).isEmpty();
        assertThat(ProtoRestHttpSupport.parseQuery("   ")).isEmpty();
        assertThat(ProtoRestHttpSupport.parseQuery("&&&")).isEmpty();
    }

    @Test
    void parseQueryKeepsEmptyValuesAndDecodesKeysAndPlus() {
        assertThat(ProtoRestHttpSupport.parseQuery("a=")).containsEntry("a", "");
        assertThat(ProtoRestHttpSupport.parseQuery("a=b+c")).containsEntry("a", "b c");
        assertThat(ProtoRestHttpSupport.parseQuery("a%20b=c")).containsEntry("a b", "c");
        assertThat(ProtoRestHttpSupport.parseQuery("a=1&b=2"))
                .containsEntry("a", "1")
                .containsEntry("b", "2");
    }

    @Test
    void parseQuerySkipsPairsWithoutKey() {
        // "=v" has an empty key and "novalue" has no '='; neither is a usable parameter.
        assertThat(ProtoRestHttpSupport.parseQuery("=v&novalue&ok=1"))
                .containsExactly(Map.entry("ok", "1"));
    }

    @Test
    void normalizeHeadersSkipsNullKeysAndValues() {
        Map<String, String> in = new HashMap<>();
        in.put("X-Token", "abc");
        in.put("X-Null", null);
        in.put(null, "orphan");
        assertThat(ProtoRestHttpSupport.normalizeHeaders(in))
                .containsExactly(Map.entry("x-token", "abc"));
        assertThat(ProtoRestHttpSupport.normalizeHeaders(Map.of())).isEmpty();
    }

    @Test
    void statusForUnwrapsNestedClientErrors() {
        Throwable wrapped400 = new RuntimeException("outer", new MalformedRequestException("bad query"));
        assertThat(ProtoRestHttpSupport.statusFor(wrapped400)).isEqualTo(400);

        // An invocation failure is a server fault even when wrapped again by the host.
        Throwable wrapped500 = new IllegalStateException("host wrapper",
                new ProtoRestInvocationException("Failed invoking Echo/ping: boom"));
        assertThat(ProtoRestHttpSupport.statusFor(wrapped500)).isEqualTo(500);
        assertThat(ProtoRestHttpSupport.statusFor(new ProtoRestException("plain"))).isEqualTo(500);
    }

    @Test
    void errorJsonFallsBackToSimpleClassNameWhenMessageIsNull() {
        String json = ProtoRestHttpSupport.errorJson(new MalformedRequestException((String) null));
        assertThat(json).contains("\"status\":400").contains("MalformedRequestException");
    }

    @Test
    void errorJsonForWrapped5xxStaysGeneric() {
        Throwable wrapped = new RuntimeException("host wrapper",
                new ProtoRestInvocationException("Failed invoking Echo/ping: secret-cause"));
        String json = ProtoRestHttpSupport.errorJson(wrapped);
        assertThat(json).contains("\"status\":500").contains("Internal server error");
        assertThat(json).doesNotContain("secret-cause");
    }

    @Test
    void allowHeaderIsEmptyForNon405Errors() {
        assertThat(ProtoRestHttpSupport.allowHeaderFor(new MalformedRequestException("bad"))).isEmpty();
        assertThat(ProtoRestHttpSupport.allowHeaderFor(new ProtoRestException("plain"))).isEmpty();
    }

    @Test
    void unwrapReturnsOriginalWhenNoGatewayExceptionInChain() {
        Throwable plain = new RuntimeException("x", new IllegalStateException("y"));
        assertThat(ProtoRestHttpSupport.unwrap(plain)).isSameAs(plain);
    }

    @Test
    void unwrapFindsNestedProtobufJsonException() {
        ProtobufJsonException jsonError = new ProtobufJsonException("bad json");
        Throwable wrapped = new RuntimeException(new IllegalStateException(jsonError));
        assertThat(ProtoRestHttpSupport.unwrap(wrapped)).isSameAs(jsonError);
    }

    @Test
    void logIfServerErrorLogsOnly5xx() {
        List<Object[]> errorCalls = new ArrayList<>();
        Logger recording = recordingLogger(errorCalls);

        ProtoRestHttpSupport.logIfServerError(recording, new MalformedRequestException("client fault"));
        assertThat(errorCalls).isEmpty();

        // 5xx errors log the generic message (the detail the client never sees) with the
        // original throwable attached for the stack trace.
        RuntimeException serverFault = new RuntimeException("server fault");
        ProtoRestHttpSupport.logIfServerError(recording, serverFault);
        assertThat(errorCalls).singleElement().satisfies(args -> {
            assertThat(args[0]).isEqualTo("Request failed with an internal error");
            assertThat(args[1]).isSameAs(serverFault);
        });
    }

    /** Minimal recording {@link Logger} via proxy; captures the arguments of every {@code error} call. */
    private static Logger recordingLogger(List<Object[]> errorCalls) {
        return (Logger) Proxy.newProxyInstance(
                ProtoRestHttpSupportEdgeCasesTest.class.getClassLoader(),
                new Class<?>[] {Logger.class},
                (proxy, method, args) -> {
                    if ("error".equals(method.getName()) && args != null && args.length > 0) {
                        errorCalls.add(args);
                    }
                    if (method.getReturnType() == boolean.class) {
                        return false;
                    }
                    if (method.getReturnType() == String.class) {
                        return "recording";
                    }
                    return null;
                });
    }
}
