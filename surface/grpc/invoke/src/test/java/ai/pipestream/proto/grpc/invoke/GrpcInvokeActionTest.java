package ai.pipestream.proto.grpc.invoke;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import com.google.protobuf.DynamicMessage;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptors;
import io.grpc.ServerMethodDefinition;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GrpcInvokeActionTest {

    private static final String PROTO = """
            syntax = "proto3";
            package invoke.test;
            message Ping { string text = 1; int32 count = 2; }
            message Pong { string text = 1; int32 index = 2; }
            service EchoService {
              rpc Echo(Ping) returns (Pong);
              rpc Split(Ping) returns (stream Pong);
              rpc Collect(stream Ping) returns (Pong);
              rpc Fail(Ping) returns (Pong);
              rpc Slow(Ping) returns (Pong);
              rpc Open(Ping) returns (stream Pong);
            }
            """;

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final AtomicReference<String> SEEN_HEADER = new AtomicReference<>();

    private static Server server;
    private static String serverName;
    private static GrpcInvokeAction action;
    private static Descriptor pong;

    @BeforeAll
    static void startDynamicServer() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("invoke/test/echo.proto", PROTO, "test")
                .build());
        FileDescriptor file = compiled.descriptorFor("invoke/test/echo.proto").orElseThrow();
        ServiceDescriptor service = file.findServiceByName("EchoService");
        Descriptor ping = file.findMessageTypeByName("Ping");
        pong = file.findMessageTypeByName("Pong");

        ServerServiceDefinition.Builder definition =
                ServerServiceDefinition.builder(service.getFullName());
        definition.addMethod(method(service, "Echo"), ServerCalls.asyncUnaryCall((request, out) -> {
            String text = (String) request.getField(ping.findFieldByName("text"));
            out.onNext(pong(text + "!", 0));
            out.onCompleted();
        }));
        definition.addMethod(method(service, "Split"), ServerCalls.asyncServerStreamingCall((request, out) -> {
            String text = (String) request.getField(ping.findFieldByName("text"));
            String[] words = text.split(" ");
            for (int i = 0; i < words.length; i++) {
                out.onNext(pong(words[i], i));
            }
            out.onCompleted();
        }));
        definition.addMethod(method(service, "Collect"),
                ServerCalls.asyncClientStreamingCall(out -> new StreamObserver<>() {
                    @Override
                    public void onNext(DynamicMessage value) {
                    }

                    @Override
                    public void onError(Throwable t) {
                    }

                    @Override
                    public void onCompleted() {
                        out.onNext(pong("collected", 0));
                        out.onCompleted();
                    }
                }));
        definition.addMethod(method(service, "Open"), ServerCalls.asyncServerStreamingCall((request, out) -> {
            out.onNext(pong("tick", 0));
            // Stream deliberately left open, like a health Watch.
        }));
        definition.addMethod(method(service, "Fail"), ServerCalls.asyncUnaryCall((request, out) ->
                out.onError(Status.INVALID_ARGUMENT.withDescription("bad ping").asRuntimeException())));
        definition.addMethod(method(service, "Slow"), ServerCalls.asyncUnaryCall((request, out) -> {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            out.onNext(pong("late", 0));
            out.onCompleted();
        }));

        serverName = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(serverName)
                .addService(ServerInterceptors.intercept(definition.build(), new io.grpc.ServerInterceptor() {
                    @Override
                    public <ReqT, RespT> io.grpc.ServerCall.Listener<ReqT> interceptCall(
                            io.grpc.ServerCall<ReqT, RespT> call, Metadata headers,
                            ServerCallHandler<ReqT, RespT> next) {
                        SEEN_HEADER.set(headers.get(
                                Metadata.Key.of("x-test", Metadata.ASCII_STRING_MARSHALLER)));
                        return next.startCall(call, headers);
                    }
                }))
                .build()
                .start();
        action = new GrpcInvokeAction(target ->
                InProcessChannelBuilder.forName(target).build());
    }

    @AfterAll
    static void stopServer() {
        server.shutdownNow();
    }

    private static io.grpc.MethodDescriptor<DynamicMessage, DynamicMessage> method(
            ServiceDescriptor service, String name) {
        return DynamicGrpcCalls.methodDescriptor(service.findMethodByName(name));
    }

    private static DynamicMessage pong(String text, int index) {
        return DynamicMessage.newBuilder(pong)
                .setField(pong.findFieldByName("text"), text)
                .setField(pong.findFieldByName("index"), index)
                .build();
    }

    private ObjectNode input(String method, ObjectNode request) {
        ObjectNode input = MAPPER.createObjectNode();
        input.put("target", serverName);
        input.put("method", method);
        ObjectNode schema = input.putObject("schema");
        schema.putObject("sources").put("invoke/test/echo.proto", PROTO);
        input.set("request", request);
        return input;
    }

    @Test
    void unaryEchoRoundTrips() throws Exception {
        ObjectNode request = MAPPER.createObjectNode().put("text", "hello");
        ObjectNode result = action.execute(input("invoke.test.EchoService/Echo", request),
                ActionContext.create());
        assertThat(result.get("ok").asBoolean()).isTrue();
        assertThat(result.get("status").asText()).isEqualTo("OK");
        assertThat(result.get("methodType").asText()).isEqualTo("UNARY");
        assertThat(result.get("responses").get(0).get("text").asText()).isEqualTo("hello!");
    }

    @Test
    void metadataHeadersReachTheServer() throws Exception {
        SEEN_HEADER.set(null);
        ObjectNode input = input("invoke.test.EchoService/Echo",
                MAPPER.createObjectNode().put("text", "hi"));
        input.putObject("metadata").put("x-test", "token-123");
        ObjectNode result = action.execute(input, ActionContext.create());
        assertThat(result.get("ok").asBoolean()).isTrue();
        assertThat(SEEN_HEADER.get()).isEqualTo("token-123");
    }

    @Test
    void serverStreamingCollectsResponsesInOrder() throws Exception {
        ObjectNode request = MAPPER.createObjectNode().put("text", "one two three");
        ObjectNode result = action.execute(input("invoke.test.EchoService/Split", request),
                ActionContext.create());
        assertThat(result.get("ok").asBoolean()).isTrue();
        assertThat(result.get("methodType").asText()).isEqualTo("SERVER_STREAMING");
        assertThat(result.get("responses").findValuesAsText("text"))
                .containsExactly("one", "two", "three");
    }

    @Test
    void serverStreamingHonorsMaxResponses() throws Exception {
        ObjectNode input = input("invoke.test.EchoService/Split",
                MAPPER.createObjectNode().put("text", "a b c d e"));
        input.put("maxResponses", 2);
        ObjectNode result = action.execute(input, ActionContext.create());
        assertThat(result.get("responses").size()).isEqualTo(2);
    }

    @Test
    void openEndedStreamReturnsPromptlyAtMaxResponses() throws Exception {
        ObjectNode input = input("invoke.test.EchoService/Open",
                MAPPER.createObjectNode().put("text", "x"));
        input.put("maxResponses", 1);
        input.put("deadlineMs", 30_000);
        long start = System.nanoTime();
        ObjectNode result = action.execute(input, ActionContext.create());
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        assertThat(result.get("ok").asBoolean()).isTrue();
        assertThat(result.get("responses").size()).isEqualTo(1);
        // Must return as soon as the cap is hit, not block on the open stream until deadline.
        assertThat(elapsedMs).isLessThan(5_000);
    }

    @Test
    void grpcStatusFailuresAreResultsNotErrors() throws Exception {
        ObjectNode result = action.execute(input("invoke.test.EchoService/Fail",
                MAPPER.createObjectNode().put("text", "x")), ActionContext.create());
        assertThat(result.get("ok").asBoolean()).isFalse();
        assertThat(result.get("status").asText()).isEqualTo("INVALID_ARGUMENT");
        assertThat(result.get("description").asText()).isEqualTo("bad ping");
    }

    @Test
    void deadlineExpiryIsDeadlineExceeded() throws Exception {
        ObjectNode input = input("invoke.test.EchoService/Slow",
                MAPPER.createObjectNode().put("text", "x"));
        input.put("deadlineMs", 50);
        ObjectNode result = action.execute(input, ActionContext.create());
        assertThat(result.get("ok").asBoolean()).isFalse();
        assertThat(result.get("status").asText()).isEqualTo("DEADLINE_EXCEEDED");
    }

    @Test
    void clientStreamingIsRejectedAsInvalidInput() {
        assertThatThrownBy(() -> action.execute(input("invoke.test.EchoService/Collect",
                MAPPER.createObjectNode()), ActionContext.create()))
                .isInstanceOfSatisfying(ActionException.class,
                        e -> assertThat(e.code()).isEqualTo("invalid-input"));
    }

    @Test
    void unknownMethodListsAvailableMethods() {
        assertThatThrownBy(() -> action.execute(input("invoke.test.EchoService/Nope",
                MAPPER.createObjectNode()), ActionContext.create()))
                .isInstanceOfSatisfying(ActionException.class, e -> {
                    assertThat(e.code()).isEqualTo("invalid-input");
                    assertThat(e.getMessage()).contains("invoke.test.EchoService/Echo");
                });
    }

    @Test
    void malformedRequestJsonIsInvalidInput() {
        // Unknown fields are ignored by the toolkit's JSON parser; a type mismatch is malformed.
        ObjectNode request = MAPPER.createObjectNode();
        request.putObject("text").put("nested", true);
        assertThatThrownBy(() -> action.execute(input("invoke.test.EchoService/Echo", request),
                ActionContext.create()))
                .isInstanceOfSatisfying(ActionException.class,
                        e -> assertThat(e.code()).isEqualTo("invalid-input"));
    }

    @Test
    void streamingEmitsEachResponseInOrderWithOkTerminal() throws Exception {
        List<ObjectNode> emitted = new ArrayList<>();
        action.executeStreaming(
                input("invoke.test.EchoService/Split",
                        MAPPER.createObjectNode().put("text", "one two three")),
                ActionContext.create(), emitted::add);
        assertThat(emitted).hasSize(4);
        assertThat(emitted.subList(0, 3).stream().map(n -> n.get("text").asText()))
                .containsExactly("one", "two", "three");
        assertThat(emitted.get(3).get("ok").asBoolean()).isTrue();
        assertThat(emitted.get(3).get("status").asText()).isEqualTo("OK");
    }

    @Test
    void streamingUnaryEmitsSingleResponseAndTerminal() throws Exception {
        List<ObjectNode> emitted = new ArrayList<>();
        action.executeStreaming(
                input("invoke.test.EchoService/Echo",
                        MAPPER.createObjectNode().put("text", "hello")),
                ActionContext.create(), emitted::add);
        assertThat(emitted).hasSize(2);
        assertThat(emitted.get(0).get("text").asText()).isEqualTo("hello!");
        assertThat(emitted.get(1).get("status").asText()).isEqualTo("OK");
    }

    @Test
    void streamingStatusFailureEndsWithErrorTerminal() throws Exception {
        List<ObjectNode> emitted = new ArrayList<>();
        action.executeStreaming(
                input("invoke.test.EchoService/Fail",
                        MAPPER.createObjectNode().put("text", "x")),
                ActionContext.create(), emitted::add);
        assertThat(emitted).hasSize(1);
        assertThat(emitted.get(0).get("ok").asBoolean()).isFalse();
        assertThat(emitted.get(0).get("status").asText()).isEqualTo("INVALID_ARGUMENT");
        assertThat(emitted.get(0).get("description").asText()).isEqualTo("bad ping");
    }

    @Test
    void streamingOpenStreamEndsWithDeadlineTerminal() throws Exception {
        ObjectNode input = input("invoke.test.EchoService/Open",
                MAPPER.createObjectNode().put("text", "x"));
        input.put("deadlineMs", 800);
        List<ObjectNode> emitted = new ArrayList<>();
        action.executeStreaming(input, ActionContext.create(), emitted::add);
        // The open stream yields its one tick, then the call deadline ends it.
        assertThat(emitted).hasSize(2);
        assertThat(emitted.get(0).get("text").asText()).isEqualTo("tick");
        assertThat(emitted.get(1).get("ok").asBoolean()).isFalse();
        assertThat(emitted.get(1).get("status").asText()).isEqualTo("DEADLINE_EXCEEDED");
    }
}
