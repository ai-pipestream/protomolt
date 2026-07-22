package ai.pipestream.proto.grpc.service;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.grpc.invoke.DynamicGrpcCalls;
import ai.pipestream.proto.grpc.invoke.ReflectionClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.util.JsonFormat;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The meta suite: ProtoMolt's own dynamic-invocation client calls ProtoMolt's own service,
 * and ProtoMolt's own reflection client discovers it — no generated stubs anywhere.
 */
class ProtoMoltGrpcServiceTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String ORDER_PROTO = """
            syntax = "proto3";
            package shop.v1;
            message Order {
              string id = 1;
              int32 qty = 2;
            }
            """;

    private static Server server;
    private static ManagedChannel channel;

    @BeforeAll
    static void start() throws Exception {
        server = InProcessServerBuilder.forName("protomolt-service-test")
                .addService(ProtoMoltGrpcService.definition(ProtoMoltCatalog.full(ActionContext.create())))
                .addService(ProtoReflectionServiceV1.newInstance())
                .build()
                .start();
        channel = InProcessChannelBuilder.forName("protomolt-service-test").build();
    }

    @AfterAll
    static void stop() {
        channel.shutdownNow();
        server.shutdownNow();
    }

    private static MethodDescriptor method(String name) {
        MethodDescriptor method = ProtoMoltServiceSchema.service().findMethodByName(name);
        assertThat(method).as("RPC %s", name).isNotNull();
        return method;
    }

    /** Calls an RPC with a JSON request and returns the response as JSON — via DynamicGrpcCalls. */
    private static JsonNode call(String rpc, String requestJson) throws Exception {
        MethodDescriptor method = method(rpc);
        DynamicMessage.Builder request = DynamicMessage.newBuilder(method.getInputType());
        JsonFormat.parser().merge(requestJson, request);
        List<DynamicMessage> responses = DynamicGrpcCalls.call(
                channel, method, request.build(),
                CallOptions.DEFAULT.withDeadlineAfter(30, TimeUnit.SECONDS),
                new Metadata(), 4);
        assertThat(responses).hasSize(1);
        return MAPPER.readTree(JsonFormat.printer().print(responses.getFirst()));
    }

    @Test
    void schemaCompilesAndDeclaresTwentyThreeRpcs() {
        assertThat(ProtoMoltServiceSchema.service().getFullName())
                .isEqualTo("ai.pipestream.protomolt.v1.ProtoMoltService");
        assertThat(ProtoMoltServiceSchema.service().getMethods()).hasSize(23);
    }

    @Test
    void everyRpcMapsToACatalogAction() {
        var names = ProtoMoltCatalog.full(ActionContext.create()).names();
        for (MethodDescriptor method : ProtoMoltServiceSchema.service().getMethods()) {
            assertThat(names).contains(CatalogBridge.actionName(method));
        }
    }

    /**
     * The other direction. {@link ProtoMoltCatalog} documents the full catalog as "exactly the
     * RPCs of ProtoMoltService", but only RPC-to-action was pinned, so a verb added to the
     * catalog without a matching RPC went unnoticed and the class javadoc's verb count drifted.
     */
    @Test
    void everyCatalogActionHasAnRpc() {
        var rpcNames = ProtoMoltServiceSchema.service().getMethods().stream()
                .map(CatalogBridge::actionName)
                .toList();

        assertThat(ProtoMoltCatalog.full(ActionContext.create()).names())
                .containsExactlyInAnyOrderElementsOf(rpcNames);
    }

    @Test
    void compileRoundTripsThroughTheTypedSurface() throws Exception {
        JsonNode result = call("Compile", """
                {"sources": {"shop/v1/order.proto": %s}}
                """.formatted(MAPPER.writeValueAsString(ORDER_PROTO)));
        assertThat(result.path("ok").asBoolean()).isTrue();
        assertThat(result.path("descriptorSetBase64").asText()).isNotEmpty();
        assertThat(result.path("files")).anySatisfy(f ->
                assertThat(f.asText()).isEqualTo("shop/v1/order.proto"));
    }

    @Test
    void listTypesGroundsAgainstInlineSources() throws Exception {
        JsonNode result = call("ListTypes", """
                {"schema": {"sources": {"shop/v1/order.proto": %s}}}
                """.formatted(MAPPER.writeValueAsString(ORDER_PROTO)));
        JsonNode types = result.path("types");
        assertThat(types.findValuesAsText("fullName")).contains("shop.v1.Order");
    }

    @Test
    void evalCelReturnsTypedValues() throws Exception {
        JsonNode result = call("EvalCel", """
                {"schema": {"sources": {"shop/v1/order.proto": %s}},
                 "type": "shop.v1.Order",
                 "message": {"id": "o-1", "qty": 3},
                 "expression": "input.qty * 2"}
                """.formatted(MAPPER.writeValueAsString(ORDER_PROTO)));
        assertThat(result.path("result").asInt()).isEqualTo(6);
        assertThat(result.path("resultType").asText()).isEqualTo("int");
    }

    @Test
    void renderJsonSchemaReturnsTheDocumentItself() throws Exception {
        JsonNode result = call("RenderJsonSchema", """
                {"schema": {"sources": {"shop/v1/order.proto": %s}}, "type": "shop.v1.Order"}
                """.formatted(MAPPER.writeValueAsString(ORDER_PROTO)));
        assertThat(result.path("$schema").asText()).contains("json-schema.org");
        assertThat(result.path("$defs").path("shop.v1.Order")
                .path("properties").path("qty").path("type").asText()).isEqualTo("integer");
    }

    @Test
    void joinMessagesSynthesizesAndJoinsThroughTheTypedSurface() throws Exception {
        String orderProto = MAPPER.writeValueAsString(ORDER_PROTO);
        JsonNode result = call("JoinMessages", """
                {"sources": [
                   {"name": "order", "schema": {"sources": {"shop/v1/order.proto": %s}},
                    "type": "shop.v1.Order", "message": {"id": "o-1", "qty": 3}},
                   {"name": "other", "schema": {"sources": {"shop/v1/order.proto": %s}},
                    "type": "shop.v1.Order", "message": {"id": "o-2", "qty": 4}}
                 ],
                 "shape": {"mode": "projection", "name": "derived.v1.Pair",
                           "fields": [{"name": "left_id", "from": "order.id"},
                                      {"name": "right_id", "from": "other.id"}]},
                 "celRules": [{"selector": "order.qty * other.qty", "target": "left_id"}]}
                """.formatted(orderProto, orderProto));
        assertThat(result.path("type").asText()).isEqualTo("derived.v1.Pair");
        assertThat(result.path("message").path("leftId").asText()).isEqualTo("12");
        assertThat(result.path("message").path("rightId").asText()).isEqualTo("o-2");
        assertThat(result.path("protoSource").asText()).contains("string left_id = 1;");
        assertThat(result.path("descriptorSetBase64").asText()).isNotEmpty();
    }

    @Test
    void mergeSchemasReportsThenEmitsThroughTheTypedSurface() throws Exception {
        String orderProto = MAPPER.writeValueAsString(ORDER_PROTO);
        String ticketProto = MAPPER.writeValueAsString("""
                syntax = "proto3";
                package support.v1;
                message Ticket {
                  string id = 1;
                  int64 qty = 2;
                  string owner = 3;
                }
                """);
        String sources = """
                "sources": [
                   {"name": "order", "schema": {"sources": {"shop/v1/order.proto": %s}},
                    "type": "shop.v1.Order"},
                   {"name": "ticket", "schema": {"sources": {"support/v1/ticket.proto": %s}},
                    "type": "support.v1.Ticket"}
                 ]""".formatted(orderProto, ticketProto);

        // Validate: int32 qty vs int64 qty is a hard clash; nothing is emitted.
        JsonNode report = call("MergeSchemas",
                "{\"name\": \"derived.v1.Case\", " + sources + "}");
        assertThat(report.path("resolved").asBoolean()).isFalse();
        JsonNode clash = report.path("clashes").get(1);
        assertThat(clash.path("field").asText()).isEqualTo("qty");
        assertThat(clash.path("kind").asText()).isEqualTo("type-clash");
        assertThat(clash.path("suggested").path("names").path("order").asText())
                .isEqualTo("order_qty");

        // Resolve and emit: the merged proto plus both rulesets in one move.
        JsonNode merged = call("MergeSchemas", "{\"name\": \"derived.v1.Case\", " + sources
                + ", \"resolutions\": {\"qty\": {\"action\": \"rename\"}}}");
        assertThat(merged.path("resolved").asBoolean()).isTrue();
        assertThat(merged.path("protoSource").asText())
                .contains("int32 order_qty")
                .contains("int64 ticket_qty");
        assertThat(merged.path("joinRules")).extracting(JsonNode::asText)
                .contains("id = order.id", "id = ticket.id", "owner = ticket.owner");
        assertThat(merged.path("unionRules").path("order").path("rules"))
                .extracting(JsonNode::asText)
                .containsExactly("id = order.id", "order_qty = order.qty");
    }

    @Test
    void checkCompatFlagsARemovedField() throws Exception {
        String oldProto = MAPPER.writeValueAsString(ORDER_PROTO);
        String newProto = MAPPER.writeValueAsString("""
                syntax = "proto3";
                package shop.v1;
                message Order {
                  string id = 1;
                }
                """);
        JsonNode result = call("CheckCompat", """
                {"old": {"sources": {"shop/v1/order.proto": %s}},
                 "new": {"sources": {"shop/v1/order.proto": %s}},
                 "mode": "BACKWARD"}
                """.formatted(oldProto, newProto));
        assertThat(result.path("mode").asText()).isEqualTo("BACKWARD");
        assertThat(result.path("changes").findValuesAsText("ruleId")).isNotEmpty();
    }

    @Test
    void actionFailuresSurfaceAsInvalidArgumentWithTheErrorCode() {
        MethodDescriptor method = method("ExtractMetadata");
        DynamicMessage.Builder request = DynamicMessage.newBuilder(method.getInputType());
        assertThatThrownBy(() -> {
            JsonFormat.parser().merge("""
                    {"schema": {"type": "no.such.Type"}}
                    """, request);
            DynamicGrpcCalls.call(channel, method, request.build(),
                    CallOptions.DEFAULT.withDeadlineAfter(10, TimeUnit.SECONDS),
                    new Metadata(), 4);
        })
                .isInstanceOf(StatusRuntimeException.class)
                .satisfies(t -> {
                    StatusRuntimeException e = (StatusRuntimeException) t;
                    assertThat(e.getStatus().getCode())
                            .isEqualTo(io.grpc.Status.Code.INVALID_ARGUMENT);
                    assertThat(e.getTrailers().get(CatalogBridge.ERROR_CODE_KEY))
                            .isNotEmpty();
                });
    }

    @Test
    void reflectionListsTheServiceAndItsDescriptors() throws Exception {
        ReflectionClient.Result discovered = ReflectionClient.discover(channel, 10_000);
        assertThat(discovered.services())
                .contains("ai.pipestream.protomolt.v1.ProtoMoltService");
        assertThat(discovered.descriptorSet().getFileList())
                .anySatisfy(f -> assertThat(f.getName())
                        .isEqualTo(ProtoMoltServiceSchema.RESOURCE_PATH));
    }

    @Test
    void protoMoltOperatesItself() throws Exception {
        // The GrpcInvoke RPC of the service, asked to call a second live ProtoMolt server's
        // ListTypes RPC, using the service's own .proto as the inline schema. Meta for meta:
        // the tool that manages the format, operated through the format, on itself.
        try (ProtoMoltGrpcServer self = ProtoMoltGrpcServer.start(
                0, ProtoMoltCatalog.full(ActionContext.create()))) {
            JsonNode result = call("GrpcInvoke", """
                    {"target": "localhost:%d",
                     "method": "ai.pipestream.protomolt.v1.ProtoMoltService/ListTypes",
                     "schema": {"sources": {"%s": %s}},
                     "request": {"schema": {"sources": {"shop/v1/order.proto": %s}}}}
                    """.formatted(self.port(), ProtoMoltServiceSchema.RESOURCE_PATH,
                    MAPPER.writeValueAsString(ProtoMoltServiceSchema.protoSource()),
                    MAPPER.writeValueAsString(ORDER_PROTO)));
            assertThat(result.path("ok").asBoolean()).isTrue();
            assertThat(result.path("status").asText()).isEqualTo("OK");
            JsonNode inner = result.path("responses").get(0);
            assertThat(inner.path("types").findValuesAsText("fullName"))
                    .contains("shop.v1.Order");
        }
    }
}
