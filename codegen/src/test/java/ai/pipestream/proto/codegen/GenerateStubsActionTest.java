package ai.pipestream.proto.codegen;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GenerateStubsActionTest {

    private static final String PROTO = """
            syntax = "proto3";
            package shop.v1;
            import "google/protobuf/timestamp.proto";
            message Order {
              string id = 1;
              google.protobuf.Timestamp placed_at = 2;
            }
            service OrderService {
              rpc GetOrder(Order) returns (Order);
            }
            """;

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final GenerateStubsAction action = new GenerateStubsAction();

    private ObjectNode input(String... generators) {
        ObjectNode input = MAPPER.createObjectNode();
        input.putObject("schema").putObject("sources").put("shop/v1/order.proto", PROTO);
        if (generators.length > 0) {
            var array = input.putArray("generators");
            for (String generator : generators) {
                array.add(generator);
            }
        }
        return input;
    }

    private static List<String> names(JsonNode result) {
        return result.get("files").findValuesAsText("name");
    }

    @Test
    void defaultGeneratorProducesJavaMessageCode() throws Exception {
        ObjectNode result = action.execute(input(), ActionContext.create());
        assertThat(result.get("ok").asBoolean()).isTrue();
        assertThat(names(result)).anySatisfy(name -> assertThat(name).endsWith("OrderOuterClass.java"));
        String content = result.get("files").get(0).get("content").asText();
        assertThat(content).contains("package shop.v1;");
    }

    @Test
    void grpcJavaGeneratesServiceStubs() throws Exception {
        ObjectNode result = action.execute(input("grpc-java"), ActionContext.create());
        assertThat(result.get("ok").asBoolean()).isTrue();
        assertThat(names(result)).anySatisfy(name ->
                assertThat(name).endsWith("OrderServiceGrpc.java"));
        String stub = result.get("files").findValues("content").get(0).asText();
        assertThat(stub).contains("OrderServiceBlockingStub");
    }

    @Test
    void multipleGeneratorsCombineIntoOneFileList() throws Exception {
        ObjectNode result = action.execute(input("java", "grpc-java"), ActionContext.create());
        assertThat(result.get("ok").asBoolean()).isTrue();
        List<String> generators = result.get("files").findValuesAsText("generator");
        assertThat(generators).contains("java", "grpc-java");
        assertThat(names(result)).anySatisfy(n -> assertThat(n).endsWith("OrderOuterClass.java"));
        assertThat(names(result)).anySatisfy(n -> assertThat(n).endsWith("OrderServiceGrpc.java"));
    }

    @Test
    void kotlinGeneratorProducesKotlinDsl() throws Exception {
        ObjectNode result = action.execute(input("kotlin"), ActionContext.create());
        assertThat(result.get("ok").asBoolean()).isTrue();
        assertThat(names(result)).anySatisfy(name -> assertThat(name).endsWith(".kt"));
    }

    @Test
    void wellKnownTypeImportsAreNotGeneratedByDefault() throws Exception {
        ObjectNode result = action.execute(input(), ActionContext.create());
        assertThat(names(result)).noneSatisfy(name ->
                assertThat(name).contains("google/protobuf"));
    }

    @Test
    void unknownGeneratorIsInvalidInput() {
        assertThatThrownBy(() -> action.execute(input("python"), ActionContext.create()))
                .isInstanceOfSatisfying(ActionException.class, e -> {
                    assertThat(e.code()).isEqualTo("invalid-input");
                    assertThat(e.getMessage()).contains("java, kotlin, grpc-java");
                });
    }

    @Test
    void unknownFileIsInvalidInput() {
        ObjectNode input = input();
        input.putArray("files").add("nope.proto");
        assertThatThrownBy(() -> action.execute(input, ActionContext.create()))
                .isInstanceOfSatisfying(ActionException.class,
                        e -> assertThat(e.code()).isEqualTo("invalid-input"));
    }
}
