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
    void pythonGeneratorProducesPb2Module() throws Exception {
        ObjectNode result = action.execute(input("python"), ActionContext.create());
        assertThat(result.get("ok").asBoolean()).isTrue();
        assertThat(names(result)).anySatisfy(name -> assertThat(name).endsWith("order_pb2.py"));
        String content = result.get("files").get(0).get("content").asText();
        assertThat(content).contains("DESCRIPTOR");
    }

    @Test
    void everyLanguageGeneratorProducesFiles() throws Exception {
        var expectations = java.util.Map.of(
                "cpp", ".pb.cc",
                "csharp", ".cs",
                "ruby", "_pb.rb",
                "php", ".php",
                "objc", ".pbobjc.m");
        for (var entry : expectations.entrySet()) {
            ObjectNode result = action.execute(input(entry.getKey()), ActionContext.create());
            assertThat(result.get("ok").asBoolean())
                    .as("generator %s", entry.getKey()).isTrue();
            assertThat(names(result))
                    .as("generator %s files", entry.getKey())
                    .anySatisfy(name -> assertThat(name).endsWith(entry.getValue()));
        }
    }

    /**
     * {@code parameter} is documented as protoc's {@code --<gen>_opt}. Nothing pinned that it
     * actually reached the generator, so a dropped pass-through would have looked like a
     * silently ignored option: {@code annotate_code} makes protoc emit a {@code .pb.meta}
     * companion that is absent without it.
     */
    @Test
    void parameterIsPassedThroughToTheGenerator() throws Exception {
        ObjectNode input = input("java");
        input.put("parameter", "annotate_code");

        ObjectNode result = action.execute(input, ActionContext.create());

        assertThat(result.get("ok").asBoolean()).isTrue();
        assertThat(names(result)).anySatisfy(name ->
                assertThat(name).endsWith("OrderOuterClass.java.pb.meta"));
        assertThat(names(action.execute(input("java"), ActionContext.create())))
                .noneSatisfy(name -> assertThat(name).endsWith(".pb.meta"));
    }

    /**
     * A generator-reported failure is a result, not an exception: {@code ok: false} carrying
     * protoc's own message and the generator that produced it, with no partial file list.
     */
    @Test
    void generatorFailureReturnsNotOkWithProtocsMessage() throws Exception {
        ObjectNode input = input("java");
        input.put("parameter", "bogus_option");

        ObjectNode result = action.execute(input, ActionContext.create());

        assertThat(result.get("ok").asBoolean()).isFalse();
        assertThat(result.get("generator").asText()).isEqualTo("java");
        assertThat(result.get("error").asText())
                .contains("Unknown generator option: bogus_option");
        assertThat(result.has("files")).isFalse();
        assertThat(result.has("fileCount")).isFalse();
    }

    /** The run stops at the failing generator; a later generator's files are not returned. */
    @Test
    void generatorFailureShortCircuitsRemainingGenerators() throws Exception {
        ObjectNode input = input("java", "grpc-java");
        input.put("parameter", "bogus_option");

        ObjectNode result = action.execute(input, ActionContext.create());

        assertThat(result.get("ok").asBoolean()).isFalse();
        assertThat(result.get("generator").asText()).isEqualTo("java");
        assertThat(result.has("files")).isFalse();
    }

    @Test
    void unknownGeneratorIsInvalidInput() {
        assertThatThrownBy(() -> action.execute(input("rust"), ActionContext.create()))
                .isInstanceOfSatisfying(ActionException.class, e -> {
                    assertThat(e.code()).isEqualTo("invalid-input");
                    assertThat(e.getMessage()).contains("java, kotlin, grpc-java, python, cpp, csharp, ruby, php, objc");
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
