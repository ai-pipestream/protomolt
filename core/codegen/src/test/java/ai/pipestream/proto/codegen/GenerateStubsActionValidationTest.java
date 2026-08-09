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

/**
 * Input validation and file-selection behavior of {@code generate-stubs}. Generator success
 * paths and protoc error surfacing live in {@link GenerateStubsActionTest}.
 */
class GenerateStubsActionValidationTest {

    private static final String COMMON_PROTO = """
            syntax = "proto3";
            package shop.v1;
            message Money {
              string currency = 1;
              int64 cents = 2;
            }
            """;

    private static final String ORDER_PROTO = """
            syntax = "proto3";
            package shop.v1;
            import "shop/v1/common.proto";
            message Order {
              string id = 1;
              Money total = 2;
            }
            """;

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final GenerateStubsAction action = new GenerateStubsAction();

    private ObjectNode singleFileInput() {
        ObjectNode input = MAPPER.createObjectNode();
        input.putObject("schema").putObject("sources").put("shop/v1/order.proto", """
                syntax = "proto3";
                package shop.v1;
                message Order {
                  string id = 1;
                }
                """);
        return input;
    }

    private ObjectNode twoFileInput() {
        ObjectNode input = MAPPER.createObjectNode();
        ObjectNode sources = input.putObject("schema").putObject("sources");
        sources.put("shop/v1/common.proto", COMMON_PROTO);
        sources.put("shop/v1/order.proto", ORDER_PROTO);
        return input;
    }

    private static List<String> names(JsonNode result) {
        return result.get("files").findValuesAsText("name");
    }

    @Test
    void missingSchemaIsInvalidInput() {
        assertThatThrownBy(() -> action.execute(MAPPER.createObjectNode(), ActionContext.create()))
                .isInstanceOfSatisfying(ActionException.class,
                        e -> assertThat(e.code()).isEqualTo("invalid-input"));
    }

    @Test
    void generatorsAsStringIsInvalidInput() {
        ObjectNode input = singleFileInput();
        input.put("generators", "java");

        assertThatThrownBy(() -> action.execute(input, ActionContext.create()))
                .isInstanceOfSatisfying(ActionException.class, e -> {
                    assertThat(e.code()).isEqualTo("invalid-input");
                    assertThat(e.getMessage()).contains("/generators");
                });
    }

    @Test
    void emptyGeneratorsArrayIsInvalidInput() {
        ObjectNode input = singleFileInput();
        input.putArray("generators");

        assertThatThrownBy(() -> action.execute(input, ActionContext.create()))
                .isInstanceOfSatisfying(ActionException.class,
                        e -> assertThat(e.code()).isEqualTo("invalid-input"));
    }

    @Test
    void explicitNullGeneratorsFallsBackToJava() throws Exception {
        ObjectNode input = singleFileInput();
        input.putNull("generators");

        ObjectNode result = action.execute(input, ActionContext.create());

        assertThat(result.get("ok").asBoolean()).isTrue();
        assertThat(names(result)).anySatisfy(name -> assertThat(name).endsWith(".java"));
    }

    @Test
    void generatorNamesAreCaseInsensitive() throws Exception {
        ObjectNode input = singleFileInput();
        input.putArray("generators").add("JAVA");

        ObjectNode result = action.execute(input, ActionContext.create());

        assertThat(result.get("ok").asBoolean()).isTrue();
        assertThat(result.get("files").findValuesAsText("generator")).containsOnly("java");
    }

    @Test
    void filesAsStringIsInvalidInput() {
        ObjectNode input = singleFileInput();
        input.put("files", "shop/v1/order.proto");

        assertThatThrownBy(() -> action.execute(input, ActionContext.create()))
                .isInstanceOfSatisfying(ActionException.class, e -> {
                    assertThat(e.code()).isEqualTo("invalid-input");
                    assertThat(e.getMessage()).contains("/files");
                });
    }

    @Test
    void emptyFilesArrayIsInvalidInput() {
        ObjectNode input = singleFileInput();
        input.putArray("files");

        assertThatThrownBy(() -> action.execute(input, ActionContext.create()))
                .isInstanceOfSatisfying(ActionException.class,
                        e -> assertThat(e.code()).isEqualTo("invalid-input"));
    }

    @Test
    void unknownFileErrorListsWhatIsPresent() {
        ObjectNode input = twoFileInput();
        input.putArray("files").add("shop/v1/absent.proto");

        assertThatThrownBy(() -> action.execute(input, ActionContext.create()))
                .isInstanceOfSatisfying(ActionException.class, e -> {
                    assertThat(e.code()).isEqualTo("invalid-input");
                    assertThat(e.getMessage())
                            .contains("shop/v1/absent.proto")
                            .contains("shop/v1/common.proto")
                            .contains("shop/v1/order.proto");
                });
    }

    @Test
    void multiFileSchemaGeneratesEveryNonGoogleFileByDefault() throws Exception {
        ObjectNode result = action.execute(twoFileInput(), ActionContext.create());

        assertThat(result.get("ok").asBoolean()).isTrue();
        assertThat(result.get("fileCount").asInt()).isEqualTo(2);
        // protoc names the outer class after the file unless a type in it collides:
        // common.proto (message Money) -> Common.java; order.proto (message Order) -> OrderOuterClass.java.
        assertThat(names(result))
                .anySatisfy(name -> assertThat(name).endsWith("Common.java"))
                .anySatisfy(name -> assertThat(name).endsWith("OrderOuterClass.java"));
    }

    @Test
    void filesSelectionGeneratesOnlyTheListedSubset() throws Exception {
        ObjectNode input = twoFileInput();
        input.putArray("files").add("shop/v1/common.proto");

        ObjectNode result = action.execute(input, ActionContext.create());

        assertThat(result.get("ok").asBoolean()).isTrue();
        assertThat(result.get("fileCount").asInt()).isEqualTo(1);
        assertThat(names(result)).allSatisfy(name -> assertThat(name).endsWith("Common.java"));
    }

    @Test
    void crossFileImportCompilesAndGenerates() throws Exception {
        // order.proto's Money field resolves through common.proto; generation must succeed
        // for both when the dependency is part of the schema.
        ObjectNode result = action.execute(twoFileInput(), ActionContext.create());

        String orderJava = result.get("files").findValues("content").stream()
                .map(JsonNode::asText)
                .filter(content -> content.contains("class OrderOuterClass"))
                .findFirst()
                .orElseThrow();
        // The imported type is referenced through common.proto's outer class.
        assertThat(orderJava).contains("Common.Money");
    }

    /** A non-textual {@code parameter} is deliberately ignored, not an error. */
    @Test
    void nonTextualParameterIsIgnored() throws Exception {
        ObjectNode input = singleFileInput();
        input.put("parameter", 123);

        ObjectNode result = action.execute(input, ActionContext.create());

        assertThat(result.get("ok").asBoolean()).isTrue();
        assertThat(names(result)).noneSatisfy(name -> assertThat(name).endsWith(".pb.meta"));
    }

    /** grpc-java emits nothing for a schema without services, and that is not an error. */
    @Test
    void grpcJavaOnMessageOnlySchemaGeneratesNoFiles() throws Exception {
        ObjectNode input = singleFileInput();
        input.putArray("generators").add("grpc-java");

        ObjectNode result = action.execute(input, ActionContext.create());

        assertThat(result.get("ok").asBoolean()).isTrue();
        assertThat(result.get("fileCount").asInt()).isZero();
        assertThat(result.get("files").size()).isZero();
    }
}
