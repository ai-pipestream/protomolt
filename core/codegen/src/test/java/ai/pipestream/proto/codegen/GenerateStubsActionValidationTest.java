package ai.pipestream.proto.codegen;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
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
        assertThatThrownBy(() -> dispatch(action, MAPPER.createObjectNode()))
                .isInstanceOfSatisfying(ActionException.class,
                        e -> assertThat(e.code()).isEqualTo("invalid-input"));
    }

    @Test
    void generatorsAsStringIsInvalidInput() {
        ObjectNode input = singleFileInput();
        input.put("generators", "java");

        assertThatThrownBy(() -> dispatch(action, input))
                .isInstanceOfSatisfying(ActionException.class, e -> {
                    assertThat(e.code()).isEqualTo("invalid-input");
                    assertThat(e.getMessage()).contains("generators");
                });
    }

    /**
     * An empty list selects Java, as the message says. proto3 delivers an omitted repeated
     * field and an empty one identically, so the contract gives the empty list a meaning
     * rather than refusing what it cannot tell apart from silence.
     */
    @Test
    void anEmptyGeneratorListSelectsJava() throws Exception {
        ObjectNode input = singleFileInput();
        input.putArray("generators");

        ObjectNode result = dispatch(action, input);

        assertThat(result.get("ok").asBoolean()).isTrue();
        assertThat(names(result)).anySatisfy(name -> assertThat(name).endsWith(".java"));
    }

    @Test
    void explicitNullGeneratorsFallsBackToJava() throws Exception {
        ObjectNode input = singleFileInput();
        input.putNull("generators");

        ObjectNode result = dispatch(action, input);

        assertThat(result.get("ok").asBoolean()).isTrue();
        assertThat(names(result)).anySatisfy(name -> assertThat(name).endsWith(".java"));
    }

    @Test
    void generatorNamesAreTheDeclaredEnumValues() {
        // The generator vocabulary is a proto enum, so a value is written exactly as the
        // enum declares it. A lowercase or differently-cased spelling is refused rather
        // than guessed at, and the published schema lists the values a caller may use.
        ObjectNode input = singleFileInput();
        input.putArray("generators").add("java");
        ActionCatalog catalog = ActionCatalog.defaults(ActionContext.create()).replace(action);

        assertThatThrownBy(() -> catalog.execute("generate-stubs", input))
                .isInstanceOf(ActionException.class)
                .hasMessageContaining("CodeGenerator");
    }

    @Test
    void filesAsStringIsInvalidInput() {
        ObjectNode input = singleFileInput();
        input.put("files", "shop/v1/order.proto");

        assertThatThrownBy(() -> dispatch(action, input))
                .isInstanceOfSatisfying(ActionException.class, e -> {
                    assertThat(e.code()).isEqualTo("invalid-input");
                    assertThat(e.getMessage()).contains("files");
                });
    }

    /** An empty list selects every non-google file, for the same reason. */
    @Test
    void anEmptyFileListSelectsEveryNonGoogleFile() throws Exception {
        ObjectNode input = twoFileInput();
        input.putArray("files");

        ObjectNode result = dispatch(action, input);

        assertThat(result.get("ok").asBoolean()).isTrue();
        assertThat(result.get("fileCount").asInt()).isEqualTo(2);
    }

    @Test
    void unknownFileErrorListsWhatIsPresent() {
        ObjectNode input = twoFileInput();
        input.putArray("files").add("shop/v1/absent.proto");

        assertThatThrownBy(() -> dispatch(action, input))
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
        ObjectNode result = dispatch(action, twoFileInput());

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

        ObjectNode result = dispatch(action, input);

        assertThat(result.get("ok").asBoolean()).isTrue();
        assertThat(result.get("fileCount").asInt()).isEqualTo(1);
        assertThat(names(result)).allSatisfy(name -> assertThat(name).endsWith("Common.java"));
    }

    @Test
    void crossFileImportCompilesAndGenerates() throws Exception {
        // order.proto's Money field resolves through common.proto; generation must succeed
        // for both when the dependency is part of the schema.
        ObjectNode result = dispatch(action, twoFileInput());

        String orderJava = result.get("files").findValues("content").stream()
                .map(JsonNode::asText)
                .filter(content -> content.contains("class OrderOuterClass"))
                .findFirst()
                .orElseThrow();
        // The imported type is referenced through common.proto's outer class.
        assertThat(orderJava).contains("Common.Money");
    }

    /** A non-textual {@code parameter} is refused by name rather than quietly ignored. */
    @Test
    void aNonTextualParameterIsRefusedByName() {
        ObjectNode input = singleFileInput();
        input.put("parameter", 123);

        assertThatThrownBy(() -> dispatch(action, input))
                .isInstanceOfSatisfying(ActionException.class, e -> {
                    assertThat(e.code()).isEqualTo("invalid-input");
                    assertThat(e.getMessage()).contains("parameter");
                });
    }

    /** grpc-java emits nothing for a schema without services, and that is not an error. */
    @Test
    void grpcJavaOnMessageOnlySchemaGeneratesNoFiles() throws Exception {
        ObjectNode input = singleFileInput();
        input.putArray("generators").add("CODE_GENERATOR_GRPC_JAVA");

        ObjectNode result = dispatch(action, input);

        assertThat(result.get("ok").asBoolean()).isTrue();
        assertThat(result.get("fileCount").asInt()).isZero();
        assertThat(result.get("files").size()).isZero();
    }

    /**
     * Dispatches the way every surface does: through a catalog holding the verb, which is
     * where the request contract is checked before the verb runs.
     */
    private static ObjectNode dispatch(ProtoAction verb, ObjectNode input)
            throws ActionException {
        return ActionCatalog.defaults(ActionContext.create())
                .replace(verb).execute(verb.name(), input);
    }

}
