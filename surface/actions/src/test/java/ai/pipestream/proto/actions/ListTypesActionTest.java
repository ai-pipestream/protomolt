package ai.pipestream.proto.actions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import static ai.pipestream.proto.actions.TestFixtures.obj;
import static org.assertj.core.api.Assertions.assertThat;

class ListTypesActionTest {

    private static final String SHOP_PROTO = """
            syntax = "proto3";
            package shop;
            message Order {
              string id = 1;
              Status status = 2;
              repeated Item items = 3;
              map<string, string> tags = 4;
              message Item {
                string sku = 1;
              }
            }
            enum Status {
              STATUS_UNSPECIFIED = 0;
            }
            service OrderService {
              rpc Get(Order) returns (Order);
            }
            """;

    private final ActionCatalog catalog = ActionCatalog.defaults(TestFixtures.personContext());

    private static ObjectNode shopInput() {
        ObjectNode input = obj("{\"schema\": {\"sources\": {}}}");
        ((ObjectNode) input.get("schema").get("sources")).put("shop.proto", SHOP_PROTO);
        return input;
    }

    @Test
    void withoutASchemaListsTheContextRegistry() throws Exception {
        ObjectNode result = catalog.execute("list-types", obj("{}"));
        assertThat(result.get("types").findValuesAsText("fullName"))
                .contains("actions.test.Person", "google.protobuf.Struct");
    }

    @Test
    void listsMessagesEnumsAndServicesWithKinds() throws Exception {
        ObjectNode result = catalog.execute("list-types", shopInput());
        JsonNode types = result.get("types");
        assertThat(kindOf(types, "shop.Order")).isEqualTo("message");
        assertThat(kindOf(types, "shop.Order.Item")).isEqualTo("message");
        assertThat(kindOf(types, "shop.Status")).isEqualTo("enum");
        assertThat(kindOf(types, "shop.OrderService")).isEqualTo("service");
        // synthetic map-entry messages are hidden
        assertThat(types.findValuesAsText("fullName"))
                .noneMatch(name -> name.contains("TagsEntry"));
        assertThat(entryFor(types, "shop.Order").get("file").asText()).isEqualTo("shop.proto");
    }

    @Test
    void messageEntriesDescribeTheirFields() throws Exception {
        ObjectNode result = catalog.execute("list-types", shopInput());
        JsonNode order = entryFor(result.get("types"), "shop.Order");
        JsonNode fields = order.get("fields");
        assertThat(fields).hasSize(4);
        JsonNode id = fields.get(0);
        assertThat(id.get("name").asText()).isEqualTo("id");
        assertThat(id.get("number").asInt()).isEqualTo(1);
        assertThat(id.get("type").asText()).isEqualTo("string");
        assertThat(id.get("label").asText()).isEqualTo("optional");
        JsonNode status = fields.get(1);
        assertThat(status.get("type").asText()).isEqualTo("enum");
        assertThat(status.get("typeName").asText()).isEqualTo("shop.Status");
        JsonNode items = fields.get(2);
        assertThat(items.get("label").asText()).isEqualTo("repeated");
        assertThat(items.get("typeName").asText()).isEqualTo("shop.Order.Item");
    }

    @Test
    void filterNarrowsByCaseInsensitiveSubstring() throws Exception {
        ObjectNode input = shopInput();
        input.put("filter", "STATUS");
        ObjectNode result = catalog.execute("list-types", input);
        assertThat(result.get("types").findValuesAsText("fullName"))
                .containsExactly("shop.Status");
    }

    @Test
    void filterWithNoMatchesYieldsAnEmptyList() throws Exception {
        ObjectNode input = shopInput();
        input.put("filter", "nothing-matches-this");
        ObjectNode result = catalog.execute("list-types", input);
        assertThat(result).isEqualTo(obj("{\"types\": []}"));
    }

    @Test
    void typeFormListsThatTypesFile() throws Exception {
        ObjectNode result = catalog.execute("list-types",
                obj("{\"schema\": {\"type\": \"actions.test.Person\"}}"));
        assertThat(result.get("types").findValuesAsText("fullName"))
                .containsExactly("actions.test.Person");
    }

    private static JsonNode entryFor(JsonNode types, String fullName) {
        for (JsonNode type : types) {
            if (type.get("fullName").asText().equals(fullName)) {
                return type;
            }
        }
        throw new AssertionError("no type " + fullName + " in " + types);
    }

    private static String kindOf(JsonNode types, String fullName) {
        return entryFor(types, fullName).get("kind").asText();
    }
}
