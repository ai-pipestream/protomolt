package ai.pipestream.proto.graph;

import ai.pipestream.proto.index.spi.IndexingPlanFactory;
import ai.pipestream.proto.shapes.SchemaInferrer;
import ai.pipestream.proto.shapes.ShapeSynthesizer;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The read -&gt; infer-schema lane end to end, no live tenant: a document's SharePoint list-item
 * columns (the data-rich JSON {@link GraphFiles#listItemFieldsOnly} returns) are inferred into a
 * typed message, and that message is fed straight back into the Graph connection-schema engine —
 * SharePoint metadata in, a typed contract out, then a search schema out of that.
 */
class GraphInferSchemaDemoTest {

    // A realistic SharePoint list-item payload: string, integer, boolean, multi-choice array, and
    // a person/lookup column (a nested object). Column names are Title-cased, as SharePoint sends.
    private static final String LIST_ITEM = """
            {
              "id": "i1",
              "fields": {
                "Title": "Q3 Report",
                "Amount": 4200,
                "Approved": true,
                "Tags": ["finance", "q3"],
                "Reviewer": {"DisplayName": "Ada Lovelace", "Email": "ada@contoso.com"}
              }
            }
            """;

    private static final String ITEM_NOT_FOUND = "{\"error\":{\"code\":\"itemNotFound\","
            + "\"message\":\"The resource could not be found.\"}}";

    private HttpServer server;
    private GraphFiles files;

    @BeforeEach
    void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1.0/drives/d1/items/i1/listItem",
                exchange -> FakeGraphSupport.respond(exchange, 200, LIST_ITEM));
        server.createContext("/v1.0/drives/d1/items/i1",
                exchange -> FakeGraphSupport.respond(exchange, 200, "{\"id\": \"i1\"}"));
        // A file with no backing list item (a plain personal-OneDrive file): the item resolves,
        // its listItem 404s. Graph sends the same 404 for an item that is not there at all.
        server.createContext("/v1.0/drives/d1/items/none",
                exchange -> FakeGraphSupport.respond(exchange, 200, "{\"id\": \"none\"}"));
        server.createContext("/v1.0/drives/d1/items/none/listItem",
                exchange -> FakeGraphSupport.respond(exchange, 404, ITEM_NOT_FOUND));
        // Neither the item nor its listItem exists: a bad driveId/itemId, not "no columns".
        server.createContext("/v1.0/drives/d1/items/ghost",
                exchange -> FakeGraphSupport.respond(exchange, 404, ITEM_NOT_FOUND));
        // A genuine failure that must not be swallowed as \"no columns\".
        server.createContext("/v1.0/drives/d1/items/denied/listItem",
                exchange -> FakeGraphSupport.respond(exchange, 403, "{\"error\":{\"code\":"
                        + "\"accessDenied\",\"message\":\"Access denied.\"}}"));
        server.start();
        String base = "http://127.0.0.1:" + server.getAddress().getPort();
        files = new GraphFiles(new GraphClient(base + "/v1.0", () -> "test-token"));
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    @Test
    void sharePointColumnsBecomeATypedMessageThatKeepsTheOriginalColumnNames() throws Exception {
        ShapeSynthesizer.SynthesizedShape shape = infer("sharepoint.v1.Documents");

        // The emitted contract: an integer column is int64, a bool is bool, a multi-choice column
        // is repeated, and the person/lookup column becomes a nested message.
        assertThat(shape.protoSource())
                .contains("int64").contains("bool").contains("repeated string")
                .contains("message");
        // Sanitized field names carry json_name so the exact SharePoint column names round-trip.
        assertThat(shape.protoSource()).contains("json_name = \"Title\"")
                .contains("json_name = \"Amount\"");

        Descriptor type = shape.type();
        assertThat(type.getFullName()).isEqualTo("sharepoint.v1.Documents");
        assertThat(fieldByColumn(type, "Amount").getJavaType())
                .isEqualTo(FieldDescriptor.JavaType.LONG);
        assertThat(fieldByColumn(type, "Approved").getJavaType())
                .isEqualTo(FieldDescriptor.JavaType.BOOLEAN);
        FieldDescriptor tags = fieldByColumn(type, "Tags");
        assertThat(tags.isRepeated()).isTrue();
        assertThat(tags.getJavaType()).isEqualTo(FieldDescriptor.JavaType.STRING);
        FieldDescriptor reviewer = fieldByColumn(type, "Reviewer");
        assertThat(reviewer.getJavaType()).isEqualTo(FieldDescriptor.JavaType.MESSAGE);
        assertThat(fieldByColumn(reviewer.getMessageType(), "Email").getJavaType())
                .isEqualTo(FieldDescriptor.JavaType.STRING);
    }

    @Test
    void theInferredMessageFeedsBackIntoTheGraphConnectionSchemaEngine() throws Exception {
        ShapeSynthesizer.SynthesizedShape shape = infer("sharepoint.v1.Documents");

        // Full loop: SharePoint metadata -> typed proto -> a Graph external-connection schema.
        var plan = IndexingPlanFactory.inferringOnly().create(shape.type());
        GraphSchemas.Rendered rendered = GraphSchemas.connectionSchema(shape.type(), plan);

        assertThat(rendered.schema()).isNotNull();
        assertThat(rendered.schema().toString()).isNotBlank().isNotEqualTo("{}");
        // Graph's property model is flat, so the nested person/lookup column cannot be rendered and
        // is reported rather than silently dropped.
        assertThat(rendered.skipped())
                .anySatisfy(s -> assertThat(s).containsIgnoringCase("reviewer"));
    }

    @Test
    void listItemFieldsOnlyIsEmptyForAFileWithNoListItem() throws Exception {
        // The personal-OneDrive case the javadoc promises: no columns, an empty object, no 404.
        assertThat(files.listItemFieldsOnly("d1", "none").isEmpty()).isTrue();
    }

    @Test
    void listItemFieldsOnlyStillPropagatesRealFailures() {
        // A 403 is not "no columns" - it must surface, not be swallowed as an empty object.
        assertThatThrownBy(() -> files.listItemFieldsOnly("d1", "denied"))
                .isInstanceOfSatisfying(GraphClient.GraphApiException.class,
                        e -> assertThat(e.status()).isEqualTo(403));
    }

    /**
     * A mistyped driveId or itemId 404s exactly like a file with no list item; reporting it as
     * "no columns" would hand infer-schema an empty sample instead of naming the bad id.
     */
    @Test
    void listItemFieldsOnlyFailsWhenTheItemItselfIsMissing() {
        assertThatThrownBy(() -> files.listItemFieldsOnly("d1", "ghost"))
                .isInstanceOfSatisfying(GraphClient.GraphApiException.class, e -> {
                    assertThat(e.status()).isEqualTo(404);
                    assertThat(e.code()).isEqualTo("itemNotFound");
                });
    }

    private ShapeSynthesizer.SynthesizedShape infer(String fullName) throws Exception {
        ObjectNode fields = files.listItemFieldsOnly("d1", "i1");
        Struct.Builder sample = Struct.newBuilder();
        JsonFormat.parser().merge(fields.toString(), sample);
        return new SchemaInferrer().infer(fullName, List.of(sample.build()));
    }

    /** The inferred field whose json_name is the original SharePoint column {@code column}. */
    private static FieldDescriptor fieldByColumn(Descriptor type, String column) {
        return type.getFields().stream()
                .filter(f -> column.equals(f.getJsonName()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no field for column " + column
                        + " in " + type.getFullName()));
    }
}
