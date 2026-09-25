package ai.protomolt.proto.http.openapi;

import ai.protomolt.proto.http.openapi.testdata.CreateRequest;
import ai.protomolt.proto.http.openapi.testdata.CreateResponse;
import ai.protomolt.proto.http.openapi.testdata.Profile;
import ai.protomolt.proto.http.openapi.testdata.ReviewState;
import ai.protomolt.proto.http.rest.ProtoRestMethod;
import ai.protomolt.proto.http.rest.ProtoRestMethodRegistry;
import ai.protomolt.proto.validate.ProtoValidator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.JsonFormat;
import com.networknt.schema.Error;
import com.networknt.schema.InputFormat;
import com.networknt.schema.Schema;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SpecificationVersion;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Checks the public OAS document against runtime validation on the same request
 * and nested response messages. OAS 3.0 uses boolean exclusive bounds; the test
 * adapter lowers those to Draft 2020-12 numeric bounds for the existing
 * networknt validator, while retaining assertions on the published OAS shape.
 */
class ProtoOpenApiValidationParityTest {
    private static final String REQUEST_TYPE =
            "ai.protomolt.proto.http.openapi.testdata.v1.CreateRequest";
    private static final String RESPONSE_TYPE =
            "ai.protomolt.proto.http.openapi.testdata.v1.CreateResponse";
    private static final String PROFILE_TYPE =
            "ai.protomolt.proto.http.openapi.testdata.v1.Profile";
    private static final ObjectMapper JSON = new ObjectMapper();
    private final ProtoOpenApiGenerator generator = new ProtoOpenApiGenerator();
    private final Map<String, Object> document = new ProtoOpenApiGenerator().generate(registry());
    private final Map<String, Object> schemas = schemas(document);

    @Test
    void operationSchemasCoverRequestAndNestedResponseInOpenApi30() {
        assertThat(document.get("openapi")).isEqualTo("3.0.3");
        assertThat(operationSchema(document, "/grpc-json/EntryService/Create", "requestBody"))
                .containsEntry("$ref", "#/components/schemas/" + REQUEST_TYPE.replace('.', '_'));
        assertThat(operationSchema(document, "/grpc-json/EntryService/Create", "responses"))
                .containsEntry("$ref", "#/components/schemas/" + RESPONSE_TYPE.replace('.', '_'));

        Map<String, Object> request = schema(REQUEST_TYPE);
        assertThat((List<Object>) request.get("required")).contains("externalId");
        assertThat(property(request, "externalId"))
                .containsEntry("minLength", 3L)
                .containsEntry("maxLength", 12L)
                .containsEntry("pattern", "^[a-z]+$");
        Map<String, Object> score = property(request, "score");
        assertThat(score).containsEntry("minimum", 0L).containsEntry("exclusiveMinimum", true)
                .containsEntry("maximum", 10L).containsEntry("exclusiveMaximum", true);
        assertThat(property(request, "formatVersion").get("enum")).isEqualTo(List.of(2L));
        assertThat(property(request, "state").get("anyOf")).isNotNull();

        Map<String, Object> tags = property(request, "tags");
        assertThat(tags).containsEntry("minItems", 1L).containsEntry("maxItems", 3L)
                .containsEntry("uniqueItems", true);
        assertThat(tags.get("items")).isInstanceOf(Map.class);
        assertThat(property(request, "metrics"))
                .containsEntry("type", "object")
                .containsEntry("minProperties", 1L)
                .containsEntry("maxProperties", 2L);

        Map<String, Object> response = schema(RESPONSE_TYPE);
        assertThat((List<Object>) response.get("required")).contains("profile");
        assertThat(property(response, "profile").get("$ref"))
                .isEqualTo("#/components/schemas/" + PROFILE_TYPE.replace('.', '_'));
        Map<String, Object> profile = schema(PROFILE_TYPE);
        assertThat(property(profile, "balance"))
                .containsEntry("type", "string").containsEntry("format", "int64");
        assertThat(property(profile, "permissions")).containsEntry("minItems", 1L)
                .containsEntry("maxItems", 2L);

        assertThat(property(profile, "observedAt").get("x-protomolt-runtime-rules"))
                .isEqualTo(List.of("timestamp"));
        assertThat(property(request, "optionalRank").get("x-protomolt-runtime-rules"))
                .isEqualTo(List.of("ignore-if-zero-value"));
        assertThat(request.get("x-protomolt-cel")).isInstanceOf(List.class);
    }

    @Test
    void clientSchemaAndRuntimeAgreeForRepresentableRequestValues() throws Exception {
        List<CreateRequest> cases = List.of(
                validRequest().build(),
                validRequest().setScore(0).build(),
                validRequest().setScore(10).build(),
                validRequest().setExternalId("AB1").build(),
                validRequest().clearExternalId().build(),
                validRequest().setTags(0, "other").build(),
                validRequest().addTags("alpha").build(),
                validRequest().clearTags().build(),
                validRequest().addTags("beta").addTags("gamma").addTags("delta").build(),
                validRequest().clearMetrics().build(),
                validRequest().putMetrics("cpu", -1).build(),
                validRequest().putMetrics("disk", 30).putMetrics("network", 40).build(),
                validRequest().setFormatVersion(3).build(),
                validRequest().setState(ReviewState.REVIEW_STATE_DONE).build(),
                validRequest().setStateValue(77).build(),
                validRequest().setSignedOffset(9_007_199_254_740_992L).build());

        for (CreateRequest request : cases) {
            assertClientRuntimeParity(request, REQUEST_TYPE);
        }
        JsonNode requestJson = JSON.readTree(JsonFormat.printer().print(validRequest().build()));
        assertThat(requestJson.path("signedOffset").isTextual()).isTrue();
        assertThat(requestJson.path("metrics").path("cpu").isTextual()).isTrue();
        assertThat(requestJson.path("metrics").path("cpu").asText()).isEqualTo("50");
        assertThat(property(schema(REQUEST_TYPE), "signedOffset"))
                .containsEntry("type", "string").containsEntry("format", "int64");
    }

    @Test
    void clientSchemaAndRuntimeAgreeForNestedResponseValuesAndCanonicalInt64Bounds() throws Exception {
        Profile valid = Profile.newBuilder().setDisplayName("Ada")
                .setBalance(9_007_199_254_740_991L)
                .addPermissions("read")
                .setObservedAt(Timestamp.newBuilder().setSeconds(1_700_000_000L))
                .build();
        List<CreateResponse> cases = List.of(
                CreateResponse.newBuilder().setProfile(valid).build(),
                CreateResponse.newBuilder().setProfile(valid.toBuilder().setDisplayName("A")).build(),
                CreateResponse.newBuilder().setProfile(valid.toBuilder().setBalance(9_007_199_254_740_992L)).build(),
                CreateResponse.newBuilder().setProfile(valid.toBuilder().addPermissions("delete")).build(),
                CreateResponse.newBuilder().setProfile(valid.toBuilder()
                        .addPermissions("write").addPermissions("read")).build(),
                CreateResponse.newBuilder().setProfile(valid.toBuilder().clearPermissions()).build(),
                CreateResponse.getDefaultInstance());

        for (CreateResponse response : cases) {
            assertClientRuntimeParity(response, RESPONSE_TYPE);
        }
        JsonNode json = JSON.readTree(JsonFormat.printer().print(cases.getFirst()));
        assertThat(json.path("profile").path("balance").isTextual()).isTrue();
        assertThat(json.path("profile").path("balance").asText())
                .isEqualTo("9007199254740991");
    }

    @Test
    void runtimeOnlyCelAndTimestampRulesAreDisclosedAndStillEnforcedAtRuntime() throws Exception {
        CreateRequest blocked = validRequest().setExternalId("blocked").build();
        assertThat(ProtoValidator.forMessageType(blocked.getDescriptorForType())
                .validate(blocked).valid()).isFalse();
        assertThat(validateOpenApi(blocked, REQUEST_TYPE)).isEmpty();
        assertThat((List<?>) schema(REQUEST_TYPE).get("x-protomolt-cel")).isNotEmpty();

        Profile before2020 = Profile.newBuilder().setDisplayName("Ada").setBalance(1)
                .addPermissions("read").setObservedAt(Timestamp.newBuilder().setSeconds(1)).build();
        assertThat(ProtoValidator.forMessageType(before2020.getDescriptorForType())
                .validate(before2020).valid()).isFalse();
        assertThat(validateOpenApi(CreateResponse.newBuilder().setProfile(before2020).build(),
                RESPONSE_TYPE)).isEmpty();
        assertThat((List<Object>) property(schema(PROFILE_TYPE), "observedAt")
                .get("x-protomolt-runtime-rules"))
                .contains("timestamp");

        // ignore_if_zero is conditional runtime behavior: proto3 JSON omits a default zero,
        // so the canonical payload passes both sides while the extension records the rule.
        CreateRequest zeroOptional = validRequest().setOptionalRank(0).build();
        assertThat(ProtoValidator.forMessageType(zeroOptional.getDescriptorForType())
                .validate(zeroOptional).valid()).isTrue();
        assertThat(validateOpenApi(zeroOptional, REQUEST_TYPE)).isEmpty();
    }

    private void assertClientRuntimeParity(Message message, String type) throws Exception {
        var runtime = ProtoValidator.forMessageType(message.getDescriptorForType()).validate(message);
        boolean runtimeAccepts = runtime.valid();
        List<Error> clientErrors = validateOpenApi(message, type);
        assertThat(clientErrors.isEmpty()).as("OpenAPI validation for %s; runtime violations: %s",
                        message, runtime.violations())
                .isEqualTo(runtimeAccepts);
    }

    private List<Error> validateOpenApi(Message message, String rootType) throws Exception {
        return validateOpenApi(JsonFormat.printer().print(message), rootType);
    }

    private List<Error> validateOpenApi(String json, String rootType) throws Exception {
        ObjectNode schema = JSON.valueToTree(schema(rootType));
        schema.put("$schema", "https://json-schema.org/draft/2020-12/schema");
        ObjectNode definitions = JSON.createObjectNode();
        schemas.forEach((name, value) -> definitions.set(name, JSON.valueToTree(value)));
        schema.set("$defs", definitions);
        adaptOpenApiToJsonSchema(schema);
        Schema validator = SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12)
                .getSchema(JSON.writeValueAsString(schema));
        return validator.validate(json, InputFormat.JSON);
    }

    private static void adaptOpenApiToJsonSchema(JsonNode node) {
        if (node.isObject()) {
            ObjectNode object = (ObjectNode) node;
            JsonNode minimum = object.get("minimum");
            if (object.path("exclusiveMinimum").asBoolean(false) && minimum != null) {
                object.set("exclusiveMinimum", minimum.deepCopy());
                object.remove("minimum");
            } else {
                object.remove("exclusiveMinimum");
            }
            JsonNode maximum = object.get("maximum");
            if (object.path("exclusiveMaximum").asBoolean(false) && maximum != null) {
                object.set("exclusiveMaximum", maximum.deepCopy());
                object.remove("maximum");
            } else {
                object.remove("exclusiveMaximum");
            }
            JsonNode ref = object.get("$ref");
            if (ref != null && ref.isTextual()) {
                object.put("$ref", ref.asText().replace("#/components/schemas/", "#/$defs/"));
            }
            object.elements().forEachRemaining(ProtoOpenApiValidationParityTest::adaptOpenApiToJsonSchema);
        } else if (node.isArray()) {
            node.elements().forEachRemaining(ProtoOpenApiValidationParityTest::adaptOpenApiToJsonSchema);
        }
    }

    private static CreateRequest.Builder validRequest() {
        return CreateRequest.newBuilder().setExternalId("abc").setScore(5)
                .addTags("alpha").putMetrics("cpu", 50).setFormatVersion(2)
                .setState(ReviewState.REVIEW_STATE_READY).setSignedOffset(12);
    }

    private static ProtoRestMethodRegistry registry() {
        ProtoRestMethodRegistry registry = new ProtoRestMethodRegistry();
        registry.register(ProtoRestMethod.builder("EntryService", "Create", request -> request)
                .methodDescriptor(CreateRequest.getDescriptor().getFile()
                        .findServiceByName("EntryService").findMethodByName("Create")).build());
        return registry;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> schemas(Map<String, Object> doc) {
        return (Map<String, Object>) ((Map<String, Object>) doc.get("components")).get("schemas");
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> schema(String type) {
        return (Map<String, Object>) schemas.get(type.replace('.', '_'));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> property(Map<String, Object> schema, String name) {
        return (Map<String, Object>) ((Map<String, Object>) schema.get("properties")).get(name);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> operationSchema(Map<String, Object> doc, String path,
                                                       String member) {
        Map<String, Object> paths = (Map<String, Object>) doc.get("paths");
        Map<String, Object> pathItem = (Map<String, Object>) paths.values().iterator().next();
        Map<String, Object> operation = (Map<String, Object>) pathItem.values().iterator().next();
        if ("requestBody".equals(member)) {
            Map<String, Object> request = (Map<String, Object>) operation.get(member);
            Map<String, Object> content = (Map<String, Object>) request.get("content");
            Map<String, Object> json = (Map<String, Object>) content.get("application/json");
            return (Map<String, Object>) json.get("schema");
        }
        Map<String, Object> responses = (Map<String, Object>) operation.get(member);
        Map<String, Object> success = (Map<String, Object>) responses.get("200");
        Map<String, Object> content = (Map<String, Object>) success.get("content");
        Map<String, Object> json = (Map<String, Object>) content.get("application/json");
        return (Map<String, Object>) json.get("schema");
    }
}
