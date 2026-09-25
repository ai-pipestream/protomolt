package ai.protomolt.proto.samples;

import ai.protomolt.proto.http.jsonschema.ProtoJsonSchemaGenerator;
import ai.protomolt.proto.http.openapi.ProtoOpenApiGenerator;
import ai.protomolt.proto.http.rest.ApiTokenRequirement;
import ai.protomolt.proto.http.rest.ProtoRestMethodRegistry;
import ai.protomolt.proto.inference.v1.EvaluateRequest;
import ai.protomolt.proto.inference.v1.EvaluateResponse;
import ai.protomolt.proto.correction.v1.CorrectedContact;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/** Records generated schemas without asserting that OpenAPI already has rule parity. */
class StarterSchemaCoverageTest {
    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void generatedSchemasExposeAvailableConstraintsAndRecordRuntimeOnlyRules() throws Exception {
        var generator = ProtoJsonSchemaGenerator.create();
        JsonNode contact = JSON.valueToTree(generator.generateRooted(CorrectedContact.getDescriptor()));
        assertThat(contact.path("required").toString()).contains("recordId", "displayName");
        assertThat(contact.at("/properties/displayName/minLength").asInt()).isEqualTo(2);
        assertThat(contact.at("/properties/displayName/maxLength").asInt()).isEqualTo(120);
        assertThat(contact.path("x-protomolt-cel").toString()).contains("deliverable-contact-method");

        JsonNode request = JSON.valueToTree(generator.generateRooted(EvaluateRequest.getDescriptor()));
        JsonNode response = JSON.valueToTree(generator.generateRooted(EvaluateResponse.getDescriptor()));
        assertThat(request.path("required").toString()).contains("requestId", "binding", "evidence", "rubric", "evaluatorProfile");
        assertThat(response.at("/properties/answers/minItems").asInt()).isEqualTo(1);
        assertThat(response.at("/properties/answers/maxItems").asInt()).isEqualTo(32);

        var service = EvaluateRequest.getDescriptor().getFile().findServiceByName("EvaluationService");
        var methods = new ProtoRestMethodRegistry();
        methods.register(service, service.findMethodByName("Evaluate"), unused -> {
            throw new AssertionError("Schema generation must not invoke an unimplemented service");
        }, ApiTokenRequirement.bearer());
        JsonNode openapi = JSON.valueToTree(new ProtoOpenApiGenerator().generate(methods));
        assertThat(openapi.path("openapi").asText()).isEqualTo("3.0.3");
        assertThat(openapi.path("paths").size()).isEqualTo(1);
        assertThat(openapi.path("components").path("schemas").toString()).contains("EvaluateRequest", "EvaluateResponse");

        // Reviewable artifacts; generated locally, not served or advertised by a host.
        Path directory = Path.of("build", "starter-contracts");
        Files.createDirectories(directory);
        JSON.writerWithDefaultPrettyPrinter().writeValue(directory.resolve("contact.schema.json").toFile(), contact);
        JSON.writerWithDefaultPrettyPrinter().writeValue(directory.resolve("evaluate-request.schema.json").toFile(), request);
        JSON.writerWithDefaultPrettyPrinter().writeValue(directory.resolve("evaluate-response.schema.json").toFile(), response);
        JSON.writerWithDefaultPrettyPrinter().writeValue(directory.resolve("evaluation.openapi.json").toFile(), openapi);
    }
}
