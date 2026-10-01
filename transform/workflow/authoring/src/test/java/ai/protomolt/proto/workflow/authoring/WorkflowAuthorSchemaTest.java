package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.http.jsonschema.ProtoJsonSchemaGenerator;
import ai.protomolt.proto.http.openapi.ProtoOpenApiGenerator;
import ai.protomolt.proto.http.rest.ApiTokenRequirement;
import ai.protomolt.proto.http.rest.ProtoRestMethodRegistry;
import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowAuthorContextResponse;
import ai.protomolt.proto.workflow.authoring.v1.ReadWorkflowAuthorEventsRequest;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowAuthorTaskServiceOuterClass;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class WorkflowAuthorSchemaTest {
    @Test
    void schemasExposeBoundsAndPreserveRuntimeRulesWithoutMountingHandlers() {
        var json = new ObjectMapper();
        var generator = ProtoJsonSchemaGenerator.create();
        JsonNode request = json.valueToTree(generator.generateRooted(ReadWorkflowAuthorEventsRequest.getDescriptor()));
        assertThat(request.at("/properties/taskId/format").asText()).isEqualTo("uuid");
        assertThat(request.at("/properties/maxEvents/maximum").asInt()).isEqualTo(64);
        JsonNode response = json.valueToTree(generator.generateRooted(GetWorkflowAuthorContextResponse.getDescriptor()));
        assertThat(response.path("x-protomolt-cel").toString())
                .contains("author-context-offer", "author-context-descriptor-size");
        var service = WorkflowAuthorTaskServiceOuterClass.getDescriptor().findServiceByName("WorkflowAuthorTaskService");
        var methods = new ProtoRestMethodRegistry();
        service.getMethods().forEach(method -> methods.register(service, method, unused -> {
            throw new AssertionError("Contract schema generation must not invoke a handler");
        }, ApiTokenRequirement.bearer()));
        JsonNode openapi = json.valueToTree(new ProtoOpenApiGenerator().generate(methods));
        assertThat(openapi.path("paths").size()).isEqualTo(5);
        assertThat(openapi.path("components").path("schemas").toString())
                .contains("x-protomolt-cel", "author-context-offer", "author-events-cursor");
    }
}
