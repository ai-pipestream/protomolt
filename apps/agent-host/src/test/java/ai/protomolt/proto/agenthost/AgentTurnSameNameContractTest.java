package ai.protomolt.proto.agenthost;

import ai.protomolt.proto.delegation.v1.DeliverableContract;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AgentTurnSameNameContractTest {
    private static final String OLD = "11111111-1111-4111-8111-111111111111";
    private static final String NEW = "22222222-2222-4222-8222-222222222222";

    @Test void candidateUsesItsTaskContractWhenAnotherTaskHasTheSameTypeName()
            throws Exception {
        Map<String, DeliverableContract> contracts = Map.of(
                OLD, DeliverableFixture.contract(), NEW, extendedContract());
        String reply = """
                {"handledEventCursors":[13],"commands":[{"tool":"delegation-candidate",
                "arguments":{"taskId":"%s","candidate":{"attempt":1,"revision":1,
                "summary":"extended report","evidence":[{"checkName":"report-written",
                "verdict":"CHECK_VERDICT_PASSED","ranAt":"2026-09-01T00:00:00Z",
                "detail":"the report was written"}],"commits":[{"repository":"example/repo",
                "commit":"%s","subject":"report"}],"result":{"@type":"%s",
                "headline":"a headline long enough","findings":2,
                "evidenceNote":"second task only"}}}}]}
                """.formatted(NEW, "a".repeat(40), DeliverableFixture.TYPE_URL);

        AgentTurn parsed = AgentTurn.parse(reply, AgentRole.WORKER, List.of(13L),
                "fixture-worker", contracts);

        assertThat(parsed.commands()).hasSize(1);
        assertThat(parsed.commands().getFirst().tool()).isEqualTo("delegation-candidate");
        assertThat(AgentTurn.outputSchema(AgentRole.WORKER, contracts).toString())
                .contains("evidenceNote");
    }

    @Test void nestedSameNamedDefinitionsStaySeparateAndRecursiveRefsResolve()
            throws Exception {
        JsonNode schema = AgentTurn.outputSchema(AgentRole.WORKER, Map.of(
                OLD, nestedContract(false), NEW, nestedContract(true)));
        JsonNode definitions = schema.path("$defs");
        var nested = definitions.properties().stream()
                .filter(entry -> entry.getKey().endsWith("delivery.v1.ReviewReport.Detail"))
                .toList();
        assertThat(nested).hasSize(2);
        assertThat(nested.stream().map(entry ->
                entry.getValue().path("properties").has("variantOnly")).toList())
                .containsExactlyInAnyOrder(true, false);
        assertThat(definitions.has("delivery.v1.ReviewReport.Detail")).isFalse();
        for (var entry : nested) {
            List<String> nestedRefs = new ArrayList<>();
            collectRefs(entry.getValue(), nestedRefs);
            assertThat(nestedRefs).contains("#/$defs/" + entry.getKey());
        }
        List<String> allRefs = new ArrayList<>();
        collectRefs(schema, allRefs);
        for (String ref : allRefs) {
            if (ref.startsWith("#/$defs/")) {
                assertThat(definitions.has(ref.substring("#/$defs/".length())))
                        .as("resolves " + ref).isTrue();
            }
        }
    }

    private static void collectRefs(JsonNode node, List<String> refs) {
        if (node.isObject()) {
            JsonNode ref = node.get("$ref");
            if (ref != null) refs.add(ref.asText());
            node.properties().forEach(entry -> collectRefs(entry.getValue(), refs));
        } else if (node.isArray()) {
            node.forEach(child -> collectRefs(child, refs));
        }
    }

    private static DeliverableContract nestedContract(boolean variant) throws Exception {
        FileDescriptorSet.Builder set = FileDescriptorSet.newBuilder().mergeFrom(
                Base64.getDecoder().decode(DeliverableFixture.descriptorSetBase64()));
        DescriptorProto.Builder report = set.getFileBuilder(0).getMessageTypeBuilder(0);
        DescriptorProto.Builder detail = DescriptorProto.newBuilder().setName("Detail")
                .addField(FieldDescriptorProto.newBuilder().setName("label")
                        .setJsonName("label").setNumber(1)
                        .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                        .setType(FieldDescriptorProto.Type.TYPE_STRING))
                .addField(FieldDescriptorProto.newBuilder().setName("next")
                        .setJsonName("next").setNumber(2)
                        .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                        .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                        .setTypeName(".delivery.v1.ReviewReport.Detail"));
        if (variant) {
            detail.addField(FieldDescriptorProto.newBuilder().setName("variant_only")
                    .setJsonName("variantOnly").setNumber(3)
                    .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                    .setType(FieldDescriptorProto.Type.TYPE_STRING));
        }
        report.addNestedType(detail);
        report.addField(FieldDescriptorProto.newBuilder().setName("detail")
                .setJsonName("detail").setNumber(3)
                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                .setTypeName(".delivery.v1.ReviewReport.Detail"));
        return DeliverableContract.newBuilder().setDescriptorSet(
                ByteString.copyFrom(set.build().toByteArray()))
                .setTypeName(DeliverableFixture.TYPE_NAME).build();
    }

    private static DeliverableContract extendedContract() throws Exception {
        FileDescriptorSet.Builder set = FileDescriptorSet.newBuilder().mergeFrom(
                Base64.getDecoder().decode(DeliverableFixture.descriptorSetBase64()));
        set.getFileBuilder(0).getMessageTypeBuilder(0).addField(
                FieldDescriptorProto.newBuilder().setName("evidence_note")
                        .setJsonName("evidenceNote").setNumber(3)
                        .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                        .setType(FieldDescriptorProto.Type.TYPE_STRING));
        return DeliverableContract.newBuilder().setDescriptorSet(
                ByteString.copyFrom(set.build().toByteArray()))
                .setTypeName(DeliverableFixture.TYPE_NAME).build();
    }
}
