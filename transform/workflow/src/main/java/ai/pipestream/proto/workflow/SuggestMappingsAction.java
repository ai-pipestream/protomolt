package ai.pipestream.proto.workflow;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.CatalogContract;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.actions.SchemaResolver;
import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.shapes.MappingSuggester;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;

import java.util.LinkedHashMap;
import java.util.Map;

/** MCP/action surface for conservative descriptor-grounded mapping candidates. */
final class SuggestMappingsAction implements ProtoAction {

    private static final int MAX_SOURCES = 64;
    private static final String SOURCE_NAME_PATTERN = "[A-Za-z_][A-Za-z0-9_]{0,127}";

    @Override
    public String name() {
        return "suggest-mappings";
    }

    @Override
    public String requiredScope() {
        return Scopes.SCHEMA_READ;
    }

    @Override
    public String description() {
        return "Suggests verifiable target=source.path mappings from named source message "
                + "types onto a target type. It returns every unambiguous or competing "
                + "candidate and never guesses across incompatible types or dynamic fields.";
    }

    @Override
    public ObjectNode inputSchema() {
        return CatalogContract.schemaFor("SuggestMappingsRequest");
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        CatalogContract.check(input, "SuggestMappingsRequest", name());
        JsonNode sourceNode = input.get("sources");
        if (!(sourceNode instanceof ArrayNode array) || array.isEmpty()) {
            throw WorkflowActionJson.invalid("'sources' must be a non-empty array", "/sources");
        }
        if (array.size() > MAX_SOURCES) {
            throw WorkflowActionJson.invalid("'sources' must contain at most " + MAX_SOURCES
                    + " entries", "/sources");
        }
        Map<String, Descriptor> sources = new LinkedHashMap<>();
        for (int index = 0; index < array.size(); index++) {
            if (!(array.get(index) instanceof ObjectNode source)) {
                throw WorkflowActionJson.invalid("each source must be an object",
                        "/sources/" + index);
            }
            String name = WorkflowActionJson.text(source, "name");
            if (!name.matches(SOURCE_NAME_PATTERN)) {
                throw WorkflowActionJson.invalid("source name must be a mapping-scope identifier",
                        "/sources/" + index + "/name");
            }
            Descriptor descriptor = SchemaResolver.resolve(source, "schema", context)
                    .message(WorkflowActionJson.optionalText(source, "type"),
                            "/sources/" + index + "/type");
            if (sources.putIfAbsent(name, descriptor) != null) {
                throw WorkflowActionJson.invalid("duplicate source name '" + name + "'",
                        "/sources/" + index + "/name");
            }
        }
        ObjectNode targetNode = WorkflowActionJson.object(input, "target");
        Descriptor target = SchemaResolver.resolve(targetNode, "schema", context)
                .message(WorkflowActionJson.optionalText(targetNode, "type"), "/target/type");
        MappingSuggester.Suggestions suggestions = MappingSuggester.suggest(sources, target);
        ObjectNode output = context.objectMapper().createObjectNode();
        output.put("targetType", suggestions.targetType());
        ArrayNode candidates = output.putArray("candidates");
        for (MappingSuggester.Candidate candidate : suggestions.candidates()) {
            ObjectNode node = candidates.addObject();
            node.put("targetPath", candidate.targetPath());
            node.put("sourcePath", candidate.sourcePath());
            node.put("rule", candidate.rule());
            node.put("basis", candidate.basis());
        }
        return output;
    }
}
