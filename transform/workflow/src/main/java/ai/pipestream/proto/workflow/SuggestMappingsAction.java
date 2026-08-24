package ai.pipestream.proto.workflow;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.CatalogContract;
import ai.pipestream.proto.actions.Fields;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.actions.Reply;
import ai.pipestream.proto.actions.SchemaResolver;
import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.shapes.MappingSuggester;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import java.util.LinkedHashMap;
import java.util.List;
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
    public Descriptor requestType() {
        return CatalogContract.request("SuggestMappingsRequest");
    }

    @Override
    public Descriptor responseType() {
        return CatalogContract.response("SuggestMappingsResponse");
    }

    @Override
    public Message execute(Message input, ActionContext context) throws ActionException {
        List<Message> declared = Fields.list(input, "sources");
        if (declared.isEmpty()) {
            throw WorkflowActionJson.invalid("'sources' must be a non-empty array", "/sources");
        }
        if (declared.size() > MAX_SOURCES) {
            throw WorkflowActionJson.invalid("'sources' must contain at most " + MAX_SOURCES
                    + " entries", "/sources");
        }
        Map<String, Descriptor> sources = new LinkedHashMap<>();
        for (int index = 0; index < declared.size(); index++) {
            Message source = declared.get(index);
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
        Message targetSource = Fields.message(input, "target");
        Descriptor target = SchemaResolver.resolve(targetSource, "schema", context)
                .message(WorkflowActionJson.optionalText(targetSource, "type"), "/target/type");
        MappingSuggester.Suggestions suggestions = MappingSuggester.suggest(sources, target);
        Reply output = Reply.of(responseType()).set("targetType", suggestions.targetType());
        for (MappingSuggester.Candidate candidate : suggestions.candidates()) {
            output.append("candidates")
                    .set("targetPath", candidate.targetPath())
                    .set("sourcePath", candidate.sourcePath())
                    .set("rule", candidate.rule())
                    .set("basis", candidate.basis())
                    .build();
        }
        return output.build();
    }
}
