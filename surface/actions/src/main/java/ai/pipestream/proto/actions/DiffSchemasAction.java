package ai.pipestream.proto.actions;

import ai.pipestream.proto.compat.SchemaChange;
import ai.pipestream.proto.compat.SchemaDiff;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;

/** Diffs two schema versions and reports every change with its compatibility impacts. */
final class DiffSchemasAction implements ProtoAction {

    @Override
    public String name() {
        return "diff-schemas";
    }

    @Override
    public String description() {
        return "Diffs two protobuf schema versions and reports every change (fields, enums, "
                + "services, oneofs, reserved ranges) with a stable ruleId, the protobuf path, "
                + "before/after snippets and the compatibility impacts it carries.";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = ActionJson.baseInputSchema();
        ObjectNode properties = schema.putObject("properties");
        properties.set("old", ActionJson.schemaSourceSchema());
        properties.set("new", ActionJson.schemaSourceSchema());
        ActionJson.required(schema, "old", "new");
        schema.put("additionalProperties", false);
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        SchemaResolver.ResolvedSchema oldSchema = SchemaResolver.resolve(input, "old", context);
        SchemaResolver.ResolvedSchema newSchema = SchemaResolver.resolve(input, "new", context);
        List<SchemaChange> changes =
                SchemaDiff.diff(oldSchema.descriptorSet(), newSchema.descriptorSet());
        ObjectNode output = context.objectMapper().createObjectNode();
        ArrayNode changesNode = output.putArray("changes");
        for (SchemaChange change : changes) {
            changesNode.add(ActionJson.change(change, context.objectMapper()));
        }
        return output;
    }
}
