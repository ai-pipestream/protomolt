package ai.pipestream.proto.actions;

import ai.pipestream.proto.compat.CompatibilityChecker;
import ai.pipestream.proto.compat.CompatibilityMode;
import ai.pipestream.proto.compat.CompatibilityResult;
import ai.pipestream.proto.compat.SchemaChange;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Arrays;

/** Checks a new schema version against an old one under a compatibility mode. */
final class CheckCompatAction implements ProtoAction {

    @Override
    public String name() {
        return "check-compat";
    }

    @Override
    public String description() {
        return "Checks whether the new schema version is compatible with the old one under a "
                + "Confluent-style compatibility mode (default BACKWARD, binary wire rules only); "
                + "optionally also enforces canonical proto3 JSON rules and generated-source rules, "
                + "returning the violations and the full change list.";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = ActionJson.baseInputSchema();
        ObjectNode properties = schema.putObject("properties");
        properties.set("old", ActionJson.schemaSourceSchema());
        properties.set("new", ActionJson.schemaSourceSchema());
        ObjectNode mode = properties.putObject("mode");
        mode.put("type", "string");
        mode.put("description", "Compatibility mode to enforce; defaults to BACKWARD.");
        mode.put("default", "BACKWARD");
        ArrayNode modes = mode.putArray("enum");
        Arrays.stream(CompatibilityMode.values()).map(Enum::name).forEach(modes::add);
        properties.putObject("includeJsonRules")
                .put("type", "boolean")
                .put("default", false)
                .put("description",
                        "Also treat canonical proto3 JSON payload breaks (field/enum name changes, "
                                + "removals) as violations.");
        properties.putObject("includeSourceRules")
                .put("type", "boolean")
                .put("default", false)
                .put("description",
                        "Also treat generated-code and gRPC surface breaks as violations.");
        ActionJson.required(schema, "old", "new");
        schema.put("additionalProperties", false);
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        SchemaResolver.ResolvedSchema oldSchema = SchemaResolver.resolve(input, "old", context);
        SchemaResolver.ResolvedSchema newSchema = SchemaResolver.resolve(input, "new", context);
        String modeName = Inputs.optionalString(input, "mode");
        CompatibilityMode mode;
        try {
            mode = modeName == null ? CompatibilityMode.BACKWARD : CompatibilityMode.valueOf(modeName);
        } catch (IllegalArgumentException e) {
            throw Inputs.invalidInput("Unknown compatibility mode '" + modeName + "'; expected one of "
                    + Arrays.toString(CompatibilityMode.values()), "/mode");
        }
        CompatibilityChecker checker = CompatibilityChecker.builder()
                .includeJsonRules(Inputs.optionalBoolean(input, "includeJsonRules", false))
                .includeSourceRules(Inputs.optionalBoolean(input, "includeSourceRules", false))
                .build();
        CompatibilityResult result =
                checker.check(oldSchema.descriptorSet(), newSchema.descriptorSet(), mode);
        ObjectNode output = context.objectMapper().createObjectNode();
        output.put("compatible", result.isCompatible());
        output.put("mode", result.mode().name());
        ArrayNode violations = output.putArray("violations");
        for (SchemaChange change : result.violations()) {
            violations.add(ActionJson.change(change, context.objectMapper()));
        }
        ArrayNode changes = output.putArray("changes");
        for (SchemaChange change : result.changes()) {
            changes.add(ActionJson.change(change, context.objectMapper()));
        }
        return output;
    }
}
