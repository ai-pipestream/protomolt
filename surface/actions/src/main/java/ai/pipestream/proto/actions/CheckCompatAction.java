package ai.pipestream.proto.actions;

import ai.pipestream.proto.compat.CompatibilityChecker;
import ai.pipestream.proto.compat.CompatibilityMode;
import ai.pipestream.proto.compat.CompatibilityResult;
import ai.pipestream.proto.compat.SchemaChange;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import com.google.protobuf.Descriptors.Descriptor;
import java.util.Arrays;

/** Checks a new schema version against an old one under a compatibility mode. */
final class CheckCompatAction implements JsonAction {

    /**
     * Proto enum values carry their type name, so the wire form of BACKWARD is
     * COMPATIBILITY_MODE_BACKWARD. The checker's own enum does not, and the two are
     * otherwise the same vocabulary.
     */
    private static final String MODE_PREFIX = "COMPATIBILITY_MODE_";

    @Override
    public String name() {
        return "check-compat";
    }

    @Override
    public String requiredScope() {
        return Scopes.SCHEMA_READ;
    }

    @Override
    public String description() {
        return "Checks whether the new schema version is compatible with the old one under a "
                + "Confluent-style compatibility mode (default BACKWARD, binary wire rules only); "
                + "optionally also enforces canonical proto3 JSON rules and generated-source rules, "
                + "returning the violations and the full change list.";
    }

    @Override
    public Descriptor requestType() {
        return CatalogContract.request("CheckCompatRequest");
    }

    @Override
    public Descriptor responseType() {
        return CatalogContract.response("CheckCompatResponse");
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        SchemaResolver.ResolvedSchema oldSchema = SchemaResolver.resolve(input, "old", context);
        SchemaResolver.ResolvedSchema newSchema = SchemaResolver.resolve(input, "new", context);
        // The contract names the mode with an enum, so an unknown one is refused before the
        // verb runs and whatever arrives is a value this checker already knows.
        String modeName = Inputs.optionalString(input, "mode");
        CompatibilityMode mode = modeName == null || modeName.isBlank()
                ? CompatibilityMode.BACKWARD
                : CompatibilityMode.valueOf(modeName.startsWith(MODE_PREFIX)
                        ? modeName.substring(MODE_PREFIX.length()) : modeName);
        CompatibilityChecker checker = CompatibilityChecker.builder()
                .includeJsonRules(Inputs.optionalBoolean(input, "includeJsonRules", false))
                .includeSourceRules(Inputs.optionalBoolean(input, "includeSourceRules", false))
                .build();
        CompatibilityResult result =
                checker.check(oldSchema.descriptorSet(), newSchema.descriptorSet(), mode);
        ObjectNode output = context.objectMapper().createObjectNode();
        output.put("compatible", result.isCompatible());
        output.put("mode", MODE_PREFIX + result.mode().name());
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
