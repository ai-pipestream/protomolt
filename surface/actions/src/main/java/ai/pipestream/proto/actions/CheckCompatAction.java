package ai.pipestream.proto.actions;

import ai.pipestream.proto.compat.CompatibilityChecker;
import ai.pipestream.proto.compat.CompatibilityMode;
import ai.pipestream.proto.compat.CompatibilityResult;
import ai.pipestream.proto.compat.SchemaChange;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import java.util.Arrays;

/** Checks a new schema version against an old one under a compatibility mode. */
final class CheckCompatAction implements ProtoAction {

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
    public Message execute(Message input, ActionContext context) throws ActionException {
        SchemaResolver.ResolvedSchema oldSchema = SchemaResolver.resolve(input, "old", context);
        SchemaResolver.ResolvedSchema newSchema = SchemaResolver.resolve(input, "new", context);
        // The contract names the mode with an enum, so an unknown one is refused before the
        // verb runs and whatever arrives is a value this checker already knows.
        String modeName = Fields.enumName(input, "mode");
        CompatibilityMode mode = modeName.isEmpty() || modeName.endsWith("UNSPECIFIED")
                ? CompatibilityMode.BACKWARD
                : CompatibilityMode.valueOf(modeName.startsWith(MODE_PREFIX)
                        ? modeName.substring(MODE_PREFIX.length()) : modeName);
        CompatibilityChecker checker = CompatibilityChecker.builder()
                .includeJsonRules(Fields.flag(input, "includeJsonRules"))
                .includeSourceRules(Fields.flag(input, "includeSourceRules"))
                .build();
        CompatibilityResult result =
                checker.check(oldSchema.descriptorSet(), newSchema.descriptorSet(), mode);
        Reply output = Reply.of(responseType())
                .set("compatible", result.isCompatible())
                .set("mode", MODE_PREFIX + result.mode().name());
        for (SchemaChange change : result.violations()) {
            ActionJson.writeChange(output, "violations", change);
        }
        for (SchemaChange change : result.changes()) {
            ActionJson.writeChange(output, "changes", change);
        }
        return output.build();
    }
}
