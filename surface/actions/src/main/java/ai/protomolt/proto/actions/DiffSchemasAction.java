package ai.protomolt.proto.actions;

import ai.protomolt.proto.compat.SchemaChange;
import ai.protomolt.proto.compat.SchemaDiff;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import java.util.List;

/** Diffs two schema versions and reports every change with its compatibility impacts. */
final class DiffSchemasAction implements ProtoAction {

    @Override
    public String name() {
        return "diff-schemas";
    }

    @Override
    public String requiredScope() {
        return Scopes.SCHEMA_READ;
    }

    @Override
    public String description() {
        return "Diffs two protobuf schema versions and reports every change (fields, enums, "
                + "services, oneofs, reserved ranges) with a stable ruleId, the protobuf path, "
                + "before/after snippets and the compatibility impacts it carries.";
    }

    @Override
    public Descriptor requestType() {
        return CatalogContract.request("DiffSchemasRequest");
    }

    @Override
    public Descriptor responseType() {
        return CatalogContract.response("DiffSchemasResponse");
    }

    @Override
    public Message execute(Message input, ActionContext context) throws ActionException {
        SchemaResolver.ResolvedSchema oldSchema = SchemaResolver.resolve(input, "old", context);
        SchemaResolver.ResolvedSchema newSchema = SchemaResolver.resolve(input, "new", context);
        List<SchemaChange> changes =
                SchemaDiff.diff(oldSchema.descriptorSet(), newSchema.descriptorSet());
        Reply output = Reply.of(responseType());
        for (SchemaChange change : changes) {
            ActionJson.writeChange(output, "changes", change);
        }
        return output.build();
    }
}
