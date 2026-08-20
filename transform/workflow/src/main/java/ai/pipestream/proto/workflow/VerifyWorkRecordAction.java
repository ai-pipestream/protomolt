package ai.pipestream.proto.workflow;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.receipt.RecordVerifier;
import ai.pipestream.proto.receipt.TrustSnapshot;
import ai.pipestream.proto.receipt.Verification;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Base64;

/** Verifies a signed work record offline against a caller-supplied trust snapshot. */
final class VerifyWorkRecordAction implements ProtoAction {

    @Override
    public String name() {
        return "verify-work-record";
    }

    @Override
    public String description() {
        return "Verifies a signed work record against a caller-supplied trust snapshot with "
                + "zero network calls: a fixed pipeline of named checks that refuses by name, "
                + "plus the non-claims naming what verification does not establish.";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = WorkflowActionJson.schema();
        ObjectNode properties = schema.putObject("properties");
        properties.putObject("recordBase64").put("type", "string")
                .put("description", "Serialized SignedWorkRecord, base64.");
        properties.putObject("trust").put("type", "object")
                .put("description", "TrustSnapshot encoded as protobuf JSON.");
        schema.putArray("required").add("recordBase64").add("trust");
        schema.put("additionalProperties", false);
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        byte[] record;
        try {
            record = Base64.getDecoder()
                    .decode(WorkflowActionJson.text(input, "recordBase64"));
        } catch (IllegalArgumentException e) {
            throw WorkflowActionJson.invalid("'recordBase64' is not valid base64",
                    "/recordBase64");
        }
        TrustSnapshot trust = (TrustSnapshot) WorkflowActionJson.parse(
                WorkflowActionJson.object(input, "trust"), TrustSnapshot.newBuilder(),
                "/trust");
        Verification verification;
        try {
            verification = RecordVerifier.verify(record, trust);
        } catch (IllegalArgumentException e) {
            throw WorkflowActionJson.invalid(e.getMessage(), "/trust");
        }
        ObjectNode output = context.objectMapper().createObjectNode();
        output.put("verified", verification.verified());
        if (!verification.manifestDigest().isEmpty()) {
            output.put("manifestDigest", verification.manifestDigest());
        }
        ArrayNode checks = output.putArray("checks");
        for (Verification.Check check : verification.checks()) {
            ObjectNode node = checks.addObject();
            node.put("id", check.id());
            node.put("status", check.status().name());
            node.put("detail", check.detail());
        }
        ArrayNode nonClaims = output.putArray("nonClaims");
        verification.nonClaims().forEach(nonClaims::add);
        return output;
    }
}
