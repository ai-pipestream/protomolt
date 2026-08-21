package ai.pipestream.proto.workflow;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.receipt.RecordVerifier;
import ai.pipestream.proto.receipt.TrustSnapshot;
import ai.pipestream.proto.receipt.Verification;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Base64;
import java.util.Map;
import java.util.function.Supplier;

/** Verifies a signed work record offline against a caller-supplied trust snapshot. */
final class VerifyWorkRecordAction implements ProtoAction {

    private final Supplier<TrustSnapshot> defaultTrust;

    VerifyWorkRecordAction() {
        this((TrustSnapshot) null);
    }

    /** With a pinned snapshot, requests may omit {@code trust}; a supplied one wins. */
    VerifyWorkRecordAction(TrustSnapshot defaultTrust) {
        this(() -> defaultTrust);
    }

    /**
     * With a snapshot source, the server's custody is read per request rather than
     * frozen at registration, so a snapshot arriving on the config lane applies to the
     * next call with no re-registration.
     */
    VerifyWorkRecordAction(Supplier<TrustSnapshot> defaultTrust) {
        this.defaultTrust = defaultTrust == null ? () -> null : defaultTrust;
    }

    @Override
    public String name() {
        return "verify-work-record";
    }

    @Override
    public String requiredScope() {
        return Scopes.WORKFLOW_RUN;
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
                .put("description", defaultTrust.get() == null
                        ? "TrustSnapshot encoded as protobuf JSON."
                        : "TrustSnapshot encoded as protobuf JSON; defaults to the "
                                + "server's pinned snapshot when omitted.");
        ObjectNode artifacts = properties.putObject("artifacts");
        artifacts.put("type", "object");
        artifacts.putObject("additionalProperties").put("type", "string");
        artifacts.put("description",
                "Referenced artifact bytes by SHA-256, base64-encoded; when present the "
                        + "rehash check runs all-or-nothing instead of being skipped.");
        var required = schema.putArray("required").add("recordBase64");
        // Read live: a node whose custody arrives on the lane stops demanding
        // 'trust' from the next request onward.
        if (defaultTrust.get() == null) {
            required.add("trust");
        }
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
        TrustSnapshot trust = TrustPin.resolve(input, defaultTrust.get());
        Map<String, byte[]> artifacts = WorkflowActionJson.base64Map(input, "artifacts");
        Verification verification;
        try {
            verification = RecordVerifier.verify(record, trust, artifacts);
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
