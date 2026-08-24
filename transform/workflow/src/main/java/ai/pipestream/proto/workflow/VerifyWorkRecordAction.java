package ai.pipestream.proto.workflow;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.CatalogContract;
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
        ObjectNode schema = CatalogContract.schemaFor("VerifyWorkRecordRequest");
        // A node with a pinned snapshot supplies trust itself, so the request may
        // omit it. Without a pin there is nothing to fall back on, and the verb
        // says so in the schema it publishes rather than only when a call fails.
        // Read live rather than at registration: a snapshot arriving on the config
        // lane changes what the next call needs, so it changes what is published.
        return defaultTrust.get() == null
                ? CatalogContract.requiring(schema, "trust") : schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        CatalogContract.check(input, "VerifyWorkRecordRequest", name());
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
