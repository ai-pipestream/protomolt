package ai.pipestream.proto.workflow;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.CatalogContract;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.actions.Reply;
import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.receipt.RecordVerifier;
import ai.pipestream.proto.receipt.TrustSnapshot;
import ai.pipestream.proto.receipt.Verification;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
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
    public Descriptor requestType() {
        return CatalogContract.request("VerifyWorkRecordRequest");
    }

    @Override
    public Descriptor responseType() {
        return CatalogContract.response("VerifyWorkRecordResponse");
    }

    /**
     * Adds trust to the required list when this node has no pinned snapshot.
     *
     * <p>One of the few contracts that cannot live in the message: the same request is
     * correct with and without the field depending on what the node can fall back on.
     */
    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = CatalogContract.schemaFor(requestType());
        // A node with a pinned snapshot supplies trust itself, so the request may
        // omit it. Without a pin there is nothing to fall back on, and the verb
        // says so in the schema it publishes rather than only when a call fails.
        // Read live rather than at registration: a snapshot arriving on the config
        // lane changes what the next call needs, so it changes what is published.
        return defaultTrust.get() == null
                ? CatalogContract.requiring(schema, "trust") : schema;
    }

    @Override
    public Message execute(Message input, ActionContext context) throws ActionException {
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
        Reply output = Reply.of(responseType())
                .set("verified", verification.verified())
                .set("manifestDigest", verification.manifestDigest())
                .addAll("nonClaims", verification.nonClaims());
        for (Verification.Check check : verification.checks()) {
            output.append("checks")
                    .set("id", check.id())
                    .set("status", check.status().name())
                    .set("detail", check.detail())
                    .build();
        }
        return output.build();
    }
}
