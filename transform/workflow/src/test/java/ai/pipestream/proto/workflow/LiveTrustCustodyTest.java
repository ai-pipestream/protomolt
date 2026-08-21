package ai.pipestream.proto.workflow;

import static org.assertj.core.api.Assertions.assertThat;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.receipt.KeyState;
import ai.pipestream.proto.receipt.SignatureAlgorithm;
import ai.pipestream.proto.receipt.TrustSnapshot;
import ai.pipestream.proto.receipt.TrustedIssuer;
import ai.pipestream.proto.receipt.TrustedKey;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * Custody read live rather than frozen at registration: the verifying verbs ask their
 * trust source per request, so a snapshot arriving after the catalog was built — off the
 * config lane, in the deployment this enables — applies to the next call. Registration
 * happens once at boot, so a source that is empty then and full later is the whole point.
 */
class LiveTrustCustodyTest {

    private static TrustSnapshot snapshot(String issuer) {
        return TrustSnapshot.newBuilder()
                .addIssuers(TrustedIssuer.newBuilder()
                        .setIssuer(issuer)
                        .addKeys(TrustedKey.newBuilder()
                                .setKeyId("key-2026")
                                .setAlgorithm(SignatureAlgorithm.SIGNATURE_ALGORITHM_ED25519)
                                .setPublicKey(ByteString.copyFrom(new byte[32]))
                                .setState(KeyState.KEY_STATE_ACTIVE))
                        .addSubjectKinds("workflow-run"))
                .build();
    }

    /** Whether the verb's schema still demands a caller-supplied snapshot. */
    private static boolean demandsTrust(ProtoAction action) {
        ObjectNode schema = action.inputSchema();
        for (var required : schema.withArray("required")) {
            if ("trust".equals(required.asText())) {
                return true;
            }
        }
        return false;
    }

    private static ProtoAction verifyVerb(AtomicReference<TrustSnapshot> custody)
            throws ActionException {
        ActionCatalog catalog = WorkflowWorkbenchActions.register(
                ActionCatalog.defaults(ActionContext.create()),
                new WorkflowRunner(), null, null, null, null, custody::get);
        return catalog.get("verify-work-record");
    }

    @Test
    void aSnapshotArrivingAfterRegistrationIsUsedByTheNextCall() throws Exception {
        AtomicReference<TrustSnapshot> custody = new AtomicReference<>();
        ProtoAction verify = verifyVerb(custody);

        // Registered with nothing: the verb has no default and says so.
        assertThat(demandsTrust(verify)).isTrue();

        custody.set(snapshot("records.protomolt.dev"));

        // No re-registration: the same action instance now defaults.
        assertThat(demandsTrust(verify)).isFalse();
    }

    @Test
    void custodyWithdrawnGoesBackToDemandingOne() throws Exception {
        AtomicReference<TrustSnapshot> custody =
                new AtomicReference<>(snapshot("records.protomolt.dev"));
        ProtoAction verify = verifyVerb(custody);
        assertThat(demandsTrust(verify)).isFalse();

        custody.set(null);
        assertThat(demandsTrust(verify)).isTrue();
    }

    @Test
    void withoutCustodyTheRefusalNamesThePinnedFileVariable() {
        ObjectNode carryingNoTrust = JsonNodeFactory.instance.objectNode();

        Throwable refusal = org.assertj.core.api.Assertions.catchThrowable(
                () -> TrustPin.resolve(carryingNoTrust, null));

        assertThat(refusal).isInstanceOf(ActionException.class);
        assertThat(refusal.getMessage()).contains(TrustPin.ENV_TRUST_SNAPSHOT);
    }

    @Test
    void aRequestsOwnSnapshotWinsOverCustody() throws Exception {
        // The source answers one issuer; the request names another and must win,
        // because custody is the default and never an override.
        ObjectNode carryingTrust = JsonNodeFactory.instance.objectNode();
        carryingTrust.set("trust", JsonNodeFactory.instance.objectNode()
                .set("issuers", JsonNodeFactory.instance.arrayNode()
                        .add(JsonNodeFactory.instance.objectNode()
                                .put("issuer", "from.the.request"))));

        TrustSnapshot resolved =
                TrustPin.resolve(carryingTrust, snapshot("from.the.custody"));

        assertThat(resolved.getIssuers(0).getIssuer()).isEqualTo("from.the.request");
    }

    @Test
    void aNullSourceBehavesLikeNoCustodyRatherThanFailing() throws Exception {
        ActionCatalog catalog = WorkflowWorkbenchActions.register(
                ActionCatalog.defaults(ActionContext.create()),
                new WorkflowRunner(), null, null, null, null,
                (java.util.function.Supplier<TrustSnapshot>) null);
        assertThat(demandsTrust(catalog.get("verify-work-record"))).isTrue();
    }
}
