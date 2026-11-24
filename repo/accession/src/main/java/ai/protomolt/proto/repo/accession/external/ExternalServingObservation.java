package ai.protomolt.proto.repo.accession.external;

import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.findOperation;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.optionalCurrentLink;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.sanitizeMessage;

import ai.protomolt.proto.repo.external.v1.ExternalLink;
import ai.protomolt.proto.repo.external.v1.ExternalProviderEvidence;
import ai.protomolt.proto.repo.external.v1.ExternalProviderSettlement;
import ai.protomolt.proto.repo.external.v1.ManagedSearchIdentity;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationLedger;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationOperationRecord;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.NodeAddress;
import com.google.protobuf.Message;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.UUID;

/**
 * Refreshes the advisory serving-layer visibility of a confirmed link from the provider's
 * read-only status endpoint. A failed refresh is reported with its status, never as a
 * replay of the last observation.
 */
final class ExternalServingObservation {

    private final ExternalLedgerStore ledgers;
    private final ExternalBindingStore bindingStore;
    private final ExternalProviderGateway gateway;
    private final ExternalProviderVerifier verifier;

    ExternalServingObservation(ExternalLedgerStore ledgers, ExternalBindingStore bindingStore,
            ExternalProviderGateway gateway, ExternalProviderVerifier verifier) {
        this.ledgers = ledgers;
        this.bindingStore = bindingStore;
        this.gateway = gateway;
        this.verifier = verifier;
    }

    ExternalPublicationCoordinator.OperationResult refreshServingObservation(
            NodeAddress ledgerAddr,
            LedgerAccess security,
            ExternalPublicationLedger ledger,
            ExternalPublicationOperationRecord op) {
        ExternalProviderGateway.OutboundAuthContext outboundAuth =
                bindingStore.resolveOutboundAuth(op.getBindingSnapshot());
        ExternalDeletionLifecycle.requireEndpointUnmoved(op.getBindingSnapshot(), outboundAuth);
        ExternalProviderGateway.ProviderStatusResult statusResult;
        try {
            statusResult = gateway.queryStatus(new ExternalProviderGateway.ProviderStatusQuery(
                    op.getOperationId(),
                    op.getIntentSha256(),
                    op.getIntent(),
                    outboundAuth,
                    true));
        } catch (StatusRuntimeException transportFailure) {
            Status.Code code = transportFailure.getStatus().getCode();
            if (code == Status.Code.UNAVAILABLE || code == Status.Code.DEADLINE_EXCEEDED) {
                throw Status.UNAVAILABLE
                        .withDescription("Serving observation unavailable: STATUS_ENDPOINT_"
                                + code.name())
                        .withCause(transportFailure)
                        .asRuntimeException();
            }
            throw transportFailure;
        }
        if (statusResult instanceof ExternalProviderGateway.ProviderStatusResult.Unavailable unavailable) {
            throw Status.UNAVAILABLE
                    .withDescription("Serving observation unavailable: " + unavailable.diagnosticCode()
                            + ": " + sanitizeMessage(unavailable.diagnosticMessage()))
                    .asRuntimeException();
        }
        if (statusResult instanceof ExternalProviderGateway.ProviderStatusResult.NoRecordedOutcome none) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("Provider has no recorded outcome for confirmed operation '"
                            + op.getOperationId() + "': " + sanitizeMessage(none.diagnosticMessage()))
                    .asRuntimeException();
        }
        ExternalProviderGateway.ProviderStatusResult.Receipt receipt =
                (ExternalProviderGateway.ProviderStatusResult.Receipt) statusResult;
        // Every receipt is verified, including one that carries no serving identity: a
        // receipt that does not verify is not a successful refresh of anything.
        verifier.verifyRecordedEvidence(receipt.evidence(), op, outboundAuth);

        ExternalPublicationLedger updated = ledgers.updateLedgerWithCas(
                ledgerAddr,
                ledger.getSourceAddress(),
                ledger.getBindingId(),
                security,
                "ext-serving-obs:" + op.getOperationId() + ":" + UUID.randomUUID(),
                current -> {
                    if (!current.hasCurrentLink()
                            || !current.getCurrentLink()
                                    .getConfirmedByOperationId()
                                    .equals(op.getOperationId())) {
                        return current;
                    }
                    if (!current.getCurrentLink().hasManagedSearch()) {
                        // The link was confirmed without a managed-search identity, so there
                        // is no confirmed identity for a serving observation to belong to.
                        return current;
                    }
                    ExternalLink updatedLink = current.getCurrentLink().toBuilder()
                            .setManagedSearch(withServingObservation(
                                    current.getCurrentLink().getManagedSearch(),
                                    receipt.evidence()))
                            .build();
                    return current.toBuilder().setCurrentLink(updatedLink).build();
                });
        return new ExternalPublicationCoordinator.OperationResult(
                findOperation(updated, op.getOperationId()).orElse(op),
                optionalCurrentLink(updated),
                true,
                false);
    }

    /**
     * The confirmed identity with only its serving observation replaced. The refreshed receipt
     * must be a PUBLISHED one that names exactly the identity the link was confirmed with: an
     * accepted-only or otherwise different receipt would overwrite proof of publication with
     * something weaker, so it is refused.
     *
     * @throws io.grpc.StatusRuntimeException FAILED_PRECONDITION when the receipt is not
     *         PUBLISHED or its durable identity differs from {@code confirmed}
     */
    static ManagedSearchIdentity withServingObservation(
            ManagedSearchIdentity confirmed, ExternalProviderEvidence refreshed) {
        if (refreshed.getSettlement()
                != ExternalProviderSettlement.EXTERNAL_PROVIDER_SETTLEMENT_PUBLISHED) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("Serving observation requires PUBLISHED evidence; provider "
                            + "returned " + refreshed.getSettlement())
                    .asRuntimeException();
        }
        if (!refreshed.hasManagedSearch()) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("Serving observation carries no managed-search identity for a "
                            + "publication that was confirmed with one")
                    .asRuntimeException();
        }
        ManagedSearchIdentity observed = refreshed.getManagedSearch();
        ManagedSearchIdentity observedDurable = observed.toBuilder()
                .setServingVisibility(confirmed.getServingVisibility())
                .build();
        if (!observedDurable.equals(confirmed)) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("Serving observation names a different durable identity than "
                            + "the one the publication was confirmed with")
                    .asRuntimeException();
        }
        return confirmed.toBuilder()
                .setServingVisibility(observed.getServingVisibility())
                .build();
    }
}
