package ai.protomolt.proto.repo.accession.external;

import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.findOperation;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.optionalCurrentLink;
import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.sanitizeMessage;

import ai.protomolt.proto.repo.external.v1.ExternalLink;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationLedger;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationOperationRecord;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.DocumentSecurity;
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
            DocumentSecurity security,
            ExternalPublicationLedger ledger,
            ExternalPublicationOperationRecord op) {
        ExternalProviderGateway.OutboundAuthContext outboundAuth =
                bindingStore.resolveOutboundAuth(op.getBindingSnapshot());
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
        if (!receipt.evidence().hasManagedSearch()) {
            // The provider answered and reports no serving-layer identity: nothing to refresh.
            return new ExternalPublicationCoordinator.OperationResult(op, optionalCurrentLink(ledger), true, false);
        }
        verifier.verifyEvidence(
                receipt.evidence(),
                op.getBindingSnapshot(),
                outboundAuth,
                op.getIntent(),
                op.getIntentSha256());

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
                    ExternalLink updatedLink = current.getCurrentLink().toBuilder()
                            .setManagedSearch(receipt.evidence().getManagedSearch())
                            .build();
                    return current.toBuilder().setCurrentLink(updatedLink).build();
                });
        return new ExternalPublicationCoordinator.OperationResult(
                findOperation(updated, op.getOperationId()).orElse(op),
                optionalCurrentLink(updated),
                true,
                false);
    }
}
