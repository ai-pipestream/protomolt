package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.mesh.runtime.v1.DeadLetterRecord;
import ai.protomolt.proto.mesh.runtime.v1.PayloadIdentity;
import ai.protomolt.proto.mesh.runtime.v1.ReconcileRecoveryRequest;
import ai.protomolt.proto.mesh.runtime.v1.ReconcileRecoveryResponse;
import ai.protomolt.proto.mesh.runtime.v1.ReconciliationClassification;
import ai.protomolt.proto.mesh.runtime.v1.ReconciliationFinding;

import java.util.Map;
import java.util.Objects;

/** Report-first reconciliation between durable dead letters and payload ownership. */
public final class PayloadRecoveryReconciler
        implements RecoveryGrpcService.RecoveryReconciler,
        RecoveryGrpcService.DeadLetterPayloadControl {

    private final DurableProcessorChannel channel;
    private final Map<String, PayloadStore> stores;

    public PayloadRecoveryReconciler(
            DurableProcessorChannel channel, Map<String, PayloadStore> stores) {
        this.channel = Objects.requireNonNull(channel, "channel");
        this.stores = Map.copyOf(stores);
    }

    @Override
    public ReconcileRecoveryResponse reconcile(
            ReconcileRecoveryRequest request, int limit) {
        ReconcileRecoveryResponse.Builder response = ReconcileRecoveryResponse.newBuilder();
        long cursor = 0;
        while (response.getFindingsCount() < limit) {
            int pageLimit = Math.min(1_000, limit - response.getFindingsCount());
            DurableProcessorChannel.DeadLetterPage page = channel.deadLetters(
                    request.getNamespace(), cursor, pageLimit);
            for (DurableProcessorChannel.DeadLetterPage.Entry entry : page.entries()) {
                response.addFindings(finding(entry.record(), request));
            }
            if (page.entries().isEmpty() || page.nextSequence() <= cursor) {
                break;
            }
            cursor = page.nextSequence();
        }
        return response.build();
    }

    @Override
    public void retain(DeadLetterRecord record, boolean retained) {
        if (!record.getInput().hasClaimCheck()) {
            return;
        }
        PayloadStore store = stores.get(record.getPayloadStoreProfile());
        if (store == null) {
            throw new IllegalStateException("dead-letter-payload-store-unavailable: profile "
                    + record.getPayloadStoreProfile());
        }
        store.hold(identity(record), retained);
    }

    private ReconciliationFinding finding(
            DeadLetterRecord record, ReconcileRecoveryRequest request) {
        ReconciliationFinding.Builder finding = ReconciliationFinding.newBuilder()
                .setStableId(record.getDeadLetterId());
        if (!record.getInput().hasClaimCheck()) {
            return finding.setClassification(ReconciliationClassification
                            .RECONCILIATION_CLASSIFICATION_CONSISTENT)
                    .setEvidence("dead letter retains an inline protobuf payload")
                    .build();
        }
        PayloadStore store = stores.get(record.getPayloadStoreProfile());
        if (store == null) {
            return unknown(finding, "payload profile is unavailable: "
                    + record.getPayloadStoreProfile());
        }
        try {
            var metadata = store.head(identity(record));
            if (metadata.getPurged()) {
                return finding.setClassification(ReconciliationClassification
                                .RECONCILIATION_CLASSIFICATION_MISSING)
                        .setEvidence("payload ledger records physical purge")
                        .build();
            }
            if (record.getRetentionHold() && !metadata.getRetentionHold()) {
                if (request.getRepair() && repairEligible(record, request)) {
                    store.hold(identity(record), true);
                    return finding.setClassification(ReconciliationClassification
                                    .RECONCILIATION_CLASSIFICATION_CONSISTENT)
                            .setEvidence("restored dead-letter retention hold")
                            .setRepaired(true)
                            .build();
                }
                if (request.getRepair()) {
                    return unknown(finding,
                            "repair age guard excludes a dead letter newer than the cutoff");
                }
                return unknown(finding,
                        "dead-letter retention hold is absent from payload ledger");
            }
            if (!record.getRetentionHold() && metadata.getRetentionHold()) {
                return unknown(finding,
                        "payload has a hold that is not owned by this dead letter");
            }
            return finding.setClassification(ReconciliationClassification
                            .RECONCILIATION_CLASSIFICATION_CONSISTENT)
                    .setEvidence("payload identity and retention evidence are available")
                    .build();
        } catch (IllegalArgumentException missing) {
            if (missing.getMessage() != null
                    && missing.getMessage().startsWith("payload-missing:")) {
                return finding.setClassification(ReconciliationClassification
                                .RECONCILIATION_CLASSIFICATION_MISSING)
                        .setEvidence(missing.getMessage())
                        .build();
            }
            return unknown(finding, bounded(missing.getMessage()));
        } catch (RuntimeException unavailable) {
            return unknown(finding, "payload store unavailable: "
                    + bounded(unavailable.getMessage()));
        }
    }

    private static boolean repairEligible(
            DeadLetterRecord record, ReconcileRecoveryRequest request) {
        return request.hasNotBefore()
                && !RemoteValidation.instant(record.getLastFailureAt()).isAfter(
                RemoteValidation.instant(request.getNotBefore()));
    }

    private static ReconciliationFinding unknown(
            ReconciliationFinding.Builder finding, String evidence) {
        return finding.setClassification(ReconciliationClassification
                        .RECONCILIATION_CLASSIFICATION_UNKNOWN)
                .setEvidence(evidence)
                .build();
    }

    private static PayloadIdentity identity(DeadLetterRecord record) {
        var claim = record.getInput().getClaimCheck();
        if (record.getPayloadStoreProfile().isBlank()) {
            throw new IllegalStateException(
                    "dead-letter-payload-profile-missing");
        }
        return PayloadIdentity.newBuilder()
                .setNamespace(claim.getPayloadNamespace().isBlank()
                        ? record.getNamespace() : claim.getPayloadNamespace())
                .setProfile(record.getPayloadStoreProfile())
                .setArtifact(claim.getArtifact())
                .setPayloadTypeName(claim.getPayloadTypeName())
                .setDescriptorFingerprint(claim.getDescriptorFingerprint())
                .build();
    }

    private static String bounded(String value) {
        String text = value == null ? "reconciliation failed" : value;
        return text.substring(0, Math.min(8_192, text.length()));
    }
}
