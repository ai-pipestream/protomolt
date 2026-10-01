package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.receipt.Completeness;
import ai.protomolt.proto.receipt.CompletenessStatus;
import ai.protomolt.proto.receipt.KeyState;
import ai.protomolt.proto.receipt.RecordSubject;
import ai.protomolt.proto.receipt.RecordVerifier;
import ai.protomolt.proto.receipt.TrustSnapshot;
import ai.protomolt.proto.receipt.WorkRecord;
import ai.protomolt.proto.receipt.WorkRecords;
import ai.protomolt.proto.workflow.RecordSigning;
import com.google.protobuf.Timestamp;
import java.time.Clock;
import java.util.Map;
import java.util.Objects;

/** Checks signing configuration without publishing evidence or invoking a service. */
final class WorkflowPreparationSigning {
    private WorkflowPreparationSigning() {}

    static void verify(RecordSigning signing, TrustSnapshot trust, Clock clock) {
        Objects.requireNonNull(clock, "clock");
        try {
            if (signing == null || trust == null) throw new IllegalArgumentException();
            WorkflowPreparationValidation.validate(trust, WorkflowPreparationValidation.RESPONSE_MAX);
            boolean active = trust.getIssuersList().stream()
                    .filter(issuer -> issuer.getIssuer().equals(signing.issuer()))
                    .flatMap(issuer -> issuer.getKeysList().stream())
                    .anyMatch(key -> key.getKeyId().equals(signing.signer().keyId())
                            && key.getState() == KeyState.KEY_STATE_ACTIVE);
            if (!active) throw new IllegalArgumentException();
            var now = clock.instant();
            String emptyDigest = WorkRecords.sha256Hex(new byte[0]);
            // A local signature challenge only. It is neither returned nor persisted as
            // evidence, and contains no task, source, provider output or artifact references.
            var challenge = WorkRecord.newBuilder().setManifestVersion(1)
                    .setRecordId("preparation-signing-check")
                    .setIssuer(signing.issuer()).setKeyId(signing.signer().keyId())
                    .setIssuedAt(Timestamp.newBuilder().setSeconds(now.getEpochSecond()).setNanos(now.getNano()))
                    .setSubject(RecordSubject.newBuilder().setKind("workflow-run")
                            .setWorkflowName("signing-check").setRunId("signing-check")
                            .setWorkflowFingerprint(emptyDigest))
                    .setCompleteness(Completeness.newBuilder()
                            .setStatus(CompletenessStatus.COMPLETENESS_STATUS_PARTIAL)
                            .addMissingReasons("Local signing check; no workflow was executed")
                            .setPolicyId("signing-check").setPolicyVersion("1")
                            .setPolicySha256(emptyDigest)).build();
            var signed = signing.signer().sign(challenge);
            if (!RecordVerifier.verify(signed.toByteArray(), trust, Map.of()).verified()) {
                throw new IllegalArgumentException();
            }
        } catch (RuntimeException failure) {
            throw new IllegalArgumentException("workflow preparation signing identity is not trusted");
        }
    }
}
