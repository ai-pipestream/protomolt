package ai.protomolt.proto.repo.accession.external;

import ai.protomolt.proto.receipt.RecordVerifier;
import ai.protomolt.proto.receipt.TrustSnapshot;
import ai.protomolt.proto.receipt.Verification;
import ai.protomolt.proto.repo.external.v1.ExternalProviderEvidence;
import ai.protomolt.proto.repo.external.v1.ExternalProviderSettlement;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationAction;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationIntent;
import ai.protomolt.proto.repo.external.v1.ExternalSystemBinding;
import ai.protomolt.proto.repo.external.v1.ManagedSearchIdentity;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.ValidationResult;
import ai.protomolt.proto.validate.protovalidate.ProtovalidateRuleSource;
import ai.protomolt.proto.validate.source.ProtomoltRuleSource;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Objects;

/**
 * Verifies {@link ExternalProviderEvidence} receipts against the expected
 * {@link ExternalSystemBinding}, live {@link ExternalProviderGateway.OutboundAuthContext},
 * canonical {@link ExternalPublicationIntent} digest, managed-search epoch rules,
 * and optional cryptographic {@code SignedWorkRecord} trust snapshots.
 *
 * <p>Publication confirmation ({@code CONFIRMED}) and deletion confirmation
 * ({@code DELETED}) are never accepted without passing {@link #verifyEvidence}.
 */
public final class ExternalProviderVerifier {

    private static final ProtoValidator VALIDATOR = ProtoValidator.create(
            List.of(new ProtomoltRuleSource(), new ProtovalidateRuleSource()));

    private final TrustSnapshot receiptTrustSnapshot;
    private final boolean requireSignedWorkRecord;

    /**
     * Creates a verifier that validates structural, binding, profile, credential-ref,
     * intent-digest, and managed-search invariants without requiring a signed work record
     * unless one is attached to the evidence.
     */
    public ExternalProviderVerifier() {
        this(null, false);
    }

    /**
     * Creates a verifier configured with an offline {@link TrustSnapshot} for
     * {@code SignedWorkRecord} cryptographic verification.
     *
     * @param receiptTrustSnapshot trust snapshot for {@link RecordVerifier}, or {@code null}
     *         when signed work records are not configured
     * @param requireSignedWorkRecord when {@code true}, every verified receipt must carry
     *         a valid {@code signed_work_record_bytes} envelope
     */
    public ExternalProviderVerifier(
            TrustSnapshot receiptTrustSnapshot, boolean requireSignedWorkRecord) {
        if (requireSignedWorkRecord && receiptTrustSnapshot == null) {
            throw new IllegalArgumentException(
                    "receiptTrustSnapshot is required when requireSignedWorkRecord is true");
        }
        this.receiptTrustSnapshot = receiptTrustSnapshot;
        this.requireSignedWorkRecord = requireSignedWorkRecord;
    }

    /**
     * Exception raised when provider evidence fails structural, identity, or signature checks.
     */
    public static final class ProviderVerificationException extends RuntimeException {
        private final String diagnosticCode;

        /**
         * Creates a verification refusal with a bounded machine code and message.
         *
         * @param diagnosticCode bounded diagnostic code
         * @param message human-readable verification refusal reason
         */
        public ProviderVerificationException(String diagnosticCode, String message) {
            super(message);
            this.diagnosticCode = Objects.requireNonNull(diagnosticCode, "diagnosticCode");
        }

        /**
         * Returns the bounded machine diagnostic code for this verification failure.
         *
         * @return diagnostic code
         */
        public String diagnosticCode() {
            return diagnosticCode;
        }
    }

    /**
     * Verifies {@code evidence} against the expected binding, live outbound profile/credential
     * context, and canonical intent. Throws {@link ProviderVerificationException} on any mismatch.
     *
     * @param evidence candidate provider receipt
     * @param expectedBinding binding configuration revision bound to the operation
     * @param expectedAuth live resolved profile, endpoint, and credential-ref digest
     * @param expectedIntent canonical semantic intent of the operation
     * @param expectedIntentSha256 lowercase hex SHA-256 of {@code expectedIntent}
     */
    public void verifyEvidence(
            ExternalProviderEvidence evidence,
            ExternalSystemBinding expectedBinding,
            ExternalProviderGateway.OutboundAuthContext expectedAuth,
            ExternalPublicationIntent expectedIntent,
            String expectedIntentSha256) {
        Objects.requireNonNull(evidence, "evidence");
        Objects.requireNonNull(expectedBinding, "expectedBinding");
        Objects.requireNonNull(expectedAuth, "expectedAuth");
        Objects.requireNonNull(expectedIntent, "expectedIntent");
        Objects.requireNonNull(expectedIntentSha256, "expectedIntentSha256");

        ValidationResult shape = VALIDATOR.validate(evidence);
        if (!shape.valid()) {
            throw new ProviderVerificationException(
                    "PROVIDER_EVIDENCE_MALFORMED",
                    "Provider evidence failed contract validation: " + shape.violations());
        }

        if (!evidence.getProvider().equals(expectedBinding.getProvider())) {
            throw new ProviderVerificationException(
                    "PROVIDER_MISMATCH",
                    "Evidence provider '" + evidence.getProvider()
                            + "' does not match binding provider '" + expectedBinding.getProvider() + "'");
        }
        if (!evidence.getContractVersion().equals(expectedBinding.getContractVersion())) {
            throw new ProviderVerificationException(
                    "CONTRACT_VERSION_MISMATCH",
                    "Evidence contract_version '" + evidence.getContractVersion()
                            + "' does not match binding contract_version '"
                            + expectedBinding.getContractVersion() + "'");
        }
        if (!evidence.getVerifiedServiceProfileId().equals(expectedAuth.serviceProfile().getName())) {
            throw new ProviderVerificationException(
                    "SERVICE_PROFILE_MISMATCH",
                    "Evidence verified_service_profile_id does not match resolved profile");
        }
        if (!evidence.getVerifiedServiceEndpointId().equals(expectedAuth.serviceEndpoint().getName())) {
            throw new ProviderVerificationException(
                    "SERVICE_ENDPOINT_MISMATCH",
                    "Evidence verified_service_endpoint_id does not match resolved endpoint");
        }
        if (!evidence.getCredentialBindingSha256().equals(expectedAuth.credentialBindingSha256())) {
            throw new ProviderVerificationException(
                    "CREDENTIAL_BINDING_MISMATCH",
                    "Evidence credential_binding_sha256 does not match resolved profile credential reference");
        }
        if (!evidence.getExternalObjectId().equals(expectedIntent.getExternalObjectId())) {
            throw new ProviderVerificationException(
                    "EXTERNAL_OBJECT_ID_MISMATCH",
                    "Evidence external_object_id '" + evidence.getExternalObjectId()
                            + "' does not match expected '" + expectedIntent.getExternalObjectId() + "'");
        }
        if (!evidence.getRequestIntentSha256().equals(expectedIntentSha256)) {
            throw new ProviderVerificationException(
                    "INTENT_DIGEST_MISMATCH",
                    "Evidence request_intent_sha256 does not match canonical operation intent SHA-256");
        }

        verifySettlementMatchesAction(evidence.getSettlement(), expectedIntent.getAction());

        if (evidence.hasManagedSearch()) {
            verifyManagedSearchIdentity(
                    evidence.getManagedSearch(), evidence.getSettlement(), expectedIntent);
        }

        if (requireSignedWorkRecord && !evidence.hasSignedWorkRecordBytes()) {
            throw new ProviderVerificationException(
                    "SIGNED_WORK_RECORD_REQUIRED",
                    "Provider evidence omitted required signed_work_record_bytes");
        }
        if (evidence.hasSignedWorkRecordBytes()) {
            if (receiptTrustSnapshot == null) {
                throw new ProviderVerificationException(
                        "TRUST_SNAPSHOT_MISSING",
                        "Provider evidence carries signed_work_record_bytes without a configured TrustSnapshot");
            }
            Verification verified = RecordVerifier.verify(
                    evidence.getSignedWorkRecordBytes().toByteArray(), receiptTrustSnapshot);
            if (!verified.verified()) {
                throw new ProviderVerificationException(
                        "SIGNED_WORK_RECORD_INVALID",
                        "SignedWorkRecord verification failed at checks: " + verified.checks());
            }
        }
    }

    private static void verifySettlementMatchesAction(
            ExternalProviderSettlement settlement, ExternalPublicationAction action) {
        if (action == ExternalPublicationAction.EXTERNAL_PUBLICATION_ACTION_PUBLISH) {
            if (settlement != ExternalProviderSettlement.EXTERNAL_PROVIDER_SETTLEMENT_ACCEPTED_ONLY
                    && settlement != ExternalProviderSettlement.EXTERNAL_PROVIDER_SETTLEMENT_PUBLISHED
                    && settlement != ExternalProviderSettlement.EXTERNAL_PROVIDER_SETTLEMENT_REJECTED) {
                throw new ProviderVerificationException(
                        "SETTLEMENT_ACTION_MISMATCH",
                        "Incompatible settlement " + settlement + " for PUBLISH intent");
            }
        } else if (action == ExternalPublicationAction.EXTERNAL_PUBLICATION_ACTION_DELETE) {
            if (settlement != ExternalProviderSettlement.EXTERNAL_PROVIDER_SETTLEMENT_DELETION_ACCEPTED
                    && settlement != ExternalProviderSettlement.EXTERNAL_PROVIDER_SETTLEMENT_DELETED
                    && settlement != ExternalProviderSettlement.EXTERNAL_PROVIDER_SETTLEMENT_REJECTED) {
                throw new ProviderVerificationException(
                        "SETTLEMENT_ACTION_MISMATCH",
                        "Incompatible settlement " + settlement + " for DELETE intent");
            }
        } else {
            throw new ProviderVerificationException(
                    "INVALID_ACTION", "Unsupported publication action: " + action);
        }
    }

    private static void verifyManagedSearchIdentity(
            ManagedSearchIdentity search,
            ExternalProviderSettlement settlement,
            ExternalPublicationIntent expectedIntent) {
        ByteString expectedDocKey = ByteString.copyFrom(
                ExternalPublicationCoordinates.logicalSourceKey(expectedIntent.getSourceAddress()));
        if (!search.getDocumentKey().equals(expectedDocKey)) {
            throw new ProviderVerificationException(
                    "MANAGED_SEARCH_DOCUMENT_KEY_MISMATCH",
                    "ManagedSearchIdentity.document_key does not match canonical repository NodeAddress tuple");
        }
        if (settlement == ExternalProviderSettlement.EXTERNAL_PROVIDER_SETTLEMENT_PUBLISHED) {
            if (!search.hasPublishedCatalogEpoch() || !search.hasAfterManifestSha256()) {
                throw new ProviderVerificationException(
                        "MANAGED_SEARCH_UNPUBLISHED",
                        "PUBLISHED managed-search evidence requires published_catalog_epoch and after_manifest_sha256");
            }
        }
        if (settlement == ExternalProviderSettlement.EXTERNAL_PROVIDER_SETTLEMENT_ACCEPTED_ONLY) {
            if (search.hasPublishedCatalogEpoch() || search.hasAfterManifestSha256()) {
                throw new ProviderVerificationException(
                        "MANAGED_SEARCH_PREMATURE_EPOCH",
                        "ACCEPTED_ONLY managed-search evidence must not claim published_catalog_epoch");
            }
        }
    }
}
