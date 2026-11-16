package ai.protomolt.proto.repo.accession.external;

import ai.protomolt.proto.receipt.Completeness;
import ai.protomolt.proto.receipt.CompletenessStatus;
import ai.protomolt.proto.receipt.RecordArtifact;
import ai.protomolt.proto.receipt.RecordSubject;
import ai.protomolt.proto.receipt.RecordVerifier;
import ai.protomolt.proto.receipt.TrustSnapshot;
import ai.protomolt.proto.receipt.Verification;
import ai.protomolt.proto.receipt.WorkRecord;
import ai.protomolt.proto.receipt.WorkRecords;
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
import com.google.protobuf.Timestamp;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Verifies {@link ExternalProviderEvidence} receipts against the expected
 * {@link ExternalSystemBinding}, live {@link ExternalProviderGateway.OutboundAuthContext},
 * canonical {@link ExternalPublicationIntent} digest, managed-search epoch rules,
 * and optional cryptographic {@code SignedWorkRecord} trust snapshots.
 *
 * <p>When a {@code SignedWorkRecord} is present (or required by {@code requireSignedWorkRecord}),
 * verification authenticates the Ed25519 container against {@link TrustSnapshot} via
 * {@link RecordVerifier} and then cryptographically binds the verified {@link WorkRecord}
 * manifest to the exact publication settlement:
 * <ul>
 *   <li>{@code subject.kind} must be {@code "workflow-run"} and
 *       {@code subject.workflow_fingerprint} must equal {@code expectedIntentSha256}.</li>
 *   <li>{@code completeness.status} must be {@link CompletenessStatus#COMPLETENESS_STATUS_COMPLETE}.</li>
 *   <li>{@code artifacts} must contain an unredacted {@link #SETTLEMENT_CLAIM_MEDIA_TYPE}
 *       {@link RecordArtifact} whose SHA-256 and byte length match
 *       {@link #canonicalSettlementClaimBytes(ExternalProviderEvidence, ExternalSystemBinding, String)},
 *       binding account, binding ID and revision, provider/dataset/namespace/contract version,
 *       verified service profile/endpoint/credential-ref digest, canonical intent SHA-256,
 *       external object ID and version, provider receipt ID, settlement stage, diagnostic code,
 *       and durable managed-search history/sequence/catalog-epoch identities.</li>
 * </ul>
 *
 * <p>Publication confirmation ({@code CONFIRMED}) and deletion confirmation
 * ({@code DELETED}) are never accepted without passing {@link #verifyEvidence}.
 */
public final class ExternalProviderVerifier {

    /**
     * Media type identifying the canonical external-publication settlement claim
     * {@link RecordArtifact} required inside a signed {@link WorkRecord} manifest.
     */
    public static final String SETTLEMENT_CLAIM_MEDIA_TYPE =
            "application/vnd.protomolt.external-settlement-claim.v1+octet-stream";

    private static final byte SETTLEMENT_CLAIM_FORMAT_VERSION = 1;

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
     *         a valid {@code signed_work_record_bytes} envelope bound to the intent and settlement
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
     * Issuance metadata for constructing a publication-bound {@link WorkRecord} manifest.
     *
     * @param recordId slug record identifier unique per issuer
     * @param issuer trusted issuer slug
     * @param keyId signing key slug under {@code issuer}
     * @param workflowName slug naming the provider publication workflow
     * @param runId slug naming the provider execution run
     * @param issuedAt claimed issuance timestamp
     * @param completeness complete evidence coverage statement
     */
    public record WorkRecordIssuance(
            String recordId,
            String issuer,
            String keyId,
            String workflowName,
            String runId,
            Timestamp issuedAt,
            Completeness completeness) {

        /** Validates non-null, non-blank issuance metadata. */
        public WorkRecordIssuance {
            requireNonBlank(recordId, "recordId");
            requireNonBlank(issuer, "issuer");
            requireNonBlank(keyId, "keyId");
            requireNonBlank(workflowName, "workflowName");
            requireNonBlank(runId, "runId");
            Objects.requireNonNull(issuedAt, "issuedAt");
            Objects.requireNonNull(completeness, "completeness");
        }

        private static void requireNonBlank(String value, String name) {
            Objects.requireNonNull(value, name);
            if (value.isBlank()) {
                throw new IllegalArgumentException(name + " must not be blank");
            }
        }
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
     * Encodes the canonical deterministic settlement claim bytes binding an
     * {@link ExternalProviderEvidence} envelope to its {@link ExternalSystemBinding}
     * configuration and canonical {@link ExternalPublicationIntent} SHA-256 digest.
     *
     * <p>Advisory point-in-time {@code serving_visibility}, {@code signed_work_record_bytes},
     * {@code receipt_sha256}, and coordinator-stamped {@code verified_at} are excluded so a
     * durable publication receipt signed at commit time remains verifiable across subsequent
     * read-only serving-visibility observations while locking every durable identity, version,
     * binding, and settlement field.
     *
     * @param evidence provider evidence envelope with durable settlement fields populated
     * @param binding exact external-system binding revision for the operation
     * @param expectedIntentSha256 lowercase 64-character hex SHA-256 of the canonical intent
     * @return canonical length-delimited settlement claim bytes
     */
    public static byte[] canonicalSettlementClaimBytes(
            ExternalProviderEvidence evidence,
            ExternalSystemBinding binding,
            String expectedIntentSha256) {
        Objects.requireNonNull(evidence, "evidence");
        Objects.requireNonNull(binding, "binding");
        Objects.requireNonNull(expectedIntentSha256, "expectedIntentSha256");
        try {
            ByteArrayOutputStream raw = new ByteArrayOutputStream(256);
            DataOutputStream out = new DataOutputStream(raw);
            out.writeByte(SETTLEMENT_CLAIM_FORMAT_VERSION);
            writeUtf8(out, binding.getAccountId());
            writeUtf8(out, binding.getBindingId());
            out.writeLong(binding.getBindingRevision());
            writeUtf8(out, binding.getProvider());
            writeUtf8(out, binding.getDataset());
            writeUtf8(out, binding.getNamespace());
            writeUtf8(out, binding.getContractVersion());
            writeUtf8(out, evidence.getVerifiedServiceProfileId());
            writeUtf8(out, evidence.getVerifiedServiceEndpointId());
            writeUtf8(out, evidence.getCredentialBindingSha256());
            writeUtf8(out, expectedIntentSha256);
            writeUtf8(out, evidence.getExternalObjectId());
            writeOptionalUtf8(out, evidence.hasExternalVersion(), evidence.getExternalVersion());
            writeUtf8(out, evidence.getProviderReceiptId());
            out.writeInt(evidence.getSettlementValue());
            writeOptionalUtf8(out, evidence.hasDiagnosticCode(), evidence.getDiagnosticCode());
            if (evidence.hasManagedSearch()) {
                out.writeBoolean(true);
                encodeDurableManagedSearchClaim(out, evidence.getManagedSearch());
            } else {
                out.writeBoolean(false);
            }
            out.flush();
            return raw.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException("In-memory settlement claim encoding failed", impossible);
        }
    }

    /**
     * Builds the required unredacted {@link RecordArtifact} committing to
     * {@link #canonicalSettlementClaimBytes(ExternalProviderEvidence, ExternalSystemBinding, String)}.
     *
     * @param evidence provider evidence envelope being attested
     * @param binding exact external-system binding revision for the operation
     * @param expectedIntentSha256 lowercase 64-character hex SHA-256 of the canonical intent
     * @return {@link RecordArtifact} with media type {@link #SETTLEMENT_CLAIM_MEDIA_TYPE}
     */
    public static RecordArtifact settlementClaimArtifact(
            ExternalProviderEvidence evidence,
            ExternalSystemBinding binding,
            String expectedIntentSha256) {
        byte[] claimBytes = canonicalSettlementClaimBytes(evidence, binding, expectedIntentSha256);
        return RecordArtifact.newBuilder()
                .setSha256(ExternalOperationKeys.sha256Hex(claimBytes))
                .setMediaType(SETTLEMENT_CLAIM_MEDIA_TYPE)
                .setSizeBytes(claimBytes.length)
                .setRedacted(false)
                .build();
    }

    /**
     * Constructs a canonical {@link WorkRecord} manifest bound to {@code evidence},
     * {@code binding}, and {@code expectedIntentSha256}, ready to be signed with
     * {@link ai.protomolt.proto.receipt.RecordSigner#sign(WorkRecord)}.
     *
     * @param issuance record identity, issuer, signing key, workflow/run slugs, and completeness
     * @param evidence provider evidence envelope being attested
     * @param binding exact external-system binding revision for the operation
     * @param expectedIntentSha256 lowercase 64-character hex SHA-256 of the canonical intent
     * @return canonical {@link WorkRecord} carrying the bound subject fingerprint and claim artifact
     */
    public static WorkRecord buildBoundWorkRecord(
            WorkRecordIssuance issuance,
            ExternalProviderEvidence evidence,
            ExternalSystemBinding binding,
            String expectedIntentSha256) {
        Objects.requireNonNull(issuance, "issuance");
        RecordArtifact claimArtifact =
                settlementClaimArtifact(evidence, binding, expectedIntentSha256);
        return WorkRecord.newBuilder()
                .setManifestVersion(WorkRecords.MANIFEST_VERSION)
                .setRecordId(issuance.recordId())
                .setIssuer(issuance.issuer())
                .setKeyId(issuance.keyId())
                .setIssuedAt(issuance.issuedAt())
                .setSubject(RecordSubject.newBuilder()
                        .setKind(WorkRecords.SUBJECT_KIND_WORKFLOW_RUN)
                        .setWorkflowName(issuance.workflowName())
                        .setWorkflowFingerprint(expectedIntentSha256)
                        .setRunId(issuance.runId()))
                .addArtifacts(claimArtifact)
                .setCompleteness(issuance.completeness())
                .build();
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

        verifyEnvelopeIdentity(
                evidence, expectedBinding, expectedAuth, expectedIntent, expectedIntentSha256);
        verifySettlementMatchesAction(evidence.getSettlement(), expectedIntent.getAction());

        requireDeclaredIdentityProof(evidence, expectedBinding);
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
            verifySignedWorkRecord(evidence, expectedBinding, expectedIntentSha256);
        }
    }

    private static void verifyEnvelopeIdentity(
            ExternalProviderEvidence evidence,
            ExternalSystemBinding expectedBinding,
            ExternalProviderGateway.OutboundAuthContext expectedAuth,
            ExternalPublicationIntent expectedIntent,
            String expectedIntentSha256) {
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
    }

    private void verifySignedWorkRecord(
            ExternalProviderEvidence evidence,
            ExternalSystemBinding expectedBinding,
            String expectedIntentSha256) {
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
        WorkRecord manifest = verified.manifest();
        if (manifest == null) {
            throw new ProviderVerificationException(
                    "SIGNED_WORK_RECORD_INVALID",
                    "Verified SignedWorkRecord did not expose a parsed WorkRecord manifest");
        }
        verifySignedManifestBinding(manifest, evidence, expectedBinding, expectedIntentSha256);
    }

    private static void verifySignedManifestBinding(
            WorkRecord manifest,
            ExternalProviderEvidence evidence,
            ExternalSystemBinding expectedBinding,
            String expectedIntentSha256) {
        if (manifest.getCompleteness().getStatus() != CompletenessStatus.COMPLETENESS_STATUS_COMPLETE) {
            throw new ProviderVerificationException(
                    "SIGNED_WORK_RECORD_INCOMPLETE",
                    "SignedWorkRecord completeness must be COMPLETE, got "
                            + manifest.getCompleteness().getStatus());
        }
        if (!WorkRecords.SUBJECT_KIND_WORKFLOW_RUN.equals(manifest.getSubject().getKind())) {
            throw new ProviderVerificationException(
                    "SIGNED_WORK_RECORD_SUBJECT_MISMATCH",
                    "SignedWorkRecord subject.kind must be '"
                            + WorkRecords.SUBJECT_KIND_WORKFLOW_RUN
                            + "', got '" + manifest.getSubject().getKind() + "'");
        }
        if (!expectedIntentSha256.equals(manifest.getSubject().getWorkflowFingerprint())) {
            throw new ProviderVerificationException(
                    "SIGNED_WORK_RECORD_INTENT_MISMATCH",
                    "SignedWorkRecord subject.workflow_fingerprint does not match expected intent SHA-256");
        }

        List<RecordArtifact> claimArtifacts = new ArrayList<>();
        for (RecordArtifact artifact : manifest.getArtifactsList()) {
            if (SETTLEMENT_CLAIM_MEDIA_TYPE.equals(artifact.getMediaType())) {
                claimArtifacts.add(artifact);
            }
        }
        if (claimArtifacts.size() != 1) {
            throw new ProviderVerificationException(
                    "SIGNED_WORK_RECORD_CLAIM_MISSING",
                    "SignedWorkRecord must contain exactly 1 '"
                            + SETTLEMENT_CLAIM_MEDIA_TYPE
                            + "' settlement claim artifact, found " + claimArtifacts.size());
        }

        RecordArtifact claimArtifact = claimArtifacts.getFirst();
        byte[] expectedClaimBytes =
                canonicalSettlementClaimBytes(evidence, expectedBinding, expectedIntentSha256);
        String expectedClaimSha256 = ExternalOperationKeys.sha256Hex(expectedClaimBytes);
        if (claimArtifact.getRedacted()
                || claimArtifact.getSizeBytes() != expectedClaimBytes.length
                || !claimArtifact.getSha256().equals(expectedClaimSha256)) {
            throw new ProviderVerificationException(
                    "SIGNED_WORK_RECORD_CLAIM_MISMATCH",
                    "SignedWorkRecord settlement claim artifact does not match expected binding, "
                            + "external object, intent digest, or settlement outcome");
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

    /**
     * A binding states what a publish receipt must prove. Evidence that only says the provider
     * accepted or published the object, without the proof the binding asks for, confirms
     * nothing.
     */
    private static void requireDeclaredIdentityProof(
            ExternalProviderEvidence evidence, ExternalSystemBinding expectedBinding) {
        boolean publishSettlement = evidence.getSettlement()
                        == ExternalProviderSettlement.EXTERNAL_PROVIDER_SETTLEMENT_ACCEPTED_ONLY
                || evidence.getSettlement()
                        == ExternalProviderSettlement.EXTERNAL_PROVIDER_SETTLEMENT_PUBLISHED;
        if (!publishSettlement) {
            return;
        }
        switch (expectedBinding.getRequiredIdentityProof()) {
            case EXTERNAL_IDENTITY_PROOF_RECEIPT -> {
                // The envelope checks already made are the whole proof.
            }
            case EXTERNAL_IDENTITY_PROOF_MANAGED_SEARCH -> {
                if (!evidence.hasManagedSearch()) {
                    throw new ProviderVerificationException(
                            "MANAGED_SEARCH_IDENTITY_REQUIRED",
                            "Binding '" + expectedBinding.getBindingId() + "' revision "
                                    + expectedBinding.getBindingRevision()
                                    + " requires ManagedSearchIdentity on publish evidence");
                }
            }
            case EXTERNAL_IDENTITY_PROOF_UNSPECIFIED, UNRECOGNIZED ->
                    throw new ProviderVerificationException(
                            "BINDING_IDENTITY_PROOF_UNDECLARED",
                            "Binding '" + expectedBinding.getBindingId() + "' revision "
                                    + expectedBinding.getBindingRevision()
                                    + " does not declare required_identity_proof");
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

    private static void encodeDurableManagedSearchClaim(
            DataOutputStream out, ManagedSearchIdentity search) throws IOException {
        writeBytes(out, search.getHistoryId());
        writeBytes(out, search.getDocumentKey());
        writeBytes(out, search.getIndexKey());
        out.writeLong(search.getAcceptedVersion());
        out.writeLong(search.getAcceptedSequence());
        out.writeBoolean(search.hasPublishedCatalogEpoch());
        if (search.hasPublishedCatalogEpoch()) {
            out.writeLong(search.getPublishedCatalogEpoch());
        }
        writeOptionalUtf8(out, search.hasAfterManifestSha256(), search.getAfterManifestSha256());
    }

    private static void writeBytes(DataOutputStream out, ByteString bytes) throws IOException {
        byte[] raw = bytes.toByteArray();
        out.writeInt(raw.length);
        out.write(raw);
    }

    private static void writeOptionalUtf8(
            DataOutputStream out, boolean present, String value) throws IOException {
        out.writeBoolean(present);
        if (present) {
            writeUtf8(out, value);
        }
    }

    private static void writeUtf8(DataOutputStream out, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }
}
