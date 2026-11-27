package ai.protomolt.proto.repo.accession.external;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.grpc.profile.v1.ServiceEndpoint;
import ai.protomolt.proto.grpc.profile.v1.ServiceProfile;
import ai.protomolt.proto.grpc.profile.v1.Transport;
import ai.protomolt.proto.receipt.Completeness;
import ai.protomolt.proto.receipt.CompletenessStatus;
import ai.protomolt.proto.receipt.Disclosure;
import ai.protomolt.proto.receipt.KeyState;
import ai.protomolt.proto.receipt.RecordKeys;
import ai.protomolt.proto.receipt.RecordSigner;
import ai.protomolt.proto.receipt.RecordSubject;
import ai.protomolt.proto.receipt.SignatureAlgorithm;
import ai.protomolt.proto.receipt.SignedWorkRecord;
import ai.protomolt.proto.receipt.TrustSnapshot;
import ai.protomolt.proto.receipt.TrustedIssuer;
import ai.protomolt.proto.receipt.TrustedKey;
import ai.protomolt.proto.receipt.WorkRecord;
import ai.protomolt.proto.receipt.WorkRecords;
import ai.protomolt.proto.repo.external.v1.ExternalBindingLifecycle;
import ai.protomolt.proto.repo.external.v1.ExternalEndpointSummary;
import ai.protomolt.proto.repo.external.v1.ExternalIdentityProof;
import ai.protomolt.proto.repo.external.v1.ExternalProviderEvidence;
import ai.protomolt.proto.repo.external.v1.ExternalProviderSettlement;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationAction;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationIntent;
import ai.protomolt.proto.repo.external.v1.ExternalRetentionPolicy;
import ai.protomolt.proto.repo.external.v1.ExternalServingVisibility;
import ai.protomolt.proto.repo.external.v1.ExternalSystemBinding;
import ai.protomolt.proto.repo.external.v1.ManagedSearchIdentity;
import ai.protomolt.proto.repo.v1.NodeAddress;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.grpc.Metadata;
import java.security.KeyPair;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ExternalProviderVerifier}: binding, profile, credential-ref,
 * intent-digest, managed-search epoch, and Ed25519 {@link SignedWorkRecord} manifest
 * binding verification.
 */
class ExternalProviderVerifierTest {

    private static final String CRED_SHA = "1".repeat(64);
    private static final String MANIFEST_SHA = "2".repeat(64);
    private static final Timestamp FIXED_TIME = Timestamp.newBuilder().setSeconds(1_770_000_000L).build();

    private static NodeAddress sourceAddress() {
        return NodeAddress.newBuilder()
                .setAccountId("tenant-alpha")
                .setGraphId("intake:tenant-alpha")
                .setGraphAddressId("ds-1")
                .setDocId("doc-7")
                .build();
    }

    private static ExternalSystemBinding binding() {
        return ExternalSystemBinding.newBuilder()
                .setBindingId("rust-search")
                .setAccountId("tenant-alpha")
                .setBindingRevision(1L)
                .setProvider("protomolt-managed-search")
                .setServiceProfileId("search-cluster-a")
                .setServiceEndpointId("ingest-grpc")
                .setDataset("corpus-main")
                .setNamespace("ns-alpha")
                .setContractVersion("managed-search.v1")
                .setEndpointSummary(ExternalEndpointSummary.newBuilder()
                        .setHost("search.internal")
                        .setPort(9443)
                        .setTlsEnabled(true))
                .setLifecycle(ExternalBindingLifecycle.EXTERNAL_BINDING_LIFECYCLE_ACTIVE)
                .setRetentionPolicy(ExternalRetentionPolicy.EXTERNAL_RETENTION_POLICY_RETAIN_HISTORY)
                .setRequiredIdentityProof(
                        ExternalIdentityProof.EXTERNAL_IDENTITY_PROOF_MANAGED_SEARCH)
                .setCreatedAt(FIXED_TIME)
                .build();
    }

    private static ExternalProviderGateway.OutboundAuthContext authContext(ExternalSystemBinding binding) {
        ServiceEndpoint endpoint = ServiceEndpoint.newBuilder()
                .setName("ingest-grpc")
                .setHost("search.internal")
                .setPort(9443)
                .setTransport(Transport.TRANSPORT_TLS)
                .setCredentialRef("cred-search-a")
                .build();
        ServiceProfile profile = ServiceProfile.newBuilder()
                .setName("search-cluster-a")
                .addEndpoints(endpoint)
                .build();
        return new ExternalProviderGateway.OutboundAuthContext(
                binding, profile, endpoint, new Metadata(), CRED_SHA);
    }

    private static ExternalPublicationIntent publishIntent() {
        return ExternalPublicationIntent.newBuilder()
                .setEncodingVersion(1)
                .setAccountId("tenant-alpha")
                .setSourceAddress(sourceAddress())
                .setSourceRevision(5L)
                .setBindingId("rust-search")
                .setBindingRevision(1L)
                .setAction(ExternalPublicationAction.EXTERNAL_PUBLICATION_ACTION_PUBLISH)
                .setExternalObjectId("search-doc-7")
                .build();
    }

    private static ExternalProviderEvidence validPublishedEvidence(
            ExternalPublicationIntent intent, String intentSha256) {
        ByteString docKey = ByteString.copyFrom(
                ExternalPublicationCoordinates.logicalSourceKey(intent.getSourceAddress()));
        ByteString historyId = ByteString.copyFrom(new byte[16]);
        return ExternalProviderEvidence.newBuilder()
                .setProvider("protomolt-managed-search")
                .setContractVersion("managed-search.v1")
                .setProviderReceiptId("rcpt-9001")
                .setReceiptSha256("5".repeat(64))
                .setSettlement(ExternalProviderSettlement.EXTERNAL_PROVIDER_SETTLEMENT_PUBLISHED)
                .setExternalObjectId(intent.getExternalObjectId())
                .setExternalVersion("epoch-12")
                .setRequestIntentSha256(intentSha256)
                .setVerifiedServiceProfileId("search-cluster-a")
                .setVerifiedServiceEndpointId("ingest-grpc")
                .setCredentialBindingSha256(CRED_SHA)
                .setVerifiedAt(FIXED_TIME)
                .setManagedSearch(ManagedSearchIdentity.newBuilder()
                        .setHistoryId(historyId)
                        .setDocumentKey(docKey)
                        .setIndexKey(ByteString.copyFromUtf8("idx-main"))
                        .setAcceptedVersion(5L)
                        .setAcceptedSequence(41L)
                        .setPublishedCatalogEpoch(12L)
                        .setAfterManifestSha256(MANIFEST_SHA)
                        .setServingVisibility(
                                ExternalServingVisibility.EXTERNAL_SERVING_VISIBILITY_UNKNOWN))
                .build();
    }

    private static Completeness completeCoverage() {
        return Completeness.newBuilder()
                .setStatus(CompletenessStatus.COMPLETENESS_STATUS_COMPLETE)
                .setPolicyId("external-publication-policy")
                .setPolicyVersion("1.0")
                .setPolicySha256("4".repeat(64))
                .build();
    }

    private static ExternalProviderVerifier.WorkRecordIssuance defaultIssuance(String keyId) {
        return new ExternalProviderVerifier.WorkRecordIssuance(
                "pub-rcpt-9001",
                "search-coordinator",
                keyId,
                "external-search-publish",
                "run-9001",
                FIXED_TIME,
                completeCoverage());
    }

    private static TrustSnapshot trustSnapshotFor(KeyPair trustedPair) {
        return TrustSnapshot.newBuilder()
                .addIssuers(TrustedIssuer.newBuilder()
                        .setIssuer("search-coordinator")
                        .addSubjectKinds("workflow-run")
                        .addKeys(TrustedKey.newBuilder()
                                .setKeyId("search-key-1")
                                .setAlgorithm(SignatureAlgorithm.SIGNATURE_ALGORITHM_ED25519)
                                .setPublicKey(ByteString.copyFrom(
                                        RecordKeys.rawPublicKey(trustedPair.getPublic())))
                                .setState(KeyState.KEY_STATE_ACTIVE)))
                .build();
    }

    @Test
    void verifiesPublishedEvidenceWithSignedWorkRecordAndRefusesUntrustedSignatures() {
        KeyPair trustedPair = RecordKeys.generate();
        RecordSigner trustedSigner = new RecordSigner("search-key-1", trustedPair.getPrivate());
        TrustSnapshot trust = trustSnapshotFor(trustedPair);

        ExternalSystemBinding binding = binding();
        ExternalProviderGateway.OutboundAuthContext auth = authContext(binding);
        ExternalPublicationIntent intent = publishIntent();
        String intentSha = ExternalOperationKeys.intentSha256(intent);
        ExternalProviderEvidence unsignedEvidence = validPublishedEvidence(intent, intentSha);

        WorkRecord workRecord = ExternalProviderVerifier.buildBoundWorkRecord(
                defaultIssuance("search-key-1"), unsignedEvidence, binding, intentSha);
        SignedWorkRecord signed = trustedSigner.sign(workRecord);

        ExternalProviderVerifier strictVerifier = new ExternalProviderVerifier(trust, true);
        assertThatThrownBy(() -> strictVerifier.verifyEvidence(
                        unsignedEvidence, binding, auth, intent, intentSha))
                .isInstanceOfSatisfying(
                        ExternalProviderVerifier.ProviderVerificationException.class,
                        ex -> assertThat(ex.diagnosticCode()).isEqualTo("SIGNED_WORK_RECORD_REQUIRED"));

        ExternalProviderEvidence signedEvidence = unsignedEvidence.toBuilder()
                .setSignedWorkRecordBytes(signed.toByteString())
                .build();
        assertThatCode(() -> strictVerifier.verifyEvidence(
                        signedEvidence, binding, auth, intent, intentSha))
                .doesNotThrowAnyException();

        // Advisory serving-visibility observation does not invalidate a durable signed publication receipt.
        ExternalProviderEvidence withServingVisible = signedEvidence.toBuilder()
                .setManagedSearch(signedEvidence.getManagedSearch().toBuilder()
                        .setServingVisibility(
                                ExternalServingVisibility.EXTERNAL_SERVING_VISIBILITY_CURRENT_ROWS))
                .build();
        assertThatCode(() -> strictVerifier.verifyEvidence(
                        withServingVisible, binding, auth, intent, intentSha))
                .doesNotThrowAnyException();

        KeyPair roguePair = RecordKeys.generate();
        SignedWorkRecord rogueSigned = new RecordSigner("rogue-key", roguePair.getPrivate())
                .sign(workRecord.toBuilder().setKeyId("rogue-key").build());
        ExternalProviderEvidence rogueEvidence = unsignedEvidence.toBuilder()
                .setSignedWorkRecordBytes(rogueSigned.toByteString())
                .build();
        assertThatThrownBy(() -> strictVerifier.verifyEvidence(
                        rogueEvidence, binding, auth, intent, intentSha))
                .isInstanceOfSatisfying(
                        ExternalProviderVerifier.ProviderVerificationException.class,
                        ex -> assertThat(ex.diagnosticCode()).isEqualTo("SIGNED_WORK_RECORD_INVALID"));
    }

    @Test
    void refusesTransplantedOrReboundSignedWorkRecordsAcrossIntentObjectBindingAndSettlement() {
        KeyPair trustedPair = RecordKeys.generate();
        RecordSigner trustedSigner = new RecordSigner("search-key-1", trustedPair.getPrivate());
        TrustSnapshot trust = trustSnapshotFor(trustedPair);
        ExternalProviderVerifier strictVerifier = new ExternalProviderVerifier(trust, true);

        ExternalSystemBinding binding = binding();
        ExternalProviderGateway.OutboundAuthContext auth = authContext(binding);
        ExternalPublicationIntent intentA = publishIntent();
        String intentShaA = ExternalOperationKeys.intentSha256(intentA);
        ExternalProviderEvidence unsignedA = validPublishedEvidence(intentA, intentShaA);
        SignedWorkRecord signedForA = trustedSigner.sign(ExternalProviderVerifier.buildBoundWorkRecord(
                defaultIssuance("search-key-1"), unsignedA, binding, intentShaA));

        // 1. Reuse valid signed_work_record_bytes from A on a new unsigned
        // evidence envelope for a different externalObjectId with a freshly recomputed intentShaB.
        ExternalPublicationIntent intentDifferentObject = intentA.toBuilder()
                .setExternalObjectId("search-doc-999")
                .build();
        String intentShaDifferentObject = ExternalOperationKeys.intentSha256(intentDifferentObject);
        ExternalProviderEvidence transplantedObjectEvidence =
                validPublishedEvidence(intentDifferentObject, intentShaDifferentObject).toBuilder()
                        .setSignedWorkRecordBytes(signedForA.toByteString())
                        .build();
        assertRefusal(
                strictVerifier,
                transplantedObjectEvidence,
                binding,
                auth,
                intentDifferentObject,
                intentShaDifferentObject,
                "SIGNED_WORK_RECORD_INTENT_MISMATCH");

        // 2. Reuse valid signed_work_record_bytes from A on a different source revision intent.
        ExternalPublicationIntent intentNextRev = intentA.toBuilder()
                .setSourceRevision(6L)
                .build();
        String intentShaNextRev = ExternalOperationKeys.intentSha256(intentNextRev);
        ExternalProviderEvidence transplantedRevEvidence =
                validPublishedEvidence(intentNextRev, intentShaNextRev).toBuilder()
                        .setSignedWorkRecordBytes(signedForA.toByteString())
                        .build();
        assertRefusal(
                strictVerifier,
                transplantedRevEvidence,
                binding,
                auth,
                intentNextRev,
                intentShaNextRev,
                "SIGNED_WORK_RECORD_INTENT_MISMATCH");

        // 3. Even if subject.workflow_fingerprint matches intentShaDifferentObject, a signed claim
        // artifact bound to "search-doc-7" cannot be transplanted onto "search-doc-999".
        WorkRecord mismatchedClaimRecord = WorkRecord.newBuilder()
                .setManifestVersion(WorkRecords.MANIFEST_VERSION)
                .setRecordId("pub-rcpt-9002")
                .setIssuer("search-coordinator")
                .setKeyId("search-key-1")
                .setIssuedAt(FIXED_TIME)
                .setSubject(RecordSubject.newBuilder()
                        .setKind("workflow-run")
                        .setWorkflowName("external-search-publish")
                        .setWorkflowFingerprint(intentShaDifferentObject)
                        .setRunId("run-9002"))
                .addArtifacts(ExternalProviderVerifier.settlementClaimArtifact(
                        unsignedA, binding, intentShaA))
                .setCompleteness(completeCoverage())
                .build();
        ExternalProviderEvidence mismatchedObjectClaimEvidence =
                validPublishedEvidence(intentDifferentObject, intentShaDifferentObject).toBuilder()
                        .setSignedWorkRecordBytes(trustedSigner.sign(mismatchedClaimRecord).toByteString())
                        .build();
        assertRefusal(
                strictVerifier,
                mismatchedObjectClaimEvidence,
                binding,
                auth,
                intentDifferentObject,
                intentShaDifferentObject,
                "SIGNED_WORK_RECORD_CLAIM_MISMATCH");

        // 4. Same intentShaA, swapped settlement: provider signed ACCEPTED_ONLY (or REJECTED),
        // and outer unsigned envelope is promoted to PUBLISHED without a new provider signature.
        ExternalProviderEvidence acceptedUnsigned = unsignedA.toBuilder()
                .setSettlement(ExternalProviderSettlement.EXTERNAL_PROVIDER_SETTLEMENT_ACCEPTED_ONLY)
                .clearExternalVersion()
                .setManagedSearch(unsignedA.getManagedSearch().toBuilder()
                        .clearPublishedCatalogEpoch()
                        .clearAfterManifestSha256())
                .build();
        SignedWorkRecord signedAcceptedOnly = trustedSigner.sign(
                ExternalProviderVerifier.buildBoundWorkRecord(
                        defaultIssuance("search-key-1"), acceptedUnsigned, binding, intentShaA));
        ExternalProviderEvidence forgedPublishedFromAccepted = unsignedA.toBuilder()
                .setSignedWorkRecordBytes(signedAcceptedOnly.toByteString())
                .build();
        assertRefusal(
                strictVerifier,
                forgedPublishedFromAccepted,
                binding,
                auth,
                intentA,
                intentShaA,
                "SIGNED_WORK_RECORD_CLAIM_MISMATCH");

        // 5. Same intentShaA and PUBLISHED settlement, swapped external_version or catalog epoch.
        ExternalProviderEvidence forgedExternalVersion = unsignedA.toBuilder()
                .setExternalVersion("epoch-999")
                .setSignedWorkRecordBytes(signedForA.toByteString())
                .build();
        assertRefusal(
                strictVerifier,
                forgedExternalVersion,
                binding,
                auth,
                intentA,
                intentShaA,
                "SIGNED_WORK_RECORD_CLAIM_MISMATCH");

        ExternalProviderEvidence forgedCatalogEpoch = unsignedA.toBuilder()
                .setManagedSearch(unsignedA.getManagedSearch().toBuilder()
                        .setPublishedCatalogEpoch(999L))
                .setSignedWorkRecordBytes(signedForA.toByteString())
                .build();
        assertRefusal(
                strictVerifier,
                forgedCatalogEpoch,
                binding,
                auth,
                intentA,
                intentShaA,
                "SIGNED_WORK_RECORD_CLAIM_MISMATCH");

        // 6. Swapped bindingRevision or system dataset/namespace.
        ExternalSystemBinding rotatedBinding = binding.toBuilder().setBindingRevision(2L).build();
        ExternalProviderGateway.OutboundAuthContext rotatedAuth = authContext(rotatedBinding);
        ExternalPublicationIntent rotatedIntent = intentA.toBuilder().setBindingRevision(2L).build();
        String rotatedIntentSha = ExternalOperationKeys.intentSha256(rotatedIntent);
        ExternalProviderEvidence transplantedAcrossBindingRev =
                validPublishedEvidence(rotatedIntent, rotatedIntentSha).toBuilder()
                        .setSignedWorkRecordBytes(signedForA.toByteString())
                        .build();
        assertRefusal(
                strictVerifier,
                transplantedAcrossBindingRev,
                rotatedBinding,
                rotatedAuth,
                rotatedIntent,
                rotatedIntentSha,
                "SIGNED_WORK_RECORD_INTENT_MISMATCH");

        ExternalSystemBinding otherDatasetBinding = binding.toBuilder()
                .setDataset("corpus-staging")
                .build();
        ExternalProviderGateway.OutboundAuthContext otherDatasetAuth = authContext(otherDatasetBinding);
        ExternalProviderEvidence signedForMainVerifiedAgainstStaging = unsignedA.toBuilder()
                .setSignedWorkRecordBytes(signedForA.toByteString())
                .build();
        assertRefusal(
                strictVerifier,
                signedForMainVerifiedAgainstStaging,
                otherDatasetBinding,
                otherDatasetAuth,
                intentA,
                intentShaA,
                "SIGNED_WORK_RECORD_CLAIM_MISMATCH");

        // 7. Generic workflow-run SignedWorkRecord omitting the settlement claim artifact refuses.
        WorkRecord unboundGenericRecord = WorkRecord.newBuilder()
                .setManifestVersion(WorkRecords.MANIFEST_VERSION)
                .setRecordId("pub-rcpt-generic")
                .setIssuer("search-coordinator")
                .setKeyId("search-key-1")
                .setIssuedAt(FIXED_TIME)
                .setSubject(RecordSubject.newBuilder()
                        .setKind("workflow-run")
                        .setWorkflowName("external-search-publish")
                        .setWorkflowFingerprint(intentShaA)
                        .setRunId("run-generic"))
                .setCompleteness(completeCoverage())
                .build();
        ExternalProviderEvidence unboundGenericEvidence = unsignedA.toBuilder()
                .setSignedWorkRecordBytes(trustedSigner.sign(unboundGenericRecord).toByteString())
                .build();
        assertRefusal(
                strictVerifier,
                unboundGenericEvidence,
                binding,
                auth,
                intentA,
                intentShaA,
                "SIGNED_WORK_RECORD_CLAIM_MISSING");

        // 8. PARTIAL completeness SignedWorkRecord refuses even if otherwise validly signed.
        Completeness partialCompleteness = completeCoverage().toBuilder()
                .setStatus(CompletenessStatus.COMPLETENESS_STATUS_PARTIAL)
                .addMissingReasons("provider skipped durable commit check")
                .build();
        WorkRecord partialRecord = ExternalProviderVerifier.buildBoundWorkRecord(
                new ExternalProviderVerifier.WorkRecordIssuance(
                        "pub-rcpt-partial",
                        "search-coordinator",
                        "search-key-1",
                        "external-search-publish",
                        "run-partial",
                        FIXED_TIME,
                        partialCompleteness),
                unsignedA,
                binding,
                intentShaA);
        ExternalProviderEvidence partialEvidence = unsignedA.toBuilder()
                .setSignedWorkRecordBytes(trustedSigner.sign(partialRecord).toByteString())
                .build();
        assertRefusal(
                strictVerifier,
                partialEvidence,
                binding,
                auth,
                intentA,
                intentShaA,
                "SIGNED_WORK_RECORD_INCOMPLETE");
    }

    @Test
    void refusesIdentityProfileIntentAndManagedSearchMismatches() {
        ExternalSystemBinding binding = binding();
        ExternalProviderGateway.OutboundAuthContext auth = authContext(binding);
        ExternalPublicationIntent intent = publishIntent();
        String intentSha = ExternalOperationKeys.intentSha256(intent);
        ExternalProviderEvidence base = validPublishedEvidence(intent, intentSha);
        ExternalProviderVerifier verifier = new ExternalProviderVerifier();

        assertThatCode(() -> verifier.verifyEvidence(base, binding, auth, intent, intentSha))
                .doesNotThrowAnyException();

        assertRefusal(verifier, base.toBuilder().setProvider("other-provider").build(),
                binding, auth, intent, intentSha, "PROVIDER_MISMATCH");
        assertRefusal(verifier, base.toBuilder().setContractVersion("managed-search.v2").build(),
                binding, auth, intent, intentSha, "CONTRACT_VERSION_MISMATCH");
        assertRefusal(verifier, base.toBuilder().setVerifiedServiceProfileId("wrong-profile").build(),
                binding, auth, intent, intentSha, "SERVICE_PROFILE_MISMATCH");
        assertRefusal(verifier, base.toBuilder().setVerifiedServiceEndpointId("wrong-ep").build(),
                binding, auth, intent, intentSha, "SERVICE_ENDPOINT_MISMATCH");
        assertRefusal(verifier, base.toBuilder().setCredentialBindingSha256("f".repeat(64)).build(),
                binding, auth, intent, intentSha, "CREDENTIAL_BINDING_MISMATCH");
        assertRefusal(verifier, base.toBuilder().setExternalObjectId("wrong-object").build(),
                binding, auth, intent, intentSha, "EXTERNAL_OBJECT_ID_MISMATCH");
        assertRefusal(verifier, base.toBuilder().setRequestIntentSha256("9".repeat(64)).build(),
                binding, auth, intent, intentSha, "INTENT_DIGEST_MISMATCH");
        assertRefusal(verifier,
                base.toBuilder()
                        .setSettlement(ExternalProviderSettlement.EXTERNAL_PROVIDER_SETTLEMENT_DELETED)
                        .build(),
                binding, auth, intent, intentSha, "SETTLEMENT_ACTION_MISMATCH");

        ExternalProviderEvidence badDocKey = base.toBuilder()
                .setManagedSearch(base.getManagedSearch().toBuilder()
                        .setDocumentKey(ByteString.copyFromUtf8("wrong-key")))
                .build();
        assertRefusal(verifier, badDocKey, binding, auth, intent, intentSha,
                "MANAGED_SEARCH_DOCUMENT_KEY_MISMATCH");

        ExternalProviderEvidence publishedWithoutEpoch = base.toBuilder()
                .setManagedSearch(base.getManagedSearch().toBuilder().clearPublishedCatalogEpoch())
                .build();
        assertRefusal(verifier, publishedWithoutEpoch, binding, auth, intent, intentSha,
                "PROVIDER_EVIDENCE_MALFORMED");

        ExternalProviderEvidence acceptedWithPrematureEpoch = base.toBuilder()
                .setSettlement(ExternalProviderSettlement.EXTERNAL_PROVIDER_SETTLEMENT_ACCEPTED_ONLY)
                .build();
        assertRefusal(verifier, acceptedWithPrematureEpoch, binding, auth, intent, intentSha,
                "PROVIDER_EVIDENCE_MALFORMED");
    }

    @Test
    void aManagedSearchBindingRefusesPublishEvidenceWithoutManagedSearchIdentity() {
        ExternalSystemBinding managed = binding();
        ExternalProviderGateway.OutboundAuthContext auth = authContext(managed);
        ExternalPublicationIntent intent = publishIntent();
        String intentSha = ExternalOperationKeys.intentSha256(intent);
        ExternalProviderVerifier verifier = new ExternalProviderVerifier();

        // Receipt-grade evidence: everything in the envelope matches, but there is no
        // history, sequence, catalog epoch or manifest to prove the document is in the index.
        ExternalProviderEvidence publishedWithoutProof =
                validPublishedEvidence(intent, intentSha).toBuilder().clearManagedSearch().build();
        assertRefusal(verifier, publishedWithoutProof, managed, auth, intent, intentSha,
                "MANAGED_SEARCH_IDENTITY_REQUIRED");
        ExternalProviderEvidence acceptedWithoutProof = publishedWithoutProof.toBuilder()
                .setSettlement(ExternalProviderSettlement.EXTERNAL_PROVIDER_SETTLEMENT_ACCEPTED_ONLY)
                .build();
        assertRefusal(verifier, acceptedWithoutProof, managed, auth, intent, intentSha,
                "MANAGED_SEARCH_IDENTITY_REQUIRED");

        // A binding that asks for the receipt alone accepts the same evidence.
        ExternalSystemBinding receiptOnly = managed.toBuilder()
                .setRequiredIdentityProof(ExternalIdentityProof.EXTERNAL_IDENTITY_PROOF_RECEIPT)
                .build();
        ExternalProviderGateway.OutboundAuthContext receiptAuth = authContext(receiptOnly);
        assertThatCode(() -> verifier.verifyEvidence(
                publishedWithoutProof, receiptOnly, receiptAuth, intent, intentSha))
                .doesNotThrowAnyException();
    }

    @Test
    void aValidlySignedReceiptWhoseTextCarriesACredentialIsRefused() {
        KeyPair trustedPair = RecordKeys.generate();
        RecordSigner trustedSigner = new RecordSigner("search-key-1", trustedPair.getPrivate());
        ExternalProviderVerifier strictVerifier = new ExternalProviderVerifier(trustSnapshotFor(trustedPair), true);
        ExternalSystemBinding binding = binding();
        ExternalProviderGateway.OutboundAuthContext auth = authContext(binding);
        ExternalPublicationIntent intent = publishIntent();
        String intentSha = ExternalOperationKeys.intentSha256(intent);
        ExternalProviderEvidence unsignedEvidence = validPublishedEvidence(intent, intentSha);
        WorkRecord workRecord = ExternalProviderVerifier.buildBoundWorkRecord(
                defaultIssuance("search-key-1"), unsignedEvidence, binding, intentSha);

        // The signature is good; the text inside the signed manifest is not safe to store.
        WorkRecord carriesToken = workRecord.toBuilder()
                .setDisclosure(Disclosure.newBuilder()
                        .setSourceManifestSha256("5".repeat(64))
                        .setPolicy("Bearer abcdefghijklmnopqrstuvwxyz0123456789"))
                .build();
        ExternalProviderEvidence signedWithToken = unsignedEvidence.toBuilder()
                .setSignedWorkRecordBytes(trustedSigner.sign(carriesToken).toByteString())
                .build();
        assertThatThrownBy(() -> strictVerifier.verifyEvidence(signedWithToken, binding, auth, intent, intentSha))
                .isInstanceOfSatisfying(
                        ExternalProviderVerifier.ProviderVerificationException.class,
                        ex -> {
                            assertThat(ex.diagnosticCode()).isEqualTo("EVIDENCE_CARRIES_CREDENTIAL");
                            assertThat(ex.getMessage()).contains("signed_work_record.disclosure.policy")
                                    .doesNotContain("abcdefghij");
                        });

        // The same signed record without the token verifies.
        ExternalProviderEvidence signedClean = unsignedEvidence.toBuilder()
                .setSignedWorkRecordBytes(trustedSigner.sign(workRecord).toByteString())
                .build();
        assertThatCode(() -> strictVerifier.verifyEvidence(signedClean, binding, auth, intent, intentSha))
                .doesNotThrowAnyException();
    }

    private static void assertRefusal(
            ExternalProviderVerifier verifier,
            ExternalProviderEvidence candidate,
            ExternalSystemBinding binding,
            ExternalProviderGateway.OutboundAuthContext auth,
            ExternalPublicationIntent intent,
            String intentSha,
            String expectedCode) {
        assertThatThrownBy(() -> verifier.verifyEvidence(candidate, binding, auth, intent, intentSha))
                .isInstanceOfSatisfying(
                        ExternalProviderVerifier.ProviderVerificationException.class,
                        ex -> assertThat(ex.diagnosticCode()).isEqualTo(expectedCode));
    }
    @Test
    void historicalSignedReceiptUsesRecordedDispatchIdentityAfterCredentialRotation() {
        var pair = RecordKeys.generate();
        var signer = new RecordSigner("search-key-1", pair.getPrivate());
        var verifier = new ExternalProviderVerifier(trustSnapshotFor(pair), true);
        var binding = binding();
        var auth = authContext(binding);
        var intent = publishIntent();
        var digest = ExternalOperationKeys.intentSha256(intent);
        var unsigned = validPublishedEvidence(intent, digest);
        var signed = signer.sign(ExternalProviderVerifier.buildBoundWorkRecord(
                defaultIssuance("search-key-1"), unsigned, binding, digest));
        var receipt = unsigned.toBuilder().setSignedWorkRecordBytes(signed.toByteString()).build();
        verifier.verifyEvidence(receipt, binding, auth, intent, digest);
        var endpoint = auth.serviceEndpoint().toBuilder().setCredentialRef("cred-search-b").build();
        var profile = auth.serviceProfile().toBuilder().clearEndpoints().addEndpoints(endpoint).build();
        var rotatedAuth = new ExternalProviderGateway.OutboundAuthContext(
                binding, profile, endpoint, new Metadata(), "9".repeat(64));
        var operation = ai.protomolt.proto.repo.external.v1.ExternalPublicationOperationRecord.newBuilder()
                .setBindingSnapshot(binding).setIntent(intent).setIntentSha256(digest)
                .addDispatchCredentialBindingSha256S(CRED_SHA).build();
        assertThatCode(() -> verifier.verifyRecordedEvidence(receipt, operation, rotatedAuth))
                .doesNotThrowAnyException();
        // A fingerprint never dispatched is refused even if it is used by today's status query.
        assertThatThrownBy(() -> verifier.verifyRecordedEvidence(
                receipt.toBuilder().setCredentialBindingSha256("9".repeat(64)).build(), operation, rotatedAuth))
                .isInstanceOf(ExternalProviderVerifier.ProviderVerificationException.class)
                .hasMessageContaining("recorded dispatch");
        // A retry may dispatch with the new identity, without forgetting the first send.
        var retried = ExternalLedgerStore.withDispatchIdentity(operation, "9".repeat(64));
        assertThat(retried.getDispatchCredentialBindingSha256SList()).containsExactly(CRED_SHA, "9".repeat(64));
        assertThatCode(() -> verifier.verifyRecordedEvidence(receipt, retried, rotatedAuth))
                .doesNotThrowAnyException();

        // A recorded operation missing dispatch_credential_binding_sha256s fails closed with DATA_LOSS.
        var missingDispatchIdentity = operation.toBuilder()
                .setOperationId("op-missing-dispatch-id")
                .clearDispatchCredentialBindingSha256S()
                .build();
        assertThatThrownBy(() -> verifier.verifyRecordedEvidence(receipt, missingDispatchIdentity, auth))
                .isInstanceOfSatisfying(io.grpc.StatusRuntimeException.class, ex -> {
                    assertThat(ex.getStatus().getCode()).isEqualTo(io.grpc.Status.Code.DATA_LOSS);
                    assertThat(ex.getStatus().getDescription()).contains("dispatch_credential_binding_sha256s");
                });
    }
}
