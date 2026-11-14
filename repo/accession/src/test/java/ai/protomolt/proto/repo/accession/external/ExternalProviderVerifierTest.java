package ai.protomolt.proto.repo.accession.external;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.grpc.profile.v1.ServiceEndpoint;
import ai.protomolt.proto.grpc.profile.v1.ServiceProfile;
import ai.protomolt.proto.grpc.profile.v1.Transport;
import ai.protomolt.proto.receipt.Completeness;
import ai.protomolt.proto.receipt.CompletenessStatus;
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
 * intent-digest, managed-search epoch, and Ed25519 {@link SignedWorkRecord} verification.
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

    @Test
    void verifiesPublishedEvidenceWithSignedWorkRecordAndRefusesUntrustedSignatures() {
        KeyPair trustedPair = RecordKeys.generate();
        RecordSigner trustedSigner = new RecordSigner("search-key-1", trustedPair.getPrivate());
        TrustSnapshot trust = TrustSnapshot.newBuilder()
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

        WorkRecord workRecord = WorkRecord.newBuilder()
                .setManifestVersion(WorkRecords.MANIFEST_VERSION)
                .setRecordId("pub-rcpt-9001")
                .setIssuer("search-coordinator")
                .setKeyId("search-key-1")
                .setIssuedAt(FIXED_TIME)
                .setSubject(RecordSubject.newBuilder()
                        .setKind("workflow-run")
                        .setWorkflowName("external-search-publish")
                        .setWorkflowFingerprint("3".repeat(64))
                        .setRunId("run-9001"))
                .setCompleteness(Completeness.newBuilder()
                        .setStatus(CompletenessStatus.COMPLETENESS_STATUS_COMPLETE)
                        .setPolicyId("external-publication-policy")
                        .setPolicyVersion("1.0")
                        .setPolicySha256("4".repeat(64)))
                .build();
        SignedWorkRecord signed = trustedSigner.sign(workRecord);

        ExternalSystemBinding binding = binding();
        ExternalProviderGateway.OutboundAuthContext auth = authContext(binding);
        ExternalPublicationIntent intent = publishIntent();
        String intentSha = ExternalOperationKeys.intentSha256(intent);

        ExternalProviderVerifier strictVerifier = new ExternalProviderVerifier(trust, true);
        ExternalProviderEvidence unsignedEvidence = validPublishedEvidence(intent, intentSha);
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
}
