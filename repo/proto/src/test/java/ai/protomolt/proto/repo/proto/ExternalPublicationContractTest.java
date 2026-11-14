package ai.protomolt.proto.repo.proto;

import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.http.jsonschema.ProtoJsonSchemaGenerator;
import ai.protomolt.proto.repo.external.v1.*;
import ai.protomolt.proto.repo.v1.DocumentPart;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.protovalidate.ProtovalidateRuleSource;
import ai.protomolt.proto.validate.source.ProtomoltRuleSource;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Contract validation fixtures exercising both house validate.v1 and portable buf.validate dialects. */
class ExternalPublicationContractTest {
    private static final ProtoValidator HOUSE_VALIDATOR =
            ProtoValidator.create(List.of(new ProtomoltRuleSource()));
    private static final ProtoValidator PROTOVALIDATE_VALIDATOR =
            ProtoValidator.create(List.of(new ProtovalidateRuleSource()));
    private static final String SHA_A = "a".repeat(64);
    private static final String SHA_B = "b".repeat(64);
    private static final Timestamp TIME = Timestamp.newBuilder().setSeconds(1_760_000_000L).build();
    private static final ByteString HISTORY_ID = ByteString.copyFrom(new byte[]{
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16});

    private static NodeAddress sourceAddress() {
        return NodeAddress.newBuilder()
                .setAccountId("tenant-alpha")
                .setGraphId("pipeline:claims")
                .setGraphAddressId("enriched")
                .setDocId("doc-101")
                .build();
    }

    private static ExternalSystemBinding activeBinding() {
        return ExternalSystemBinding.newBuilder()
                .setBindingId("search-primary")
                .setAccountId("tenant-alpha")
                .setBindingRevision(1L)
                .setProvider("protomolt-search")
                .setServiceProfileId("search-cluster-profile")
                .setServiceEndpointId("grpc-primary")
                .setDataset("claims-index")
                .setNamespace("prod-us")
                .setContractVersion("managed-search/v1")
                .setEndpointSummary(ExternalEndpointSummary.newBuilder()
                        .setHost("search.internal.example.org")
                        .setPort(8443)
                        .setTlsEnabled(true))
                .setLifecycle(ExternalBindingLifecycle.EXTERNAL_BINDING_LIFECYCLE_ACTIVE)
                .setRetentionPolicy(ExternalRetentionPolicy.EXTERNAL_RETENTION_POLICY_RETAIN_HISTORY)
                .setConfigLaneVersion(3L)
                .putLabels("tier", "gold")
                .setCreatedAt(TIME)
                .build();
    }

    private static ManagedSearchIdentity publishedManagedSearch() {
        return ManagedSearchIdentity.newBuilder()
                .setHistoryId(HISTORY_ID)
                .setDocumentKey(ByteString.copyFrom("k1-binary-key", StandardCharsets.UTF_8))
                .setIndexKey(ByteString.copyFrom("claims-v1", StandardCharsets.UTF_8))
                .setAcceptedVersion(4L)
                .setAcceptedSequence(99L)
                .setPublishedCatalogEpoch(12L)
                .setAfterManifestSha256(SHA_B)
                .setServingVisibility(ExternalServingVisibility.EXTERNAL_SERVING_VISIBILITY_CURRENT_ROWS)
                .build();
    }

    private static ExternalProviderEvidence publishedEvidence() {
        return ExternalProviderEvidence.newBuilder()
                .setProvider("protomolt-search")
                .setContractVersion("managed-search/v1")
                .setVerifiedServiceProfileId("search-cluster-profile")
                .setVerifiedServiceEndpointId("grpc-primary")
                .setCredentialBindingSha256(SHA_A)
                .setExternalObjectId("claims-index:doc-101")
                .setExternalVersion("v4-epoch12")
                .setRequestIntentSha256(SHA_A)
                .setReceiptSha256(SHA_B)
                .setProviderReceiptId("decision-99")
                .setSettlement(ExternalProviderSettlement.EXTERNAL_PROVIDER_SETTLEMENT_PUBLISHED)
                .setManagedSearch(publishedManagedSearch())
                .setVerifiedAt(TIME)
                .build();
    }

    private static ExternalLink confirmedLink() {
        return ExternalLink.newBuilder()
                .setBindingId("search-primary")
                .setBindingRevision(1L)
                .setBindingSnapshot(activeBinding())
                .setExternalObjectId("claims-index:doc-101")
                .setExternalVersion("v4-epoch12")
                .setSourceAddress(sourceAddress())
                .setSourceRevision(5L)
                .setProcessing(ExternalProcessingIdentity.newBuilder()
                        .addSourcePartDigests(ExternalPartDigest.newBuilder()
                                .setPart(DocumentPart.DOCUMENT_PART_CORE)
                                .setSha256(SHA_A)
                                .setSizeBytes(512L))
                        .addSourcePartDigests(ExternalPartDigest.newBuilder()
                                .setPart(DocumentPart.DOCUMENT_PART_EMBEDDINGS)
                                .setSubKey("seg-1:model-a")
                                .setSha256(SHA_B)
                                .setSizeBytes(2048L))
                        .addRetainedSchemaSha256S(SHA_A)
                        .setMappingId("claims-projection-v1")
                        .setGenerationDigest(SHA_B)
                        .setNlpInputSha256(SHA_A)
                        .setNlpResultSha256(SHA_B)
                        .setNlpReceiptSha256(SHA_A))
                .setManagedSearch(publishedManagedSearch())
                .setDisplayUrl("https://console.example.org/search/claims-index/items/doc-101")
                .setConfirmedByOperationId("op-publish-5")
                .setConfirmationEvidence(publishedEvidence())
                .setProjectionLifecycle(ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_ACTIVE)
                .setConfirmedAt(TIME)
                .build();
    }

    private static ExternalPublicationIntent publishIntent() {
        return ExternalPublicationIntent.newBuilder()
                .setEncodingVersion(1)
                .setAccountId("tenant-alpha")
                .setSourceAddress(sourceAddress())
                .setSourceRevision(5L)
                .setBindingId("search-primary")
                .setBindingRevision(1L)
                .setAction(ExternalPublicationAction.EXTERNAL_PUBLICATION_ACTION_PUBLISH)
                .setExternalObjectId("claims-index:doc-101")
                .setDisplayUrl("https://console.example.org/search/claims-index/items/doc-101")
                .build();
    }

    private static void checkBothDialects(Message value, boolean expected) throws Exception {
        for (ProtoValidator validator : List.of(HOUSE_VALIDATOR, PROTOVALIDATE_VALIDATOR)) {
            for (Message candidate : List.of(
                    value,
                    DynamicMessage.parseFrom(value.getDescriptorForType(), value.toByteString()))) {
                var result = validator.validate(candidate);
                assertThat(result.valid())
                        .as("%s violations: %s",
                                candidate.getDescriptorForType().getName(),
                                result.violations())
                        .isEqualTo(expected);
            }
        }
    }

    @Test
    void bindingEnforcesNonSecretSummaryRevisionAndRetirementRulesInBothDialects() throws Exception {
        ExternalSystemBinding active = activeBinding();
        checkBothDialects(active, true);

        ExternalSystemBinding retired = active.toBuilder()
                .setLifecycle(ExternalBindingLifecycle.EXTERNAL_BINDING_LIFECYCLE_RETIRED)
                .setRetiredReason("migrated to search-secondary")
                .setRetiredAt(TIME)
                .build();
        checkBothDialects(retired, true);

        checkBothDialects(active.toBuilder().setRetiredReason("unexpected").build(), false);
        checkBothDialects(active.toBuilder().setRetiredAt(TIME).build(), false);
        checkBothDialects(retired.toBuilder().clearRetiredReason().build(), false);
        checkBothDialects(retired.toBuilder().clearRetiredAt().build(), false);
        checkBothDialects(active.toBuilder().setBindingRevision(0L).build(), false);
        checkBothDialects(active.toBuilder().setBindingId("../bad").build(), false);
        checkBothDialects(active.toBuilder().setLifecycleValue(99).build(), false);
        checkBothDialects(active.toBuilder().setRetentionPolicyValue(0).build(), false);
        checkBothDialects(active.toBuilder().setEndpointSummary(
                active.getEndpointSummary().toBuilder().setPort(0)).build(), false);
        checkBothDialects(active.toBuilder().setEndpointSummary(
                active.getEndpointSummary().toBuilder().setPort(70_000)).build(), false);
        checkBothDialects(active.toBuilder().setEndpointSummary(
                active.getEndpointSummary().toBuilder().setHost("user:pass@example.org")).build(), false);
    }

    @Test
    void processingAndManagedSearchIdentitiesValidatePartSubKeysAndCatalogEpochs() throws Exception {
        ExternalPartDigest core = ExternalPartDigest.newBuilder()
                .setPart(DocumentPart.DOCUMENT_PART_CORE)
                .setSha256(SHA_A)
                .setSizeBytes(0L)
                .build();
        checkBothDialects(core, true);
        checkBothDialects(core.toBuilder().setSubKey("unexpected").build(), false);
        checkBothDialects(core.toBuilder().setPart(DocumentPart.DOCUMENT_PART_CHUNKS).build(), false);
        checkBothDialects(core.toBuilder()
                .setPart(DocumentPart.DOCUMENT_PART_CHUNKS)
                .setSubKey("seg-a")
                .build(), true);
        checkBothDialects(core.toBuilder()
                .setPart(DocumentPart.DOCUMENT_PART_EMBEDDINGS)
                .setSubKey(" ")
                .build(), false);

        ExternalProcessingIdentity duplicateSchemas = ExternalProcessingIdentity.newBuilder()
                .addRetainedSchemaSha256S(SHA_A)
                .addRetainedSchemaSha256S(SHA_A)
                .build();
        checkBothDialects(duplicateSchemas, false);

        ManagedSearchIdentity acceptedOnlySearch = publishedManagedSearch().toBuilder()
                .clearPublishedCatalogEpoch()
                .clearAfterManifestSha256()
                .setServingVisibility(ExternalServingVisibility.EXTERNAL_SERVING_VISIBILITY_UNKNOWN)
                .build();
        ExternalProviderEvidence acceptedEvidence = publishedEvidence().toBuilder()
                .setSettlement(ExternalProviderSettlement.EXTERNAL_PROVIDER_SETTLEMENT_ACCEPTED_ONLY)
                .setManagedSearch(acceptedOnlySearch)
                .build();
        checkBothDialects(acceptedEvidence, true);

        // ACCEPTED_ONLY cannot claim a published catalog epoch; PUBLISHED must carry it.
        checkBothDialects(acceptedEvidence.toBuilder()
                .setManagedSearch(publishedManagedSearch())
                .build(), false);
        checkBothDialects(publishedEvidence().toBuilder()
                .setManagedSearch(acceptedOnlySearch)
                .build(), false);
        checkBothDialects(publishedManagedSearch().toBuilder()
                .setHistoryId(ByteString.copyFrom(new byte[]{1, 2, 3}))
                .build(), false);
    }

    @Test
    void externalLinkRequiresPublishedProviderEvidenceExplicitVersionParityAndSafeDisplayUrls() throws Exception {
        ExternalLink valid = confirmedLink();
        checkBothDialects(valid, true);

        // Explicit absence of external_version is valid when evidence also omits external_version.
        ExternalLink unversioned = valid.toBuilder()
                .clearExternalVersion()
                .setConfirmationEvidence(valid.getConfirmationEvidence().toBuilder().clearExternalVersion())
                .build();
        assertThat(unversioned.hasExternalVersion()).isFalse();
        checkBothDialects(unversioned, true);

        // Version mismatch or presence mismatch between link and confirmation evidence is refused.
        checkBothDialects(valid.toBuilder().clearExternalVersion().build(), false);
        checkBothDialects(valid.toBuilder().setExternalVersion("forged-v9").build(), false);

        // Mere ACCEPTED_ONLY or REJECTED provider settlement cannot form a confirmed ExternalLink.
        checkBothDialects(valid.toBuilder()
                .setConfirmationEvidence(valid.getConfirmationEvidence().toBuilder()
                        .setSettlement(ExternalProviderSettlement.EXTERNAL_PROVIDER_SETTLEMENT_ACCEPTED_ONLY)
                        .clearManagedSearch())
                .build(), false);

        // Display URLs reject non-http schemes, embedded userinfo, and secret/bearer query tokens.
        for (String unsafeUrl : List.of(
                "javascript:alert(1)",
                "data:text/html;base64,PHNjcmlwdD4=",
                "ftp://files.example.org/item",
                "https://bearer-token@console.example.org/items/1",
                "https://user:secret@console.example.org/items/1",
                "https://console.example.org/items/1?access_token=secret123",
                "https://console.example.org/items/1?foo=1&Bearer=eyJ0eXAi",
                "https://console.example.org/items/1?api_key=xyz",
                "https://console.example.org/items/1?X-Amz-Signature=deadbeef")) {
            checkBothDialects(valid.toBuilder().setDisplayUrl(unsafeUrl).build(), false);
        }

        // Cross-account mismatch between source_address and binding_snapshot is refused.
        checkBothDialects(valid.toBuilder()
                .setSourceAddress(sourceAddress().toBuilder().setAccountId("tenant-other"))
                .build(), false);
        checkBothDialects(valid.toBuilder().setSourceRevision(0L).build(), false);
    }

    @Test
    void intentAndOperationEnforcePublishVsPurgeRevisionsAndEvidenceBackedOutcomes() throws Exception {
        ExternalPublicationIntent publish = publishIntent();
        checkBothDialects(publish, true);
        checkBothDialects(publish.toBuilder().setSourceRevision(0L).build(), false);
        checkBothDialects(publish.toBuilder().setPurgeId("purge-1").build(), false);

        ExternalPublicationIntent tombstoneDelete = publish.toBuilder()
                .setAction(ExternalPublicationAction.EXTERNAL_PUBLICATION_ACTION_DELETE)
                .clearDisplayUrl()
                .build();
        checkBothDialects(tombstoneDelete, true);
        checkBothDialects(tombstoneDelete.toBuilder()
                .setDisplayUrl("https://console.example.org/items/1")
                .build(), false);

        ExternalPublicationIntent purgeDelete = tombstoneDelete.toBuilder()
                .setSourceRevision(0L)
                .setPurgeId("purge-corr-7")
                .build();
        checkBothDialects(purgeDelete, true);
        checkBothDialects(purgeDelete.toBuilder().clearPurgeId().build(), false);

        ExternalPublicationAttempt confirmedAttempt = ExternalPublicationAttempt.newBuilder()
                .setAttemptNumber(1)
                .setTrigger(ExternalAttemptTrigger.EXTERNAL_ATTEMPT_TRIGGER_INITIAL)
                .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_CONFIRMED)
                .setStartedAt(TIME)
                .setFinishedAt(TIME)
                .setEvidence(publishedEvidence())
                .build();
        checkBothDialects(confirmedAttempt, true);
        checkBothDialects(confirmedAttempt.toBuilder().clearEvidence().build(), false);

        ExternalPublicationAttempt pendingAttempt = ExternalPublicationAttempt.newBuilder()
                .setAttemptNumber(1)
                .setTrigger(ExternalAttemptTrigger.EXTERNAL_ATTEMPT_TRIGGER_INITIAL)
                .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_PENDING)
                .setPendingPhase(ExternalPendingPhase.EXTERNAL_PENDING_PHASE_UNRESOLVED_LOST_REPLY)
                .setStartedAt(TIME)
                .setDiagnosticCode("ENDPOINT_UNAVAILABLE")
                .build();
        checkBothDialects(pendingAttempt, true);
        checkBothDialects(pendingAttempt.toBuilder()
                .setPendingPhase(ExternalPendingPhase.EXTERNAL_PENDING_PHASE_UNSPECIFIED)
                .build(), false);

        ExternalPublicationOperationRecord confirmedOp = ExternalPublicationOperationRecord.newBuilder()
                .setOperationId("op-publish-5")
                .setIntentSha256(SHA_A)
                .setIntent(publish)
                .setBindingSnapshot(activeBinding())
                .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_CONFIRMED)
                .addAttempts(confirmedAttempt)
                .setTerminalEvidence(publishedEvidence())
                .setCreatedAt(TIME)
                .setUpdatedAt(TIME)
                .build();
        checkBothDialects(confirmedOp, true);

        // Mismatched request_intent_sha256 or missing terminal evidence cannot be CONFIRMED.
        checkBothDialects(confirmedOp.toBuilder().clearTerminalEvidence().build(), false);
        checkBothDialects(confirmedOp.toBuilder()
                .setIntentSha256(SHA_B)
                .build(), false);

        ExternalPublicationLedger ledger = ExternalPublicationLedger.newBuilder()
                .setAccountId("tenant-alpha")
                .setBindingId("search-primary")
                .setSourceAddress(sourceAddress())
                .setLogicalSourceKey(ByteString.copyFrom("k1-source", StandardCharsets.UTF_8))
                .setHighestFencedSourceRevision(5L)
                .setCurrentLink(confirmedLink())
                .addOperations(confirmedOp)
                .build();
        checkBothDialects(ledger, true);

        // Purged ledger cannot retain an ACTIVE current_link or omit last_purge_id.
        checkBothDialects(ledger.toBuilder()
                .setSourcePurged(true)
                .setLastPurgeId("purge-1")
                .build(), false);
        checkBothDialects(ledger.toBuilder()
                .setSourcePurged(true)
                .setLastPurgeId("purge-1")
                .clearCurrentLink()
                .addHistoricalLinks(confirmedLink().toBuilder()
                        .setProjectionLifecycle(ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_DELETED))
                .build(), true);
    }

    @Test
    void jsonSchemaProjectionAndTypeIdentitiesRemainStable() {
        var schema = new ObjectMapper().valueToTree(
                ProtoJsonSchemaGenerator.create().generateRooted(ExternalLink.getDescriptor()));
        assertThat(schema.path("required").toString())
                .contains("bindingId", "bindingRevision", "externalObjectId", "sourceAddress", "sourceRevision");
        assertThat(schema.path("x-protomolt-cel").toString())
                .contains("external-link-verified-evidence", "external-link-safe-display-url");
        assertThat(Any.pack(activeBinding()).getTypeUrl())
                .isEqualTo("type.googleapis.com/ai.protomolt.proto.repo.external.v1.ExternalSystemBinding");
        assertThat(Any.pack(confirmedLink()).getTypeUrl())
                .isEqualTo("type.googleapis.com/ai.protomolt.proto.repo.external.v1.ExternalLink");
        assertThat(DescriptorFingerprints.fingerprintOf(ExternalPublicationLedger.getDescriptor()))
                .hasSize(64);
    }
}
