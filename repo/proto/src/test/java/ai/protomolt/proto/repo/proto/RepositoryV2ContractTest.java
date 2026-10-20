package ai.protomolt.proto.repo.proto;

import ai.protomolt.proto.repo.semantic.v1.*;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.repo.v1.WriteProvenance;
import ai.protomolt.proto.repo.v2.*;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

/** Runs the v2 contract's house validate.v1 rules on generated and dynamic messages. */
class RepositoryV2ContractTest {
    private static final ProtoValidator VALIDATOR = ProtoValidator.create();
    private static final String SHA = "a".repeat(64);
    private static final NodeAddress ADDRESS = NodeAddress.newBuilder().setDocId("doc-1")
            .setGraphAddressId("ds-1").setAccountId("acct-1").setGraphId("intake:acct-1").build();

    @Test void selectorsNameExactlyOneObject() throws Exception {
        check(PartSelector.getDefaultInstance(), false);
        check(part(DocumentPart.DOCUMENT_PART_CORE), true);
        check(part(DocumentPart.DOCUMENT_PART_BLOBS), true);
        check(part(DocumentPart.DOCUMENT_PART_PARSED), true);
        // CHUNKS and EMBEDDINGS are selected by their IDs, never by part.
        check(part(DocumentPart.DOCUMENT_PART_CHUNKS), false);
        check(part(DocumentPart.DOCUMENT_PART_EMBEDDINGS), false);
        check(part(DocumentPart.DOCUMENT_PART_UNSPECIFIED), false);
        check(PartSelector.newBuilder().setPartValue(99).build(), false);
        check(PartSelector.newBuilder().setSegmentationId("sentences").build(), true);
        check(PartSelector.newBuilder().setSegmentationId(" ").build(), false);
        check(PartSelector.newBuilder().setSegmentationId("").build(), false);
        check(PartSelector.newBuilder().setEmbeddingSet(EmbeddingSetKey.newBuilder()
                .setSegmentationId("sentences").setEmbeddingSetId("model2vec")).build(), true);
        check(PartSelector.newBuilder().setEmbeddingSet(EmbeddingSetKey.newBuilder()
                .setSegmentationId("sentences")).build(), false);
    }

    @Test void expectedRevisionIsNoneOrPositive() throws Exception {
        check(ExpectedRevision.getDefaultInstance(), false);
        check(ExpectedRevision.newBuilder().setNone(true).build(), true);
        check(ExpectedRevision.newBuilder().setNone(false).build(), false);
        check(ExpectedRevision.newBuilder().setRevision(0).build(), false);
        check(ExpectedRevision.newBuilder().setRevision(-1).build(), false);
        check(ExpectedRevision.newBuilder().setRevision(3).build(), true);
        check(RevisionSelector.getDefaultInstance(), true); // unset means current
        check(RevisionSelector.newBuilder().setRevision(0).build(), false);
        check(RevisionSelector.newBuilder().setCurrent(false).build(), false);
    }

    @Test void targetsRejectBlankDocumentIdsAndIncompleteAddresses() throws Exception {
        check(DocumentTarget.getDefaultInstance(), false);
        check(DocumentTarget.newBuilder().setAddress(ADDRESS).build(), true);
        check(DocumentTarget.newBuilder().setAddress(ADDRESS.toBuilder().setDocId("")).build(), false);
        check(DocumentTarget.newBuilder().setAddress(ADDRESS.toBuilder().setDocId(" \t")).build(), false);
        check(DocumentTarget.newBuilder().setAddress(ADDRESS.toBuilder().setGraphId("")).build(), false);
        check(DocumentTarget.newBuilder().setAddress(ADDRESS.toBuilder().setAccountId(" ")).build(), false);
        check(DocumentTarget.newBuilder().setAddress(ADDRESS.toBuilder().setGraphAddressId("")).build(), false);
        check(DocumentTarget.newBuilder().setUseDatasourceId("ds-1").build(), true);
        check(DocumentTarget.newBuilder().setUseDatasourceId(" ").build(), false);
        assertThat(DocumentTarget.getDescriptor().findFieldByName("graph_location_id")).isNull();
    }

    @Test void saveRequestsRequireOwnerOperationAndIntakeDocumentId() throws Exception {
        var save = save();
        check(save.build(), true);
        check(save.clone().clearOwner().build(), false);
        check(save.clone().setOperationId("").build(), false);
        check(save.clone().setOperationId("  ").build(), false);
        check(save.clone().clearExpected().build(), false);
        check(save.clone().clearTarget().build(), false);
        check(save.clone().setValidationModeValue(7).build(), false);

        var intake = save.clone().setTarget(DocumentTarget.newBuilder().setUseDatasourceId("ds-1"))
                .clearWrites().addWrites(put(DocumentPart.DOCUMENT_PART_CORE));
        check(intake.clone().setDocument(ai.protomolt.proto.repo.v1.Document.newBuilder().setDocId("doc-1")).build(), true);
        check(intake.clone().setDocument(ai.protomolt.proto.repo.v1.Document.newBuilder().setDocId(" ")).build(), false);
        check(intake.clone().clearDocument().build(), false);
        // doc_id and ownership are CORE content, so a datasource target needs a CORE PUT.
        check(intake.clone().setDocument(ai.protomolt.proto.repo.v1.Document.newBuilder().setDocId("doc-1"))
                .clearWrites().addWrites(put(DocumentPart.DOCUMENT_PART_PARSED)).build(), false);

        var attestation = OpaqueAttestation.newBuilder().setOccurrence(locator()).setTypeUrl("type.test/x.Y");
        check(save.clone().addOpaqueAttestations(attestation).build(), false);
        check(save.clone().addOpaqueAttestations(attestation)
                .setValidationMode(ValidationMode.VALIDATION_MODE_OPAQUE_ALLOWED).build(), true);
    }

    @Test void ownersAreIntakeOrStepWithModule() throws Exception {
        check(SaveOwner.getDefaultInstance(), false);
        check(SaveOwner.newBuilder().setIntake(IntakeOrigin.newBuilder().setDatasourceId("ds-1")).build(), true);
        check(SaveOwner.newBuilder().setIntake(IntakeOrigin.newBuilder().setDatasourceId(" ")).build(), false);
        check(SaveOwner.newBuilder().setIntake(IntakeOrigin.newBuilder().setDatasourceId("ds-1").setConnectorId("")).build(), false);
        check(SaveOwner.newBuilder().setStep(WriteProvenance.newBuilder().setModuleId("chunker")).build(), true);
        check(SaveOwner.newBuilder().setStep(WriteProvenance.newBuilder().setNodeId("n1")).build(), false);
        check(SaveOwner.newBuilder().setStep(WriteProvenance.newBuilder().setModuleId(" ")).build(), false);
    }

    @Test void writesCarryActionsAndDeletionReasons() throws Exception {
        check(put(DocumentPart.DOCUMENT_PART_PARSED), true);
        check(put(DocumentPart.DOCUMENT_PART_PARSED).toBuilder().setDeletedReason("x").build(), false);
        check(put(DocumentPart.DOCUMENT_PART_PARSED).toBuilder().setAction(WriteAction.WRITE_ACTION_UNSPECIFIED).build(), false);
        var delete = put(DocumentPart.DOCUMENT_PART_BLOBS).toBuilder().setAction(WriteAction.WRITE_ACTION_DELETE);
        check(delete.build(), false);
        check(delete.setDeletedReason("RTBF").build(), true);
        check(PartSource.newBuilder().setSelector(part(DocumentPart.DOCUMENT_PART_CORE))
                .setFromAddress(ADDRESS).build(), false);
        check(PartSource.newBuilder().setSelector(part(DocumentPart.DOCUMENT_PART_CORE))
                .setFromAddress(ADDRESS).setFromRevision(2).build(), true);
    }

    @Test void semanticResultsBindVectorsToSegmentations() throws Exception {
        var segmentation = Segmentation.newBuilder().setSegmentationId("sentences")
                .setKind(SegmentationKind.SEGMENTATION_KIND_SENTENCE)
                .setOffsetUnit(OffsetUnit.OFFSET_UNIT_UTF16_CODE_UNIT)
                .setSource(TextSource.newBuilder().setFieldPath("search_metadata.body")
                        .setBinding(SourceTextBinding.newBuilder().setResolvedTextSha256(SHA).setUtf8Length(5)))
                .addSegments(Segment.newBuilder().setSegmentId("s0").setText("hello").setStart(0).setEnd(5));
        check(segmentation.build(), true);
        check(segmentation.clone().setOffsetUnit(OffsetUnit.OFFSET_UNIT_UNSPECIFIED).build(), false);
        check(segmentation.clone().setContentDigest("bad").build(), false);
        check(segmentation.clone().setSource(segmentation.getSource().toBuilder().clearBinding()).build(), false);
        check(segmentation.clone().setSource(TextSource.newBuilder()
                .setBinding(segmentation.getSource().getBinding())).build(), false);
        check(segmentation.clone().setSegments(0, segmentation.getSegments(0).toBuilder().setStart(6)).build(), false);

        var embeddings = EmbeddingSet.newBuilder().setEmbeddingSetId("m2v").setSegmentationId("sentences")
                .setSegmentationDigest(SHA).setModel("model2vec").setDimensions(2)
                .setSimilarity(Similarity.SIMILARITY_COSINE)
                .addVectors(SegmentVector.newBuilder().setSegmentId("s0").addValues(1f).addValues(0f));
        check(embeddings.build(), true);
        check(embeddings.clone().setDimensions(3).build(), false);
        check(embeddings.clone().setVectors(0, embeddings.getVectors(0).toBuilder().setValues(0, Float.NaN)).build(), false);
        check(embeddings.clone().setVectors(0, embeddings.getVectors(0).toBuilder()
                .setValues(0, Float.POSITIVE_INFINITY)).build(), false);
        check(embeddings.clone().clearSegmentationDigest().build(), false);

        // D5: additive fields on v1 SearchMetadata; field 13 stays until consumers move.
        assertThat(SearchMetadata.getDescriptor().findFieldByNumber(16).getMessageType())
                .isEqualTo(Segmentation.getDescriptor());
        assertThat(SearchMetadata.getDescriptor().findFieldByNumber(17).getMessageType())
                .isEqualTo(EmbeddingSet.getDescriptor());
        assertThat(SearchMetadata.getDescriptor().findFieldByNumber(13).getName()).isEqualTo("semantic_results");
    }

    @Test void validationRecordsNeverClaimTypedValidityWithOpaqueContent() throws Exception {
        var validation = validation();
        check(validation.build(), true);
        check(validation.clone().clearStatus().build(), false);
        check(validation.clone().clearCandidateSha256().build(), false);
        check(validation.clone().addRuleResults(RuleResult.newBuilder().setRuleId("r")
                .setState(RuleResultState.RULE_RESULT_STATE_NOT_EVALUATED)).build(), false);
        check(validation.clone().setStatus(ValidationStatus.VALIDATION_STATUS_OPAQUE_ACCEPTED)
                .addRuleResults(RuleResult.newBuilder().setRuleId("r")
                        .setState(RuleResultState.RULE_RESULT_STATE_NOT_EVALUATED)).build(), true);
        assertThat(RevisionValidation.getDescriptor().findFieldByNumber(7)).isNull();
    }

    @Test void accessionsRequireAuthorityAndTombstonesMakeNoValidationClaim() throws Exception {
        var accession = Accession.newBuilder().setAddress(ADDRESS).setRevision(1)
                .setCommittedAt(Timestamp.newBuilder().setSeconds(1))
                .setManifest(DocumentManifest.newBuilder().setAddress(ADDRESS).setRevision(1))
                .setValidation(validation()).setOwner(intakeOwner());
        check(accession.build(), true);
        check(accession.clone().clearOwner().build(), false);
        check(accession.clone().setValidation(validation().clearProcessAuthority()).build(), false);
        var tombstoneValidation = RevisionValidation.newBuilder().setStatus(ValidationStatus.VALIDATION_STATUS_NOT_APPLICABLE)
                .setProcessAuthority(ProcessAuthorityEvidence.newBuilder().setPrincipal("deleter"));
        var tombstone = accession.clone().clearOwner().setTombstone(true).setValidation(tombstoneValidation)
                .setManifest(accession.getManifest().toBuilder().setTombstone(true));
        check(tombstone.build(), true);
        check(DeleteDocumentRequest.newBuilder().setOperationId("delete").setAddress(ADDRESS)
                .setExpected(ExpectedRevision.newBuilder().setRevision(1)).build(), true);
        check(DeleteDocumentRequest.newBuilder().setOperationId("delete").setAddress(ADDRESS)
                .setExpected(ExpectedRevision.newBuilder().setNone(true)).build(), false);
        check(tombstone.clone().setValidation(tombstoneValidation.clone().clearProcessAuthority()).build(), false);
        check(tombstone.clone().setValidation(validation()).build(), false);
        check(accession.clone().setValidation(tombstoneValidation).build(), false);
        check(tombstone.clone().setValidation(tombstoneValidation.clone().setCandidateSha256(SHA)).build(), false);
        check(tombstone.clone().setValidation(tombstoneValidation.clone().setValidatorProfile("typed")).build(), false);
        check(DeleteDocumentResponse.newBuilder().setOperationId("delete").setAccession(tombstone).build(), true);
        check(accession.clone().setRevision(0).build(), false);
        check(SaveDocumentResponse.newBuilder().setOperationId("op").setAccession(accession).build(), true);
        check(SaveDocumentResponse.newBuilder().setOperationId("op")
                .setAccession(accession.clone().setTombstone(true)).build(), false);
    }

    @Test void eventsSeparateAccessionsFromPurges() throws Exception {
        var event = DocumentEvent.newBuilder().setEventId("e1").setAddress(ADDRESS).setRevision(2)
                .setPreviousRevision(1).setOperationId("op").setOccurredAt(Timestamp.newBuilder().setSeconds(1))
                .setAccessioned(Accessioned.newBuilder()
                        .setValidationStatus(ValidationStatus.VALIDATION_STATUS_TYPED_VALID)
                        .addChanges(PartChange.newBuilder().setSelector(part(DocumentPart.DOCUMENT_PART_PARSED))
                                .setKind(ChangeKind.CHANGE_KIND_REPLACED).setSha256(SHA)));
        check(event.build(), true);
        check(event.clone().setAccessioned(event.getAccessioned().toBuilder()
                .setValidationStatus(ValidationStatus.VALIDATION_STATUS_NOT_APPLICABLE)).build(), false);
        check(event.clone().clearOperationId().build(), false);
        check(event.clone().setRevision(0).build(), false);
        check(event.clone().clearAccessioned().build(), false);
        check(event.clone().setRevision(0).clearOperationId()
                .setPurgeMarked(PurgeMarked.newBuilder().setPurgeId("p1")).build(), true);
        check(event.clone().setPurgeMarked(PurgeMarked.newBuilder().setPurgeId("p1")).build(), false);
    }

    @Test void grantsAndIntentsKeepTheirInvariants() throws Exception {
        var grant = WriterGrant.newBuilder().setGrantId("g1").setAccountId("acct-1")
                .setCredential(WriterCredential.newBuilder().setIssuer("issuer")
                        .setCredentialId("2f1e3c1a-4a7b-4d39-9b7e-0c9ac2d3f001"))
                .setStep(StepWriterScope.newBuilder().setModuleId("parser")).addOpaqueTypeUrls("type.test/x.Y");
        check(grant.build(), true);
        check(grant.clone().setIntake(IntakeOrigin.newBuilder().setDatasourceId("ds-1")).build(), false);
        check(grant.clone().clearStep().build(), false);
        check(grant.clone().addOpaqueTypeUrls("type.test/x.Y").build(), false);
        check(grant.clone().setCredential(grant.getCredential().toBuilder().setCredentialId("nope")).build(), false);
        assertThat(WriterCredential.getDescriptor().findFieldByNumber(3)).isNull();

        var intent = SaveIntent.newBuilder().setEncodingVersion(1).setAddress(ADDRESS)
                .setExpected(ExpectedRevision.newBuilder().setNone(true))
                .setValidationMode(ValidationMode.VALIDATION_MODE_TYPED).setOwner(intakeOwner())
                .setOperationKind("save")
                .addWrites(IntentWrite.newBuilder().setWrite(put(DocumentPart.DOCUMENT_PART_CORE))
                        .setContentSha256(SHA).setContentLength(10));
        check(intent.build(), true);
        check(intent.clone().setOperationKind("delete").build(), false);
        check(intent.clone().setEncodingVersion(2).build(), false);
        check(intent.clone().clearValidationMode().build(), false); // normalized to TYPED before hashing

        var stored = StoredManifest.newBuilder().setAddress(ADDRESS).setRevision(1).setOperationId("op")
                .setIntentSha256(SHA).setValidation(validation()).setOwner(intakeOwner())
                .setOwnership(ai.protomolt.proto.repo.v1.OwnershipContext.newBuilder().setAccountId("acct-1"))
                .setCommittedAt(Timestamp.newBuilder().setSeconds(1))
                .addParts(StoredPartEntry.newBuilder().setEntry(PartManifestEntry.newBuilder()
                        .setSelector(part(DocumentPart.DOCUMENT_PART_CORE)).setState(PartState.PART_STATE_PRESENT)
                        .setSha256(SHA).setSizeBytes(10).setWrittenInRevision(1).setOwner(intakeOwner()))
                        .setRef(StackRef.newBuilder().setStackId("s1").setKey("k").setSha256(SHA).setLength(10)));
        check(stored.build(), true);
        check(stored.clone().clearOwnership().build(), false);
        check(stored.clone().clearOwner().build(), false);
    }

    @Test void outcomesAndErrorsAreExplicit() throws Exception {
        check(GetOperationOutcomeResponse.getDefaultInstance(), false);
        check(GetOperationOutcomeResponse.newBuilder().setNone(NoRecordedOutcome.getDefaultInstance()).build(), true);
        check(RecordedOutcome.newBuilder().setRecordedAt(Timestamp.newBuilder().setSeconds(1)).build(), false);
        check(RecordedOutcome.newBuilder().setRecordedAt(Timestamp.newBuilder().setSeconds(1))
                .setRejection(SaveRejection.newBuilder()
                        .setReason(RejectionReason.REJECTION_REASON_VALIDATION_FAILED)).build(), true);
        check(SaveRejection.getDefaultInstance(), false);
        check(ItemError.newBuilder().setCode(0).build(), false);
        check(ItemError.newBuilder().setCode(10).setConflict(RevisionConflict.newBuilder().setAddress(ADDRESS)
                .setExpected(ExpectedRevision.newBuilder().setRevision(1)).setCurrentRevision(2)).build(), true);
        check(UploadDocumentRequest.getDefaultInstance(), false);
        check(UploadDocumentRequest.newBuilder().setData(ByteString.EMPTY).build(), true);
    }

    private static SaveDocumentRequest.Builder save() {
        return SaveDocumentRequest.newBuilder().setOperationId("op-1")
                .setTarget(DocumentTarget.newBuilder().setAddress(ADDRESS))
                .setExpected(ExpectedRevision.newBuilder().setNone(true))
                .addWrites(put(DocumentPart.DOCUMENT_PART_PARSED)).setOwner(intakeOwner());
    }

    private static RevisionValidation.Builder validation() {
        return RevisionValidation.newBuilder().setStatus(ValidationStatus.VALIDATION_STATUS_TYPED_VALID)
                .setCandidateSha256(SHA).setValidatorProfile("test/v1")
                .setProcessAuthority(ProcessAuthorityEvidence.newBuilder().setPrincipal("intake"));
    }

    private static SaveOwner intakeOwner() {
        return SaveOwner.newBuilder().setIntake(IntakeOrigin.newBuilder().setDatasourceId("ds-1")).build();
    }

    private static OccurrenceLocator locator() {
        return OccurrenceLocator.newBuilder().setPart(part(DocumentPart.DOCUMENT_PART_CORE))
                .addSteps(ai.protomolt.proto.repo.v1.RepositorySchemaOccurrenceStep.newBuilder().setFieldNumber(4))
                .build();
    }

    private static PartWrite put(DocumentPart part) {
        return PartWrite.newBuilder().setSelector(part(part)).setAction(WriteAction.WRITE_ACTION_PUT).build();
    }

    private static PartSelector part(DocumentPart part) {
        return PartSelector.newBuilder().setPart(part).build();
    }

    private static void check(Message value, boolean valid) throws Exception {
        var result = VALIDATOR.validate(value);
        assertThat(result.valid()).as("generated %s: %s", value, result).isEqualTo(valid);
        var dynamic = DynamicMessage.parseFrom(value.getDescriptorForType(), value.toByteString());
        assertThat(VALIDATOR.validate(dynamic).valid()).as("dynamic %s", value).isEqualTo(valid);
    }
}
