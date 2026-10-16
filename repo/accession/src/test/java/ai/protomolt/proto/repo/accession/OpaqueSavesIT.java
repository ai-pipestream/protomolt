package ai.protomolt.proto.repo.accession;

import static ai.protomolt.proto.repo.accession.AccessionFixture.*;
import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.ParseStatus;
import ai.protomolt.proto.repo.v1.ParserResult;
import ai.protomolt.proto.repo.v1.RepositorySchemaOccurrenceStep;
import ai.protomolt.proto.repo.v1.WriteProvenance;
import ai.protomolt.proto.repo.v2.*;
import ai.protomolt.proto.repo.v2.DocumentPart;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import java.sql.DriverManager;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Explicit opaque occurrences (requirements §9, §15.6, §15.19, §15.22, §15.23) on
 * real PostgreSQL and LocalStack S3, through the engine.
 */
class OpaqueSavesIT {
    private static final String OPAQUE_URL = "type.test/opaque.Widget";
    private static final ByteString OPAQUE_VALUE = ByteString.copyFromUtf8("opaque-payload-v1");

    private AccessionFixture f;
    private String account;
    private UUID credential;
    private RepositoryCaller caller;

    @BeforeEach void open() throws Exception {
        f = new AccessionFixture();
        account = "acct-" + UUID.randomUUID();
        credential = UUID.randomUUID();
        f.catalog.registerCredential(ISSUER, credential, "chunker");
        caller = writer(account, "chunker", credential, 1);
    }

    @AfterEach void close() { f.close(); }

    // §9: OPAQUE_ALLOWED is refused when the host policy is off, and recorded.
    @Test void hostPolicyOffRejectsOpaqueAndRecordsIt() {
        f.opaqueStepGrant(account, credential, "chunker", true, "grant-producer", OPAQUE_URL);
        var address = address(account, "doc-host-off");
        assertRejected(() -> f.saves.save(caller, opaqueCreate("op-1", address, "doc-host-off")),
                Status.Code.PERMISSION_DENIED, RejectionReason.REJECTION_REASON_OPAQUE_NOT_ALLOWED);
        assertThat(f.catalog.outcome(account, "op-1")).isPresent();
        assertThat(f.catalog.current(address)).isEmpty();
    }

    // §15.6: an attested occurrence is accepted undecoded under its producer grant.
    @Test void creationAcceptsOpaqueWithProducerGrant() {
        f.opaqueStepGrant(account, credential, "chunker", true, "grant-producer", OPAQUE_URL);
        var address = address(account, "doc-opaque");
        var saved = f.opaqueSaves.save(caller, opaqueCreate("op-1", address, "doc-opaque"));

        var validation = saved.getAccession().getValidation();
        assertThat(validation.getStatus()).isEqualTo(ValidationStatus.VALIDATION_STATUS_OPAQUE_ACCEPTED);
        assertThat(validation.getOpaqueOccurrencesList()).singleElement().satisfies(occurrence -> {
            assertThat(occurrence.getOccurrence()).isEqualTo(coreLocator());
            assertThat(occurrence.getTypeUrl()).isEqualTo(OPAQUE_URL);
            assertThat(occurrence.getSha256()).isEqualTo(DocumentSaves.sha256(OPAQUE_VALUE));
            assertThat(occurrence.getLength()).isEqualTo(OPAQUE_VALUE.size());
            assertThat(occurrence.getOwner().getModuleId()).isEqualTo("chunker");
            assertThat(occurrence.getWriterGrant().getWriterGrantId()).isEqualTo("grant-producer");
            assertThat(occurrence.hasCarriedFrom()).isFalse();
        });
        assertThat(validation.getRuleResultsList()).singleElement().satisfies(rule ->
                assertThat(rule.getState()).isEqualTo(RuleResultState.RULE_RESULT_STATE_NOT_EVALUATED));
        var stored = f.catalog.manifest(address, 1).orElseThrow();
        assertThat(entry(stored, DocumentPart.DOCUMENT_PART_CORE).getEntry().getOpaque()).isTrue();
        assertThat(entry(stored, DocumentPart.DOCUMENT_PART_PARSED).getEntry().getOpaque()).isFalse();

        assertThatThrownBy(() -> f.reads.getDocument(caller, GetDocumentRequest.newBuilder().setAddress(address).build()))
                .isInstanceOfSatisfying(AccessionException.class, failure ->
                        assertThat(failure.code()).isEqualTo(Status.Code.FAILED_PRECONDITION));
        var read = f.reads.getDocument(caller, GetDocumentRequest.newBuilder().setAddress(address)
                .setAllowOpaque(true).build());
        assertThat(read.getDocument().getStructuredData().getValue()).isEqualTo(OPAQUE_VALUE);

        var replay = f.opaqueSaves.save(caller, opaqueCreate("op-1", address, "doc-opaque"));
        assertThat(replay.getReplayed()).isTrue();
    }

    // §9: OPAQUE_ALLOWED with no opaque occurrence stays typed-valid.
    @Test void opaqueModeWithoutOccurrencesStaysTypedValid() {
        f.opaqueStepGrant(account, credential, "chunker", true, "grant-producer", OPAQUE_URL);
        var address = address(account, "doc-typed");
        var request = create("op-1", address, document(account, "doc-typed", "v1", "chunker"))
                .setOwner(stepOwner("chunker"))
                .setValidationMode(ValidationMode.VALIDATION_MODE_OPAQUE_ALLOWED).build();
        var saved = f.opaqueSaves.save(caller, request);
        assertThat(saved.getAccession().getValidation().getStatus())
                .isEqualTo(ValidationStatus.VALIDATION_STATUS_TYPED_VALID);
        assertThat(saved.getAccession().getValidation().getOpaqueOccurrencesList()).isEmpty();
    }

    // §9: an unattested Any stays typed; there is no fallback to opaque.
    @Test void unattestedUnknownAnyStaysTypedFailure() {
        f.opaqueStepGrant(account, credential, "chunker", true, "grant-producer", OPAQUE_URL);
        var address = address(account, "doc-unattested");
        var request = create("op-1", address, opaqueDocument("doc-unattested"))
                .setOwner(stepOwner("chunker"))
                .setValidationMode(ValidationMode.VALIDATION_MODE_OPAQUE_ALLOWED).build();
        assertRejected(() -> f.opaqueSaves.save(caller, request), Status.Code.FAILED_PRECONDITION,
                RejectionReason.REJECTION_REASON_SCHEMA_MISSING);
    }

    // §9: a producer without a grant for the exact type is OPAQUE_UNOWNED.
    @Test void producerGrantWithoutTypeIsUnowned() {
        f.stepGrant(account, credential, "chunker", true);
        var address = address(account, "doc-no-type");
        assertRejected(() -> f.opaqueSaves.save(caller, opaqueCreate("op-1", address, "doc-no-type")),
                Status.Code.FAILED_PRECONDITION, RejectionReason.REJECTION_REASON_OPAQUE_UNOWNED);
        assertThat(f.catalog.outcome(account, "op-1")).isPresent();
    }

    // §7/§9: the attestation must match the actual Any at its locator.
    @Test void attestationTypeMismatchIsUnowned() {
        f.opaqueStepGrant(account, credential, "chunker", true, "grant-producer", "type.test/opaque.Other");
        var address = address(account, "doc-mismatch");
        var request = create("op-1", address, opaqueDocument("doc-mismatch")).setOwner(stepOwner("chunker"))
                .setValidationMode(ValidationMode.VALIDATION_MODE_OPAQUE_ALLOWED)
                .addOpaqueAttestations(attestation("type.test/opaque.Other")).build();
        assertRejected(() -> f.opaqueSaves.save(caller, request), Status.Code.FAILED_PRECONDITION,
                RejectionReason.REJECTION_REASON_OPAQUE_UNOWNED);
    }

    // §9: newly produced opaque content is creation-only, grant or not.
    @Test void newOpaqueOnEditIsUnownedEvenWithProducerGrant() {
        f.opaqueStepGrant(account, credential, "chunker", true, "grant-producer", OPAQUE_URL);
        var address = address(account, "doc-edit-new");
        f.saves.save(processAuthority(), create("op-1", address, document(account, "doc-edit-new", "v1", "chunker")).build());
        var edit = SaveDocumentRequest.newBuilder().setOperationId("op-2")
                .setTarget(DocumentTarget.newBuilder().setAddress(address)).setExpected(at(1))
                .setDocument(opaqueCore("doc-edit-new"))
                .addWrites(put(DocumentPart.DOCUMENT_PART_CORE)).setOwner(stepOwner("chunker"))
                .setValidationMode(ValidationMode.VALIDATION_MODE_OPAQUE_ALLOWED)
                .addOpaqueAttestations(attestation(OPAQUE_URL)).build();
        assertRejected(() -> f.opaqueSaves.save(caller, edit), Status.Code.FAILED_PRECONDITION,
                RejectionReason.REJECTION_REASON_OPAQUE_UNOWNED);
        assertThat(f.catalog.current(address).orElseThrow().revision()).isEqualTo(1);
    }

    // §15.19: an unchanged opaque part carries whole after its producer grant is revoked.
    @Test void implicitWholePartCarrySurvivesProducerRevocation() {
        f.opaqueStepGrant(account, credential, "chunker", true, "grant-save");
        f.opaqueStepGrant(account, credential, "chunker", false, "grant-producer", OPAQUE_URL);
        var address = address(account, "doc-carry");
        var saved = f.opaqueSaves.save(caller, opaqueCreate("op-1", address, "doc-carry"));
        var original = saved.getAccession().getValidation().getOpaqueOccurrencesList().getFirst();
        assertThat(original.getWriterGrant().getWriterGrantId()).isEqualTo("grant-producer");
        f.catalog.revokeGrant("grant-producer");

        var parsed = Document.newBuilder().putParserResults("docling", ParserResult.newBuilder()
                .setParserName("docling").setParserVersion("2").setStatus(ParseStatus.PARSE_STATUS_OK).build()).build();
        var edit = SaveDocumentRequest.newBuilder().setOperationId("op-2")
                .setTarget(DocumentTarget.newBuilder().setAddress(address)).setExpected(at(1)).setDocument(parsed)
                .addWrites(put(DocumentPart.DOCUMENT_PART_PARSED)).setOwner(stepOwner("chunker"))
                .setValidationMode(ValidationMode.VALIDATION_MODE_OPAQUE_ALLOWED).build();
        var second = f.opaqueSaves.save(caller, edit);
        assertThat(second.getAccession().getValidation().getStatus())
                .isEqualTo(ValidationStatus.VALIDATION_STATUS_OPAQUE_ACCEPTED);
        assertThat(second.getAccession().getValidation().getOpaqueOccurrencesList()).containsExactly(original);
    }

    // §9: explicit carried_from moves an occurrence inside a changed part, byte-identically.
    @Test void explicitCarriedFromPreservesOriginalAttribution() {
        f.opaqueStepGrant(account, credential, "chunker", true, "grant-save");
        f.opaqueStepGrant(account, credential, "chunker", false, "grant-producer", OPAQUE_URL);
        var address = address(account, "doc-move");
        var saved = f.opaqueSaves.save(caller, opaqueCreate("op-1", address, "doc-move"));
        var original = saved.getAccession().getValidation().getOpaqueOccurrencesList().getFirst();
        f.catalog.revokeGrant("grant-producer");

        var carried = attestation(OPAQUE_URL).toBuilder().setCarriedFrom(OpaqueSource.newBuilder()
                .setSource(SourceRevision.newBuilder().setAddress(address).setRevision(1))
                .setOccurrence(coreLocator())).build();
        var edit = SaveDocumentRequest.newBuilder().setOperationId("op-2")
                .setTarget(DocumentTarget.newBuilder().setAddress(address)).setExpected(at(1))
                .setDocument(opaqueCore("doc-move"))
                .addWrites(put(DocumentPart.DOCUMENT_PART_CORE)).setOwner(stepOwner("chunker"))
                .setValidationMode(ValidationMode.VALIDATION_MODE_OPAQUE_ALLOWED)
                .addOpaqueAttestations(carried).build();
        var second = f.opaqueSaves.save(caller, edit);
        assertThat(second.getAccession().getValidation().getOpaqueOccurrencesList()).singleElement()
                .satisfies(occurrence -> {
                    assertThat(occurrence.getOwner()).isEqualTo(original.getOwner());
                    assertThat(occurrence.getWriterGrant()).isEqualTo(original.getWriterGrant());
                    assertThat(occurrence.getCarriedFrom()).isEqualTo(carried.getCarriedFrom());
                });

        var changed = opaqueCore("doc-move").toBuilder().setStructuredData(Any.newBuilder()
                .setTypeUrl(OPAQUE_URL).setValue(ByteString.copyFromUtf8("different bytes"))).build();
        var tampered = edit.toBuilder().setOperationId("op-3").setExpected(at(2)).setDocument(changed).build();
        assertRejected(() -> f.opaqueSaves.save(caller, tampered), Status.Code.FAILED_PRECONDITION,
                RejectionReason.REJECTION_REASON_OPAQUE_UNOWNED);
    }

    // §5/§9: a carried_from naming an unreadable revision is SOURCE_NOT_FOUND.
    @Test void carriedFromUnreadableSourceIsNotFound() {
        f.opaqueStepGrant(account, credential, "chunker", true, "grant-producer", OPAQUE_URL);
        var source = address(account, "doc-origin");
        f.opaqueSaves.save(caller, opaqueCreate("op-1", source, "doc-origin"));
        var derived = address(account, "doc-derived");
        var carried = attestation(OPAQUE_URL).toBuilder().setCarriedFrom(OpaqueSource.newBuilder()
                .setSource(SourceRevision.newBuilder().setAddress(source).setRevision(99))
                .setOccurrence(coreLocator())).build();
        var request = create("op-2", derived, opaqueDocument("doc-derived")).setOwner(stepOwner("chunker"))
                .setValidationMode(ValidationMode.VALIDATION_MODE_OPAQUE_ALLOWED)
                .addOpaqueAttestations(carried)
                .addDerivedFrom(SourceRevision.newBuilder().setAddress(source).setRevision(1)).build();
        assertRejected(() -> f.opaqueSaves.save(caller, request), Status.Code.FAILED_PRECONDITION,
                RejectionReason.REJECTION_REASON_SOURCE_NOT_FOUND);
    }

    // §15.23: overlapping producer grants resolve by unsigned grant ID; intake may
    // attest only through an eligible step grant of its own credential.
    @Test void grantOrderDeterminesProducerAndIntakeNeedsStepGrant() {
        f.intakeGrant(account, credential, "ds-1", true);
        f.opaqueStepGrant(account, credential, "chunker", false, "grant-b", OPAQUE_URL);
        f.opaqueStepGrant(account, credential, "chunker", false, "grant-a", OPAQUE_URL);
        var first = address(account, "doc-order-1");
        var saved = f.opaqueSaves.save(caller, intakeOpaqueCreate("op-1", first, "doc-order-1"));
        assertThat(saved.getAccession().getValidation().getOpaqueOccurrencesList().getFirst()
                .getWriterGrant().getWriterGrantId()).isEqualTo("grant-a");

        f.catalog.revokeGrant("grant-a");
        var secondAddress = address(account, "doc-order-2");
        var second = f.opaqueSaves.save(caller, intakeOpaqueCreate("op-2", secondAddress, "doc-order-2"));
        assertThat(second.getAccession().getValidation().getOpaqueOccurrencesList().getFirst()
                .getWriterGrant().getWriterGrantId()).isEqualTo("grant-b");

        var intakeOnly = UUID.randomUUID();
        f.catalog.registerCredential(ISSUER, intakeOnly, "crawler");
        f.intakeGrant(account, intakeOnly, "ds-1", true);
        var denied = address(account, "doc-order-3");
        assertRejected(() -> f.opaqueSaves.save(writer(account, "crawler", intakeOnly, 1),
                intakeOpaqueCreate("op-3", denied, "doc-order-3")),
                Status.Code.FAILED_PRECONDITION, RejectionReason.REJECTION_REASON_OPAQUE_UNOWNED);
    }

    // Decision 46: derived creation still needs allow_create on the save grant.
    @Test void derivedCreationNeedsAllowCreate() {
        var source = address(account, "doc-lineage-src");
        f.saves.save(processAuthority(), create("op-src", source, document(account, "doc-lineage-src", "v1", "chunker")).build());
        f.stepGrant(account, credential, "chunker", false);
        var derived = address(account, "doc-derived-ac");
        var request = create("op-1", derived, document(account, "doc-derived-ac", "v1", "chunker"))
                .setOwner(stepOwner("chunker"))
                .addDerivedFrom(SourceRevision.newBuilder().setAddress(source).setRevision(1)).build();
        assertRejected(() -> f.saves.save(caller, request), Status.Code.PERMISSION_DENIED,
                RejectionReason.REJECTION_REASON_WRITE_DENIED);
        assertThat(f.catalog.outcome(account, "op-1")).isEmpty();

        f.stepGrant(account, credential, "chunker", true);
        assertThat(f.saves.save(caller, request).getAccession().getRevision()).isEqualTo(1);
    }

    // §15.22: corrupt retained grant evidence is DATA_LOSS, never a live substitution.
    @Test void corruptRetainedGrantEvidenceIsDataLoss() throws Exception {
        f.opaqueStepGrant(account, credential, "chunker", true, "grant-producer", OPAQUE_URL);
        var address = address(account, "doc-corrupt");
        f.opaqueSaves.save(caller, opaqueCreate("op-1", address, "doc-corrupt"));
        corruptStoredGrantEvidence();

        var parsed = Document.newBuilder().putParserResults("docling", ParserResult.newBuilder()
                .setParserName("docling").setParserVersion("2").setStatus(ParseStatus.PARSE_STATUS_OK).build()).build();
        var edit = SaveDocumentRequest.newBuilder().setOperationId("op-2")
                .setTarget(DocumentTarget.newBuilder().setAddress(address)).setExpected(at(1)).setDocument(parsed)
                .addWrites(put(DocumentPart.DOCUMENT_PART_PARSED)).setOwner(stepOwner("chunker"))
                .setValidationMode(ValidationMode.VALIDATION_MODE_OPAQUE_ALLOWED).build();
        assertThatThrownBy(() -> f.opaqueSaves.save(caller, edit)).isInstanceOfSatisfying(
                AccessionException.class, failure -> assertThat(failure.code()).isEqualTo(Status.Code.DATA_LOSS));
        assertThat(f.catalog.current(address).orElseThrow().revision()).isEqualTo(1);
        assertThat(f.catalog.outcome(account, "op-2")).isEmpty();
    }

    // ------------------------------------------------------------------ helpers

    private Document opaqueDocument(String doc) {
        return document(account, doc, "v1", "chunker").toBuilder()
                .setStructuredData(Any.newBuilder().setTypeUrl(OPAQUE_URL).setValue(OPAQUE_VALUE)).build();
    }

    /** A CORE-only document carrying the opaque Any, for CORE-only edits. */
    private Document opaqueCore(String doc) {
        return Document.newBuilder().setDocId(doc).setOwnership(ownership(account, "chunker"))
                .setSearchMetadata(ai.protomolt.proto.repo.v1.SearchMetadata.newBuilder().setTitle("v2"))
                .setStructuredData(Any.newBuilder().setTypeUrl(OPAQUE_URL).setValue(OPAQUE_VALUE)).build();
    }

    private SaveDocumentRequest opaqueCreate(String operation, NodeAddress address, String doc) {
        return create(operation, address, opaqueDocument(doc)).setOwner(stepOwner("chunker"))
                .setValidationMode(ValidationMode.VALIDATION_MODE_OPAQUE_ALLOWED)
                .addOpaqueAttestations(attestation(OPAQUE_URL)).build();
    }

    private SaveDocumentRequest intakeOpaqueCreate(String operation, NodeAddress address, String doc) {
        return create(operation, address, document(account, doc, "v1", "chunker", "crawler").toBuilder()
                .setStructuredData(Any.newBuilder().setTypeUrl(OPAQUE_URL).setValue(OPAQUE_VALUE)).build())
                .setValidationMode(ValidationMode.VALIDATION_MODE_OPAQUE_ALLOWED)
                .addOpaqueAttestations(attestation(OPAQUE_URL).toBuilder()
                        .setOwner(WriteProvenance.newBuilder().setModuleId("chunker")
                                .setNodeId("node-1").setGraphId("graph-1").setGraphVersion(3))).build();
    }

    private static OccurrenceLocator coreLocator() {
        return OccurrenceLocator.newBuilder()
                .setPart(PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE))
                .addSteps(RepositorySchemaOccurrenceStep.newBuilder().setFieldNumber(4)).build();
    }

    private static OpaqueAttestation attestation(String typeUrl) {
        return OpaqueAttestation.newBuilder().setOccurrence(coreLocator()).setTypeUrl(typeUrl).build();
    }

    /** Rewrites revision 1's stored manifest with a wrong grant-evidence digest, past the immutability trigger. */
    private void corruptStoredGrantEvidence() throws Exception {
        try (var connection = DriverManager.getConnection(f.url,
                AccessionFixture.POSTGRES.getUsername(), AccessionFixture.POSTGRES.getPassword())) {
            byte[] bytes;
            try (var select = connection.prepareStatement("SELECT manifest FROM catalog_revisions WHERE revision = 1");
                 var rows = select.executeQuery()) {
                assertThat(rows.next()).isTrue();
                bytes = rows.getBytes(1);
            }
            var stored = StoredManifest.parseFrom(bytes).toBuilder();
            var validation = stored.getValidationBuilder();
            var occurrence = validation.getOpaqueOccurrencesBuilder(0);
            var sha = occurrence.getWriterGrant().getWriterGrantSha256();
            occurrence.getWriterGrantBuilder()
                    .setWriterGrantSha256((sha.startsWith("a") ? "b" : "a") + sha.substring(1));
            var corrupt = stored.build().toByteString();
            connection.createStatement().execute("ALTER TABLE catalog_revisions DISABLE TRIGGER catalog_revisions_immutable");
            try (var update = connection.prepareStatement(
                    "UPDATE catalog_revisions SET manifest = ?, manifest_sha256 = ? WHERE revision = 1")) {
                update.setBytes(1, corrupt.toByteArray());
                update.setString(2, DocumentSaves.sha256(corrupt));
                assertThat(update.executeUpdate()).isEqualTo(1);
            } finally {
                connection.createStatement().execute("ALTER TABLE catalog_revisions ENABLE TRIGGER catalog_revisions_immutable");
            }
        }
    }

    private static StoredPartEntry entry(StoredManifest manifest, DocumentPart part) {
        return manifest.getPartsList().stream().filter(p -> p.getEntry().getSelector().getPart() == part)
                .findFirst().orElseThrow();
    }

    private static void assertRejected(ThrowingCall call, Status.Code code, RejectionReason reason) {
        assertThatThrownBy(call::run).isInstanceOfSatisfying(AccessionException.class, failure -> {
            assertThat(failure.code()).isEqualTo(code);
            if (reason != null) assertThat(((SaveRejection) failure.detail().orElseThrow()).getReason()).isEqualTo(reason);
        });
    }

    @FunctionalInterface private interface ThrowingCall { void run() throws Exception; }
}
