package ai.protomolt.proto.repo.accession;

import static ai.protomolt.proto.repo.accession.AccessionFixture.*;
import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.repo.semantic.v1.EmbeddingSetKey;
import ai.protomolt.proto.repo.semantic.v1.Segmentation;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.ParseStatus;
import ai.protomolt.proto.repo.v1.ParserResult;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.repo.v2.*;
import ai.protomolt.proto.repo.v2.DocumentPart;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnknownFieldSet;
import io.grpc.Status;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/** Slice 1 acceptance (requirements §15) on real PostgreSQL and LocalStack S3, through the engine. */
class DocumentSavesIT {
    private AccessionFixture f;
    private String account;

    @BeforeEach void open() throws Exception {
        f = new AccessionFixture();
        account = "acct-" + UUID.randomUUID();
    }

    @AfterEach void close() { f.close(); }

    // §15.1 (on PARSED): a partial save writes one object and reads none.
    @Test void partialSaveCarriesSiblingReferencesWithoutReadingOrWritingThem() {
        var address = address(account, "doc-1");
        var first = f.saves.save(processAuthority(), create("op-1", address, document(account, "doc-1", "v1")).build());
        var before = f.catalog.manifest(address, 1).orElseThrow();
        f.stack.reset();

        var parsed = Document.newBuilder().putParserResults("docling", ParserResult.newBuilder()
                .setParserName("docling").setParserVersion("2").setStatus(ParseStatus.PARSE_STATUS_OK).build()).build();
        var second = f.saves.save(processAuthority(), SaveDocumentRequest.newBuilder().setOperationId("op-2")
                .setTarget(DocumentTarget.newBuilder().setAddress(address)).setExpected(at(1)).setDocument(parsed)
                .addWrites(put(DocumentPart.DOCUMENT_PART_PARSED)).setOwner(intakeOwner()).build());

        assertThat(f.stack.puts.get()).isEqualTo(1);
        assertThat(f.stack.opens.get()).as("no sibling is read; no rule spans parts").isZero();
        var after = f.catalog.manifest(address, 2).orElseThrow();
        for (var part : List.of(DocumentPart.DOCUMENT_PART_CORE, DocumentPart.DOCUMENT_PART_BLOBS)) {
            assertThat(entry(after, part).getRef()).isEqualTo(entry(before, part).getRef());
            assertThat(entry(after, part).getEntry().getWrittenInRevision()).isEqualTo(1);
        }
        assertThat(entry(after, DocumentPart.DOCUMENT_PART_PARSED).getRef().getKey())
                .isNotEqualTo(entry(before, DocumentPart.DOCUMENT_PART_PARSED).getRef().getKey());
        assertThat(second.getAccession().getRevision()).isEqualTo(2);
        assertThat(first.getAccession().getRevision()).isEqualTo(1);

        var read = f.reads.getDocument(processAuthority(), GetDocumentRequest.newBuilder().setAddress(address).build());
        assertThat(read.getDocument().getSearchMetadata().getTitle()).isEqualTo("v1");
        assertThat(read.getDocument().getParserResultsMap()).containsOnlyKeys("docling");
        var events = f.catalog.events(address);
        assertThat(events.get(1).getAccessioned().getChangesList()).singleElement().satisfies(change -> {
            assertThat(change.getSelector().getPart()).isEqualTo(DocumentPart.DOCUMENT_PART_PARSED);
            assertThat(change.getKind()).isEqualTo(ChangeKind.CHANGE_KIND_REPLACED);
        });
    }

    // §15.3
    @Test void concurrentSavesWithOneExpectedRevisionCommitOnce() throws Exception {
        var address = address(account, "doc-race");
        f.saves.save(processAuthority(), create("op-0", address, document(account, "doc-race", "v1")).build());
        var results = race(2, index -> f.saves.save(processAuthority(), titleSave("op-race-" + index, address, 1,
                "racer " + index)));
        assertThat(results.stream().filter(result -> result instanceof SaveDocumentResponse)).hasSize(1);
        assertThat(results.stream().filter(result -> result instanceof AccessionException)).singleElement()
                .satisfies(error -> {
                    var failure = (AccessionException) error;
                    assertThat(failure.code()).isEqualTo(Status.Code.ABORTED);
                    assertThat(((RevisionConflict) failure.detail().orElseThrow()).getCurrentRevision()).isEqualTo(2);
                });
        assertThat(f.catalog.current(address).orElseThrow().revision()).isEqualTo(2);
    }

    // §15.4, §15.12
    @Test void replaysReturnTheRecordedOutcomeAndChangedIntentFails() throws Exception {
        var address = address(account, "doc-replay");
        var request = create("op-a", address, document(account, "doc-replay", "v1")).build();
        var original = f.saves.save(processAuthority(), request);
        f.saves.save(processAuthority(), titleSave("op-b", address, 1, "v2"));

        var replay = f.saves.save(processAuthority(), request); // expected "none" is now stale
        assertThat(replay.getReplayed()).isTrue();
        assertThat(replay.getAccession()).isEqualTo(original.getAccession());

        var changed = request.toBuilder().setDocument(document(account, "doc-replay", "other")).build();
        assertThatThrownBy(() -> f.saves.save(processAuthority(), changed)).isInstanceOfSatisfying(AccessionException.class,
                failure -> {
                    assertThat(failure.code()).isEqualTo(Status.Code.ALREADY_EXISTS);
                    assertThat(failure.detail().orElseThrow()).isInstanceOf(OperationReuse.class);
                });

        var same = titleSave("op-c", address, 2, "v3");
        var results = race(2, index -> f.saves.save(processAuthority(), same));
        assertThat(results).allSatisfy(result -> assertThat(result).isInstanceOf(SaveDocumentResponse.class));
        assertThat(results.stream().map(result -> ((SaveDocumentResponse) result).getAccession()).distinct()).hasSize(1);
        assertThat(f.catalog.current(address).orElseThrow().revision()).isEqualTo(3);
        assertThat(f.catalog.revisions(address, Long.MAX_VALUE, 10)).hasSize(3);
    }

    // §15.5
    @Test void invalidComposedDocumentsDoNotCommitAndTheirRejectionIsRecorded() {
        var address = address(account, "doc-invalid");
        var invalid = document(account, "doc-invalid", "v1").toBuilder()
                .setStructuredData(Any.pack(EmbeddingSetKey.newBuilder().setSegmentationId("s").build())).build();
        var request = create("op-invalid", address, invalid).build();
        assertRejected(() -> f.saves.save(processAuthority(), request), Status.Code.INVALID_ARGUMENT,
                RejectionReason.REJECTION_REASON_VALIDATION_FAILED);
        assertThat(f.stack.puts.get()).as("validation runs before any upload").isZero();
        assertThat(f.catalog.current(address)).isEmpty();
        assertThat(f.catalog.outcome(account, "op-invalid").orElseThrow().outcome().hasRejection()).isTrue();
        assertRejected(() -> f.saves.save(processAuthority(), request), Status.Code.INVALID_ARGUMENT,
                RejectionReason.REJECTION_REASON_VALIDATION_FAILED);

        var missing = document(account, "doc-invalid", "v1").toBuilder().setStructuredData(Any.newBuilder()
                .setTypeUrl("type.googleapis.com/unregistered.Thing").setValue(ByteString.copyFromUtf8("x"))).build();
        assertRejected(() -> f.saves.save(processAuthority(), create("op-missing", address, missing).build()),
                Status.Code.FAILED_PRECONDITION, RejectionReason.REJECTION_REASON_SCHEMA_MISSING);

        var garbage = document(account, "doc-invalid", "v1").toBuilder().setStructuredData(Any.newBuilder()
                .setTypeUrl(KEY_URL).setValue(ByteString.copyFrom(new byte[]{(byte) 0xff, (byte) 0xff, 0x7f}))).build();
        assertRejected(() -> f.saves.save(processAuthority(), create("op-garbage", address, garbage).build()),
                Status.Code.INVALID_ARGUMENT, RejectionReason.REJECTION_REASON_INVALID_BYTES);
        assertThat(f.catalog.current(address)).isEmpty();
    }

    // §15.7
    @Test void contentAndInt64ValuesSurviveSaveRestartAndRead() throws Exception {
        var address = address(account, "doc-fidelity");
        var document = document(account, "doc-fidelity", "fidelity");
        f.saves.save(processAuthority(), create("op-1", address, document).build());
        try (var restarted = f.restarted()) {
            var read = restarted.reads.getDocument(processAuthority(), GetDocumentRequest.newBuilder().setAddress(address).build());
            assertThat(read.getDocument()).isEqualTo(document);
            assertThat(read.getDocument().getBlobBag().getBlob().getSizeBytes()).isEqualTo(Long.MAX_VALUE - 7);
            assertThat(read.getDocument().getStructuredData().unpack(com.google.protobuf.Int64Value.class).getValue())
                    .isEqualTo(Long.MAX_VALUE);
            assertThat(read.getValidation().getResolvedOccurrencesList()).singleElement()
                    .satisfies(occurrence -> assertThat(occurrence.getSchema().getTypeUrl()).isEqualTo(INT64_URL));
        }
    }

    // §15.7: typed admission cannot validate unknown fields, so it refuses them instead of dropping them.
    @Test void unknownFieldsAreRefusedByTypedAdmissionNotDropped() {
        var address = address(account, "doc-unknown");
        var unknown = UnknownFieldSet.newBuilder().addField(4000, UnknownFieldSet.Field.newBuilder().addVarint(7).build()).build();
        var document = document(account, "doc-unknown", "u").toBuilder().setSearchMetadata(
                SearchMetadata.newBuilder().setTitle("u").setUnknownFields(unknown)).build();
        assertRejected(() -> f.saves.save(processAuthority(), create("op-u", address, document).build()),
                Status.Code.INVALID_ARGUMENT, RejectionReason.REJECTION_REASON_VALIDATION_FAILED);
        assertThat(f.catalog.current(address)).isEmpty();
    }

    // §15.8
    @Test void historyPagesAndExactRevisionReadsReturnOldContent() {
        var address = address(account, "doc-history");
        f.saves.save(processAuthority(), create("op-1", address, document(account, "doc-history", "v1")).build());
        f.saves.save(processAuthority(), titleSave("op-2", address, 1, "v2"));
        f.saves.save(processAuthority(), titleSave("op-3", address, 2, "v3"));

        var page = f.reads.listRevisions(processAuthority(), ListRevisionsRequest.newBuilder().setAddress(address)
                .setPageSize(2).build());
        assertThat(page.getRevisionsList()).extracting(RevisionSummary::getRevision).containsExactly(3L, 2L);
        assertThat(page.getRevisions(0).getChangesList()).extracting(change -> change.getSelector().getPart())
                .containsExactly(DocumentPart.DOCUMENT_PART_CORE);
        var rest = f.reads.listRevisions(processAuthority(), ListRevisionsRequest.newBuilder().setAddress(address)
                .setPageSize(2).setPageToken(page.getNextPageToken()).build());
        assertThat(rest.getRevisionsList()).extracting(RevisionSummary::getRevision).containsExactly(1L);
        assertThat(rest.getNextPageToken()).isEmpty();

        var old = f.reads.getDocument(processAuthority(), GetDocumentRequest.newBuilder().setAddress(address)
                .setRevision(RevisionSelector.newBuilder().setRevision(1)).build());
        assertThat(old.getDocument().getSearchMetadata().getTitle()).isEqualTo("v1");
        assertThat(old.getManifest().getRevision()).isEqualTo(1);
        var latest = f.reads.getDocument(processAuthority(), GetDocumentRequest.newBuilder().setAddress(address).build());
        assertThat(latest.getDocument().getSearchMetadata().getTitle()).isEqualTo("v3");
        assertThatThrownBy(() -> f.reads.getDocument(processAuthority(), GetDocumentRequest.newBuilder().setAddress(address)
                .setRevision(RevisionSelector.newBuilder().setRevision(9)).build()))
                .isInstanceOfSatisfying(AccessionException.class, failure ->
                        assertThat(failure.code()).isEqualTo(Status.Code.NOT_FOUND));

        var selected = f.reads.getDocument(processAuthority(), GetDocumentRequest.newBuilder().setAddress(address)
                .setSelection(PartSelection.newBuilder().addSelectors(PartSelector.newBuilder()
                        .setPart(DocumentPart.DOCUMENT_PART_PARSED))).build());
        assertThat(selected.getDocument().hasSearchMetadata()).isFalse();
        assertThat(selected.getDocument().getParserResultsMap()).containsKey("tika");
        assertThat(selected.getManifest().getPartsList()).extracting(PartManifestEntry::getContentIncluded)
                .containsExactly(false, false, true);
    }

    // §15.9
    @Test void eachAccessionWritesOneEventAndRelayRetriesDeliverDuplicatesOnly() throws Exception {
        var address = address(account, "doc-events");
        f.saves.save(processAuthority(), create("op-1", address, document(account, "doc-events", "v1")).build());
        f.saves.save(processAuthority(), titleSave("op-2", address, 1, "v2"));
        var events = f.catalog.events(address);
        assertThat(events).extracting(DocumentEvent::getRevision).containsExactly(1L, 2L);
        assertThat(events.getFirst().getAccessioned().getChangesList()).extracting(PartChange::getKind)
                .containsOnly(ChangeKind.CHANGE_KIND_ADDED).hasSize(3);

        var delivered = new ArrayList<String>();
        var failOnce = new java.util.concurrent.atomic.AtomicBoolean(true);
        var relay = new OutboxRelay(f.catalog, (key, event) -> {
            delivered.add(event.getEventId());
            if (event.getRevision() == 2 && failOnce.getAndSet(false))
                throw new IOException("acknowledgement lost after delivery");
        });
        assertThat(relay.relayOnce(10)).isEqualTo(1);
        assertThat(relay.relayOnce(10)).isEqualTo(1);
        assertThat(relay.relayOnce(10)).isZero();
        assertThat(delivered).containsExactly(events.get(0).getEventId(), events.get(1).getEventId(),
                events.get(1).getEventId());
        assertThat(f.catalog.events(address)).hasSize(2);
    }

    // §15.11, §15.17
    @Test void crashAfterUploadBeforeCommitLeavesNoAccessionAndTheRetrySucceeds() {
        var address = address(account, "doc-crash");
        var request = create("op-crash", address, document(account, "doc-crash", "v1")).build();
        f.stack.afterPut = () -> { throw new IOException("host died after upload"); };
        assertThatThrownBy(() -> f.saves.save(processAuthority(), request)).isInstanceOfSatisfying(AccessionException.class,
                failure -> assertThat(failure.code()).isEqualTo(Status.Code.UNAVAILABLE));
        assertThat(f.stack.puts.get()).isEqualTo(1);
        assertThat(f.catalog.current(address)).isEmpty();
        var lookup = f.reads.outcome(processAuthority(), GetOperationOutcomeRequest.newBuilder().setAccountId(account)
                .setOperationId("op-crash").build());
        assertThat(lookup.hasNone()).isTrue();

        f.stack.afterPut = () -> {};
        var retried = f.saves.save(processAuthority(), request);
        assertThat(retried.getReplayed()).isFalse();
        assertThat(retried.getAccession().getRevision()).isEqualTo(1);
        var committed = f.reads.outcome(processAuthority(), GetOperationOutcomeRequest.newBuilder().setAccountId(account)
                .setOperationId("op-crash").build());
        assertThat(committed.getCommitted().getAccession()).isEqualTo(retried.getAccession());
    }

    // §15.13
    @Test void corruptOrTruncatedStoredContentFailsTheReadWithDataLoss() {
        var address = address(account, "doc-corrupt");
        f.saves.save(processAuthority(), create("op-1", address, document(account, "doc-corrupt", "v1")).build());
        var stored = f.catalog.manifest(address, 1).orElseThrow();
        var core = entry(stored, DocumentPart.DOCUMENT_PART_CORE).getRef();
        overwrite(core.getKey(), Document.newBuilder().setDocId("doc-corrupt").build().toByteArray());
        assertDataLoss(address, DocumentPart.DOCUMENT_PART_CORE);

        var blobs = entry(stored, DocumentPart.DOCUMENT_PART_BLOBS).getRef();
        var original = readObject(blobs.getKey());
        overwrite(blobs.getKey(), java.util.Arrays.copyOf(original, original.length - 3));
        assertDataLoss(address, DocumentPart.DOCUMENT_PART_BLOBS);

        var manifest = f.reads.getManifest(processAuthority(), GetManifestRequest.newBuilder().setAddress(address).build());
        assertThat(manifest.getManifest().getPartsCount()).isEqualTo(3);
    }

    // §15.18: missing owner, unknown owner, credential mismatch and revoked grant are distinct.
    @Test void ownerAndGrantFailuresAreDistinctAndClaimNoOperationId() {
        var credential = UUID.randomUUID();
        f.catalog.registerCredential(ISSUER, credential, "chunker");
        var address = address(account, "doc-grants");
        f.saves.save(processAuthority(), create("op-0", address, document(account, "doc-grants", "v1", "chunker")).build());
        var caller = writer(account, "chunker", credential, 1);

        var noOwner = titleSave("op-x", address, 1, "v2").toBuilder().clearOwner().build();
        assertRejected(() -> f.saves.save(caller, noOwner), Status.Code.INVALID_ARGUMENT, null);
        var step = titleSave("op-x", address, 1, "v2").toBuilder().setOwner(stepOwner("chunker")).build();
        assertRejected(() -> f.saves.save(caller, step), Status.Code.FAILED_PRECONDITION,
                RejectionReason.REJECTION_REASON_OWNER_UNKNOWN);

        var grant = f.stepGrant(account, credential, "chunker", false);
        var stale = writer(account, "chunker", credential, 2);
        assertRejected(() -> f.saves.save(stale, step), Status.Code.PERMISSION_DENIED,
                RejectionReason.REJECTION_REASON_WRITE_DENIED);
        var other = writer(account, "chunker", UUID.randomUUID(), 1);
        assertRejected(() -> f.saves.save(other, step), Status.Code.PERMISSION_DENIED,
                RejectionReason.REJECTION_REASON_WRITE_DENIED);
        f.catalog.revokeGrant(grant.getGrantId());
        assertRejected(() -> f.saves.save(caller, step), Status.Code.PERMISSION_DENIED,
                RejectionReason.REJECTION_REASON_WRITE_DENIED);
        assertThat(f.catalog.outcome(account, "op-x")).as("unauthorized attempts claim nothing").isEmpty();

        var replacement = f.stepGrant(account, credential, "chunker", false);
        var saved = f.saves.save(caller, step);
        assertThat(saved.getAccession().getValidation().getWriterGrant().getWriterGrantId())
                .isEqualTo(replacement.getGrantId());
        assertThat(saved.getAccession().getValidation().getWriterGrant().getGrantSnapshot())
                .isEqualTo(replacement.toByteString());
        assertThat(saved.getAccession().getManifest().getParts(0).getOwner()).isEqualTo(step.getOwner());
        assertThat(saved.getAccession().getManifest().getParts(1).getOwner()).isEqualTo(intakeOwner());
    }

    // §15.18, §15.22: commit and revocation in both orderings around an upload.
    @Test void revocationOrRotationDuringUploadDeniesTheCommit() {
        var credential = UUID.randomUUID();
        f.catalog.registerCredential(ISSUER, credential, "chunker");
        var grant = f.stepGrant(account, credential, "chunker", false);
        var address = address(account, "doc-revoke");
        f.saves.save(processAuthority(), create("op-0", address, document(account, "doc-revoke", "v1", "chunker")).build());
        var caller = writer(account, "chunker", credential, 1);

        f.stack.afterPut = () -> f.catalog.revokeGrant(grant.getGrantId());
        var step = titleSave("op-1", address, 1, "v2").toBuilder().setOwner(stepOwner("chunker")).build();
        assertRejected(() -> f.saves.save(caller, step), Status.Code.PERMISSION_DENIED,
                RejectionReason.REJECTION_REASON_WRITE_DENIED);
        assertThat(f.catalog.current(address).orElseThrow().revision()).isEqualTo(1);
        assertThat(f.catalog.outcome(account, "op-1")).isEmpty();

        var second = f.stepGrant(account, credential, "chunker", false);
        f.stack.afterPut = () -> f.catalog.rotateCredential(ISSUER, credential);
        assertRejected(() -> f.saves.save(caller, step), Status.Code.PERMISSION_DENIED,
                RejectionReason.REJECTION_REASON_WRITE_DENIED);
        f.stack.afterPut = () -> {};

        // Rotation keeps the grant: the new generation writes under it.
        var rotated = writer(account, "chunker", credential, 2);
        var committed = f.saves.save(rotated, step);
        assertThat(committed.getAccession().getValidation().getWriterGrant().getCredentialGeneration()).isEqualTo(2);
        // A save committed before revocation stays valid history.
        f.catalog.revokeGrant(second.getGrantId());
        var history = f.reads.getManifest(processAuthority(), GetManifestRequest.newBuilder().setAddress(address).build());
        assertThat(history.getValidation().getWriterGrant().getWriterGrantId()).isEqualTo(second.getGrantId());
    }

    // §15.20
    @Test void retryIdentityCoversMetadataOwnerAndLineageAndReplayNeedsCurrentAccess() {
        var credential = UUID.randomUUID();
        f.catalog.registerCredential(ISSUER, credential, "chunker");
        var grant = f.stepGrant(account, credential, "chunker", false);
        var address = address(account, "doc-identity");
        f.saves.save(processAuthority(), create("op-0", address, document(account, "doc-identity", "v1", "chunker")).build());
        var caller = writer(account, "chunker", credential, 1);
        var request = titleSave("op-1", address, 1, "v2").toBuilder().setOwner(stepOwner("chunker"))
                .putMetadata("crawl_id", "c1").build();
        var saved = f.saves.save(caller, request);

        for (var changed : List.of(request.toBuilder().putMetadata("crawl_id", "c2").build(),
                request.toBuilder().setOwner(stepOwner("embedder")).build(),
                request.toBuilder().addDerivedFrom(SourceRevision.newBuilder().setAddress(address).setRevision(1)).build()))
            assertThatThrownBy(() -> f.saves.save(caller, changed)).isInstanceOfSatisfying(AccessionException.class,
                    failure -> assertThat(failure.code()).isEqualTo(Status.Code.ALREADY_EXISTS));

        f.catalog.revokeGrant(grant.getGrantId());
        assertThat(f.saves.save(caller, request).getAccession()).as("replay needs no live write grant")
                .isEqualTo(saved.getAccession());

        var locked = Document.newBuilder().setDocId("doc-identity").setOwnership(ownership(account, "someone-else"))
                .setSearchMetadata(SearchMetadata.newBuilder().setTitle("v3")).build();
        f.saves.save(processAuthority(), SaveDocumentRequest.newBuilder().setOperationId("op-2")
                .setTarget(DocumentTarget.newBuilder().setAddress(address)).setExpected(at(2)).setDocument(locked)
                .addWrites(put(DocumentPart.DOCUMENT_PART_CORE)).setOwner(intakeOwner()).build());
        assertThatThrownBy(() -> f.saves.save(caller, request)).isInstanceOfSatisfying(AccessionException.class,
                failure -> assertThat(failure.code()).isEqualTo(Status.Code.PERMISSION_DENIED));
    }

    // §15.24
    @Test void processAuthorityWritesOnlyAsIntakeAndRecordsItsPrincipal() {
        var address = address(account, "doc-process");
        var saved = f.saves.save(processAuthority(), create("op-1", address, document(account, "doc-process", "v1")).build());
        assertThat(saved.getAccession().getValidation().getProcessAuthority().getPrincipal()).isEqualTo("intake-host");
        assertThat(saved.getAccession().getValidation().hasWriterGrant()).isFalse();

        var step = titleSave("op-2", address, 1, "v2").toBuilder().setOwner(stepOwner("chunker")).build();
        assertRejected(() -> f.saves.save(processAuthority(), step), Status.Code.PERMISSION_DENIED,
                RejectionReason.REJECTION_REASON_WRITE_DENIED);
        var opaque = titleSave("op-2", address, 1, "v2").toBuilder()
                .setValidationMode(ValidationMode.VALIDATION_MODE_OPAQUE_ALLOWED)
                .addOpaqueAttestations(OpaqueAttestation.newBuilder().setTypeUrl("type.test/x.Y")
                        .setOccurrence(OccurrenceLocator.newBuilder().setPart(PartSelector.newBuilder()
                                .setPart(DocumentPart.DOCUMENT_PART_CORE)).addSteps(
                                ai.protomolt.proto.repo.v1.RepositorySchemaOccurrenceStep.newBuilder().setFieldNumber(4)))).build();
        assertRejected(() -> f.saves.save(processAuthority(), opaque), Status.Code.PERMISSION_DENIED,
                RejectionReason.REJECTION_REASON_WRITE_DENIED);
        assertThat(f.catalog.outcome(account, "op-2")).isEmpty();
    }

    @Test void grantCreationNeedsAllowCreate() {
        var credential = UUID.randomUUID();
        f.catalog.registerCredential(ISSUER, credential, "crawler");
        var caller = writer(account, "crawler", credential, 1);
        var address = address(account, "doc-create");
        var request = create("op-1", address, document(account, "doc-create", "v1", "crawler")).build();
        f.intakeGrant(account, credential, "ds-1", false);
        assertRejected(() -> f.saves.save(caller, request), Status.Code.PERMISSION_DENIED,
                RejectionReason.REJECTION_REASON_WRITE_DENIED);
        var creator = f.intakeGrant(account, credential, "ds-1", true);
        var saved = f.saves.save(caller, request);
        var evidence = saved.getAccession().getValidation().getWriterGrant();
        assertThat(evidence.getWriterGrantId()).isEqualTo(creator.getGrantId());
        assertThat(evidence.getWriterGrantSha256()).isEqualTo(DocumentSaves.sha256(evidence.getGrantSnapshot()));
        assertThat(f.catalog.artifact(evidence.getSnapshotSchema().getDescriptorSha256())).isPresent();
    }

    @Test void requestStructureFailuresAreRefusedAndNotRecorded() {
        var address = address(account, "doc-shape");
        var stray = create("op-s", address, document(account, "doc-shape", "v1")).clearWrites()
                .addWrites(put(DocumentPart.DOCUMENT_PART_CORE)).addWrites(put(DocumentPart.DOCUMENT_PART_PARSED)).build();
        assertRejected(() -> f.saves.save(processAuthority(), stray), Status.Code.INVALID_ARGUMENT,
                RejectionReason.REJECTION_REASON_MALFORMED_REQUEST);
        var duplicate = create("op-s", address, document(account, "doc-shape", "v1"))
                .addWrites(put(DocumentPart.DOCUMENT_PART_CORE)).build();
        assertRejected(() -> f.saves.save(processAuthority(), duplicate), Status.Code.INVALID_ARGUMENT,
                RejectionReason.REJECTION_REASON_MALFORMED_REQUEST);
        var segmented = document(account, "doc-shape", "v1").toBuilder().setSearchMetadata(SearchMetadata.newBuilder()
                .addSegmentations(Segmentation.newBuilder().setSegmentationId("s"))).build();
        assertRejected(() -> f.saves.save(processAuthority(), create("op-s", address, segmented).build()),
                Status.Code.INVALID_ARGUMENT, RejectionReason.REJECTION_REASON_MALFORMED_REQUEST);
        var blank = create("op-s", address, document(account, "doc-shape", "v1"))
                .setTarget(DocumentTarget.newBuilder().setUseDatasourceId("ds-1"))
                .setDocument(document(account, " ", "v1")).build();
        assertRejected(() -> f.saves.save(processAuthority(), blank), Status.Code.INVALID_ARGUMENT, null);
        assertThat(f.catalog.outcome(account, "op-s")).isEmpty();

        var intake = create("op-s", address, document(account, "doc-shape", "v1"))
                .setTarget(DocumentTarget.newBuilder().setUseDatasourceId("ds-1")).build();
        var saved = f.saves.save(processAuthority(), intake);
        assertThat(saved.getAccession().getAddress()).isEqualTo(address);
    }

    // ------------------------------------------------------------------ helpers

    private static SaveDocumentRequest titleSave(String operation, NodeAddress address, long expected, String title) {
        var core = Document.newBuilder().setDocId(address.getDocId())
                .setOwnership(ownership(address.getAccountId(), "chunker"))
                .setSearchMetadata(SearchMetadata.newBuilder().setTitle(title)).build();
        return SaveDocumentRequest.newBuilder().setOperationId(operation)
                .setTarget(DocumentTarget.newBuilder().setAddress(address)).setExpected(at(expected)).setDocument(core)
                .addWrites(put(DocumentPart.DOCUMENT_PART_CORE)).setOwner(intakeOwner()).build();
    }

    private static StoredPartEntry entry(StoredManifest manifest, DocumentPart part) {
        return manifest.getPartsList().stream().filter(p -> p.getEntry().getSelector().getPart() == part)
                .findFirst().orElseThrow();
    }

    private void assertDataLoss(NodeAddress address, DocumentPart part) {
        assertThatThrownBy(() -> f.reads.getDocument(processAuthority(), GetDocumentRequest.newBuilder().setAddress(address)
                .setSelection(PartSelection.newBuilder().addSelectors(PartSelector.newBuilder().setPart(part))).build()))
                .isInstanceOfSatisfying(AccessionException.class, failure ->
                        assertThat(failure.code()).isEqualTo(Status.Code.DATA_LOSS));
    }

    private static void overwrite(String key, byte[] bytes) {
        AccessionFixture.s3().putObject(PutObjectRequest.builder().bucket(BUCKET).key(key).build(), RequestBody.fromBytes(bytes));
    }

    private static byte[] readObject(String key) {
        return AccessionFixture.s3().getObjectAsBytes(software.amazon.awssdk.services.s3.model.GetObjectRequest.builder()
                .bucket(BUCKET).key(key).build()).asByteArray();
    }

    private static void assertRejected(ThrowingCall call, Status.Code code, RejectionReason reason) {
        assertThatThrownBy(call::run).isInstanceOfSatisfying(AccessionException.class, failure -> {
            assertThat(failure.code()).isEqualTo(code);
            if (reason != null) assertThat(((SaveRejection) failure.detail().orElseThrow()).getReason()).isEqualTo(reason);
        });
    }

    @FunctionalInterface private interface ThrowingCall { void run() throws Exception; }
    @FunctionalInterface private interface Indexed { Object call(int index) throws Exception; }

    private static List<Object> race(int count, Indexed call) throws Exception {
        var executor = Executors.newVirtualThreadPerTaskExecutor();
        var start = new CountDownLatch(1);
        try {
            var futures = new ArrayList<Future<Object>>();
            for (int i = 0; i < count; i++) {
                int index = i;
                futures.add(executor.submit((Callable<Object>) () -> {
                    start.await();
                    try {
                        return call.call(index);
                    } catch (AccessionException failure) {
                        return failure;
                    }
                }));
            }
            start.countDown();
            var results = new ArrayList<Object>();
            for (var future : futures) results.add(future.get());
            return results;
        } finally {
            executor.shutdownNow();
        }
    }
}
