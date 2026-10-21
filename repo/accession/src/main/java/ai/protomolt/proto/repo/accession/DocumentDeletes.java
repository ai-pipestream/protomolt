package ai.protomolt.proto.repo.accession;

import ai.protomolt.proto.descriptors.MessageFingerprints;
import ai.protomolt.proto.repo.admission.BuiltinDocumentSchema;
import ai.protomolt.proto.repo.admission.DocumentSchemaRetention;
import ai.protomolt.proto.repo.catalog.*;
import ai.protomolt.proto.repo.spi.DocumentAccessPolicy;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.v1.Access;
import ai.protomolt.proto.repo.v2.*;
import io.grpc.Status;
import java.time.Clock;
import java.util.List;
import java.util.Optional;
import java.util.Objects;

/** Tombstones use the same atomic revision, receipt and outbox commit as saves. */
public final class DocumentDeletes {
    private final Catalog catalog;
    private final Clock clock;
    private final WriterAuthority authority;

    public DocumentDeletes(Catalog catalog, Clock clock) {
        this.catalog = Objects.requireNonNull(catalog);
        this.clock = Objects.requireNonNull(clock);
        authority = new WriterAuthority(catalog, DocumentSchemaRetention.retain(
                BuiltinDocumentSchema.definition(WriterGrant.getDescriptor())));
    }

    public DeleteDocumentResponse delete(RepositoryCaller caller, DeleteDocumentRequest request) {
        try {
            return checked(caller, request);
        } catch (CatalogDataLoss loss) {
            throw new AccessionException(Status.Code.DATA_LOSS, loss.getMessage(), null, loss);
        } catch (CatalogException failure) {
            if (failure.unavailable()) throw new AccessionException(Status.Code.UNAVAILABLE, "Catalog unavailable", null, failure);
            throw failure;
        } catch (RepositoryException failure) {
            throw DocumentReads.translate(failure);
        }
    }

    private DeleteDocumentResponse checked(RepositoryCaller caller, DeleteDocumentRequest request) {
        if (!DocumentSaves.VALIDATOR.validate(request).valid() || !request.getExpected().hasRevision()
                || !request.getUnknownFields().asMap().isEmpty() || !request.getExpected().getUnknownFields().asMap().isEmpty())
            throw DocumentSaves.malformedException("Delete requires a valid request and an exact expected revision");
        DocumentReads.requireBinding(caller);
        var address = request.getAddress();
        DocumentReads.requireAddress(address);
        DocumentReads.requireAccount(caller, address.getAccountId());
        var intent = DeleteIntent.newBuilder().setEncodingVersion(1).setAddress(address)
                .setExpected(request.getExpected()).setReason(request.getReason()).setOperationKind("delete").build();
        var digest = SaveIntents.sha256Hex(MessageFingerprints.deterministicBytes(intent));
        var recorded = catalog.outcome(address.getAccountId(), request.getOperationId());
        if (recorded.isPresent()) return replay(caller, request, digest, recorded.get());
        var current = catalog.current(address);
        if (current.isEmpty()) throw new AccessionException(Status.Code.NOT_FOUND, "Document not found", null);
        var base = catalog.manifest(address, current.get().revision()).orElseThrow(() ->
                new AccessionException(Status.Code.DATA_LOSS, "Current revision has no manifest", null));
        if (!DocumentAccessPolicy.allows(caller, address.getAccountId(), base.getOwnership().getSecurity(), null, Access.ACCESS_WRITE))
            throw WriterAuthority.denied("The caller may not delete this document");
        var authorized = authority.authorizeDelete(caller, base.getOwnership());
        if (base.getTombstone() || base.getRevision() != request.getExpected().getRevision()) {
            var raced = catalog.outcome(address.getAccountId(), request.getOperationId());
            if (raced.isPresent()) return replay(caller, request, digest, raced.get());
            throw DocumentSaves.aborted(address, request.getExpected(), base.getRevision(), base.getTombstone());
        }
        var validation = RevisionValidation.newBuilder().setStatus(ValidationStatus.VALIDATION_STATUS_NOT_APPLICABLE);
        authorized.grant().ifPresent(validation::setWriterGrant);
        authorized.process().ifPresent(validation::setProcessAuthority);
        var now = DocumentSaves.timestamp(clock.instant());
        var stored = StoredManifest.newBuilder().setAddress(address).setRevision(Math.addExact(base.getRevision(), 1))
                .setPreviousRevision(base.getRevision()).setTombstone(true).setValidation(validation)
                .setOperationId(request.getOperationId()).setIntentSha256(digest).setOwnership(base.getOwnership())
                .setCommittedAt(now).build();
        var accession = DocumentReads.accession(stored);
        var outcome = RecordedOutcome.newBuilder().setAccession(accession).setRecordedAt(now).build();
        var event = DocumentEvent.newBuilder().setAddress(address).setRevision(stored.getRevision())
                .setEventId(DocumentSaves.eventId(address, stored.getRevision())).setOperationId(request.getOperationId())
                .setPreviousRevision(base.getRevision()).setOccurredAt(now)
                .setTombstoned(Tombstoned.newBuilder().setReason(request.getReason())).build();
        var response = DeleteDocumentResponse.newBuilder().setOperationId(request.getOperationId()).setAccession(accession).build();
        if (!DocumentSaves.VALIDATOR.validate(stored).valid() || !DocumentSaves.VALIDATOR.validate(response).valid()
                || !DocumentSaves.VALIDATOR.validate(event).valid())
            throw new IllegalStateException("Invalid tombstone record");
        var result = catalog.commit(new CatalogRecords.Commit(address, request.getOperationId(), digest, request.getExpected(),
                stored, outcome, event, List.of(), List.of(), authority.grantSchema().artifacts(), authorized.grantIds(), authorized.credential(),
                Optional.empty()));
        return switch (result) {
            case CatalogRecords.Committed ignored -> response;
            case CatalogRecords.Recorded existing -> replay(caller, request, digest, existing.outcome());
            case CatalogRecords.Conflict conflict -> throw DocumentSaves.aborted(address, request.getExpected(), conflict.currentRevision(), conflict.currentTombstone());
            case CatalogRecords.WriteDenied denied -> throw WriterAuthority.denied(denied.reason());
            case CatalogRecords.TargetPurged ignored -> throw new AccessionException(Status.Code.NOT_FOUND, "Document was purged", null);
            default -> throw new IllegalStateException("Delete without object references received " + result);
        };
    }

    private DeleteDocumentResponse replay(RepositoryCaller caller, DeleteDocumentRequest request, String digest, CatalogRecords.Outcome recorded) {
        if (recorded.outcome().hasRejection()) RejectionAccess.require(catalog, caller, recorded);
        if (!recorded.intentSha256().equals(digest))
            throw new AccessionException(Status.Code.ALREADY_EXISTS, "Operation ID was used with a different intent",
                    OperationReuse.newBuilder().setOperationId(request.getOperationId()).setRecordedIntentSha256(recorded.intentSha256())
                            .setRequestIntentSha256(digest).build());
        if (!recorded.outcome().hasAccession()) throw new AccessionException(Status.Code.DATA_LOSS, "Delete outcome has no accession", null);
        var accession = recorded.outcome().getAccession();
        var latest = catalog.current(accession.getAddress()).flatMap(c -> catalog.manifest(accession.getAddress(), c.revision()));
        if (latest.isEmpty() || !DocumentReads.canRead(caller, accession.getAddress(), latest.get().getOwnership()))
            throw new AccessionException(Status.Code.PERMISSION_DENIED, "The caller may not read this document", null);
        return DeleteDocumentResponse.newBuilder().setOperationId(request.getOperationId()).setAccession(accession).setReplayed(true).build();
    }
}
