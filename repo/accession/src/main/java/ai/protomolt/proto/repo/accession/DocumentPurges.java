package ai.protomolt.proto.repo.accession;

import ai.protomolt.proto.repo.catalog.*;
import ai.protomolt.proto.repo.spi.PurgeAuthorization;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.v2.*;
import io.grpc.Context;
import io.grpc.Status;
import java.io.IOException;
import java.time.Clock;
import java.util.Objects;

/** Durable mark, exact provider deletion, then durable completion. Retries resume the recorded plan. */
public final class DocumentPurges {
    private final Catalog catalog;
    private final DocumentSaves.Drives drives;
    private final PurgeAuthorization authorization;
    private final Clock clock;

    public DocumentPurges(Catalog catalog, DocumentSaves.Drives drives, PurgeAuthorization authorization, Clock clock) {
        this.catalog = Objects.requireNonNull(catalog);
        this.drives = Objects.requireNonNull(drives);
        this.authorization = Objects.requireNonNull(authorization);
        this.clock = Objects.requireNonNull(clock);
    }

    public PurgeDocumentResponse purge(RepositoryCaller caller, PurgeDocumentRequest request) {
        try {
            if (!DocumentSaves.VALIDATOR.validate(request).valid() || !request.getUnknownFields().asMap().isEmpty())
                throw DocumentSaves.malformedException("Purge requires a valid request");
            if (caller.processAuthority()) throw WriterAuthority.denied("Process authority cannot purge documents");
            DocumentReads.requireAddress(request.getAddress());
            try {
                DocumentReads.requireBinding(caller);
                DocumentReads.requireAccount(caller, request.getAddress().getAccountId());
            } catch (AccessionException denied) {
                if (denied.code() != Status.Code.PERMISSION_DENIED) throw denied;
                throw new AccessionException(Status.Code.PERMISSION_DENIED, denied.getMessage(),
                        DocumentSaves.rejection(RejectionReason.REJECTION_REASON_WRITE_DENIED, denied.getMessage()), denied);
            }
            var decision = Objects.requireNonNull(authorization.evaluate(caller, request.getAddress().getAccountId()),
                    "Purge authorization returned no decision");
            if (!decision.administrator() || !decision.accountWriteAllowed())
                throw WriterAuthority.denied("Purge requires administrator status and account write access");
            var state = catalog.markPurged(request, DocumentSaves.timestamp(clock.instant())).orElseThrow(() ->
                    new AccessionException(Status.Code.NOT_FOUND, "Document not found", null));
            var plan = state.record();
            if (!DocumentSaves.VALIDATOR.validate(plan).valid())
                throw new CatalogDataLoss("Invalid retained purge plan");
            if (!state.completed()) {
                for (var ref : plan.getObjectsToDeleteList()) {
                    if (Context.current().isCancelled())
                        throw new AccessionException(Status.Code.CANCELLED, "Purge cancelled; recorded deletion can be resumed", null);
                    drives.byId(ref.getStackId()).delete(ref);
                }
                catalog.completePurge(request.getAddress().getAccountId(), request.getPurgeId(), DocumentSaves.timestamp(clock.instant()));
            }
            return PurgeDocumentResponse.newBuilder().setPurgeId(request.getPurgeId())
                    .setReferencesPurged(plan.getReferencesPurged()).setObjectsToDelete(plan.getObjectsToDeleteCount())
                    .setObjectsRetainedShared(plan.getObjectsRetainedShared()).build();
        } catch (CatalogRecords.PurgeConflict conflict) {
            throw new AccessionException(Status.Code.ALREADY_EXISTS, conflict.getMessage(), null, conflict);
        } catch (CatalogDataLoss loss) {
            throw new AccessionException(Status.Code.DATA_LOSS, loss.getMessage(), null, loss);
        } catch (CatalogException failure) {
            if (failure.unavailable()) throw new AccessionException(Status.Code.UNAVAILABLE, "Catalog unavailable", null, failure);
            throw failure;
        } catch (RepositoryException failure) {
            throw DocumentReads.translate(failure);
        } catch (IOException failure) {
            throw new AccessionException(Status.Code.UNAVAILABLE, "Purge deletion failed; recorded deletion can be resumed", null, failure);
        }
    }
}
