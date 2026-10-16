package ai.protomolt.proto.repo.accession;

import ai.protomolt.proto.repo.catalog.CatalogDataLoss;
import ai.protomolt.proto.repo.catalog.CatalogException;
import ai.protomolt.proto.repo.catalog.PostgresCatalog;
import ai.protomolt.proto.repo.spi.DocumentAccessPolicy;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.stack.StackDataLoss;
import ai.protomolt.proto.repo.v1.Access;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.OwnershipContext;
import ai.protomolt.proto.repo.v1.RepositorySchemaAssetReference;
import ai.protomolt.proto.repo.v2.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The v2 reads (requirements §8; change map §4 steps R1-R7). A read resolves the
 * current revision once, or reads the exact revision asked for and never
 * another. Part bytes are verified against their stack reference before they
 * are decoded; a mismatch is DATA_LOSS. Access follows the document's current
 * ownership, stored as binary protobuf in the catalog.
 */
public final class DocumentReads {
    private final PostgresCatalog catalog;
    private final DocumentSaves.Drives drives;
    private final RepositorySchemaAssetReference container;

    public DocumentReads(PostgresCatalog catalog, DocumentSaves.Drives drives, RepositorySchemaAssetReference container) {
        this.catalog = Objects.requireNonNull(catalog);
        this.drives = Objects.requireNonNull(drives);
        this.container = Objects.requireNonNull(container);
    }

    public GetDocumentResponse getDocument(RepositoryCaller caller, GetDocumentRequest request) {
        return guarded(() -> {
            valid(request);
            var stored = resolve(caller, request.getAddress(), request.getRevision());
            var selected = selected(request.getSelection());
            var document = Document.newBuilder();
            for (var part : stored.getPartsList()) {
                var entry = part.getEntry();
                if (!selected.apply(entry.getSelector()) || entry.getState() != PartState.PART_STATE_PRESENT) continue;
                if (entry.getOpaque() && !request.getAllowOpaque())
                    throw new AccessionException(Status.Code.FAILED_PRECONDITION,
                            "The revision holds opaque content; request its opaque representation explicitly", null);
                requireContainer(stored, entry.getSelector());
                try {
                    document.mergeFrom(read(part.getRef()));
                } catch (InvalidProtocolBufferException corrupt) {
                    throw new AccessionException(Status.Code.DATA_LOSS, "Stored part does not decode", null, corrupt);
                }
            }
            return GetDocumentResponse.newBuilder().setDocument(document)
                    .setManifest(manifest(stored, selected)).setValidation(stored.getValidation()).build();
        });
    }

    public GetManifestResponse getManifest(RepositoryCaller caller, GetManifestRequest request) {
        return guarded(() -> {
            valid(request);
            var stored = resolve(caller, request.getAddress(), request.getRevision());
            return GetManifestResponse.newBuilder().setManifest(manifest(stored, selector -> false))
                    .setValidation(stored.getValidation()).build();
        });
    }

    public ListRevisionsResponse listRevisions(RepositoryCaller caller, ListRevisionsRequest request) {
        return guarded(() -> {
            valid(request);
            current(caller, request.getAddress());
            long before = Long.MAX_VALUE;
            if (!request.getPageToken().isEmpty()) {
                try {
                    before = Long.parseLong(request.getPageToken());
                } catch (NumberFormatException invalid) {
                    before = 0;
                }
                if (before < 1 || !Long.toString(before).equals(request.getPageToken()))
                    throw new AccessionException(Status.Code.INVALID_ARGUMENT, "Invalid page token", null);
            }
            var page = catalog.revisions(request.getAddress(), before, request.getPageSize() + 1);
            var events = catalog.events(request.getAddress()).stream()
                    .collect(Collectors.toMap(DocumentEvent::getRevision, Function.identity()));
            var response = ListRevisionsResponse.newBuilder();
            for (var stored : page.subList(0, Math.min(page.size(), request.getPageSize()))) {
                var event = events.get(stored.getRevision());
                if (event == null) throw new AccessionException(Status.Code.DATA_LOSS, "Revision has no event", null);
                var summary = RevisionSummary.newBuilder().setRevision(stored.getRevision())
                        .setPreviousRevision(stored.getPreviousRevision()).setCommittedAt(stored.getCommittedAt())
                        .setOperationId(stored.getOperationId()).setTombstone(stored.getTombstone())
                        .setValidationStatus(stored.getValidation().getStatus())
                        .addAllChanges(event.getAccessioned().getChangesList());
                if (stored.hasOwner()) summary.setOwner(stored.getOwner());
                response.addRevisions(summary);
            }
            if (page.size() > request.getPageSize())
                response.setNextPageToken(Long.toString(page.get(request.getPageSize() - 1).getRevision()));
            return response.build();
        });
    }

    public GetOperationOutcomeResponse outcome(RepositoryCaller caller, GetOperationOutcomeRequest request) {
        return guarded(() -> {
            valid(request);
            requireBinding(caller);
            requireAccount(caller, request.getAccountId());
            var recorded = catalog.outcome(request.getAccountId(), request.getOperationId());
            if (recorded.isEmpty())
                return GetOperationOutcomeResponse.newBuilder().setNone(NoRecordedOutcome.getDefaultInstance()).build();
            var outcome = recorded.get().outcome();
            if (outcome.hasAccession()) current(caller, outcome.getAccession().getAddress());
            else if (outcome.hasRejection()) RejectionAccess.require(catalog, caller, recorded.get());
            return GetOperationOutcomeResponse.newBuilder().setCommitted(outcome).build();
        });
    }

    // ------------------------------------------------------------------ resolution

    /** The current revision's manifest, after read access is checked against it. */
    private StoredManifest current(RepositoryCaller caller, NodeAddress address) {
        requireBinding(caller);
        requireAddress(address);
        requireAccount(caller, address.getAccountId());
        var state = catalog.current(address).orElseThrow(() -> notFound("Document not found"));
        var stored = catalog.manifest(address, state.revision())
                .orElseThrow(() -> new AccessionException(Status.Code.DATA_LOSS, "Current revision has no manifest", null));
        if (!canRead(caller, address, stored.getOwnership()))
            throw new AccessionException(Status.Code.PERMISSION_DENIED, "The caller may not read this document", null);
        if (stored.getTombstone()) throw notFound("Document is deleted");
        return stored;
    }

    private StoredManifest resolve(RepositoryCaller caller, NodeAddress address, RevisionSelector selector) {
        var latest = current(caller, address);
        if (!selector.hasRevision() || selector.getRevision() == latest.getRevision()) return latest;
        return catalog.manifest(address, selector.getRevision())
                .orElseThrow(() -> notFound("Revision " + selector.getRevision() + " does not exist"));
    }

    /** Selected parts decode with the container schema retained at save time, never a newer one. */
    private void requireContainer(StoredManifest stored, PartSelector part) {
        var binding = stored.getValidation().getPartSchemasList().stream()
                .filter(schema -> schema.getPart().equals(part)).findFirst()
                .orElseThrow(() -> new AccessionException(Status.Code.DATA_LOSS, "Part has no retained schema binding", null));
        if (!binding.getFragmentSchema().getDescriptorSha256().equals(container.getDescriptorSha256()))
            throw new AccessionException(Status.Code.FAILED_PRECONDITION,
                    "The revision was saved under another Document schema; decoding it with the retained schema is not supported yet",
                    null);
    }

    private ByteString read(StackRef ref) {
        try (var stream = drives.byId(ref.getStackId()).open(ref)) {
            return ByteString.readFrom(stream);
        } catch (StackDataLoss loss) {
            throw new AccessionException(Status.Code.DATA_LOSS, loss.getMessage(), null, loss);
        } catch (IOException failure) {
            throw new AccessionException(Status.Code.UNAVAILABLE, "Stack read failed", null, failure);
        }
    }

    private static Function<PartSelector, Boolean> selected(PartSelection selection) {
        if (selection.getSelectorsCount() == 0
                && !selection.getAllSegmentations() && !selection.getAllEmbeddingSets()) return selector -> true;
        var selectors = new HashSet<PartSelector>();
        for (var selector : selection.getSelectorsList())
            if (!selectors.add(selector)) throw new AccessionException(Status.Code.INVALID_ARGUMENT, "Duplicate selector", null);
        return selector -> selectors.contains(selector)
                || (selection.getAllSegmentations() && selector.hasSegmentationId())
                || (selection.getAllEmbeddingSets() && selector.hasEmbeddingSet());
    }

    private static DocumentManifest manifest(StoredManifest stored, Function<PartSelector, Boolean> included) {
        var manifest = DocumentManifest.newBuilder().setAddress(stored.getAddress()).setRevision(stored.getRevision())
                .setTombstone(stored.getTombstone());
        for (var part : stored.getPartsList())
            manifest.addParts(part.getEntry().toBuilder().setContentIncluded(
                    part.getEntry().getState() == PartState.PART_STATE_PRESENT && included.apply(part.getEntry().getSelector())));
        return manifest.build();
    }

    // ------------------------------------------------------------------ shared rules

    /** The public accession of a stored manifest. */
    static Accession accession(StoredManifest stored) {
        var accession = Accession.newBuilder().setAddress(stored.getAddress()).setRevision(stored.getRevision())
                .setPreviousRevision(stored.getPreviousRevision()).setCommittedAt(stored.getCommittedAt())
                .setManifest(manifest(stored, selector -> false)).setValidation(stored.getValidation())
                .setTombstone(stored.getTombstone()).addAllDerivedFrom(stored.getDerivedFromList());
        if (stored.hasOwner()) accession.setOwner(stored.getOwner());
        return accession.build();
    }

    static void requireBinding(RepositoryCaller caller) {
        Objects.requireNonNull(caller, "caller");
        if (!caller.processAuthority() && caller.accountIds().isEmpty())
            throw new AccessionException(Status.Code.PERMISSION_DENIED, "The caller is bound to no account", null);
    }

    static void requireAccount(RepositoryCaller caller, String accountId) {
        if (!caller.processAuthority() && !caller.accountIds().contains(accountId))
            throw new AccessionException(Status.Code.PERMISSION_DENIED, "The caller is not bound to this account", null);
    }

    /** NodeAddress imports no validation rules, so every handler checks its four components. */
    static void requireAddress(NodeAddress address) {
        if (address.getDocId().isBlank() || address.getGraphAddressId().isBlank() || address.getAccountId().isBlank()
                || address.getGraphId().isBlank() || !address.getUnknownFields().asMap().isEmpty())
            throw DocumentSaves.malformedException("An address needs a non-blank doc_id, graph_address_id, account_id and graph_id");
    }

    /** Inherited policy has no resolver in v2, so an inheriting document is refused rather than read as resolved. */
    static boolean canRead(RepositoryCaller caller, NodeAddress address, OwnershipContext ownership) {
        return DocumentAccessPolicy.allows(caller, address.getAccountId(), ownership.hasSecurity() ? ownership.getSecurity() : null,
                null, Access.ACCESS_READ);
    }

    static AccessionException translate(RepositoryException failure) {
        var code = switch (failure.code()) {
            case INVALID_ARGUMENT -> Status.Code.INVALID_ARGUMENT;
            case NOT_FOUND -> Status.Code.NOT_FOUND;
            case FAILED_PRECONDITION -> Status.Code.FAILED_PRECONDITION;
            case PERMISSION_DENIED -> Status.Code.PERMISSION_DENIED;
            case CONFLICT -> Status.Code.ABORTED;
            case RESOURCE_EXHAUSTED -> Status.Code.RESOURCE_EXHAUSTED;
            case UNAVAILABLE -> Status.Code.UNAVAILABLE;
            case DATA_LOSS -> Status.Code.DATA_LOSS;
            default -> Status.Code.INTERNAL;
        };
        return new AccessionException(code, failure.getMessage(), null, failure);
    }

    private static void valid(com.google.protobuf.Message request) {
        var result = DocumentSaves.VALIDATOR.validate(request);
        if (!result.valid())
            throw new AccessionException(Status.Code.INVALID_ARGUMENT, "Invalid request: " + result.violations().getFirst(), null);
    }

    private static AccessionException notFound(String message) {
        return new AccessionException(Status.Code.NOT_FOUND, message, null);
    }

    private static <T> T guarded(java.util.function.Supplier<T> read) {
        try {
            return read.get();
        } catch (CatalogDataLoss loss) {
            throw new AccessionException(Status.Code.DATA_LOSS, loss.getMessage(), null, loss);
        } catch (CatalogException failure) {
            if (failure.unavailable()) throw new AccessionException(Status.Code.UNAVAILABLE, "Catalog unavailable", null, failure);
            throw failure;
        } catch (RepositoryException failure) {
            throw translate(failure);
        }
    }

}
