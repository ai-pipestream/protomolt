package ai.protomolt.proto.repo.accession;

import ai.protomolt.proto.repo.catalog.CatalogDataLoss;
import ai.protomolt.proto.repo.catalog.CatalogException;
import ai.protomolt.proto.repo.admission.DocumentAdmissionReservations;
import ai.protomolt.proto.repo.admission.DocumentCandidateAdmission;
import ai.protomolt.proto.repo.admission.DocumentRetainedDefinitions;
import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.catalog.Catalog;
import ai.protomolt.proto.repo.spi.DocumentAccessPolicy;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.stack.StackDataLoss;
import ai.protomolt.proto.repo.v1.Access;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.OwnershipContext;
import ai.protomolt.proto.repo.v2.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import java.io.IOException;
import java.time.Clock;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * The v2 reads (requirements §8; change map §4 steps R1-R7). A read resolves the
 * current revision once, or reads the exact revision asked for and never
 * another. Part bytes are verified against their stack reference before they
 * are decoded; a mismatch is DATA_LOSS. Access follows the document's current
 * ownership, stored as binary protobuf in the catalog.
 * <p>
 * A typed read decodes each selected part with the schemas its revision retained
 * (requirements §9, §15.14): the part's {@code fragment_schema} and the schema of
 * every Any occurrence recorded for it, read from the catalog and checked against
 * their digests. There is no schema registry here, so a stopped registry cannot
 * affect a read and no live schema can stand in for a retained one. An unreachable
 * catalog is UNAVAILABLE; a retained artifact that is absent from a reachable
 * catalog, or fails its digest, is DATA_LOSS.
 */
public final class DocumentReads {
    private final Catalog catalog;
    private final DocumentSaves.Drives drives;
    private final DocumentSchemaAdmission.Limits limits;
    private final DocumentAdmissionReservations reservations;
    private final Clock clock;

    public DocumentReads(Catalog catalog, DocumentSaves.Drives drives, DocumentSchemaAdmission.Limits limits,
            DocumentAdmissionReservations reservations, Clock clock) {
        this.catalog = Objects.requireNonNull(catalog);
        this.drives = Objects.requireNonNull(drives);
        this.limits = Objects.requireNonNull(limits);
        this.reservations = Objects.requireNonNull(reservations);
        this.clock = Objects.requireNonNull(clock);
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
                var bytes = read(part.getRef());
                // Opaque content makes no typed claim and is not decoded (requirements §9).
                if (!entry.getOpaque()) decodeRetained(stored, entry.getSelector(), bytes);
                // Raw blobs live behind claim checks: the server fills storage_ref (decision 45).
                if (entry.getSelector().getPart() == DocumentPart.DOCUMENT_PART_BLOBS)
                    bytes = RawBlobs.withClaimChecks(bytes, part.getBlobRefsList());
                try {
                    document.mergeFrom(bytes);
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
            var shown = page.subList(0, Math.min(page.size(), request.getPageSize()));
            // One bounded query for this page's events, not the document's whole outbox (#327).
            var events = new java.util.HashMap<Long, DocumentEvent>();
            for (var event : catalog.events(request.getAddress(), shown.stream().map(StoredManifest::getRevision).toList()))
                events.put(event.getRevision(), event);
            var response = ListRevisionsResponse.newBuilder();
            for (var stored : shown) {
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

    /**
     * Decodes one stored fragment with the schemas its revision retained: the part's
     * fragment schema and every recorded Any occurrence's schema, through the same
     * admission engine a save runs. The occurrences found must be the ones recorded.
     */
    private void decodeRetained(StoredManifest stored, PartSelector part, ByteString bytes) {
        var validation = stored.getValidation();
        var binding = validation.getPartSchemasList().stream().filter(schema -> schema.getPart().equals(part)).findFirst()
                .orElseThrow(() -> new AccessionException(Status.Code.DATA_LOSS, "Part has no retained schema binding", null));
        var recorded = validation.getResolvedOccurrencesList().stream()
                .filter(occurrence -> occurrence.getOccurrence().getPart().equals(part)).toList();
        DocumentRetainedDefinitions.Reader retained = catalog::artifact;
        try {
            var container = DocumentRetainedDefinitions.definition(binding.getFragmentSchema(), retained);
            var byTypeUrl = new HashMap<String, DocumentSchemaAdmission.Definition>();
            for (var occurrence : recorded) {
                var definition = DocumentRetainedDefinitions.definition(occurrence.getSchema(), retained);
                var previous = byTypeUrl.putIfAbsent(occurrence.getSchema().getTypeUrl(), definition);
                if (previous != null && !previous.equals(definition))
                    throw lost("The revision retains two schemas for " + occurrence.getSchema().getTypeUrl(), null);
            }
            var fragment = new DocumentCandidateAdmission.Fragment(DocumentSaves.v1(part.getPart()), bytes);
            try (var result = DocumentCandidateAdmission.check(new DocumentCandidateAdmission.Request(
                    stored.getAddress().getDocId(), List.of(fragment), container),
                    selection -> byTypeUrl.get(selection.typeUrl()), limits, reservations, clock.instant(), () -> {})) {
                if (result instanceof DocumentCandidateAdmission.Rejected rejected)
                    throw lost("Stored part fails its retained rules: " + rejected.violations().getFirst().message(), null);
                var found = new HashSet<ResolvedOccurrence>();
                for (var occurrence : ((DocumentCandidateAdmission.Accepted) result).occurrences())
                    found.add(ResolvedOccurrence.newBuilder().setOccurrence(OccurrenceLocator.newBuilder().setPart(part)
                                    .addAllSteps(occurrence.steps())).setSchema(occurrence.schema())
                            .setPayloadSha256(occurrence.payloadSha256()).setPayloadLength(occurrence.payloadLength()).build());
                if (!found.equals(new HashSet<>(recorded)))
                    throw lost("Stored part's Any occurrences differ from the retained record", null);
            }
        } catch (DocumentSchemaAdmission.DataLoss | DocumentCandidateAdmission.SchemaMissing
                 | DocumentCandidateAdmission.InvalidPayload | DocumentCandidateAdmission.ContentRefused
                 | DocumentCandidateAdmission.HostFailure | InvalidProtocolBufferException failure) {
            throw lost("Stored part does not decode with its retained schema: " + failure.getMessage(), failure);
        } catch (DocumentCandidateAdmission.BudgetExhausted budget) {
            throw new AccessionException(Status.Code.RESOURCE_EXHAUSTED, "Retained schema budget exhausted: "
                    + budget.getMessage(), null, budget);
        }
    }

    private static AccessionException lost(String message, Throwable cause) {
        return new AccessionException(Status.Code.DATA_LOSS, message, null, cause);
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
