package ai.protomolt.proto.repo.accession;

import ai.protomolt.proto.config.ConceptSchemeMounts;
import ai.protomolt.proto.repo.catalog.*;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.v2.*;
import ai.protomolt.proto.types.ConceptRef;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.grpc.Status;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Current document metadata listing (requirements §8): the catalog applies the
 * scope, the typed filters and the sort; access filtering follows, before
 * pagination, so a page never carries a row or a count the caller cannot read.
 * A concept filter (requirements §17) is expanded against the host's mounted
 * concept schemes once per page, from one snapshot, and the catalog matches the
 * expanded pairs against the stored tags; the page reports the expansion's
 * evidence.
 */
public final class DocumentLists {
    /** The page token format this listing issues and accepts. */
    static final int TOKEN_VERSION = 2;
    private final Catalog catalog;
    private final Optional<ConceptSchemeMounts> concepts;

    /** A listing on a host that mounts no concept scheme: a concept filter is refused. */
    public DocumentLists(Catalog catalog) {
        this.catalog = Objects.requireNonNull(catalog);
        this.concepts = Optional.empty();
    }

    /**
     * @param catalog the catalog
     * @param concepts the host's mounted concept schemes, which expand concept filters
     */
    public DocumentLists(Catalog catalog, ConceptSchemeMounts concepts) {
        this.catalog = Objects.requireNonNull(catalog);
        this.concepts = Optional.of(Objects.requireNonNull(concepts));
    }

    public ListDocumentsResponse list(RepositoryCaller caller, ListDocumentsRequest request) {
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

    private ListDocumentsResponse checked(RepositoryCaller caller, ListDocumentsRequest request) {
        if (!DocumentSaves.VALIDATOR.validate(request).valid() || !known(request))
            throw DocumentSaves.malformedException("Invalid document listing request");
        requireConsistentFilter(request.getFilter());
        requireConsistentSort(request.getSort());
        DocumentReads.requireBinding(caller);
        DocumentReads.requireAccount(caller, request.getAccountId());
        var after = cursor(request);
        var response = ListDocumentsResponse.newBuilder();
        List<ConceptRef> expanded = List.of();
        if (request.getFilter().hasConcept()) {
            var expansion = expand(request.getFilter().getConcept());
            expanded = expansion.concepts();
            response.setConceptEvidence(evidence(expansion));
        }
        var listing = new CatalogRecords.Listing(request.getAccountId(), request.getGraphId(), request.getGraphAddressId(),
                request.getIncludeTombstoned(), request.getFilter(), request.getSort(), expanded);
        CatalogRecords.Cursor lastReturned = null;
        // Each query is bounded. Skip denied rows without leaking their identities in tokens.
        while (true) {
            var rows = catalog.documents(listing, after, request.getPageSize() + 1);
            for (var row : rows) {
                after = Optional.of(row.cursor());
                if (!DocumentReads.canRead(caller, row.address(), row.entry().getOwnership())) continue;
                if (response.getDocumentsCount() == request.getPageSize())
                    return response.setNextPageToken(token(request, lastReturned)).build();
                if (!row.tombstone() && !row.entry().hasLookup())
                    throw new CatalogDataLoss("Catalog entry of " + row.address().getDocId() + " has no lookup; rebuild the entries");
                var summary = DocumentSummary.newBuilder().setAddress(row.address()).setRevision(row.revision())
                        .setCommittedAt(row.committedAt()).setTombstone(row.tombstone());
                if (row.entry().hasLookup()) summary.setLookup(row.entry().getLookup());
                if (row.entry().hasOwner()) summary.setOwner(row.entry().getOwner());
                response.addDocuments(summary);
                lastReturned = row.cursor();
            }
            if (rows.size() < request.getPageSize() + 1) return response.build();
        }
    }

    /**
     * Expands a concept filter against one snapshot of the mounted schemes. A host
     * without mounts, or a queried scheme that is not mounted, is FAILED_PRECONDITION;
     * a concept the scheme lacks, or an expansion over the mount's bound, is
     * INVALID_ARGUMENT, never a truncated match.
     */
    private ConceptSchemeMounts.Expansion expand(ConceptFilter filter) {
        var mounts = concepts.orElseThrow(() -> new AccessionException(Status.Code.FAILED_PRECONDITION,
                "This host mounts no concept schemes; a concept filter cannot be expanded", null));
        try {
            return mounts.expand(filter.getConcept(), filter.getExpansion());
        } catch (IllegalStateException unmounted) {
            throw new AccessionException(Status.Code.FAILED_PRECONDITION, unmounted.getMessage(), null, unmounted);
        } catch (IllegalArgumentException refused) {
            throw new AccessionException(Status.Code.INVALID_ARGUMENT, refused.getMessage(), null, refused);
        }
    }

    private static ConceptFilterEvidence evidence(ConceptSchemeMounts.Expansion expansion) {
        var evidence = ConceptFilterEvidence.newBuilder().setExpandedConcepts(expansion.concepts().size());
        expansion.mountedVersions().forEach((scheme, version) ->
                evidence.addMountedSchemes(MountedConceptScheme.newBuilder().setSchemeId(scheme).setVersion(version)));
        for (var skipped : expansion.unresolvedMappings())
            evidence.addUnresolvedMappings(UnresolvedConceptMapping.newBuilder().setFrom(skipped.from())
                    .setRelation(skipped.relation()).setTarget(skipped.target()).setReason(skipped.reason()));
        return evidence.build();
    }

    /** Whether the request and every message inside it carry only known fields. */
    private static boolean known(Message message) {
        if (!message.getUnknownFields().asMap().isEmpty()) return false;
        for (var field : message.getAllFields().entrySet()) {
            if (field.getKey().getJavaType() != Descriptors.FieldDescriptor.JavaType.MESSAGE) continue;
            if (field.getValue() instanceof Message nested) { if (!known(nested)) return false; }
            else if (field.getValue() instanceof List<?> nestedList)
                for (var element : nestedList) if (element instanceof Message nested && !known(nested)) return false;
        }
        return true;
    }

    /**
     * Refuses a filter that contradicts itself by name. The committed range and
     * the field rules are checked by the validator; metadata pairs are checked
     * here, since one key with two values cannot both hold. The timestamps must
     * also fit the catalog's sort key.
     */
    static void requireConsistentFilter(DocumentFilter filter) {
        var seen = new HashMap<String, String>();
        for (var match : filter.getMetadataList()) {
            var previous = seen.putIfAbsent(match.getKey(), match.getValue());
            if (previous != null && !previous.equals(match.getValue()))
                throw DocumentSaves.malformedException("Metadata filter gives key \"" + match.getKey() + "\" two values");
        }
        if (filter.hasCommittedAfter()) requireCatalogTime(filter.getCommittedAfter(), "filter.committed_after");
        if (filter.hasCommittedBefore()) requireCatalogTime(filter.getCommittedBefore(), "filter.committed_before");
    }

    /** A direction without a field orders nothing; it is refused rather than ignored. */
    static void requireConsistentSort(DocumentSort sort) {
        if (sort.getDescending() && sort.getField() == DocumentSortField.DOCUMENT_SORT_FIELD_UNSPECIFIED)
            throw DocumentSaves.malformedException("sort.descending needs a sort field");
    }

    private static void requireCatalogTime(Timestamp value, String field) {
        if (!Timestamps.isValid(value) || Timestamps.compare(value, CatalogEntries.MIN_COMMITTED) < 0
                || Timestamps.compare(value, CatalogEntries.MAX_COMMITTED) > 0)
            throw DocumentSaves.malformedException(field + " is outside the catalog's time range");
    }

    private static Optional<CatalogRecords.Cursor> cursor(ListDocumentsRequest request) {
        if (request.getPageToken().isEmpty()) return Optional.empty();
        try {
            var token = CatalogPageToken.parseFrom(Base64.getUrlDecoder().decode(request.getPageToken()));
            var field = request.getSort().getField();
            if (token.getVersion() != TOKEN_VERSION || !token.getUnknownFields().asMap().isEmpty() || !token.hasAfter()
                    || !token.getAccountId().equals(request.getAccountId()) || !token.getGraphId().equals(request.getGraphId())
                    || !token.getGraphAddressId().equals(request.getGraphAddressId())
                    || token.getIncludeTombstoned() != request.getIncludeTombstoned()
                    || !token.getFilter().equals(request.getFilter()) || !token.getSort().equals(request.getSort())
                    || token.hasAfterCommittedAt() != (field == DocumentSortField.DOCUMENT_SORT_FIELD_COMMITTED_AT)
                    || token.hasAfterTitle() != (field == DocumentSortField.DOCUMENT_SORT_FIELD_TITLE)
                    || !token.getAfter().getAccountId().equals(request.getAccountId())
                    || (!request.getGraphId().isEmpty() && !token.getAfter().getGraphId().equals(request.getGraphId()))
                    || (!request.getGraphAddressId().isEmpty() && !token.getAfter().getGraphAddressId().equals(request.getGraphAddressId())))
                throw DocumentSaves.malformedException("Page token does not match the listing");
            DocumentReads.requireAddress(token.getAfter());
            if (token.hasAfterCommittedAt()) requireCatalogTime(token.getAfterCommittedAt(), "page_token");
            return Optional.of(new CatalogRecords.Cursor(token.getAfter(), token.getAfterCommittedAt(), token.getAfterTitle()));
        } catch (IllegalArgumentException | InvalidProtocolBufferException invalid) {
            throw new AccessionException(Status.Code.INVALID_ARGUMENT, "Invalid page token", null, invalid);
        }
    }

    private static String token(ListDocumentsRequest request, CatalogRecords.Cursor after) {
        var token = CatalogPageToken.newBuilder().setVersion(TOKEN_VERSION)
                .setAccountId(request.getAccountId()).setGraphId(request.getGraphId()).setGraphAddressId(request.getGraphAddressId())
                .setIncludeTombstoned(request.getIncludeTombstoned()).setAfter(after.after())
                .setFilter(request.getFilter()).setSort(request.getSort());
        switch (request.getSort().getField()) {
            case DOCUMENT_SORT_FIELD_COMMITTED_AT -> token.setAfterCommittedAt(after.committedAt());
            case DOCUMENT_SORT_FIELD_TITLE -> token.setAfterTitle(after.title());
            default -> {}
        }
        return Base64.getUrlEncoder().withoutPadding().encodeToString(token.build().toByteArray());
    }
}
