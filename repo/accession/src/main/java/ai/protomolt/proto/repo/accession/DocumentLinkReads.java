package ai.protomolt.proto.repo.accession;

import ai.protomolt.proto.repo.admission.DocumentLinks;
import ai.protomolt.proto.repo.catalog.CatalogRecords;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v2.Backlink;
import ai.protomolt.proto.repo.v2.BacklinkPageToken;
import ai.protomolt.proto.repo.v2.DynamicLinkSelector;
import ai.protomolt.proto.repo.v2.ListBacklinksRequest;
import ai.protomolt.proto.repo.v2.ListBacklinksResponse;
import ai.protomolt.proto.repo.v2.ListDocumentsRequest;
import ai.protomolt.proto.repo.v2.ListLinksRequest;
import ai.protomolt.proto.repo.v2.ListLinksResponse;
import ai.protomolt.proto.repo.v2.ResolveLinkRequest;
import ai.protomolt.proto.repo.v2.ResolveLinkResponse;
import ai.protomolt.proto.repo.v2.ResolvedLink;
import ai.protomolt.proto.repo.v2.StaticLinkState;
import ai.protomolt.proto.repo.v2.StaticTarget;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * The link reads of requirements §17. {@code ListLinks} decodes a revision's CORE
 * part with its retained schema and reports each static target's state for the
 * reader; {@code ResolveLink} runs a dynamic link's selector as a #527 listing in
 * the linking document's account, under the reader's access, expanding a concept
 * filter against the mounted schemes when it runs; {@code ListBacklinks}
 * reads the catalog edge table and keeps only edges whose linking document the
 * reader can read, before pagination, so a page never carries an edge the caller
 * cannot see.
 */
public final class DocumentLinkReads {
    /** The backlink page token format this listing issues and accepts. */
    static final int TOKEN_VERSION = 1;
    private final DocumentReads reads;
    private final DocumentLists lists;

    public DocumentLinkReads(DocumentReads reads, DocumentLists lists) {
        this.reads = Objects.requireNonNull(reads);
        this.lists = Objects.requireNonNull(lists);
    }

    /** The catalog rows of a CORE part's static links. */
    static List<CatalogRecords.Link> rows(List<DocumentLinks.Link> links) {
        var rows = new ArrayList<CatalogRecords.Link>();
        for (var link : links) {
            if (!link.link().hasStaticTarget()) continue;
            var target = link.link().getStaticTarget();
            rows.add(new CatalogRecords.Link(link.fieldPath(), link.ordinal(), link.field().getRelation(), target.getAddress(),
                    target.hasRevision() ? OptionalLong.of(target.getRevision()) : OptionalLong.empty()));
        }
        return List.copyOf(rows);
    }

    /**
     * The links of one revision.
     *
     * @param caller the reader
     * @param request the linking document and revision
     * @return each link with its field path, relation and target
     */
    public ListLinksResponse listLinks(RepositoryCaller caller, ListLinksRequest request) {
        return DocumentReads.guarded(() -> {
            DocumentReads.valid(request);
            var stored = reads.resolve(caller, request.getAddress(), request.getRevision());
            var response = ListLinksResponse.newBuilder().setRevision(stored.getRevision());
            for (var link : reads.coreLinks(stored)) {
                var resolved = ResolvedLink.newBuilder().setFieldPath(link.fieldPath()).setOrdinal(link.ordinal())
                        .setRelation(link.field().getRelation());
                if (link.link().hasLabel()) resolved.setLabel(link.link().getLabel());
                if (link.link().hasStaticTarget()) {
                    resolved.setStaticTarget(state(caller, request.getAddress().getAccountId(), link.link().getStaticTarget()));
                } else {
                    var dynamic = link.link().getDynamicTarget();
                    resolved.setDynamicTarget(DynamicLinkSelector.newBuilder().setFilter(dynamic.getFilter())
                            .setSort(dynamic.getSort()));
                }
                response.addLinks(resolved);
            }
            return response.build();
        });
    }

    /**
     * A static target as this reader sees it. Missing, purged, in another account,
     * unreadable, or pinned to a revision that does not exist: unavailable. Otherwise
     * a tombstoned current revision is tombstoned; a pinned revision stays readable.
     */
    private StaticLinkState state(RepositoryCaller caller, String account, StaticTarget target) {
        var address = target.getAddress();
        var state = StaticLinkState.newBuilder().setAddress(address);
        if (target.hasRevision()) state.setPinnedRevision(target.getRevision());
        var pinned = target.hasRevision() ? OptionalLong.of(target.getRevision()) : OptionalLong.empty();
        var found = address.getAccountId().equals(account) ? reads.catalog().linkTarget(address, pinned)
                : Optional.<CatalogRecords.LinkTarget>empty();
        if (found.isEmpty() || !DocumentReads.canRead(caller, address, found.get().entry().getOwnership())
                || (pinned.isPresent() && found.get().pinned().isEmpty()))
            return state.setTargetUnavailable(true).build();
        return state.setTargetTombstoned(found.get().tombstone()).build();
    }

    /**
     * One page of a dynamic link's targets: the selector runs as a listing in the
     * linking document's account under the reader's access, tombstones excluded.
     *
     * @param caller the reader
     * @param request the link and the page
     * @return the targets the reader can read
     */
    public ResolveLinkResponse resolveLink(RepositoryCaller caller, ResolveLinkRequest request) {
        return DocumentReads.guarded(() -> {
            DocumentReads.valid(request);
            var stored = reads.resolve(caller, request.getAddress(), request.getRevision());
            var link = reads.coreLinks(stored).stream()
                    .filter(found -> found.fieldPath().equals(request.getFieldPath()) && found.ordinal() == request.getOrdinal())
                    .findFirst().orElseThrow(() -> DocumentReads.notFound("Revision " + stored.getRevision()
                            + " has no link at " + request.getFieldPath() + "[" + request.getOrdinal() + "]"));
            if (!link.link().hasDynamicTarget())
                throw new AccessionException(Status.Code.FAILED_PRECONDITION,
                        "The link is static; ListLinks returns its target", null);
            var dynamic = link.link().getDynamicTarget();
            var listed = lists.list(caller, ListDocumentsRequest.newBuilder().setAccountId(request.getAddress().getAccountId())
                    .setIncludeTombstoned(false).setPageSize(request.getPageSize()).setPageToken(request.getPageToken())
                    .setFilter(dynamic.getFilter()).setSort(dynamic.getSort()).build());
            var response = ResolveLinkResponse.newBuilder().addAllDocuments(listed.getDocumentsList())
                    .setNextPageToken(listed.getNextPageToken());
            if (listed.hasConceptEvidence()) response.setConceptEvidence(listed.getConceptEvidence());
            return response.build();
        });
    }

    /**
     * Static links into a document from current revisions the reader can read.
     *
     * @param caller the reader
     * @param request the target, an optional relation and the page
     * @return the edges in linking call number, field path and ordinal order
     */
    public ListBacklinksResponse listBacklinks(RepositoryCaller caller, ListBacklinksRequest request) {
        return DocumentReads.guarded(() -> {
            DocumentReads.valid(request);
            if (!request.getUnknownFields().asMap().isEmpty() || !request.getTarget().getUnknownFields().asMap().isEmpty())
                throw DocumentSaves.malformedException("Invalid backlink listing request");
            DocumentReads.requireBinding(caller);
            DocumentReads.requireAddress(request.getTarget());
            DocumentReads.requireAccount(caller, request.getTarget().getAccountId());
            var relation = request.hasRelation() ? Optional.of(request.getRelation()) : Optional.<String>empty();
            var after = cursor(request);
            var response = ListBacklinksResponse.newBuilder();
            CatalogRecords.BacklinkCursor lastReturned = null;
            // Each query is bounded. Skip edges from unreadable documents without leaking them in tokens.
            while (true) {
                var rows = reads.catalog().backlinks(request.getTarget(), relation, after, request.getPageSize() + 1);
                for (var row : rows) {
                    after = Optional.of(new CatalogRecords.BacklinkCursor(row.source(), row.link().fieldPath(), row.link().ordinal()));
                    if (!DocumentReads.canRead(caller, row.source(), row.entry().getOwnership())) continue;
                    if (response.getBacklinksCount() == request.getPageSize())
                        return response.setNextPageToken(token(request, lastReturned)).build();
                    var backlink = Backlink.newBuilder().setAddress(row.source()).setRevision(row.revision())
                            .setFieldPath(row.link().fieldPath()).setOrdinal(row.link().ordinal())
                            .setRelation(row.link().relation());
                    row.link().pinnedRevision().ifPresent(backlink::setPinnedRevision);
                    response.addBacklinks(backlink);
                    lastReturned = new CatalogRecords.BacklinkCursor(row.source(), row.link().fieldPath(), row.link().ordinal());
                }
                if (rows.size() < request.getPageSize() + 1) return response.build();
            }
        });
    }

    private static Optional<CatalogRecords.BacklinkCursor> cursor(ListBacklinksRequest request) {
        if (request.getPageToken().isEmpty()) return Optional.empty();
        try {
            var token = BacklinkPageToken.parseFrom(Base64.getUrlDecoder().decode(request.getPageToken()));
            if (token.getVersion() != TOKEN_VERSION || !token.getUnknownFields().asMap().isEmpty() || !token.hasAfter()
                    || !token.getTarget().equals(request.getTarget()) || token.hasRelation() != request.hasRelation()
                    || !token.getRelation().equals(request.getRelation())
                    || !token.getAfter().getAccountId().equals(request.getTarget().getAccountId())
                    || token.getAfterFieldPath().isEmpty() || token.getAfterOrdinal() < 0)
                throw DocumentSaves.malformedException("Page token does not match the listing");
            DocumentReads.requireAddress(token.getAfter());
            return Optional.of(new CatalogRecords.BacklinkCursor(token.getAfter(), token.getAfterFieldPath(), token.getAfterOrdinal()));
        } catch (IllegalArgumentException | InvalidProtocolBufferException invalid) {
            throw new AccessionException(Status.Code.INVALID_ARGUMENT, "Invalid page token", null, invalid);
        }
    }

    private static String token(ListBacklinksRequest request, CatalogRecords.BacklinkCursor after) {
        var token = BacklinkPageToken.newBuilder().setVersion(TOKEN_VERSION).setTarget(request.getTarget())
                .setAfter(after.source()).setAfterFieldPath(after.fieldPath()).setAfterOrdinal(after.ordinal());
        if (request.hasRelation()) token.setRelation(request.getRelation());
        return Base64.getUrlEncoder().withoutPadding().encodeToString(token.build().toByteArray());
    }
}
