package ai.protomolt.proto.repo.accession;

import ai.protomolt.proto.repo.catalog.*;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v2.*;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import java.util.Base64;
import java.util.Objects;
import java.util.Optional;

/** Current document metadata listing, with access filtering before pagination. */
public final class DocumentLists {
    private final Catalog catalog;

    public DocumentLists(Catalog catalog) { this.catalog = Objects.requireNonNull(catalog); }

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
        if (!DocumentSaves.VALIDATOR.validate(request).valid() || !request.getUnknownFields().asMap().isEmpty())
            throw DocumentSaves.malformedException("Invalid document listing request");
        DocumentReads.requireBinding(caller);
        DocumentReads.requireAccount(caller, request.getAccountId());
        var after = cursor(request);
        var response = ListDocumentsResponse.newBuilder();
        NodeAddress lastReturned = null;
        // Each query is bounded. Skip denied rows without leaking their identities in tokens.
        while (true) {
            var rows = catalog.documents(request.getAccountId(), request.getGraphId(), request.getGraphAddressId(),
                    request.getIncludeTombstoned(), after, request.getPageSize() + 1);
            for (var row : rows) {
                after = Optional.of(row.address());
                if (!DocumentReads.canRead(caller, row.address(), row.entry().getOwnership())) continue;
                if (response.getDocumentsCount() == request.getPageSize())
                    return response.setNextPageToken(token(request, lastReturned)).build();
                response.addDocuments(DocumentSummary.newBuilder().setAddress(row.address()).setRevision(row.revision())
                        .setCommittedAt(row.committedAt()).setTombstone(row.tombstone()));
                lastReturned = row.address();
            }
            if (rows.size() < request.getPageSize() + 1) return response.build();
        }
    }

    private static Optional<NodeAddress> cursor(ListDocumentsRequest request) {
        if (request.getPageToken().isEmpty()) return Optional.empty();
        try {
            var token = CatalogPageToken.parseFrom(Base64.getUrlDecoder().decode(request.getPageToken()));
            if (token.getVersion() != 1 || !token.getUnknownFields().asMap().isEmpty() || !token.hasAfter()
                    || !token.getAccountId().equals(request.getAccountId()) || !token.getGraphId().equals(request.getGraphId())
                    || !token.getGraphAddressId().equals(request.getGraphAddressId())
                    || token.getIncludeTombstoned() != request.getIncludeTombstoned()
                    || !token.getAfter().getAccountId().equals(request.getAccountId())
                    || (!request.getGraphId().isEmpty() && !token.getAfter().getGraphId().equals(request.getGraphId()))
                    || (!request.getGraphAddressId().isEmpty() && !token.getAfter().getGraphAddressId().equals(request.getGraphAddressId())))
                throw DocumentSaves.malformedException("Page token does not match the listing");
            DocumentReads.requireAddress(token.getAfter());
            return Optional.of(token.getAfter());
        } catch (IllegalArgumentException | InvalidProtocolBufferException invalid) {
            throw new AccessionException(Status.Code.INVALID_ARGUMENT, "Invalid page token", null, invalid);
        }
    }

    private static String token(ListDocumentsRequest request, NodeAddress after) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(CatalogPageToken.newBuilder().setVersion(1)
                .setAccountId(request.getAccountId()).setGraphId(request.getGraphId()).setGraphAddressId(request.getGraphAddressId())
                .setIncludeTombstoned(request.getIncludeTombstoned()).setAfter(after).build().toByteArray());
    }
}
