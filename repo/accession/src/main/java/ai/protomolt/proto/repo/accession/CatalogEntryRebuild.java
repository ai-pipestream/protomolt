package ai.protomolt.proto.repo.accession;

import ai.protomolt.proto.repo.catalog.Catalog;
import ai.protomolt.proto.repo.catalog.CatalogEntries;
import ai.protomolt.proto.repo.catalog.CatalogRecords;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v2.DocumentLookup;
import ai.protomolt.proto.repo.v2.PartState;
import ai.protomolt.proto.repo.v2.StoredManifest;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import java.util.Objects;
import java.util.Optional;

/**
 * Rebuilds catalog entries from the stored parts (requirements §7: entries are
 * never the source of truth). Each document's current manifest supplies identity,
 * ownership and owner; its CORE part, read and verified from the stack, supplies
 * the lookup. An administrative routine for the host, not a DocumentService call.
 */
public final class CatalogEntryRebuild {
    private static final int PAGE = 500;
    private final Catalog catalog;
    private final DocumentReads reads;

    public CatalogEntryRebuild(Catalog catalog, DocumentReads reads) {
        this.catalog = Objects.requireNonNull(catalog);
        this.reads = Objects.requireNonNull(reads);
    }

    /**
     * Rebuilds every current entry of an account, tombstones included.
     *
     * @param accountId the account
     * @return how many entries were rewritten; a document that moved to a new
     *         revision while being rebuilt is left to that revision's commit
     */
    public int rebuild(String accountId) {
        var listing = CatalogRecords.Listing.of(accountId, "", "", true);
        Optional<CatalogRecords.Cursor> after = Optional.empty();
        int rebuilt = 0;
        while (true) {
            var rows = catalog.documents(listing, after, PAGE);
            for (var row : rows) {
                after = Optional.of(row.cursor());
                var manifest = catalog.manifest(row.address(), row.revision())
                        .orElseThrow(() -> new AccessionException(Status.Code.DATA_LOSS, "Current revision has no manifest", null));
                if (catalog.rewriteEntry(row.address(), row.revision(), lookup(row.address(), manifest))) rebuilt++;
            }
            if (rows.size() < PAGE) return rebuilt;
        }
    }

    /** The lookup of a revision: empty for a tombstone, else projected from its verified CORE bytes. */
    Optional<DocumentLookup> lookup(NodeAddress address, StoredManifest manifest) {
        if (manifest.getTombstone()) return Optional.empty();
        var core = manifest.getPartsList().stream()
                .filter(part -> part.getEntry().getSelector().equals(DocumentSaves.CORE_SELECTOR)).findFirst()
                .orElseThrow(() -> new AccessionException(Status.Code.DATA_LOSS,
                        "Revision " + manifest.getRevision() + " of " + address.getDocId() + " has no CORE part", null));
        if (core.getEntry().getState() != PartState.PART_STATE_PRESENT || !core.hasRef())
            throw new AccessionException(Status.Code.DATA_LOSS,
                    "Revision " + manifest.getRevision() + " of " + address.getDocId() + " has no CORE object", null);
        reads.requirePartWithinCeiling(core.getRef());
        try {
            return Optional.of(CatalogEntries.lookup(Document.parseFrom(reads.read(core.getRef()))));
        } catch (InvalidProtocolBufferException corrupt) {
            throw new AccessionException(Status.Code.DATA_LOSS, "Stored CORE part does not decode", null, corrupt);
        }
    }
}
