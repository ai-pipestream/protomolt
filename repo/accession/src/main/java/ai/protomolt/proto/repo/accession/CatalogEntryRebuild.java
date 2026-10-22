package ai.protomolt.proto.repo.accession;

import ai.protomolt.proto.repo.catalog.Catalog;
import ai.protomolt.proto.repo.catalog.CatalogEntries;
import ai.protomolt.proto.repo.catalog.CatalogRecords;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v2.DocumentLookup;
import ai.protomolt.proto.repo.v2.PartState;
import ai.protomolt.proto.repo.v2.StackRef;
import ai.protomolt.proto.repo.v2.StoredManifest;
import ai.protomolt.proto.repo.v2.StoredPartEntry;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Rebuilds catalog entries from the stored parts (requirements §7: entries are
 * never the source of truth). Each document's current manifest supplies identity,
 * ownership and owner; its CORE part, read and verified from the stack, supplies
 * the lookup, and the same part decoded with its retained schema supplies the
 * static link rows (requirements §17). Every earlier revision's CORE part
 * supplies that revision's admitted structured_data type, which pinned links
 * check. An administrative routine for the host, not a DocumentService call.
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
     * Rebuilds every current entry of an account, tombstones included, and the
     * structured_data type of every revision of those documents.
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
                // One CORE read serves the lookup, the link entries and the revision's type.
                var core = core(row.address(), manifest);
                var lookup = core.map(bytes -> lookup(bytes, manifest));
                var links = core.map(bytes -> DocumentLinkReads.rows(reads.decodeRetained(manifest, DocumentSaves.CORE_SELECTOR, bytes)))
                        .orElse(List.of());
                if (catalog.rewriteEntry(row.address(), row.revision(), lookup, links)) rebuilt++;
                rebuildRevisionTypes(row.address(), manifest, lookup);
            }
            if (rows.size() < PAGE) return rebuilt;
        }
    }

    /**
     * Rewrites the admitted structured_data type of every revision, newest first.
     * Revisions that share one CORE object share its lookup, so each object is read once.
     */
    private void rebuildRevisionTypes(NodeAddress address, StoredManifest current, Optional<DocumentLookup> lookup) {
        catalog.rewriteRevisionType(address, current.getRevision(), type(lookup));
        var known = new HashMap<String, Optional<DocumentLookup>>();
        coreRef(current).ifPresent(ref -> known.put(ref.getKey(), lookup));
        long before = current.getRevision();
        while (before > 1) {
            var page = catalog.revisions(address, before, PAGE);
            if (page.isEmpty()) return;
            for (var revision : page) {
                var ref = coreRef(revision);
                var revisionLookup = ref.isPresent() && known.containsKey(ref.get().getKey())
                        ? known.get(ref.get().getKey()).map(found -> DocumentSaves.admittedLookup(found, revision.getValidation()))
                        : core(address, revision).map(bytes -> lookup(bytes, revision));
                ref.ifPresent(found -> known.putIfAbsent(found.getKey(), revisionLookup));
                catalog.rewriteRevisionType(address, revision.getRevision(), type(revisionLookup));
                before = revision.getRevision();
            }
        }
    }

    private static Optional<String> type(Optional<DocumentLookup> lookup) {
        return lookup.filter(DocumentLookup::hasStructuredDataType).map(DocumentLookup::getStructuredDataType);
    }

    private static Optional<StackRef> coreRef(StoredManifest manifest) {
        if (manifest.getTombstone()) return Optional.empty();
        return manifest.getPartsList().stream()
                .filter(part -> part.getEntry().getSelector().equals(DocumentSaves.CORE_SELECTOR) && part.hasRef())
                .findFirst().map(StoredPartEntry::getRef);
    }

    /** A revision's verified CORE bytes; empty for a tombstone. */
    private Optional<ByteString> core(NodeAddress address, StoredManifest manifest) {
        if (manifest.getTombstone()) return Optional.empty();
        var core = manifest.getPartsList().stream()
                .filter(part -> part.getEntry().getSelector().equals(DocumentSaves.CORE_SELECTOR)).findFirst()
                .orElseThrow(() -> new AccessionException(Status.Code.DATA_LOSS,
                        "Revision " + manifest.getRevision() + " of " + address.getDocId() + " has no CORE part", null));
        if (core.getEntry().getState() != PartState.PART_STATE_PRESENT || !core.hasRef())
            throw new AccessionException(Status.Code.DATA_LOSS,
                    "Revision " + manifest.getRevision() + " of " + address.getDocId() + " has no CORE object", null);
        reads.requirePartWithinCeiling(core.getRef());
        return Optional.of(reads.read(core.getRef()));
    }

    /** The lookup projected from a revision's CORE bytes, with only an admitted structured_data type. */
    private static DocumentLookup lookup(ByteString core, StoredManifest manifest) {
        try {
            return DocumentSaves.admittedLookup(CatalogEntries.lookup(Document.parseFrom(core)), manifest.getValidation());
        } catch (InvalidProtocolBufferException corrupt) {
            throw new AccessionException(Status.Code.DATA_LOSS, "Stored CORE part does not decode", null, corrupt);
        }
    }
}
