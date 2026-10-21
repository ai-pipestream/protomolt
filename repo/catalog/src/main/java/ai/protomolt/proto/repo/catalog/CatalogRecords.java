package ai.protomolt.proto.repo.catalog;

import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v2.CatalogEntry;
import ai.protomolt.proto.repo.v2.DocumentEvent;
import ai.protomolt.proto.repo.v2.DocumentFilter;
import ai.protomolt.proto.repo.v2.DocumentLookup;
import ai.protomolt.proto.repo.v2.DocumentSort;
import ai.protomolt.proto.repo.v2.ExpectedRevision;
import ai.protomolt.proto.repo.v2.RecordedOutcome;
import ai.protomolt.proto.repo.v2.StackRef;
import ai.protomolt.proto.repo.v2.StoredManifest;
import ai.protomolt.proto.repo.v2.WriterGrant;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** Values the catalog reads and writes. */
public final class CatalogRecords {
    private CatalogRecords() {}

    /** One row of a listing: the current-revision pointer, its entry and its place in the order. */
    public record ListedDocument(NodeAddress address, long revision, boolean tombstone,
            Timestamp committedAt, CatalogEntry entry, Cursor cursor) {}

    /**
     * A row's position in every supported order: the address, the commit time as
     * the database holds it (microseconds), and the title or the empty string.
     * A listing resumes strictly after it.
     */
    public record Cursor(NodeAddress after, Timestamp committedAt, String title) {
        public Cursor { Objects.requireNonNull(after); Objects.requireNonNull(committedAt); Objects.requireNonNull(title); }
    }

    /** What a listing selects and how it is ordered. */
    public record Listing(String accountId, String graphId, String graphAddressId, boolean includeTombstoned,
            DocumentFilter filter, DocumentSort sort) {
        public Listing {
            Objects.requireNonNull(accountId); Objects.requireNonNull(graphId); Objects.requireNonNull(graphAddressId);
            Objects.requireNonNull(filter); Objects.requireNonNull(sort);
        }

        /** The unfiltered address-ordered listing of one account. */
        public static Listing of(String accountId, String graphId, String graphAddressId, boolean includeTombstoned) {
            return new Listing(accountId, graphId, graphAddressId, includeTombstoned,
                    DocumentFilter.getDefaultInstance(), DocumentSort.getDefaultInstance());
        }
    }

    public record PurgeState(ai.protomolt.proto.repo.v2.CatalogPurge record, boolean completed) {}
    /** An idempotency key already belongs to a different document. */
    public static final class PurgeConflict extends IllegalStateException {
        public PurgeConflict() { super("Purge ID belongs to another document"); }
    }

    public record Current(long revision, boolean tombstone) {}

    /** A recorded terminal outcome and the intent it was recorded for. */
    public record Outcome(String intentSha256, RecordedOutcome outcome, Optional<RejectionAccess> rejectionAccess) {
        public Outcome { Objects.requireNonNull(intentSha256); Objects.requireNonNull(outcome); Objects.requireNonNull(rejectionAccess); }
    }

    /** Internal access context; never supplied by the request or exposed in the outcome. */
    public record RejectionAccess(NodeAddress address, boolean create, Optional<CredentialIdentity> creator) {}
    public record CredentialIdentity(String issuer, UUID credentialId) {}

    /** Commit-time recheck of the presented credential generation. */
    public record CredentialCheck(String issuer, UUID credentialId, long generation) {}

    /**
     * Everything one accession writes. The manifest, outcome and event are built
     * for revision {@code expected + 1}; the commit refuses any other numbering.
     * {@code grantIds} are every writer grant authorizing this write, not
     * historical producer evidence; the commit rechecks each against revocation
     * under a share lock. {@code lookup} is the lookup of a written or sourced CORE
     * part; empty when CORE is carried, in which case the catalog keeps the row's
     * current lookup. A tombstone has none; a create needs one.
     */
    public record Commit(NodeAddress address, String operationId, String intentSha256, ExpectedRevision expected,
                         StoredManifest manifest, RecordedOutcome outcome, DocumentEvent event,
                         List<StackRef> newObjects, List<StackRef> carriedObjects, Map<String, ByteString> artifacts,
                         List<String> grantIds, Optional<CredentialCheck> credential,
                         Optional<DocumentLookup> lookup) {
        public Commit {
            Objects.requireNonNull(address); Objects.requireNonNull(operationId); Objects.requireNonNull(intentSha256);
            Objects.requireNonNull(expected); Objects.requireNonNull(manifest); Objects.requireNonNull(outcome);
            Objects.requireNonNull(event); Objects.requireNonNull(lookup);
            if (manifest.getTombstone() && lookup.isPresent())
                throw new IllegalArgumentException("A tombstone commit carries no lookup");
            if (expected.hasNone() && !manifest.getTombstone() && lookup.isEmpty())
                throw new IllegalArgumentException("A creating commit needs the lookup of its CORE part");
            newObjects = List.copyOf(newObjects); carriedObjects = List.copyOf(carriedObjects);
            artifacts = Map.copyOf(artifacts);
            // Deduplicated and in unsigned UTF-8 order: the commit locks grants in this order.
            grantIds = grantIds.stream().map(Objects::requireNonNull).distinct().sorted(CatalogRecords::compareUtf8).toList();
            Objects.requireNonNull(credential);
        }
    }

    /** A rejection either finds/records an outcome or loses writer authority. */
    public sealed interface RejectionResult permits Recorded, WriteDenied {}

    /** What a commit did. */
    public sealed interface CommitResult {}
    /** The accession, outcome and event were written and the pointer moved. */
    public record Committed() implements CommitResult {}
    /** The operation has a recorded outcome, either found or newly stored as a rejection. */
    public record Recorded(Outcome outcome) implements CommitResult, RejectionResult {}
    /** The current revision differs from the expected one, and no outcome is recorded. */
    public record Conflict(long currentRevision, boolean currentTombstone) implements CommitResult {}
    /** A carried-forward object was purged before the commit. */
    public record SourcePurged(StackRef ref) implements CommitResult {}
    /** The destination address was permanently purged and cannot accept another revision. */
    public record TargetPurged() implements CommitResult {}
    /** The writer grant or credential was revoked or rotated before the commit. */
    public record WriteDenied(String reason) implements CommitResult, RejectionResult {}

    /** One provisioned grant with its exact stored snapshot. */
    public record Grant(WriterGrant grant, ByteString snapshot, String snapshotSha256, boolean revoked) {}

    /** One credential authority row. */
    public record Credential(long generation, String principal, boolean revoked) {}

    /** One outbox row awaiting relay. */
    public record OutboxEvent(String eventId, String key, DocumentEvent event, int attempts) {}

    /**
     * The outbox partition key of an event: every address component in canonical
     * order (account, graph, graph address, document), each prefixed by its UTF-8
     * length so separators inside a component stay unambiguous and two documents
     * never share a key, whatever characters their addresses hold.
     */
    public static String eventKey(NodeAddress address) {
        var key = new StringBuilder();
        for (var component : List.of(address.getAccountId(), address.getGraphId(), address.getGraphAddressId(),
                address.getDocId()))
            key.append(component.getBytes(java.nio.charset.StandardCharsets.UTF_8).length).append('/').append(component);
        return key.toString();
    }

    /** Unsigned UTF-8 byte order, which equals code point order. */
    public static int compareUtf8(String left, String right) {
        return Objects.compare(left, right, (a, b) -> {
            int i = 0, j = 0;
            while (i < a.length() && j < b.length()) {
                int x = a.codePointAt(i), y = b.codePointAt(j);
                if (x != y) return Integer.compare(x, y);
                i += Character.charCount(x); j += Character.charCount(y);
            }
            return Integer.compare(a.length() - i, b.length() - j);
        });
    }
}
