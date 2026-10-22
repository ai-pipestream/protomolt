package ai.protomolt.proto.repo.catalog;

import ai.protomolt.proto.repo.catalog.CatalogRecords.Commit;
import ai.protomolt.proto.repo.catalog.CatalogRecords.CommitResult;
import ai.protomolt.proto.repo.catalog.CatalogRecords.CredentialCheck;
import ai.protomolt.proto.repo.catalog.CatalogRecords.Credential;
import ai.protomolt.proto.repo.catalog.CatalogRecords.Current;
import ai.protomolt.proto.repo.catalog.CatalogRecords.Grant;
import ai.protomolt.proto.repo.catalog.CatalogRecords.OutboxEvent;
import ai.protomolt.proto.repo.catalog.CatalogRecords.RejectionResult;
import ai.protomolt.proto.repo.catalog.CatalogRecords.Outcome;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v2.DocumentEvent;
import ai.protomolt.proto.repo.v2.DocumentLookup;
import ai.protomolt.proto.repo.v2.RecordedOutcome;
import ai.protomolt.proto.repo.v2.StackRecord;
import ai.protomolt.proto.repo.v2.StoredManifest;
import ai.protomolt.proto.repo.v2.WriterGrant;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.function.Predicate;

/**
 * Where revisions, part references, operation outcomes and the event outbox live
 * (requirements §2, §7). The save and read handlers depend on this interface and
 * {@link CatalogRecords} only, never on a backend. Every operation runs in its own
 * transaction. An unreachable backend throws {@link CatalogException} with
 * {@link CatalogException#unavailable()} set; a stored record that fails its
 * digest or does not decode throws {@link CatalogDataLoss}.
 */
public interface Catalog extends AutoCloseable {
    // ------------------------------------------------------------------ stacks

    /** Returns the stack ID recorded for this namespace, recording it on first use. */
    String registerStack(StackRecord record);

    // ------------------------------------------------------------------ reads

    /** The recorded outcome for an operation ID, read in a fresh transaction. */
    Optional<Outcome> outcome(String accountId, String operationId);

    /** The document's current-revision pointer. */
    Optional<Current> current(NodeAddress address);

    /** One accession's stored manifest; its stored digest is verified. */
    Optional<StoredManifest> manifest(NodeAddress address, long revision);

    /**
     * The document's current-revision manifest, read in a single transaction.
     *
     * @param address the document address
     * @return the current stored manifest, or empty when the document does not exist or is purged
     */
    default Optional<StoredManifest> currentManifest(NodeAddress address) {
        var state = current(address);
        if (state.isEmpty()) return Optional.empty();
        return Optional.of(manifest(address, state.get().revision())
                .orElseThrow(() -> new CatalogDataLoss("Current revision has no manifest")));
    }

    /** Stored manifests below {@code beforeRevision}, newest first. */
    List<StoredManifest> revisions(NodeAddress address, long beforeRevision, int limit);

    /**
     * Retained schema bytes by SHA-256. Empty when the catalog holds no such
     * artifact; bytes that differ from their digest are {@link CatalogDataLoss}.
     */
    Optional<ByteString> artifact(String sha256);

    /**
     * The stored length of a retained schema artifact, read without loading its
     * bytes, so a reader can reserve that length before it buffers them.
     *
     * @param sha256 the artifact's SHA-256
     * @return the length in bytes, or empty when the catalog holds no such artifact
     */
    OptionalLong artifactLength(String sha256);

    /**
     * Current metadata rows matching the listing, in its order, strictly after the
     * cursor. The order is the sort field, then the address in unsigned UTF-8 order.
     *
     * @param listing the scope, filter and sort
     * @param after the last row of the previous page; empty for the first page
     * @param limit the most rows to return
     * @return the rows, with each row's cursor
     */
    List<CatalogRecords.ListedDocument> documents(CatalogRecords.Listing listing, Optional<CatalogRecords.Cursor> after, int limit);

    /**
     * Rewrites a document's catalog entry and its derived lookup columns from the
     * stored manifest of {@code revision} and the given lookup (requirements §7:
     * entries are rebuildable from stored parts). Nothing changes when the current
     * revision is no longer {@code revision}.
     *
     * @param address the document
     * @param revision the current revision the lookup was read for
     * @param lookup the lookup of that revision's CORE part; empty for a tombstone
     * @return whether the entry was rewritten
     */
    boolean rewriteEntry(NodeAddress address, long revision, Optional<DocumentLookup> lookup);

    // ------------------------------------------------------------------ commit

    /** Commits one accession, or reports why it did not. Never partial. */
    CommitResult commit(Commit commit);

    /**
     * Records a terminal rejection after rechecking, in the storing transaction, that
     * every named grant and the credential still authorize the write. Returns the
     * stored outcome, which is another request's when the operation ID was already
     * recorded, or the denial when authority was revoked or rotated in between.
     */
    RejectionResult recordRejection(String accountId, String operationId, String intentSha256, RecordedOutcome outcome,
            List<String> grantIds, Optional<CredentialCheck> credential, NodeAddress address, boolean creating);

    /**
     * Records the outcome of a save that changed nothing: {@code outcome.unchanged}
     * names {@code expectedRevision} of {@code address}, and the outcome is stored
     * under the operation ID bound to that revision, with no new revision, event or
     * pointer move. In the storing transaction it rechecks every named grant and the
     * credential and that {@code expectedRevision} is still the live current revision.
     *
     * @return {@code Committed} when stored; the found outcome when the operation ID was
     *         already recorded; {@code Conflict} or {@code TargetPurged} when the document
     *         moved; {@code WriteDenied} when authority was revoked or rotated
     */
    CommitResult recordUnchanged(NodeAddress address, String operationId, String intentSha256, long expectedRevision,
            RecordedOutcome outcome, List<String> grantIds, Optional<CredentialCheck> credential);

    /** Marks all document references and durably records exact deletion targets; empty if the address is absent. */
    Optional<CatalogRecords.PurgeState> markPurged(ai.protomolt.proto.repo.v2.PurgeDocumentRequest request,
            com.google.protobuf.Timestamp markedAt);

    /** Completes a recorded purge once, after its provider deletes have succeeded. */
    void completePurge(String accountId, String purgeId, com.google.protobuf.Timestamp completedAt);

    // ------------------------------------------------------------------ writer authority

    /** Grants for one account and credential, by unsigned UTF-8 grant ID. */
    List<Grant> grants(String accountId, String issuer, UUID credentialId);

    /** Whether any grant in the account covers this owner scope, for any credential. */
    boolean ownerKnown(String accountId, Predicate<WriterGrant> covers);

    /** The credential's authority row. */
    Optional<Credential> credential(String issuer, UUID credentialId);

    /** Provisions an immutable grant. Administrative; never reachable from DocumentService. */
    Grant installGrant(WriterGrant grant);

    /** Revokes a grant. Final; prior evidence is kept. */
    void revokeGrant(String grantId);

    /** Registers a credential at generation 1. Administrative. */
    void registerCredential(String issuer, UUID credentialId, String principal);

    /** Moves a credential to its next generation. Administrative. */
    long rotateCredential(String issuer, UUID credentialId);

    /** Revokes a credential's current generation. Final. */
    void revokeCredential(String issuer, UUID credentialId);

    // ------------------------------------------------------------------ outbox

    /** Unrelayed events in commit order. */
    List<OutboxEvent> pendingEvents(int limit);

    /** Stamps an event relayed after the broker acknowledged it. */
    boolean markRelayed(String eventId);

    /**
     * Stamps a batch of events relayed after the broker acknowledged them.
     *
     * @param eventIds the event IDs acknowledged by the broker
     * @return the number of unrelayed rows stamped relayed
     */
    default int markRelayed(List<String> eventIds) {
        int count = 0;
        for (var id : eventIds) if (markRelayed(id)) count++;
        return count;
    }

    /** Counts a failed relay attempt; the event stays pending. */
    void markAttempt(String eventId);

    /**
     * Counts a failed relay attempt for each named event; the events stay pending.
     *
     * @param eventIds the event IDs whose publish attempt failed
     */
    default void markAttempts(List<String> eventIds) {
        for (var id : eventIds) markAttempt(id);
    }

    /** Every outbox event of one document, in revision order. */
    List<DocumentEvent> events(NodeAddress address);

    /**
     * The outbox events of the given revisions of one document, in revision order:
     * one bounded query for one history page. A revision without an event is absent.
     */
    List<DocumentEvent> events(NodeAddress address, List<Long> revisions);

    @Override void close();
}
