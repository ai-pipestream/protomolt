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
import ai.protomolt.proto.repo.v2.RecordedOutcome;
import ai.protomolt.proto.repo.v2.StackRecord;
import ai.protomolt.proto.repo.v2.StoredManifest;
import ai.protomolt.proto.repo.v2.WriterGrant;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Optional;
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

    /** Stored manifests below {@code beforeRevision}, newest first. */
    List<StoredManifest> revisions(NodeAddress address, long beforeRevision, int limit);

    /**
     * Retained schema bytes by SHA-256. Empty when the catalog holds no such
     * artifact; bytes that differ from their digest are {@link CatalogDataLoss}.
     */
    Optional<ByteString> artifact(String sha256);

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
            List<String> grantIds, Optional<CredentialCheck> credential);

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

    /** Counts a failed relay attempt; the event stays pending. */
    void markAttempt(String eventId);

    /** Every outbox event of one document, in revision order. */
    List<DocumentEvent> events(NodeAddress address);

    @Override void close();
}
