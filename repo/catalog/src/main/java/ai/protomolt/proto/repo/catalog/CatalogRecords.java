package ai.protomolt.proto.repo.catalog;

import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v2.DocumentEvent;
import ai.protomolt.proto.repo.v2.ExpectedRevision;
import ai.protomolt.proto.repo.v2.RecordedOutcome;
import ai.protomolt.proto.repo.v2.StackRef;
import ai.protomolt.proto.repo.v2.StoredManifest;
import ai.protomolt.proto.repo.v2.WriterGrant;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** Values the catalog reads and writes. */
public final class CatalogRecords {
    private CatalogRecords() {}

    /** A document's current-revision pointer. */
    public record Current(long revision, boolean tombstone) {}

    /** A recorded terminal outcome and the intent it was recorded for. */
    public record Outcome(String intentSha256, RecordedOutcome outcome) {
        public Outcome { Objects.requireNonNull(intentSha256); Objects.requireNonNull(outcome); }
    }

    /** Commit-time recheck of the presented credential generation. */
    public record CredentialCheck(String issuer, UUID credentialId, long generation) {}

    /**
     * Everything one accession writes. The manifest, outcome and event are built
     * for revision {@code expected + 1}; the commit refuses any other numbering.
     */
    public record Commit(NodeAddress address, String operationId, String intentSha256, ExpectedRevision expected,
                         StoredManifest manifest, RecordedOutcome outcome, DocumentEvent event,
                         List<StackRef> newObjects, List<StackRef> carriedObjects, Map<String, ByteString> artifacts,
                         Optional<String> grantId, Optional<CredentialCheck> credential) {
        public Commit {
            Objects.requireNonNull(address); Objects.requireNonNull(operationId); Objects.requireNonNull(intentSha256);
            Objects.requireNonNull(expected); Objects.requireNonNull(manifest); Objects.requireNonNull(outcome);
            Objects.requireNonNull(event);
            newObjects = List.copyOf(newObjects); carriedObjects = List.copyOf(carriedObjects);
            artifacts = Map.copyOf(artifacts);
            Objects.requireNonNull(grantId); Objects.requireNonNull(credential);
        }
    }

    /** What a commit did. */
    public sealed interface CommitResult {}
    /** The accession, outcome and event were written and the pointer moved. */
    public record Committed() implements CommitResult {}
    /** An outcome was already recorded for the operation ID; nothing was written. */
    public record Recorded(Outcome outcome) implements CommitResult {}
    /** The current revision differs from the expected one, and no outcome is recorded. */
    public record Conflict(long currentRevision, boolean currentTombstone) implements CommitResult {}
    /** A carried-forward object was purged before the commit. */
    public record SourcePurged(StackRef ref) implements CommitResult {}
    /** The writer grant or credential was revoked or rotated before the commit. */
    public record WriteDenied(String reason) implements CommitResult {}

    /** One provisioned grant with its exact stored snapshot. */
    public record Grant(WriterGrant grant, ByteString snapshot, String snapshotSha256, boolean revoked) {}

    /** One credential authority row. */
    public record Credential(long generation, String principal, boolean revoked) {}

    /** One outbox row awaiting relay. */
    public record OutboxEvent(String eventId, String key, DocumentEvent event, int attempts) {}
}
