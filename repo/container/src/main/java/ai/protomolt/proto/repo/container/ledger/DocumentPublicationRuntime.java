package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.blob.spi.BackendIdentity;
import ai.protomolt.proto.repo.blob.spi.OpenedBlobStore;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.codec.PartObject;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import ai.protomolt.proto.repo.v1.DocumentPublicationMember;
import ai.protomolt.proto.repo.v1.DocumentPublicationResult;
import ai.protomolt.proto.repo.v1.DocumentPublicationRejection;
import com.google.protobuf.InvalidProtocolBufferException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Host composition of native publication and its resource lifetime. This is not a
 * transport endpoint. The host supplies authenticated callers, qualified placements
 * and authorized schema resolution. Database and provider clients remain borrowed
 * until shutdownStep succeeds; a timeout or failure leaves shutdown retryable.
 */
public final class DocumentPublicationRuntime implements AutoCloseable {
    /** Authorized durable rejection, distinct from retryable execution failures. */
    public static final class Rejected extends RuntimeException {
        private final DocumentPublicationRejection receipt;
        private Rejected(DocumentPublicationRejection receipt) {
            super("Publication has a durable terminal rejection");
            this.receipt = Objects.requireNonNull(receipt);
        }
        public DocumentPublicationRejection receipt() { return receipt; }
    }
    public enum Mode { TYPED, OPAQUE }
    public record PayloadKey(String member, int revisionOrdinal) {
        public PayloadKey {
            Objects.requireNonNull(member);
            if (revisionOrdinal < 0) throw new IllegalArgumentException("Negative revision ordinal");
        }
    }
    public static final class Placement {
        private final DocumentUploadPlan.Placement selected;
        /** Capture an immutable drive snapshot; admission rechecks it under SQL locks. */
        public Placement(DriveRecord drive, String generation, ManagedBackendLedger.Profile profile) {
            selected = DocumentUploadPlan.Placement.sample(drive, generation, profile);
        }
    }
    public record Backend(BackendIdentity identity, OpenedBlobStore opened) {
        public Backend { Objects.requireNonNull(identity); Objects.requireNonNull(opened); }
    }
    @FunctionalInterface public interface Backends {
        /** Exact, preconfigured generation/profile lookup; no fallback or client creation. */
        Backend resolve(String generation, ManagedBackendLedger.Profile profile);
    }
    @FunctionalInterface public interface Schemas {
        /** Resolve under this caller's authority; definitions must include their complete imports. */
        DocumentSchemaAdmission.Definition resolve(RepositoryCaller caller, DocumentPublicationMember member,
                DocumentSchemaAdmission.Selection occurrence);
    }

    private final DocumentUploadCoordinator uploads;
    private final DocumentPublicationSessions sessions;
    private final DocumentReadLifecycle reads;
    private boolean stopping;
    private boolean stopped;

    /**
     * Reader and ledger belong exclusively to this runtime. The same budget bounds
     * staging/admission; the supplied reader must use the host's chosen shared budget.
     * The caller retains constructor-failure cleanup of the supplied reader/ledger.
     */
    public <R extends DocumentRetainedReader & DocumentReadLifecycle.Reader> DocumentPublicationRuntime(
            Tx tx, DriveLedger drives, DocumentReadLedger ledger, R reader, PayloadBudget budget,
            Backends backends, DocumentRevisionAssembly.Limits assemblyLimits, SqlTimeouts sqlTimeouts,
            int parallelism, Duration flushAge, Duration lease, int maxSessions, long maxCommandBytes,
            int cleanupBatchSize, boolean deliverEvents) {
        Objects.requireNonNull(backends);
        reads = new DocumentReadLifecycle(ledger, reader, cleanupBatchSize);
        uploads = new DocumentUploadCoordinator(tx, drives, budget, (generation, profile) -> {
            var selected = Objects.requireNonNull(backends.resolve(generation, profile));
            return new DocumentUploadCoordinator.Backend(selected.identity(), selected.opened());
        }, parallelism, flushAge, sqlTimeouts);
        var execution = new DocumentPublicationExecution(tx, drives, ledger, uploads, reader, budget,
                assemblyLimits, deliverEvents);
        sessions = new DocumentPublicationSessions(tx, execution, lease, maxSessions, maxCommandBytes);
    }

    /** Borrowed payloads must remain stable until return, including after caller cancellation. */
    public DocumentPublicationResult execute(RepositoryCaller caller, DocumentPublicationCommand command,
            Map<UUID, Placement> placements, Map<PayloadKey, PartObject> bodies, Map<String, String> attributes,
            Map<String, Mode> modes, Optional<DocumentSchemaAdmission.Definition> container,
            Schemas schemas, RepositoryReadControl control) throws InvalidProtocolBufferException {
        Objects.requireNonNull(schemas);
        Objects.requireNonNull(command);
        if (bodies.size() > DocumentPublicationCommand.MAX_PARTS
                || placements.size() > command.intent().getMembersCount()
                || modes.size() > command.intent().getMembersCount())
            throw new IllegalArgumentException("Publication inputs exceed command bounds");
        var payloads = new HashMap<DocumentUploadPayloads.Key, PartObject>();
        bodies.forEach((key, body) -> payloads.put(new DocumentUploadPayloads.Key(key.member(), key.revisionOrdinal()), body));
        try {
            return sessions.execute(caller, command, placements(placements), payloads, attributes, modes(modes), container,
                    (member, occurrence) -> schemas.resolve(caller, member, occurrence), control);
        } catch (DocumentPublicationReplay.Terminated rejected) {
            throw new Rejected(rejected.receipt());
        }
    }

    /**
     * Trusted host-authorized takeover; do not expose directly as a client RPC.
     * Empty grants ownership only, not publication or acceptance.
     */
    public Optional<DocumentPublicationResult> recover(RepositoryCaller caller, DocumentPublicationCommand command,
            Map<UUID, Placement> placements, long predecessorGeneration, Map<String, Mode> modes,
            RepositoryReadControl control) {
        if (placements.size() > command.intent().getMembersCount() || modes.size() > command.intent().getMembersCount())
            throw new IllegalArgumentException("Recovery inputs exceed command bounds");
        try {
            return sessions.recover(caller, command, placements(placements), predecessorGeneration, modes(modes), control);
        } catch (DocumentPublicationReplay.Terminated rejected) {
            throw new Rejected(rejected.receipt());
        }
    }

    public boolean retireSuperseded(RepositoryCaller caller, DocumentPublicationCommand command, RepositoryReadControl control) {
        return sessions.retireSuperseded(caller, command, control);
    }

    private static Map<UUID, DocumentUploadPlan.Placement> placements(Map<UUID, Placement> selected) {
        var result = new HashMap<UUID, DocumentUploadPlan.Placement>();
        selected.forEach((id, placement) -> result.put(id, placement.selected));
        return result;
    }

    private static Map<String, DocumentPublicationCandidate.Mode> modes(Map<String, Mode> selected) {
        var result = new HashMap<String, DocumentPublicationCandidate.Mode>();
        selected.forEach((member, mode) -> result.put(member, switch (mode) {
            case TYPED -> DocumentPublicationCandidate.Mode.TYPED;
            case OPAQUE -> DocumentPublicationCandidate.Mode.OPAQUE;
        }));
        return result;
    }

    /** Refuse new calls. Accepted work may finish; this does not release borrowed resources. */
    @Override public void close() { sessions.close(); }

    /** One bounded maintenance pass; the host schedules and retries failures. */
    public synchronized int tick() {
        if (stopping) throw new IllegalStateException("Publication runtime is stopping");
        return reads.tick();
    }

    /** Local wait budget excludes SQL time; configure database statement/network timeouts separately. */
    public synchronized boolean shutdownStep(Duration waitBudget) throws InterruptedException {
        Objects.requireNonNull(waitBudget);
        if (waitBudget.isNegative()) throw new IllegalArgumentException("Shutdown wait must not be negative");
        long budget = waitBudget.toNanos(), start = System.nanoTime();
        if (stopped) return true;
        stopping = true;
        close();
        if (!sessions.awaitIdle(remaining(budget, start))) return false;
        uploads.close();
        if (!uploads.awaitIdle(remaining(budget, start))) return false;
        stopped = reads.shutdownStep(remaining(budget, start));
        return stopped;
    }

    private static Duration remaining(long budget, long start) {
        return Duration.ofNanos(Math.max(0, budget - (System.nanoTime() - start)));
    }
}
