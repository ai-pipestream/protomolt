package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.blob.spi.BackendIdentity;
import ai.protomolt.proto.repo.blob.spi.OpenedBlobStore;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.codec.PartObject;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.DocumentPublicationInput;
import ai.protomolt.proto.repo.spi.DocumentPublicationRepository;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.v1.PublishDocumentResponse;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import ai.protomolt.proto.repo.v1.DocumentPublicationMember;
import ai.protomolt.proto.repo.v1.DocumentPublicationResult;
import ai.protomolt.proto.repo.v1.DocumentPublicationRejection;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.nio.file.Path;
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
    /**
     * Trusted host configuration for retained validation rejections. The bundle describes
     * the immutable standard-JAR runtime; it is observed at construction, never accepted
     * from a client as an attestation. Expiry and decision windows use database checks.
     */
    public record Assessments(Path runtimeBundle, Duration retention, Duration minimumRemaining) {
        public Assessments {
            Objects.requireNonNull(runtimeBundle);
            DocumentPublicationAssessmentExecution.requireWindows(retention, minimumRemaining);
        }
    }
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

    @FunctionalInterface public interface SchemaScopes {
        /** Open under the actual caller; the scope must authorize each occurrence, including cache hits. */
        DocumentSchemaAdmission.Resolution open(RepositoryCaller caller, DocumentPublicationMember member,
                RepositoryReadControl control);
    }

    /** Server-owned selection; request fields never supply storage credentials or descriptors. */
    public record PublicationSelection(Map<UUID, Placement> placements, Map<String,String> attributes,
            Optional<DocumentSchemaAdmission.Definition> container, SchemaScopes schemas) {
        public PublicationSelection {
            placements=Map.copyOf(placements); attributes=Map.copyOf(attributes);
            Objects.requireNonNull(container); Objects.requireNonNull(schemas);
        }
    }
    @FunctionalInterface public interface PublicationSelector {
        /**
         * Invoked after input validation and replay, never for a terminal receipt.
         * Return immutable host snapshots and a lazy schema-scope factory, not unowned
         * open resources. Clean up partial acquisition on failure. Calls may overlap.
         */
        PublicationSelection select(RepositoryCaller caller, DocumentPublicationCommand command, RepositoryReadControl control);
    }

    private final DocumentUploadCoordinator uploads;
    private final DocumentPublicationSessions sessions;
    private final DocumentReadLifecycle reads;
    private final java.util.function.Function<RepositoryOperationLedger.Key, RepositoryCaller> drainAuthority;
    private final ExternalWorkers externalWorkers;
    private final RepositoryManagedRecovery recovery;
    private final RepositoryInstalledHistoricalAttempts historical;
    private final DocumentPublicationReplay publicationReplay;
    private final PayloadBudget publicationBudget;
    private final java.util.concurrent.Semaphore publicationPermits;
    private final boolean journaledPublication;
    private final DocumentPublicationScopeCalls scopeCalls = new DocumentPublicationScopeCalls();
    private boolean stopping;
    private boolean stopped;

    /** Trusted host lookup. Request ownership fields cannot grant process authority. */
    @FunctionalInterface public interface DrainAuthority {
        RepositoryCaller forOperation(String account, String principal, UUID operationId);
    }

    /** Separate trusted host opt-in for exact-key recovery, never inferred from drain or request authority. */
    @FunctionalInterface public interface RecoveryAuthority {
        RepositoryCaller forOperation(String account, String principal, UUID operationId);
    }

    /**
     * Exclusively owned host workers outside the publication runtime, including abandoned
     * schema loads. Close admission without blocking; awaitIdle must include every accepted
     * worker. SQL and provider resources remain borrowed until shutdown succeeds.
     */
    public interface ExternalWorkers {
        void closeAdmission();
        boolean awaitIdle(Duration timeout) throws InterruptedException;
    }

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
        this(tx, drives, ledger, reader, budget, backends, assemblyLimits, sqlTimeouts, parallelism,
                flushAge, lease, maxSessions, maxCommandBytes, cleanupBatchSize, deliverEvents,
                (DocumentPublicationAssessmentExecution) null);
    }

    /**
     * Enable retained validation rejection with a reader supporting both source and
     * assessment reads. Runtime observation failure aborts construction; it never
     * selects the unobserved path. The caller owns reader cleanup on failure.
     * Retry identity remains in this runtime's bounded sessions, not a durable host store.
     */
    public <R extends DocumentRetainedReader & DocumentAssessmentReader & DocumentReadLifecycle.Reader> DocumentPublicationRuntime(
            Tx tx, DriveLedger drives, DocumentReadLedger ledger, R reader, PayloadBudget budget,
            Backends backends, DocumentRevisionAssembly.Limits assemblyLimits, SqlTimeouts sqlTimeouts,
            int parallelism, Duration flushAge, Duration lease, int maxSessions, long maxCommandBytes,
            int cleanupBatchSize, boolean deliverEvents, Assessments assessments) throws IOException {
        this(tx, drives, ledger, reader, budget, backends, assemblyLimits, sqlTimeouts, parallelism,
                flushAge, lease, maxSessions, maxCommandBytes, cleanupBatchSize, deliverEvents,
                observeAssessments(tx, drives, ledger, reader, budget, assemblyLimits, assessments));
    }

    private static DocumentPublicationAssessmentExecution observeAssessments(Tx tx, DriveLedger drives,
            DocumentReadLedger ledger, DocumentAssessmentReader reader, PayloadBudget budget,
            DocumentRevisionAssembly.Limits limits, Assessments assessments) throws IOException {
        Objects.requireNonNull(assessments);
        var observed = DocumentAssessmentRuntimeObserver.observe(assessments.runtimeBundle(), RepositoryReadControl.NONE::check);
        return new DocumentPublicationAssessmentExecution(tx, drives, ledger, reader, budget, limits,
                observed, assessments.retention(), assessments.minimumRemaining());
    }

    private <R extends DocumentRetainedReader & DocumentReadLifecycle.Reader> DocumentPublicationRuntime(
            Tx tx, DriveLedger drives, DocumentReadLedger ledger, R reader, PayloadBudget budget,
            Backends backends, DocumentRevisionAssembly.Limits assemblyLimits, SqlTimeouts sqlTimeouts,
            int parallelism, Duration flushAge, Duration lease, int maxSessions, long maxCommandBytes,
            int cleanupBatchSize, boolean deliverEvents, DocumentPublicationAssessmentExecution assessments) {
        this(tx, drives, ledger, reader, budget, backends, assemblyLimits, sqlTimeouts, parallelism,
                flushAge, lease, maxSessions, maxCommandBytes, cleanupBatchSize, deliverEvents, assessments, null);
    }

    /** Private host opt-in. Drain authority is supplied independently of request ownership. */
    static <R extends DocumentRetainedReader & DocumentAssessmentReader & DocumentReadLifecycle.Reader>
            DocumentPublicationRuntime journaled(
            Tx tx, DriveLedger drives, DocumentReadLedger ledger, R reader, PayloadBudget budget,
            Backends backends, DocumentRevisionAssembly.Limits assemblyLimits, SqlTimeouts sqlTimeouts,
            int parallelism, Duration flushAge, Duration lease, int maxSessions, long maxCommandBytes,
            int cleanupBatchSize, boolean deliverEvents, Assessments assessments,
            java.util.function.Function<RepositoryOperationLedger.Key, RepositoryCaller> drainAuthority) throws IOException {
        Objects.requireNonNull(drainAuthority);
        return new DocumentPublicationRuntime(tx, drives, ledger, reader, budget, backends, assemblyLimits, sqlTimeouts,
                parallelism, flushAge, lease, maxSessions, maxCommandBytes, cleanupBatchSize, deliverEvents,
                observeAssessments(tx, drives, ledger, reader, budget, assemblyLimits, assessments), drainAuthority);
    }

    /**
     * Trusted managed-host composition, not a transport endpoint or successor grant.
     * Successful construction transfers lifecycle ownership of the supplied external workers.
     * On construction failure the caller retains cleanup responsibility for all inputs.
     * Whole-host drain is attested only after runtime and external workers both stop.
     */
    public static <R extends DocumentRetainedReader & DocumentAssessmentReader & DocumentReadLifecycle.Reader>
            DocumentPublicationRuntime managedJournaled(
            Tx tx, DriveLedger drives, DocumentReadLedger ledger, R reader, PayloadBudget budget,
            Backends backends, DocumentRevisionAssembly.Limits assemblyLimits, SqlTimeouts sqlTimeouts,
            int parallelism, Duration flushAge, Duration lease, int maxSessions, long maxCommandBytes,
            int cleanupBatchSize, boolean deliverEvents, Assessments assessments,
            DrainAuthority authority, ExternalWorkers externalWorkers) throws IOException {
        return managedJournaled(tx,drives,ledger,reader,budget,backends,assemblyLimits,sqlTimeouts,
                parallelism,flushAge,lease,maxSessions,maxCommandBytes,cleanupBatchSize,deliverEvents,
                assessments,authority,externalWorkers,null);
    }

    /** Explicit recovery opt-in; accepted calls retain one scope through recovery and publication. */
    public static <R extends DocumentRetainedReader & DocumentAssessmentReader & DocumentReadLifecycle.Reader>
            DocumentPublicationRuntime managedJournaled(
            Tx tx, DriveLedger drives, DocumentReadLedger ledger, R reader, PayloadBudget budget,
            Backends backends, DocumentRevisionAssembly.Limits assemblyLimits, SqlTimeouts sqlTimeouts,
            int parallelism, Duration flushAge, Duration lease, int maxSessions, long maxCommandBytes,
            int cleanupBatchSize, boolean deliverEvents, Assessments assessments,
            DrainAuthority authority, ExternalWorkers externalWorkers, RecoveryAuthority recoveryAuthority) throws IOException {
        Objects.requireNonNull(authority); Objects.requireNonNull(externalWorkers);
        return new DocumentPublicationRuntime(tx, drives, ledger, reader, budget, backends, assemblyLimits, sqlTimeouts,
                parallelism, flushAge, lease, maxSessions, maxCommandBytes, cleanupBatchSize, deliverEvents,
                observeAssessments(tx, drives, ledger, reader, budget, assemblyLimits, assessments),
                key -> authority.forOperation(key.account(), key.principal(), key.operationId()), externalWorkers,recoveryAuthority);
    }

    private <R extends DocumentRetainedReader & DocumentReadLifecycle.Reader> DocumentPublicationRuntime(
            Tx tx, DriveLedger drives, DocumentReadLedger ledger, R reader, PayloadBudget budget,
            Backends backends, DocumentRevisionAssembly.Limits assemblyLimits, SqlTimeouts sqlTimeouts,
            int parallelism, Duration flushAge, Duration lease, int maxSessions, long maxCommandBytes,
            int cleanupBatchSize, boolean deliverEvents, DocumentPublicationAssessmentExecution assessments,
            java.util.function.Function<RepositoryOperationLedger.Key, RepositoryCaller> drainAuthority) {
        this(tx, drives, ledger, reader, budget, backends, assemblyLimits, sqlTimeouts, parallelism,
                flushAge, lease, maxSessions, maxCommandBytes, cleanupBatchSize, deliverEvents, assessments, drainAuthority, null);
    }

    private <R extends DocumentRetainedReader & DocumentReadLifecycle.Reader> DocumentPublicationRuntime(
            Tx tx, DriveLedger drives, DocumentReadLedger ledger, R reader, PayloadBudget budget,
            Backends backends, DocumentRevisionAssembly.Limits assemblyLimits, SqlTimeouts sqlTimeouts,
            int parallelism, Duration flushAge, Duration lease, int maxSessions, long maxCommandBytes,
            int cleanupBatchSize, boolean deliverEvents, DocumentPublicationAssessmentExecution assessments,
            java.util.function.Function<RepositoryOperationLedger.Key, RepositoryCaller> drainAuthority,
            ExternalWorkers externalWorkers) {
        this(tx,drives,ledger,reader,budget,backends,assemblyLimits,sqlTimeouts,parallelism,flushAge,lease,
                maxSessions,maxCommandBytes,cleanupBatchSize,deliverEvents,assessments,drainAuthority,externalWorkers,null);
    }

    /** Internal lifecycle composition; does not enable historical request dispatch. */
    static <R extends DocumentRetainedReader & DocumentAssessmentReader & DocumentHistoricalRetainedReader & DocumentReadLifecycle.Reader>
            DocumentPublicationRuntime historicalJournaled(
            Tx tx, DriveLedger drives, DocumentReadLedger ledger, R reader, PayloadBudget budget,
            Backends backends, DocumentRevisionAssembly.Limits assemblyLimits, SqlTimeouts sqlTimeouts,
            int parallelism, Duration flushAge, Duration lease, int maxSessions, long maxCommandBytes,
            int cleanupBatchSize, boolean deliverEvents, Assessments assessments,
            DrainAuthority authority, ExternalWorkers externalWorkers, int historicalCapacity) throws IOException {
        Objects.requireNonNull(authority); Objects.requireNonNull(externalWorkers);
        var historical = new RepositoryInstalledHistoricalAttempts(tx.withTimeouts(sqlTimeouts), budget, drives, historicalCapacity);
        return new DocumentPublicationRuntime(tx, drives, ledger, reader, budget, backends, assemblyLimits, sqlTimeouts,
                parallelism, flushAge, lease, maxSessions, maxCommandBytes, cleanupBatchSize, deliverEvents,
                observeAssessments(tx, drives, ledger, reader, budget, assemblyLimits, assessments),
                key -> authority.forOperation(key.account(), key.principal(), key.operationId()), externalWorkers, null, historical);
    }

    /** For the internal accepted-call driver; callers must hold the runtime's outer scope. */
    RepositoryInstalledHistoricalAttempts historicalAttempts() {
        if (historical == null) throw new IllegalStateException("Historical lifecycle is not configured");
        return historical;
    }

    private <R extends DocumentRetainedReader & DocumentReadLifecycle.Reader> DocumentPublicationRuntime(
            Tx tx, DriveLedger drives, DocumentReadLedger ledger, R reader, PayloadBudget budget,
            Backends backends, DocumentRevisionAssembly.Limits assemblyLimits, SqlTimeouts sqlTimeouts,
            int parallelism, Duration flushAge, Duration lease, int maxSessions, long maxCommandBytes,
            int cleanupBatchSize, boolean deliverEvents, DocumentPublicationAssessmentExecution assessments,
            java.util.function.Function<RepositoryOperationLedger.Key, RepositoryCaller> drainAuthority,
            ExternalWorkers externalWorkers, RecoveryAuthority recoveryAuthority) {
        this(tx, drives, ledger, reader, budget, backends, assemblyLimits, sqlTimeouts, parallelism,
                flushAge, lease, maxSessions, maxCommandBytes, cleanupBatchSize, deliverEvents, assessments,
                drainAuthority, externalWorkers, recoveryAuthority, null);
    }

    private <R extends DocumentRetainedReader & DocumentReadLifecycle.Reader> DocumentPublicationRuntime(
            Tx tx, DriveLedger drives, DocumentReadLedger ledger, R reader, PayloadBudget budget,
            Backends backends, DocumentRevisionAssembly.Limits assemblyLimits, SqlTimeouts sqlTimeouts,
            int parallelism, Duration flushAge, Duration lease, int maxSessions, long maxCommandBytes,
            int cleanupBatchSize, boolean deliverEvents, DocumentPublicationAssessmentExecution assessments,
            java.util.function.Function<RepositoryOperationLedger.Key, RepositoryCaller> drainAuthority,
            ExternalWorkers externalWorkers, RecoveryAuthority recoveryAuthority,
            RepositoryInstalledHistoricalAttempts historical) {
        Objects.requireNonNull(backends);
        this.historical = historical;
        this.drainAuthority = drainAuthority;
        this.externalWorkers = externalWorkers;
        publicationReplay=new DocumentPublicationReplay(tx.withTimeouts(sqlTimeouts));
        publicationBudget=Objects.requireNonNull(budget);
        publicationPermits=new java.util.concurrent.Semaphore(maxSessions);
        journaledPublication=drainAuthority!=null;
        reads = new DocumentReadLifecycle(ledger, reader, cleanupBatchSize);
        uploads = new DocumentUploadCoordinator(tx, drives, budget, (generation, profile) -> {
            var selected = Objects.requireNonNull(backends.resolve(generation, profile));
            return new DocumentUploadCoordinator.Backend(selected.identity(), selected.opened());
        }, parallelism, flushAge, sqlTimeouts);
        var execution = new DocumentPublicationExecution(tx, drives, ledger, uploads, reader, budget,
                assemblyLimits, deliverEvents, assessments);
        sessions = drainAuthority == null
                ? new DocumentPublicationSessions(tx, execution, lease, maxSessions, maxCommandBytes)
                : DocumentPublicationSessions.journaled(tx, execution, lease, maxSessions, maxCommandBytes, budget);
        recovery=recoveryAuthority==null ? null
                : new RepositoryManagedRecovery(tx,budget,sessions,lease,sqlTimeouts,maxSessions,recoveryAuthority);
    }

    /** Borrowed payloads must remain stable until return, including after caller cancellation. */
    public DocumentPublicationResult execute(RepositoryCaller caller, DocumentPublicationCommand command,
            Map<UUID, Placement> placements, Map<PayloadKey, PartObject> bodies, Map<String, String> attributes,
            Map<String, Mode> modes, Optional<DocumentSchemaAdmission.Definition> container,
            Schemas schemas, RepositoryReadControl control) throws InvalidProtocolBufferException {
        try (var call=scopeCalls.enter(); var operation=operationCall(caller,command)) {
            return executeAccepted(caller,command,placements,bodies,attributes,modes,container,schemas,control);
        }
    }

    /** Creates a shared library/transport boundary; it borrows this runtime and closes with it. */
    public DocumentPublicationRepository repository(PublicationSelector selector) {
        return repository(selector,(int)DocumentPublicationInput.MAX_UPLOAD_BYTES);
    }

    /** Enforces a host upload-object bound before replay, selection or durable admission. */
    public DocumentPublicationRepository repository(PublicationSelector selector, int maxObjectBytes) {
        Objects.requireNonNull(selector);
        if (!journaledPublication) throw new IllegalStateException("Managed publication requires durable mode journals");
        return new DocumentPublicationFacade(scopeCalls,publicationBudget,publicationPermits,
                (caller,input,control) -> publishValidated(caller,input,selector,control),maxObjectBytes);
    }

    private PublishDocumentResponse publishValidated(RepositoryCaller caller, DocumentPublicationInput input,
            PublicationSelector selector, RepositoryReadControl control) {
        var command=input.command();
        var selectedModes=new HashMap<String,Mode>();
        input.modes().forEach((member,mode) -> selectedModes.put(member,switch(mode) {
            case DOCUMENT_PUBLICATION_MODE_TYPED -> Mode.TYPED;
            case DOCUMENT_PUBLICATION_MODE_OPAQUE -> Mode.OPAQUE;
            default -> throw new IllegalArgumentException("Invalid publication mode");
        }));
        var fixedModes=modes(selectedModes);
        try (var operation=operationCall(caller,command)) {
            var observed=publicationReplay.observe(caller,command,fixedModes,control);
            if (observed.result().isPresent() || observed.rejection().isPresent()) return response(observed);
            observed.requireNotTerminated();
            var selected=Objects.requireNonNull(selector.select(caller,command,control));
            control.check();
            var bodies=new HashMap<PayloadKey,PartObject>();
            for (var member:command.intent().getMembersList()) for (int ordinal=0;ordinal<member.getPartsCount();ordinal++) {
                var part=member.getParts(ordinal);
                if (!part.hasUpload()) continue;
                control.check();
                var bytes=input.payloads().get(new DocumentPublicationInput.PayloadKey(member.getMemberId(),ordinal));
                bodies.put(new PayloadKey(member.getMemberId(),ordinal),new PartObject(part.getSlot().getPart(),
                        part.getSlot().getSubKey(),bytes.toByteArray(),part.getUpload().getSha256()));
            }
            PublishDocumentResponse expected;
            try (var scopes=new DocumentPublicationSchemaScopes(caller,command,selected.schemas(),control)) {
                try {
                    var result=executeAccepted(caller,command,selected.placements(),bodies,selected.attributes(),selectedModes,
                            selected.container(),scopes,control);
                    expected=PublishDocumentResponse.newBuilder().setCommitted(result).build();
                } catch (Rejected rejected) {
                    expected=PublishDocumentResponse.newBuilder().setRejected(rejected.receipt()).build();
                }
            } catch (InvalidProtocolBufferException malformed) {
                throw new RepositoryException(RepositoryException.Code.INVALID_ARGUMENT,"Publication content is not valid protobuf",malformed);
            }
            // Another owner may finish between the initial observation and execution replay.
            var confirmed=response(publicationReplay.observe(caller,command,fixedModes,control));
            if (!confirmed.equals(expected)) throw new RepositoryException(RepositoryException.Code.DATA_LOSS,
                    "Publication execution differs from its durable receipt");
            return confirmed;
        }
    }

    private static PublishDocumentResponse response(DocumentPublicationReplay.Observation observed) {
        if (observed.result().isPresent()) return PublishDocumentResponse.newBuilder().setCommitted(observed.result().orElseThrow()).build();
        if (observed.rejection().isPresent()) return PublishDocumentResponse.newBuilder().setRejected(observed.rejection().orElseThrow()).build();
        throw new RepositoryException(RepositoryException.Code.DATA_LOSS,"Publication returned without a durable terminal receipt");
    }

    private DocumentPublicationResult executeAccepted(RepositoryCaller caller, DocumentPublicationCommand command,
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
        try (var verified=recovery==null ? null : recovery.prepare(caller,command,payloads,modes(modes),control)) {
            return sessions.execute(caller, command, placements(placements), verified==null ? payloads : verified.bodies(), attributes, modes(modes), container,
                    (member, occurrence) -> schemas.resolve(caller, member, occurrence), control);
        } catch (DocumentPublicationReplay.Terminated rejected) {
            throw new Rejected(rejected.receipt());
        }
    }

    /**
     * Own lazily opened member resolution scopes until synchronous publication returns.
     * Replay, opaque members and failures before schema selection open no scopes.
     * Open/select/close run on the calling thread. The host still owns container schema
     * inputs and the shared resolver/provider; all proof copies precede scope closure.
     */
    public DocumentPublicationResult executeScoped(RepositoryCaller caller, DocumentPublicationCommand command,
            Map<UUID, Placement> placements, Map<PayloadKey, PartObject> bodies, Map<String, String> attributes,
            Map<String, Mode> modes, Optional<DocumentSchemaAdmission.Definition> container,
            SchemaScopes schemas, RepositoryReadControl control) throws InvalidProtocolBufferException {
        Objects.requireNonNull(command);
        try (var call = scopeCalls.enter(); var operation=operationCall(caller,command);
             var scopes = new DocumentPublicationSchemaScopes(caller, command, schemas, control)) {
            return executeAccepted(caller, command, placements, bodies, attributes, modes, container, scopes, control);
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
        try (var call=scopeCalls.enter(); var operation=operationCall(caller,command)) {
            return sessions.recover(caller, command, placements(placements), predecessorGeneration, modes(modes), control);
        } catch (DocumentPublicationReplay.Terminated rejected) {
            throw new Rejected(rejected.receipt());
        }
    }

    public boolean retireSuperseded(RepositoryCaller caller, DocumentPublicationCommand command, RepositoryReadControl control) {
        try (var call=scopeCalls.enter(); var operation=operationCall(caller,command)) {
            return sessions.retireSuperseded(caller, command, control);
        }
    }

    /** Package-private until the host's coordinator ownership protocol is qualified. */
    DocumentPublicationResult resumeStarted(RepositoryCaller caller, DocumentPublicationCommand command,
            RepositoryOperationLedger.Owner owner, RepositoryReadControl control) {
        try (var call=scopeCalls.enter(); var operation=operationCall(caller,command)) {
            return sessions.resumeStarted(caller, command, owner, control);
        }
        catch (DocumentPublicationReplay.Terminated rejected) { throw new Rejected(rejected.receipt()); }
    }

    private static Map<UUID, DocumentUploadPlan.Placement> placements(Map<UUID, Placement> selected) {
        var result = new HashMap<UUID, DocumentUploadPlan.Placement>();
        selected.forEach((id, placement) -> result.put(id, placement.selected));
        return result;
    }

    private RepositoryPublicationCalls.Call operationCall(RepositoryCaller caller, DocumentPublicationCommand command) {
        return recovery==null ? null : recovery.calls.enter(caller,command);
    }

    private static Map<String, DocumentPublicationCandidate.Mode> modes(Map<String, Mode> selected) {
        var result = new HashMap<String, DocumentPublicationCandidate.Mode>();
        selected.forEach((member, mode) -> result.put(member, switch (mode) {
            case TYPED -> DocumentPublicationCandidate.Mode.TYPED;
            case OPAQUE -> DocumentPublicationCandidate.Mode.OPAQUE;
        }));
        return result;
    }

    /** Refuse new calls and provider starts. Permitted transfers may settle; resources remain borrowed. */
    @Override public void close() {
        scopeCalls.close();
        // Recovery callers may still need activation, lazy schema resolution and provider starts.
        // Their complete scope is drained before closing those nested resources.
        if (recovery==null && historical==null) closeNestedAdmission();
    }

    private void closeNestedAdmission() {
        uploads.stopProviderStarts();
        try { sessions.close(); }
        catch (RuntimeException | Error failure) {
            try { if (externalWorkers != null) externalWorkers.closeAdmission(); }
            catch (RuntimeException | Error cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
            throw failure;
        }
        if (externalWorkers != null) externalWorkers.closeAdmission();
    }

    /** One bounded maintenance pass; the host schedules and retries failures. */
    public synchronized int tick() {
        if (stopping) throw new IllegalStateException("Publication runtime is stopping");
        return reads.tick();
    }

    /** Local wait budget excludes SQL time; configure database statement/network timeouts separately. */
    public synchronized boolean shutdownStep(Duration waitBudget) throws InterruptedException {
        return shutdownStep(waitBudget, RepositoryReadControl.NONE);
    }

    /** Host cancellation does not discard registrations or bypass their durable admission fence. */
    synchronized boolean shutdownStep(Duration waitBudget, RepositoryReadControl control) throws InterruptedException {
        Objects.requireNonNull(waitBudget);
        Objects.requireNonNull(control).check();
        if (waitBudget.isNegative()) throw new IllegalArgumentException("Shutdown wait must not be negative");
        long budget = waitBudget.toNanos(), start = System.nanoTime();
        if (stopped) return true;
        stopping = true;
        close();
        if (recovery!=null || historical!=null) {
            if (!scopeCalls.awaitIdle(remaining(budget,start))) return false;
            control.check();
            if (recovery!=null && !recovery.detach(remaining(budget,start),control)) return false;
            if (historical!=null) {
                historical.close();
                if (!historical.detachClosed(remaining(budget,start),drainAuthority,control)) return false;
            }
            closeNestedAdmission();
        }
        if (drainAuthority != null) {
            var progress = sessions.drainRegistrations(remaining(budget, start), drainAuthority, control);
            if (!progress.registrationsIdle() || progress.unresolved() != 0) return false;
        }
        if (!sessions.awaitIdle(remaining(budget, start))) return false;
        control.check();
        if (!scopeCalls.awaitIdle(remaining(budget, start))) return false;
        control.check();
        uploads.close();
        if (!uploads.awaitIdle(remaining(budget, start))) return false;
        control.check();
        if (!reads.shutdownStep(remaining(budget, start))) return false;
        control.check();
        if (externalWorkers != null) {
            if (!externalWorkers.awaitIdle(remaining(budget, start))) return false;
            control.check();
            if (!sessions.attestLocalDrain(drainAuthority, control)) return false;
        }
        control.check();
        stopped = true;
        return true;
    }

    private static Duration remaining(long budget, long start) {
        return Duration.ofNanos(Math.max(0, budget - (System.nanoTime() - start)));
    }
}
