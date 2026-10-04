package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.codec.PartObject;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import ai.protomolt.proto.repo.v1.DocumentPublicationResult;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** One already-admitted native publication, including authorized replay and owned cleanup. */
final class DocumentPublicationExecution {
    private final DocumentPublicationReplay replay;
    private final DocumentSchemaPolicies policies;
    private final DocumentOperationUploadAdmission admission;
    private final DocumentReadLedger reads;
    private final DocumentPublicationPreparation preparation;
    private final RepositorySchemaArtifacts artifacts;
    private final DocumentPublicationCommit publication;
    private final DocumentRevisionAssembly.Limits opaqueLimits;

    DocumentPublicationExecution(Tx tx, DriveLedger drives, DocumentReadLedger reads,
            DocumentUploadCoordinator uploads, DocumentRetainedReader retained, PayloadBudget budget,
            DocumentRevisionAssembly.Limits opaqueLimits, boolean deliverEvents) {
        this.reads = Objects.requireNonNull(reads); this.opaqueLimits = Objects.requireNonNull(opaqueLimits);
        replay = new DocumentPublicationReplay(tx); policies = new DocumentSchemaPolicies(tx);
        admission = new DocumentOperationUploadAdmission(tx, drives);
        preparation = new DocumentPublicationPreparation(uploads, retained, budget);
        artifacts = new RepositorySchemaArtifacts(tx);
        publication = new DocumentPublicationCommit(tx, drives, false, deliverEvents);
    }

    /** Retained host session: serial execution, exact admission retry, and authorized terminal replay. */
    DocumentPublicationResult execute(RepositoryCaller caller, DocumentPublicationSession session,
            Map<DocumentUploadPayloads.Key, PartObject> bodies, Map<String, String> attributes,
            Map<String, DocumentPublicationCandidate.Mode> modes,
            Optional<DocumentSchemaAdmission.Definition> container, DocumentPublicationCandidate.Resolver resolver,
            RepositoryReadControl control) throws InvalidProtocolBufferException {
        try (var execution = session.begin(caller, control)) {
            var prepared = session.prepared();
            var observed = replay.observe(caller, prepared.plan().command());
            control.check();
            observed.requireNotTerminated();
            if (observed.result().isPresent()) return observed.result().orElseThrow();
            var selectedModes = execution.bindModes(modes);
            final RepositoryOperationLedger.Owner owner;
            try {
                var admitted = session.admit(caller, control);
                if (admitted.isEmpty()) return replayWithoutOwner(caller, prepared, control);
                owner = admitted.orElseThrow();
            } catch (RepositoryOperationLedger.TerminalOperationException terminal) {
                return replayWithoutOwner(caller, prepared, control);
            }
            return executeNew(caller, owner, prepared, bodies, attributes, selectedModes, container, resolver, control);
        }
    }

    private DocumentPublicationResult replayWithoutOwner(RepositoryCaller caller,
            DocumentOperationUploadAdmission.Prepared prepared, RepositoryReadControl control) {
        var completed = replay.observe(caller, prepared.plan().command());
        control.check();
        completed.requireNotTerminated();
        return completed.result().orElseThrow(() -> new RepositoryException(
                RepositoryException.Code.CONFLICT, "Publication session has neither executable ownership nor a committed result"));
    }

    /**
     * The host retains owner nonce and attempt identities across uncertain outcomes.
     * This method never admits/takes over an operation or changes its identity.
     * Caller batches and candidate byte owners close here; closed source-pin handles
     * remain ledger-owned until lifecycle cleanup succeeds. The host must schedule
     * that cleanup and preserve provider/SQL resources through shutdown.
     */
    DocumentPublicationResult execute(RepositoryCaller caller, RepositoryOperationLedger.Owner owner,
            DocumentOperationUploadAdmission.Prepared prepared, Map<DocumentUploadPayloads.Key, PartObject> bodies,
            Map<String, String> attributes, Map<String, DocumentPublicationCandidate.Mode> modes,
            Optional<DocumentSchemaAdmission.Definition> container, DocumentPublicationCandidate.Resolver resolver,
            RepositoryReadControl control) throws InvalidProtocolBufferException {
        Objects.requireNonNull(owner); Objects.requireNonNull(prepared); Objects.requireNonNull(control).check();
        var command = prepared.plan().command();
        DocumentAdmissionAuthorization.requireCaller(caller, owner, command.intent().getAccountId());
        if (!owner.key().operationId().equals(command.operationId())
                || !owner.key().account().equals(command.intent().getAccountId()))
            throw new IllegalArgumentException("Publication owner differs from command scope");
        var observed = replay.observe(caller, command);
        control.check();
        observed.requireNotTerminated();
        if (observed.result().isPresent()) return observed.result().orElseThrow();
        return executeNew(caller, owner, prepared, bodies, attributes, modes, container, resolver, control);
    }

    private DocumentPublicationResult executeNew(RepositoryCaller caller, RepositoryOperationLedger.Owner owner,
            DocumentOperationUploadAdmission.Prepared prepared, Map<DocumentUploadPayloads.Key, PartObject> bodies,
            Map<String, String> attributes, Map<String, DocumentPublicationCandidate.Mode> modes,
            Optional<DocumentSchemaAdmission.Definition> container, DocumentPublicationCandidate.Resolver resolver,
            RepositoryReadControl control) throws InvalidProtocolBufferException {
        var command = prepared.plan().command();
        var policy = policies.read(command.intent().getAccountId(), control::check);
        var settings = new DocumentPublicationPreparation.Admission(policy, modes, container, resolver, opaqueLimits);
        var pinned = reads.capture(admission, caller, owner, prepared);
        try (var candidate = preparation.prepare(caller, owner, prepared, bodies, attributes, pinned, settings, control)) {
            control.check();
            candidate.candidate().schemas().stage(artifacts, owner, control::check);
            return publication.commit(caller, owner, prepared, candidate.candidate().opaque(), candidate.selections(),
                    candidate.candidate().schemas(), control::check);
        } finally {
            // Local close only. Failed/unfinished provider work still owns its Use;
            // lifecycle cleanup releases SQL pins only after actual drain.
            pinned.close();
        }
    }
}
