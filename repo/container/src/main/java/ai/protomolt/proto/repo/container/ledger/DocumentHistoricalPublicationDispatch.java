package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.codec.PartObject;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.PublishDocumentResponse;
import com.google.protobuf.InvalidProtocolBufferException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

/** Synchronous historical dispatch inside the runtime's already accepted public call. */
final class DocumentHistoricalPublicationDispatch {
    private final RepositoryInstalledHistoricalAttempts attempts;
    private final RepositoryCoordinatorRecoveryDiscovery discovery;
    private final DocumentPublicationReplay replay;
    private final DocumentPublicationRuntime.RecoveryAuthority authority;
    private final DocumentReadLedger reads;
    private final DocumentRetainedReader ordinaryReader;
    private final DocumentHistoricalRetainedReader historicalReader;
    private final DocumentAssessmentReader assessmentReader;
    private final DocumentUploadCoordinator uploads;
    private final DocumentSchemaPolicies policies;
    private final RepositorySchemaArtifacts artifacts;
    private final DocumentPublicationCommit publication;
    private final DocumentRevisionAssembly.Limits limits;
    private final DocumentAssessmentRuntimeObserver.Observation observation;
    private final Duration lease, retention, minimumRemaining;
    private final SqlTimeouts timeouts;

    DocumentHistoricalPublicationDispatch(Tx tx, DriveLedger drives, RepositoryInstalledHistoricalAttempts attempts,
            DocumentPublicationRuntime.RecoveryAuthority authority, DocumentReadLedger reads,
            DocumentRetainedReader ordinaryReader, DocumentHistoricalRetainedReader historicalReader,
            DocumentAssessmentReader assessmentReader, DocumentUploadCoordinator uploads,
            DocumentRevisionAssembly.Limits limits, DocumentAssessmentRuntimeObserver.Observation observation,
            Duration lease, Duration retention, Duration minimumRemaining, SqlTimeouts timeouts, boolean deliverEvents) {
        this.attempts = Objects.requireNonNull(attempts); this.authority = Objects.requireNonNull(authority);
        this.reads = Objects.requireNonNull(reads); this.ordinaryReader = Objects.requireNonNull(ordinaryReader);
        this.historicalReader = Objects.requireNonNull(historicalReader); this.assessmentReader = Objects.requireNonNull(assessmentReader);
        this.uploads = Objects.requireNonNull(uploads); this.limits = Objects.requireNonNull(limits);
        this.observation = Objects.requireNonNull(observation); this.lease = Objects.requireNonNull(lease);
        this.retention = Objects.requireNonNull(retention); this.minimumRemaining = Objects.requireNonNull(minimumRemaining);
        this.timeouts = Objects.requireNonNull(timeouts);
        var bounded = tx.withTimeouts(timeouts);
        discovery = new RepositoryCoordinatorRecoveryDiscovery(tx, timeouts);
        replay = new DocumentPublicationReplay(bounded); policies = new DocumentSchemaPolicies(bounded);
        artifacts = new RepositorySchemaArtifacts(bounded); publication = new DocumentPublicationCommit(bounded, drives, false, deliverEvents);
    }

    PublishDocumentResponse publish(RepositoryCaller caller, DocumentPublicationInput input,
            DocumentPublicationRuntime.PublicationSelector selector,
            Map<String, DocumentPublicationCandidate.Mode> modes, RepositoryReadControl control) {
        var command = input.command();
        var observed = replay.observe(caller, command, modes, control);
        if (terminal(observed)) {
            attempts.markTerminal(caller, command);
            return response(observed);
        }
        observed.requireNotTerminated();
        tick(control);
        var key = new RepositoryOperationLedger.Key(command.intent().getAccountId(), caller.principalName(), command.operationId());
        var coordinator = Objects.requireNonNull(authority.forOperation(key.account(), key.principal(), key.operationId()),
                "Private historical recovery authority");
        var local = attempts.inspectSelected(caller, command);
        RepositoryInstalledHistoricalAttempts.Attempt selectedAttempt;
        DocumentPublicationRuntime.PublicationSelection selected = null;
        if (local.isPresent() && !local.orElseThrow().attached()) {
            // Preserve an uncertain reservation even if discovery would see its committed successor.
            selectedAttempt = attempts.resume(caller, command).orElseThrow();
        } else {
            var discovered = discovery.inspect(coordinator, key, command.sha256(), control);
            if (discovered.status() == RepositoryCoordinatorRecoveryDiscovery.Status.TERMINAL) {
                var terminal = replay.observe(caller, command, modes, control);
                if (!terminal(terminal)) throw conflict("Historical terminal outcome changed during routing");
                attempts.markTerminal(caller, command);
                return response(terminal);
            }
            if (local.isPresent()) {
                var current = local.orElseThrow();
                selectedAttempt = discovered.status() == RepositoryCoordinatorRecoveryDiscovery.Status.EXPIRED_BOUND
                        && !current.disposalOnly()
                        ? attempts.beginSuccessor(coordinator, caller, current.identity(), command, modes, discovered, lease, timeouts)
                        : attempts.resume(caller, command).orElseThrow();
            } else if (discovered.status() == RepositoryCoordinatorRecoveryDiscovery.Status.ABSENT) {
                selected = Objects.requireNonNull(selector.select(caller, command, control));
                control.check();
                var record = new DocumentPublicationPreparationRecord(key, command, DocumentPublicationSeeds.mint(key, command),
                        DocumentPublicationRuntime.placements(selected.placements()), lease, 0);
                selectedAttempt = attempts.beginInitial(caller, record, modes, UUID.randomUUID());
            } else if (discovered.candidate().isPresent() || discovered.unactivated().isPresent()) {
                selectedAttempt = attempts.beginColdProposed(caller, command, modes, discovered, lease, timeouts);
            } else throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                    "Historical operation is not eligible for retry: " + discovered.status());
        }
        try (var attempt = selectedAttempt) {
            var bodies = payloads(input, control);
            attempt.prepareExecution(coordinator, modes, bodies, reads, control);
            var started = attempt.start(retention, control);
            if (!attempt.progress(control).assessmentPrepared()) {
                if (selected == null) selected = Objects.requireNonNull(selector.select(caller, command, control));
                var policy = policies.read(command.intent().getAccountId(), control::check);
                try (var scopes = new DocumentPublicationSchemaScopes(caller, command, selected.schemas(), control)) {
                    attempt.stageAndPrepareAssessment(uploads, bodies, selected.attributes(), reads, ordinaryReader, policy,
                            selected.container(), (member, occurrence) -> scopes.resolve(caller, member, occurrence),
                            limits, Instant.now(), historicalReader, control);
                }
            }
            var selections = attempt.assessmentSelections(control);
            var progress = attempt.progress(control);
            var execution = progress.execution().orElseThrow();
            if (execution.publicationAttempted())
                throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                        "Historical publication outcome is unresolved; retry requires terminal replay or fenced recovery");
            if (progress.acknowledgedCreation().isEmpty()) {
                if (execution.createAttempted()) {
                    if (attempt.reconcileAssessment(selections, observation, control).isEmpty())
                        throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                                "Historical CREATE outcome is unresolved");
                } else attempt.createAssessment(selections, observation, artifacts, started, control);
            }
            boolean rejected = attempt.withAssessment(assessment -> {
                var failed = new boolean[1];
                assessment.inspect(inspection -> failed[0] = inspection.snapshot().failure().isPresent(), control);
                return failed[0];
            }, control);
            PublishDocumentResponse expected;
            if (rejected) expected = response(attempt.rejectAssessment(selections, reads, assessmentReader, limits,
                    observation, minimumRemaining, control));
            else expected = PublishDocumentResponse.newBuilder().setCommitted(attempt.publishAssessment(selections,
                    observation, artifacts, publication, control)).build();
            var confirmed = response(replay.observe(caller, command, modes, control));
            if (!expected.equals(confirmed)) throw new RepositoryException(RepositoryException.Code.DATA_LOSS,
                    "Historical publication differs from its durable receipt");
            attempts.markTerminal(caller, command);
            return confirmed;
        } catch (InvalidProtocolBufferException malformed) {
            throw new RepositoryException(RepositoryException.Code.INVALID_ARGUMENT, "Publication content is not valid protobuf", malformed);
        }
    }

    int tick(RepositoryReadControl control) {
        try { return attempts.retireReady(16, key -> authority.forOperation(key.account(), key.principal(), key.operationId()), control); }
        catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            var failure = new java.util.concurrent.CancellationException("Historical retirement interrupted");
            failure.initCause(interrupted);
            throw failure;
        }
    }

    private static Map<DocumentUploadPayloads.Key, PartObject> payloads(DocumentPublicationInput input, RepositoryReadControl control) {
        var bodies = new HashMap<DocumentUploadPayloads.Key, PartObject>();
        for (var member : input.command().intent().getMembersList()) for (int ordinal = 0; ordinal < member.getPartsCount(); ordinal++) {
            var part = member.getParts(ordinal);
            if (!part.hasUpload()) continue;
            control.check();
            var bytes = input.payloads().get(new DocumentPublicationInput.PayloadKey(member.getMemberId(), ordinal));
            bodies.put(new DocumentUploadPayloads.Key(member.getMemberId(), ordinal), new PartObject(part.getSlot().getPart(),
                    part.getSlot().getSubKey(), bytes.toByteArray(), part.getUpload().getSha256()));
        }
        return bodies;
    }

    private static boolean terminal(DocumentPublicationReplay.Observation observed) {
        return observed.result().isPresent() || observed.rejection().isPresent();
    }
    private static PublishDocumentResponse response(DocumentPublicationReplay.Observation observed) {
        if (observed.result().isPresent()) return PublishDocumentResponse.newBuilder().setCommitted(observed.result().orElseThrow()).build();
        if (observed.rejection().isPresent()) return PublishDocumentResponse.newBuilder().setRejected(observed.rejection().orElseThrow()).build();
        throw new RepositoryException(RepositoryException.Code.DATA_LOSS, "Historical publication has no terminal receipt");
    }
    private static RepositoryException conflict(String message) { return new RepositoryException(RepositoryException.Code.CONFLICT, message); }
}
