package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.DocumentPublicationResult;
import com.google.protobuf.InvalidProtocolBufferException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/** Internal opt-in retained rejection flow. The owning host keeps reader, ledger and observation alive. */
final class DocumentPublicationAssessmentExecution {
    private final DocumentAssessmentCreation creation;
    private final DocumentAssessmentDiscovery discovery;
    private final RepositorySchemaArtifacts artifacts;
    private final DocumentAssessmentRejections rejections;
    private final DocumentPublicationReplay replay;
    private final DocumentReadLedger reads;
    private final DocumentAssessmentReader reader;
    private final PayloadBudget budget;
    private final DocumentRevisionAssembly.Limits limits;
    private final DocumentAssessmentRuntimeObserver.Observation observation;
    private final Duration retention;

    DocumentPublicationAssessmentExecution(Tx tx, DriveLedger drives, DocumentReadLedger reads,
            DocumentAssessmentReader reader, PayloadBudget budget, DocumentRevisionAssembly.Limits limits,
            DocumentAssessmentRuntimeObserver.Observation observation, Duration retention, Duration minimumWindow) {
        this.reads = Objects.requireNonNull(reads); this.reader = Objects.requireNonNull(reader);
        this.budget = Objects.requireNonNull(budget); this.limits = Objects.requireNonNull(limits);
        this.observation = Objects.requireNonNull(observation); this.retention = Objects.requireNonNull(retention);
        requireWindows(retention, minimumWindow);
        rejections = new DocumentAssessmentRejections(tx, minimumWindow);
        creation = new DocumentAssessmentCreation(tx, drives); discovery = new DocumentAssessmentDiscovery(tx);
        artifacts = new RepositorySchemaArtifacts(tx); replay = new DocumentPublicationReplay(tx);
    }

    static void requireWindows(Duration retention, Duration minimumWindow) {
        Objects.requireNonNull(retention); Objects.requireNonNull(minimumWindow);
        if (minimumWindow.isNegative() || minimumWindow.isZero() || minimumWindow.compareTo(Duration.ofDays(1)) > 0
                || minimumWindow.getNano() % 1000 != 0)
            throw new IllegalArgumentException("Minimum assessment window must be positive exact microseconds within one day");
        if (retention.isNegative() || retention.isZero() || retention.compareTo(Duration.ofDays(1)) > 0
                || retention.getNano() % 1000 != 0 || retention.compareTo(minimumWindow) <= 0)
            throw new IllegalArgumentException("Assessment retention must be exact microseconds within one day and exceed its decision window");
    }

    /** Sources remain pinned until this returns or throws. No decision occurs in the writer callback. */
    void stage(RepositoryCaller caller, RepositoryOperationLedger.Owner owner, DocumentOperationUploadAdmission.Prepared plan,
            DocumentPublicationPreparation.Assessed assessed, DocumentPublicationSession.Execution execution,
            RepositoryReadControl control) throws InvalidProtocolBufferException {
        Objects.requireNonNull(control).check();
        if (execution.assessmentStageStarted()) throw new IllegalStateException("Original assessment must be reconciled");
        var assessment = assessed.assessment();
        if (assessment.failure().isEmpty()) throw new IllegalArgumentException("Accepted assessment cannot be staged for rejection");
        assessment.withRetentionEvidence(owner, observation, control::check, evidence -> {
            artifacts.stage(owner, plan.plan().command(), List.copyOf(evidence.artifacts(control::check).values()), control::check);
            UUID id = UUID.randomUUID();
            Instant deadline = Instant.now().truncatedTo(ChronoUnit.MICROS).plus(retention);
            control.check();
            execution.beginAssessmentStage();
            return creation.create(caller, owner, plan, assessed.selections(), evidence, id, deadline, budget, control::check);
        });
    }

    /** Source pins must already be closed; original stage recovery performs no upload or schema resolution. */
    DocumentPublicationResult resume(RepositoryCaller caller, RepositoryOperationLedger.Owner owner,
            DocumentPublicationCommand command, RepositoryReadControl control) {
        return resume(caller, owner, command, null, control);
    }

    DocumentPublicationResult resume(RepositoryCaller caller, RepositoryOperationLedger.Owner owner,
            DocumentPublicationCommand command, DocumentAssessmentStartJournal.Started expected,
            RepositoryReadControl control) {
        Objects.requireNonNull(control).check();
        var existing = replay.observe(caller, command);
        control.check();
        existing.requireNotTerminated();
        if (existing.result().isPresent()) return existing.result().orElseThrow();
        var original = discovery.discover(caller, owner, command, control::check).orElseThrow(() ->
                new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                        "Assessment stage outcome is unresolved; explicit recovery is required"));
        reads.releaseDrainedAtCapacity(1);
        var stage = original.stage();
        if (expected != null && (!expected.assessment().equals(stage.assessment())
                || !expected.retainUntil().equals(stage.retainUntil())))
            throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                    "Discovered assessment differs from retained start");
        try (var capture = reads.captureAssessment(caller, owner, command, original.selections(), stage.assessment(),
                stage.manifestSha256(), stage.retainUntil(), budget, control::check)) {
            var decided = rejections.reject(caller, owner, command, capture, reader, budget, limits, observation, control);
            decided.requireNotTerminated();
            return decided.result().orElseThrow(() -> new IllegalStateException("Assessment decision has no terminal outcome"));
        }
    }
}
