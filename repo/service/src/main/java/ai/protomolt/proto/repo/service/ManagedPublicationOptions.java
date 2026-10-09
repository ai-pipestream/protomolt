package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.authz.AuthenticatedCaller;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;

/**
 * Explicit journaled publication composition. The assessment bundle is local,
 * trusted host code. Authorities must resolve current permission for the exact
 * operation on every invocation; request identities are not permission grants.
 * Callback resources remain host-owned and may receive concurrent calls.
 */
public record ManagedPublicationOptions(Path assessmentRuntimeBundle, Duration assessmentRetention,
        Duration minimumAssessmentRemaining, OperationAuthority drainAuthority,
        Optional<OperationAuthority> recoveryAuthority, Optional<Transport> transport,
        Optional<Historical> historical) {

    /** Capacity counts retained generations, including predecessors still draining. */
    public record Historical(int generationCapacity) {
        public Historical {
            if (generationCapacity < 1) throw new IllegalArgumentException("Historical generation capacity must be positive");
        }
    }

    public ManagedPublicationOptions(Path bundle, Duration retention, Duration minimumRemaining,
            OperationAuthority drainAuthority, Optional<OperationAuthority> recoveryAuthority, Optional<Transport> transport) {
        this(bundle, retention, minimumRemaining, drainAuthority, recoveryAuthority, transport, Optional.empty());
    }

    /** Separate host policies select drain and recovery authority; neither implies the other. */
    @FunctionalInterface public interface OperationAuthority {
        RepositoryCaller forOperation(String account, String principal, UUID operation);
    }

    /**
     * Trusted authentication-to-repository binding, with bounded application delivery
     * bytes and active calls. Parser/network buffers are separate. The host installs
     * authentication; the adapter preserves principal, authority and credential identity.
     */
    public record Transport(Function<AuthenticatedCaller, RepositoryCaller> bindings,
            long deliveryBudgetBytes, int maxConcurrentCalls) {
        public Transport {
            Objects.requireNonNull(bindings, "bindings");
            if (deliveryBudgetBytes < DocumentPublicationGrpcService.MAX_CALL_RESERVATION_BYTES)
                throw new IllegalArgumentException("Publication delivery budget must cover one maximum request and response: "
                        +DocumentPublicationGrpcService.MAX_CALL_RESERVATION_BYTES+" bytes");
            if (maxConcurrentCalls <= 0)
                throw new IllegalArgumentException("Publication call limit must be positive");
        }
    }

    /** Library publication with managed drain; no recovery authority or RPC is inferred. */
    public ManagedPublicationOptions(Path bundle, Duration retention, Duration minimumRemaining,
            OperationAuthority drainAuthority) {
        this(bundle, retention, minimumRemaining, drainAuthority, Optional.empty(), Optional.empty());
    }

    public ManagedPublicationOptions {
        Objects.requireNonNull(drainAuthority, "drainAuthority");
        Objects.requireNonNull(recoveryAuthority, "recoveryAuthority");
        Objects.requireNonNull(transport, "transport");
        Objects.requireNonNull(historical, "historical");
        if (historical.isPresent() && recoveryAuthority.isEmpty())
            throw new IllegalArgumentException("Historical publication requires explicit recovery authority");
        // Use the runtime's window rules so composition cannot accept a divergent policy.
        new ai.protomolt.proto.repo.container.ledger.DocumentPublicationRuntime.Assessments(
                assessmentRuntimeBundle, assessmentRetention, minimumAssessmentRemaining);
        assessmentRuntimeBundle = assessmentRuntimeBundle.toAbsolutePath().normalize();
    }

    public ManagedPublicationOptions withRecovery(OperationAuthority authority) {
        return new ManagedPublicationOptions(assessmentRuntimeBundle, assessmentRetention, minimumAssessmentRemaining,
                drainAuthority, Optional.of(authority), transport, historical);
    }

    public ManagedPublicationOptions withTransport(Transport access) {
        return new ManagedPublicationOptions(assessmentRuntimeBundle, assessmentRetention, minimumAssessmentRemaining,
                drainAuthority, recoveryAuthority, Optional.of(access), historical);
    }

    /** Explicit opt-in after configuring recovery authority; no authority or capacity is inferred. */
    public ManagedPublicationOptions withHistoricalPublication(int generationCapacity) {
        return new ManagedPublicationOptions(assessmentRuntimeBundle, assessmentRetention, minimumAssessmentRemaining,
                drainAuthority, recoveryAuthority, transport, Optional.of(new Historical(generationCapacity)));
    }

    ManagedDocumentServices.Journaled journaled() {
        return new ManagedDocumentServices.Journaled(
                new ai.protomolt.proto.repo.container.ledger.DocumentPublicationRuntime.Assessments(
                        assessmentRuntimeBundle, assessmentRetention, minimumAssessmentRemaining),
                drainAuthority::forOperation,
                recoveryAuthority.<ai.protomolt.proto.repo.container.ledger.DocumentPublicationRuntime.RecoveryAuthority>
                        map(authority -> authority::forOperation).orElse(null), transport.orElse(null), historical);
    }
}
