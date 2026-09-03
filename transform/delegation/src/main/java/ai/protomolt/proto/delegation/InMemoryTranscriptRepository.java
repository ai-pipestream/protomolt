package ai.protomolt.proto.delegation;

import ai.protomolt.proto.delegation.v1.Transcript;

import java.util.Optional;

/** Thread-safe, process-local transcript repository for tests and ephemeral deployments. */
public final class InMemoryTranscriptRepository implements TranscriptRepository {

    private Transcript transcript;

    @Override
    public synchronized Optional<Transcript> load() {
        return Optional.ofNullable(transcript);
    }

    @Override
    public synchronized void save(Transcript candidate) {
        DelegationValidation.validate(candidate);
        DelegationReducer.Result result = new DelegationReducer().reduce(candidate);
        if (!result.clean()) {
            DelegationReducer.Finding finding = result.findings().getLast();
            throw new IllegalArgumentException(finding.kind() + ": " + finding.error());
        }
        transcript = candidate;
    }
}
