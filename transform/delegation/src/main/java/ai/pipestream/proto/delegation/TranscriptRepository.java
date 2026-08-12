package ai.pipestream.proto.delegation;

import ai.pipestream.proto.delegation.v1.Transcript;

import java.util.Optional;

/**
 * Durable boundary for the delegation transcript. Implementations store the complete validated
 * transcript snapshot atomically. The coordinator keeps its live projection in memory and uses
 * this repository as the restart source of truth.
 */
public interface TranscriptRepository {

    /** Returns the latest durable transcript, or empty before the first accepted frame. */
    Optional<Transcript> load();

    /**
     * Atomically replaces the durable transcript after validating the candidate snapshot.
     * Returning normally means a subsequent {@link #load()} observes this transcript or a newer
     * one.
     */
    void save(Transcript transcript);
}
