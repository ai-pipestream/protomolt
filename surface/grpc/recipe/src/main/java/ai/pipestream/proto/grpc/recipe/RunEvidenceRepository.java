package ai.pipestream.proto.grpc.recipe;

import ai.pipestream.proto.grpc.recipe.v1.RunEvidence;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/** Durable execution evidence indexed by stable run identity. */
public interface RunEvidenceRepository {

    /** Finds one run by its exact identity. */
    Optional<RunEvidence> find(String runId) throws IOException;

    /** Lists up to {@code limit} runs for one recipe in repository-defined stable order. */
    List<RunEvidence> list(String recipeName, int limit) throws IOException;

    /** Saves a run snapshot without weakening identity or artifact validation. */
    void save(RunEvidence evidence) throws IOException;
}
