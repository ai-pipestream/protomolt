package ai.pipestream.proto.chain;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Optional;

/**
 * Resolves named chains for {@code run-chain}. The registry's Git store is the canonical
 * implementation; anything that can hand back the stored {@code ChainDefinition} JSON
 * qualifies.
 */
public interface ChainRepository {

    Optional<ObjectNode> chain(String name);
}
