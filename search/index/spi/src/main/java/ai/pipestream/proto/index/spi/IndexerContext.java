package ai.pipestream.proto.index.spi;

import ai.pipestream.proto.mapper.ProtoFieldMapper;

import java.util.Objects;

/** Context shared by {@link SearchEngineIndexer} plugins. */
public record IndexerContext(ProtoFieldMapper fieldMapper) {
    public IndexerContext {
        Objects.requireNonNull(fieldMapper, "fieldMapper");
    }
}
