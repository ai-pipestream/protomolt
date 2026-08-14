package ai.pipestream.proto.index.spi;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.mapper.ProtoFieldMapper;

import java.util.Objects;

/**
 * Context shared by {@link SearchEngineIndexer} plugins.
 *
 * <p>{@code descriptorRegistry} is the same registry NDJSON accepts for {@code Any} JSON
 * rendering. Write-time Any expansion resolves {@code type_url} against it. Engines that
 * already take a {@link ProtoFieldMapper} should read the registry from this context (the
 * mapper is typically built with the same instance), not from a process-wide singleton.
 */
public record IndexerContext(
        ProtoFieldMapper fieldMapper,
        DescriptorRegistry descriptorRegistry,
        IndexingPlanFactory planFactory) {

    public IndexerContext {
        Objects.requireNonNull(fieldMapper, "fieldMapper");
        descriptorRegistry = descriptorRegistry != null
                ? descriptorRegistry
                : Objects.requireNonNull(fieldMapper.getDescriptorRegistry(), "descriptorRegistry");
        planFactory = planFactory != null
                ? planFactory
                : IndexingPlanFactory.defaults(new CatalogIndexingHintSource());
    }

    public IndexerContext(ProtoFieldMapper fieldMapper) {
        this(fieldMapper, null, null);
    }

    public IndexerContext(ProtoFieldMapper fieldMapper, DescriptorRegistry descriptorRegistry) {
        this(fieldMapper, descriptorRegistry, null);
    }
}
