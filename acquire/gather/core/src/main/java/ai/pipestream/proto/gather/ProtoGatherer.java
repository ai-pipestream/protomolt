package ai.pipestream.proto.gather;

import ai.pipestream.proto.sources.ProtoSourceSet;

/**
 * Collects {@code .proto} sources from somewhere — a directory tree, a jar, a git repository,
 * a Maven artifact — into a self-contained {@link ProtoSourceSet} whose import paths resolve
 * against each other.
 *
 * <p>Gatherers are the runtime counterpart of build-time proto staging: instead of copying
 * files into a build directory, they produce an in-memory source set that
 * {@code ProtoSourceCompiler} turns into descriptors (see {@link GatheringDescriptorLoader}).</p>
 */
public interface ProtoGatherer {

    /**
     * Gathers all {@code .proto} sources this gatherer is configured for.
     *
     * @return the gathered sources, keyed by import path
     * @throws GatherException on missing inputs, I/O failure, or conflicting content
     */
    ProtoSourceSet gather() throws GatherException;

    /**
     * Human-readable description of where this gatherer's sources come from, e.g.
     * {@code git:https://…@main} or {@code jar:common-protos-1.2.jar}. Feeds
     * {@link ai.pipestream.proto.sources.ProtoSource#origin()} and diagnostics.
     */
    String origin();

    /** Whether this gatherer can currently gather (e.g. an offline cache is present). */
    default boolean isAvailable() {
        return true;
    }
}
