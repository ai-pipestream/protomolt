package ai.protomolt.proto.sources.publish;

import ai.protomolt.proto.sources.ProtoSourceSet;

/**
 * Registers a {@link ProtoSourceSet} with a schema registry — the write-side counterpart of
 * {@code DescriptorLoader}.
 *
 * <p>Implementations must register files in reverse-topological import order
 * ({@link ProtoSourceSet#topologicalOrder()}), so every reference exists before the file that
 * imports it, and must be idempotent: re-publishing identical content reports
 * {@link PublishResult.Action#UNCHANGED} rather than creating spurious versions. Compatibility
 * checking is the registry's: a server-side compatibility rejection surfaces as a
 * {@link PublishResult.Action#FAILED} outcome for that file, never as a thrown exception, so
 * one incompatible schema does not abort the rest of the set.</p>
 */
public interface SchemaPublisher {

    /**
     * Publishes every file in the set.
     *
     * @throws SchemaPublishException only for registry-level failures (unreachable,
     *         authentication); per-file failures are reported in the result
     */
    PublishResult publish(ProtoSourceSet sources, PublishOptions options) throws SchemaPublishException;

    /** Human-readable target description, e.g. {@code apicurio:http://…/apis/registry/v3}. */
    String target();
}
