package ai.protomolt.proto.metric.spi;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Process-wide counters of the expensive steps behind {@link MetricRowPredicates}: synthetic
 * {@code CelRowContext} descriptor builds and CEL parse-and-check compilations. Tests read
 * them to prove the caches share one build per mapping and one compilation per expression.
 */
final class MetricRowPredicateStats {
    private static final AtomicInteger DESCRIPTOR_BUILDS = new AtomicInteger();
    private static final AtomicInteger CEL_COMPILATIONS = new AtomicInteger();

    private MetricRowPredicateStats() {
    }

    static void recordDescriptorBuild() {
        DESCRIPTOR_BUILDS.incrementAndGet();
    }

    static void recordCelCompilation() {
        CEL_COMPILATIONS.incrementAndGet();
    }

    /** @return synthetic row-context descriptor builds since the last {@link #reset()} */
    static int descriptorBuilds() {
        return DESCRIPTOR_BUILDS.get();
    }

    /** @return CEL parse-and-check compilations since the last {@link #reset()} */
    static int celCompilations() {
        return CEL_COMPILATIONS.get();
    }

    static void reset() {
        DESCRIPTOR_BUILDS.set(0);
        CEL_COMPILATIONS.set(0);
    }
}
