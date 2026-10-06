package ai.protomolt.proto.repo.service;

/**
 * Explicit limits for an embedded managed archive backed by bounded Redis objects.
 * The host owns one budget shared by local puts and every bounded Netty listener.
 * The budget must cover one maximum transport put: two request allowances for
 * unary decoding plus a request and four inline-payload copy allowances.
 * It need not cover maximum-sized calls at the full configured concurrency;
 * excess work fails with RESOURCE_EXHAUSTED rather than waiting for capacity.
 * These allowances do not measure decoded heap, read responses or network buffers.
 */
public record BoundedArchiveOptions(int maxObjectBytes, int maxRequestBytes, int maxRenditions,
        long payloadBudgetBytes, int maxConcurrentRequests) {
    public BoundedArchiveOptions {
        // Reuse the engine limits without exposing engine types in the public API.
        new ai.protomolt.proto.repo.engine.ArchivePutAdmission.Limits(maxObjectBytes, maxRequestBytes, maxRenditions);
        if (maxConcurrentRequests < 1 || maxConcurrentRequests > 1024)
            throw new IllegalArgumentException("Archive concurrency must be between 1 and 1024");
        if (payloadBudgetBytes < 7L * maxRequestBytes)
            throw new IllegalArgumentException("Archive payload budget must cover seven maximum request allowances");
    }

    BoundedArchiveProfile profile() {
        return new BoundedArchiveProfile(new ai.protomolt.proto.repo.engine.ArchivePutAdmission.Limits(
                maxObjectBytes, maxRequestBytes, maxRenditions),
                new ai.protomolt.proto.repo.blob.spi.PayloadBudget(payloadBudgetBytes), maxConcurrentRequests);
    }
}
