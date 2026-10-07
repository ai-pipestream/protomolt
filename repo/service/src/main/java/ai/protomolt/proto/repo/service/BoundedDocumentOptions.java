package ai.protomolt.proto.repo.service;

/**
 * Explicit limits for managed documents backed by bounded Redis objects.
 * The object limit applies to each uploaded part, including retry inputs. The
 * shared payload budget covers publication and retained-read allowances; it is
 * not a measurement of total heap, parser buffers or network buffers. Transport
 * delivery budgets and call limits remain in their respective transport options.
 * Excess concurrent work fails with RESOURCE_EXHAUSTED rather than waiting for
 * memory capacity. Redis persistence and eviction policy are operator obligations.
 */
public record BoundedDocumentOptions(int maxObjectBytes, long payloadBudgetBytes) {
    public BoundedDocumentOptions {
        new BoundedDocumentProfile(maxObjectBytes, payloadBudgetBytes);
    }

    BoundedDocumentProfile profile() {
        return new BoundedDocumentProfile(maxObjectBytes, payloadBudgetBytes);
    }
}
