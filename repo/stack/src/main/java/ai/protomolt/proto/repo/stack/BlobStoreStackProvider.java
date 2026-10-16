package ai.protomolt.proto.repo.stack;

import ai.protomolt.proto.repo.blob.spi.BlobCapability;
import ai.protomolt.proto.repo.blob.spi.BlobStores;
import ai.protomolt.proto.repo.v2.StackRecord;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Stacks over the installed {@link ai.protomolt.proto.repo.blob.spi.BlobStoreProvider}
 * named {@code s3}. The record's namespace is {@code <bucket>/<prefix>}; the options
 * are the byte storage provider's own. The opened store's lifetime is the stack's.
 */
public final class BlobStoreStackProvider implements StackProvider {
    private final BlobStores stores;

    /** ServiceLoader constructor: byte storage providers are discovered the same way. */
    public BlobStoreStackProvider() { this(BlobStores.discover()); }

    public BlobStoreStackProvider(BlobStores stores) { this.stores = stores; }

    @Override public String id() { return "s3"; }

    @Override public Stack open(StackRecord record, Map<String, String> options) throws IOException {
        var namespace = record.getNamespace();
        int slash = namespace.indexOf('/');
        if (slash < 1 || slash == namespace.length() - 1)
            throw new IllegalArgumentException("An s3 stack namespace is <bucket>/<prefix>");
        var opened = stores.open(id(), options, Set.of(BlobCapability.STREAMING_WRITE));
        try {
            return new BlobStoreStack(record.getStackId(), opened.store(), namespace.substring(0, slash),
                    namespace.substring(slash + 1), BlobStoreStack.DEFAULT_PART_SIZE, opened::close);
        } catch (RuntimeException failure) {
            try { opened.close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }
}
