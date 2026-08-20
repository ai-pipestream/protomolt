package ai.pipestream.proto.platform;

import ai.pipestream.proto.validate.spi.PostalCodeCatalog;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A postal-code catalog handed to gates before the config lane exists: doors are wired
 * inside the composer boot, while {@code PostalCodeMounts} needs the booted node's
 * {@code DistributedConfig}. Until the mounts swap in, the delegate is the empty catalog —
 * for postal grammar that means every region is unmounted and therefore unchecked, the
 * pack's documented per-region opt-in stance, never a fail-closed gate.
 */
final class SwappablePostalCodes implements PostalCodeCatalog {

    private final AtomicReference<PostalCodeCatalog> delegate =
            new AtomicReference<>(PostalCodeCatalog.empty());

    @Override
    public Optional<PostalCodeCatalog.Mounted> region(String regionCode) {
        return delegate.get().region(regionCode);
    }

    /** Swaps the live delegate; the next validation sees the new pack. */
    void swap(PostalCodeCatalog catalog) {
        if (catalog == null) {
            throw new IllegalArgumentException("catalog must not be null");
        }
        delegate.set(catalog);
    }
}
