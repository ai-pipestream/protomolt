package ai.protomolt.proto.platform;

import ai.protomolt.proto.validate.spi.TaxonomyCatalog;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A catalog handed to gates before the config lane exists: services are wired
 * inside the composer boot, while {@code TaxonomyMounts} needs the booted
 * node's {@code DistributedConfig}. Until the mounts swap in, the delegate
 * is the empty catalog, so declared taxonomies refuse fail-closed rather
 * than pass unchecked — exactly the stance a gate must hold while its data
 * is not yet mounted.
 */
final class SwappableTaxonomyCatalog implements TaxonomyCatalog {

    private final AtomicReference<TaxonomyCatalog> delegate =
            new AtomicReference<>(TaxonomyCatalog.empty());

    @Override
    public Optional<TaxonomyCatalog.Mounted> taxonomy(String name) {
        return delegate.get().taxonomy(name);
    }

    /** Swaps the live delegate; the next validation sees the new mounts. */
    void swap(TaxonomyCatalog catalog) {
        if (catalog == null) {
            throw new IllegalArgumentException("catalog must not be null");
        }
        delegate.set(catalog);
    }
}
