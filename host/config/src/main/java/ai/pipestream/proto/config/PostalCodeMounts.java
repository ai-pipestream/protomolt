package ai.pipestream.proto.config;

import ai.pipestream.proto.types.PostalCodePack;
import ai.pipestream.proto.types.RegionPostalMasks;
import ai.pipestream.proto.validate.spi.PostalCodeCatalog;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link PostalCodeCatalog} that follows the postal-code pack off the
 * config lane: one subject ({@value #SUBJECT}), applied by the host's
 * {@link DistributedConfig} refresh. Verify-then-swap upstream means only a
 * document passing {@link PostalCodePack}'s own declared rules ever reaches
 * a mount; a pack duplicating a region is refused here (the listener
 * throws, the refresh logs it, and the previous pack stays live). A
 * validator constructed over this catalog sees each swap on its next
 * validation; a region the pack does not carry stays unchecked, the
 * data-free default.
 */
public final class PostalCodeMounts implements PostalCodeCatalog {

    /** The pack's config subject; one pack per lane. */
    public static final String SUBJECT = "postal-codes";

    private final AtomicReference<Map<String, PostalCodeCatalog.Mounted>> mounted =
            new AtomicReference<>(Map.of());

    private PostalCodeMounts() {
    }

    /**
     * Subscribes the pack subject on {@code config} and returns the catalog
     * the subscription feeds.
     *
     * @param config the consumer to subscribe on; the host drives its cadence
     * @return the catalog
     */
    public static PostalCodeMounts follow(DistributedConfig config) {
        Objects.requireNonNull(config, "config");
        PostalCodeMounts mounts = new PostalCodeMounts();
        config.subscribe(SUBJECT, PostalCodePack.getDefaultInstance())
                .onChange(mounts::mount);
        return mounts;
    }

    private void mount(PostalCodePack pack, String version) {
        Map<String, PostalCodeCatalog.Mounted> regions = new HashMap<>();
        for (RegionPostalMasks region : pack.getRegionsList()) {
            PostalCodeCatalog.Mounted previous = regions.put(region.getRegionCode(),
                    new PostalCodeCatalog.Mounted(region.getRegionCode(), version,
                            List.copyOf(region.getMasksList())));
            if (previous != null) {
                throw new IllegalArgumentException("postal-code pack duplicates region '"
                        + region.getRegionCode() + "'");
            }
        }
        mounted.set(Map.copyOf(regions));
    }

    @Override
    public Optional<PostalCodeCatalog.Mounted> region(String regionCode) {
        return Optional.ofNullable(mounted.get().get(regionCode));
    }
}
