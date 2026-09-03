package ai.protomolt.proto.config;

import ai.protomolt.proto.types.Taxonomy;
import ai.protomolt.proto.validate.spi.TaxonomyCatalog;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A {@link TaxonomyCatalog} that follows taxonomy config documents off the
 * config lane: one subject per taxonomy ({@code taxonomy:<name>} — the
 * subject is the identity, the document carries no name), applied by the
 * host's {@link DistributedConfig} refresh. Verify-then-swap upstream means
 * only a document passing {@link Taxonomy}'s own declared rules ever reaches
 * a mount, and a refused or absent document leaves the mounted taxonomy
 * serving. A validator constructed over this catalog sees each swap on its
 * next validation: updating a taxonomy is one config publish, no schema
 * change and no restart.
 */
public final class TaxonomyMounts implements TaxonomyCatalog {

    /** Subject prefix; the remainder is the taxonomy name schemas declare. */
    public static final String SUBJECT_PREFIX = "taxonomy:";

    private final Map<String, TaxonomyCatalog.Mounted> mounted = new ConcurrentHashMap<>();

    private TaxonomyMounts() {
    }

    /**
     * Subscribes {@code taxonomy:<name>} for each name on {@code config} and
     * returns the catalog those subscriptions feed. Mounts appear and swap as
     * the host's refresh applies documents; until a first document applies, a
     * name is unmounted and fields bound to it refuse fail-closed.
     *
     * @param config the consumer to subscribe on; the host drives its cadence
     * @param names the taxonomy names to follow
     * @return the catalog
     */
    public static TaxonomyMounts follow(DistributedConfig config, Collection<String> names) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(names, "names");
        TaxonomyMounts mounts = new TaxonomyMounts();
        for (String name : names) {
            if (name == null || name.isBlank()) {
                throw new IllegalArgumentException("taxonomy name must not be blank");
            }
            config.subscribe(SUBJECT_PREFIX + name, Taxonomy.getDefaultInstance())
                    .onChange((taxonomy, version) -> mounts.mount(name, taxonomy, version));
        }
        return mounts;
    }

    private void mount(String name, Taxonomy taxonomy, String version) {
        List<List<String>> entries = taxonomy.getEntriesList().stream()
                .map(entry -> List.copyOf(entry.getSegmentsList()))
                .toList();
        mounted.put(name, TaxonomyCatalog.Mounted.of(name, version, entries));
    }

    @Override
    public Optional<TaxonomyCatalog.Mounted> taxonomy(String name) {
        return Optional.ofNullable(mounted.get(name));
    }
}
