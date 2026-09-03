package ai.protomolt.proto.validate.spi;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * The validator's window onto mounted taxonomies: the runtime data behind the
 * {@code taxonomy} field rule. The rule itself is schema truth — a TreePath
 * field declares which taxonomy its values belong to, forever — while the
 * taxonomy's content is mount configuration, typically followed off the config
 * lane and swapped live. Given a mounted version the verdict is deterministic,
 * and the version rides every membership violation as evidence.
 *
 * <p>Implementations must be thread-safe; the validator consults the catalog on
 * every validation of a bound field. A catalog with nothing mounted makes every
 * declared taxonomy refuse ({@code taxonomy.unmounted}): fail-closed, never a
 * silent pass.</p>
 */
public interface TaxonomyCatalog {

    /**
     * The taxonomy mounted under {@code name}, or empty when none is.
     *
     * @param name the taxonomy name a schema declares
     * @return the mounted taxonomy, if any
     */
    Optional<Mounted> taxonomy(String name);

    /** A catalog with nothing mounted: every declared taxonomy refuses. */
    static TaxonomyCatalog empty() {
        return name -> Optional.empty();
    }

    /**
     * One mounted taxonomy, its nodes as rendered root-first paths.
     *
     * @param name the mount name
     * @param version the config source's version of the document, the evidence
     *        a membership violation cites
     * @param nodes every node, rendered as its "/"-joined path from the root
     */
    record Mounted(String name, String version, Set<String> nodes) {

        public Mounted {
            if (name == null || name.isBlank()) {
                throw new IllegalArgumentException("name must not be blank");
            }
            Objects.requireNonNull(version, "version");
            nodes = Set.copyOf(Objects.requireNonNull(nodes, "nodes"));
        }

        /**
         * Builds the mount from entries, each a root-first segment list. An
         * entry names a node by its full path, and every ancestor along the
         * way is a node too, so listing the leaves is enough.
         *
         * @param name the mount name
         * @param version the config source's version of the document
         * @param entries the entries as root-first segment lists
         * @return the mount with the full node set expanded
         */
        public static Mounted of(
                String name, String version,
                Collection<? extends List<String>> entries) {
            Set<String> nodes = new HashSet<>();
            for (List<String> entry : Objects.requireNonNull(entries, "entries")) {
                StringBuilder chain = new StringBuilder();
                for (String segment : entry) {
                    if (chain.length() > 0) {
                        chain.append('/');
                    }
                    chain.append(segment);
                    nodes.add(chain.toString());
                }
            }
            return new Mounted(name, version, nodes);
        }
    }
}
