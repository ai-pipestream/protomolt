package ai.protomolt.proto.config;

import ai.protomolt.proto.formats.Formats;
import ai.protomolt.proto.types.Concept;
import ai.protomolt.proto.types.ConceptExpansion;
import ai.protomolt.proto.types.ConceptRef;
import ai.protomolt.proto.types.ConceptScheme;
import ai.protomolt.proto.types.ConceptSchemeRules;
import ai.protomolt.proto.validate.spi.ConceptSchemeCatalog;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * A {@link ConceptSchemeCatalog} that follows concept-scheme config documents off the
 * config lane, one subject per scheme ({@code concept-scheme:<scheme_id>}: the subject is
 * the identity, the document carries no id), and expands queries against what is mounted.
 *
 * <p>Mounting is verify-then-swap. Before a document applies, the host's
 * {@link DistributedConfig} enforces {@link ConceptScheme}'s declared field rules and then
 * {@link ConceptSchemeRules} (unique ids, label integrity, references resolve, acyclic
 * {@code broader}, {@code related} outside the hierarchy, live {@code replaced_by} chains).
 * A document failing either is refused with its reasons and the previously mounted version
 * keeps serving. A cross-scheme mapping to a scheme that is not mounted, or to an id the
 * mounted scheme lacks, does not block a mount: it is unresolved, skipped by expansion and
 * reported in the expansion's evidence.</p>
 *
 * <p>Every mount swap publishes one immutable snapshot of all mounted schemes. An
 * expansion captures that snapshot once and reads only it, so a swap during the expansion
 * does not change its result, and every traversal (the {@code exact_match} closure, the
 * {@code replaced_by} predecessors, the {@code narrower} descendants) keeps a visited set,
 * so each concept is entered once. Given the snapshot's versions, which the result reports,
 * the expansion is deterministic.</p>
 */
public final class ConceptSchemeMounts implements ConceptSchemeCatalog {

    /** Subject prefix; the remainder is the scheme id schemas bind. */
    public static final String SUBJECT_PREFIX = "concept-scheme:";

    /** The default bound on one query's expanded concept set. */
    public static final int DEFAULT_MAX_EXPANDED_CONCEPTS = 10_000;

    private static final int MAX_SCHEME_ID_LENGTH = 200;

    private final int maxExpandedConcepts;
    private volatile Snapshot snapshot = new Snapshot(Map.of());

    private ConceptSchemeMounts(int maxExpandedConcepts) {
        this.maxExpandedConcepts = maxExpandedConcepts;
    }

    /**
     * Mounts that follow no scheme, for a host that has none configured. Nothing ever
     * mounts: a bound field refuses fail-closed ({@code concept.unmounted}) and a query
     * on any scheme is refused as unmounted, exactly as a followed scheme behaves before
     * its first document applies.
     *
     * @return the empty mounts
     */
    public static ConceptSchemeMounts empty() {
        return new ConceptSchemeMounts(DEFAULT_MAX_EXPANDED_CONCEPTS);
    }

    /**
     * As {@link #follow(DistributedConfig, Collection, int)} with
     * {@link #DEFAULT_MAX_EXPANDED_CONCEPTS}.
     *
     * @param config the consumer to subscribe on; the host drives its cadence
     * @param schemeIds the scheme ids to follow
     * @return the mounts
     */
    public static ConceptSchemeMounts follow(DistributedConfig config, Collection<String> schemeIds) {
        return follow(config, schemeIds, DEFAULT_MAX_EXPANDED_CONCEPTS);
    }

    /**
     * Subscribes {@code concept-scheme:<scheme_id>} for each id on {@code config} and returns
     * the mounts those subscriptions feed. Until a first document applies, a scheme is
     * unmounted: fields bound to it refuse fail-closed and queries on it are refused.
     *
     * @param config the consumer to subscribe on; the host drives its cadence
     * @param schemeIds the scheme ids to follow, each a slug of at most 200 characters
     * @param maxExpandedConcepts the most concepts one expansion may return; a query that
     *        expands further is refused, never truncated
     * @return the mounts
     */
    public static ConceptSchemeMounts follow(
            DistributedConfig config, Collection<String> schemeIds, int maxExpandedConcepts) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(schemeIds, "schemeIds");
        if (maxExpandedConcepts < 1) {
            throw new IllegalArgumentException(
                    "maxExpandedConcepts must be positive, got " + maxExpandedConcepts);
        }
        ConceptSchemeMounts mounts = new ConceptSchemeMounts(maxExpandedConcepts);
        for (String schemeId : schemeIds) {
            if (schemeId == null || !Formats.isSlug(schemeId)
                    || schemeId.length() > MAX_SCHEME_ID_LENGTH) {
                throw new IllegalArgumentException("scheme id \"" + schemeId
                        + "\" is not a slug of at most " + MAX_SCHEME_ID_LENGTH + " characters");
            }
            config.subscribe(SUBJECT_PREFIX + schemeId, ConceptScheme.getDefaultInstance(),
                            scheme -> ConceptSchemeRules.check(schemeId, scheme))
                    .onChange((scheme, version) -> mounts.mount(schemeId, scheme, version));
        }
        return mounts;
    }

    private synchronized void mount(String schemeId, ConceptScheme scheme, String version) {
        Map<String, Scheme> schemes = new HashMap<>(snapshot.schemes());
        schemes.put(schemeId, Scheme.of(schemeId, version, scheme));
        snapshot = new Snapshot(Map.copyOf(schemes));
    }

    @Override
    public Optional<ConceptSchemeCatalog.Mounted> scheme(String schemeId) {
        return Optional.ofNullable(snapshot.schemes().get(schemeId)).map(Scheme::membership);
    }

    @Override
    public ConceptSchemeCatalog snapshot() {
        Snapshot pin = this.snapshot;
        return schemeId -> Optional.ofNullable(pin.schemes().get(schemeId)).map(Scheme::membership);
    }

    /**
     * Expands a query on {@code query} against the mounted schemes. Always included: the
     * queried concept, its {@code exact_match} closure across mounted schemes (symmetric and
     * transitive), and the deprecated concepts whose {@code replaced_by} chain reaches one of
     * those. With {@code narrower}, every in-scheme descendant of each concept so far is
     * added once however many paths reach it. With {@code close_match}, the queried concept's
     * {@code close_match} targets are added, one hop and symmetric. {@code broader},
     * {@code related}, {@code broad_match}, {@code narrow_match} and {@code related_match}
     * never expand a query.
     *
     * @param query the queried concept
     * @param expansion the optional steps
     * @return the expanded concepts with the evidence that makes them reproducible
     * @throws IllegalStateException when the queried scheme is not mounted
     * @throws IllegalArgumentException when the queried concept is not in its mounted scheme,
     *         or the expansion exceeds the bound (the INVALID_ARGUMENT cases)
     */
    public Expansion expand(ConceptRef query, ConceptExpansion expansion) {
        Objects.requireNonNull(query, "query");
        Objects.requireNonNull(expansion, "expansion");
        return new Expander(snapshot, Key.of(query), maxExpandedConcepts).run(expansion);
    }

    /**
     * One expansion's result.
     *
     * @param concepts the expanded concepts, ordered by scheme id then concept id
     * @param mountedVersions the mounted version of every scheme the expansion read, by id;
     *        the symmetric mapping lookups read every mounted scheme, including those that
     *        contributed nothing, because a new version of any of them can change the result
     * @param unresolvedMappings the mappings followed but skipped because their target scheme
     *        is not mounted or lacks the target id, in a stable order
     */
    public record Expansion(
            List<ConceptRef> concepts,
            SortedMap<String, String> mountedVersions,
            List<UnresolvedMapping> unresolvedMappings) {

        public Expansion {
            concepts = List.copyOf(concepts);
            mountedVersions = Collections.unmodifiableSortedMap(new TreeMap<>(mountedVersions));
            unresolvedMappings = List.copyOf(unresolvedMappings);
        }
    }

    /**
     * A mapping an expansion followed but could not resolve.
     *
     * @param from the concept carrying the mapping
     * @param relation the mapping field, {@code exact_match} or {@code close_match}
     * @param target the mapping's target
     * @param reason why it did not resolve, naming the mounted version when there is one
     */
    public record UnresolvedMapping(
            ConceptRef from, String relation, ConceptRef target, String reason) {
    }

    /** A scheme-qualified concept id, ordered by scheme then concept. */
    private record Key(String schemeId, String conceptId) implements Comparable<Key> {

        private static final Comparator<Key> ORDER =
                Comparator.comparing(Key::schemeId).thenComparing(Key::conceptId);

        static Key of(ConceptRef ref) {
            return new Key(ref.getSchemeId(), ref.getConceptId());
        }

        ConceptRef ref() {
            return ConceptRef.newBuilder().setSchemeId(schemeId).setConceptId(conceptId).build();
        }

        @Override
        public int compareTo(Key other) {
            return ORDER.compare(this, other);
        }

        @Override
        public String toString() {
            return schemeId + ":" + conceptId;
        }
    }

    /** All mounted schemes, published as one immutable value per swap. */
    private record Snapshot(Map<String, Scheme> schemes) {

        /** Concepts whose {@code relation} mapping names {@code target}, across schemes. */
        List<Key> reverseMappings(Key target, String relation) {
            List<Key> sources = new ArrayList<>();
            for (Scheme scheme : schemes.values()) {
                sources.addAll(scheme.inbound(relation).getOrDefault(target, List.of()));
            }
            return sources;
        }
    }

    /**
     * One mounted scheme with the indexes expansion needs: the inverse of {@code broader},
     * the deprecated predecessors of each concept, and its outbound exact and close mappings
     * keyed by target so other schemes' symmetric lookups stay one map read.
     */
    private record Scheme(
            String schemeId,
            String version,
            Map<String, Concept> concepts,
            Map<String, List<String>> narrower,
            Map<String, List<String>> predecessors,
            Map<Key, List<Key>> exactInbound,
            Map<Key, List<Key>> closeInbound,
            ConceptSchemeCatalog.Mounted membership) {

        static Scheme of(String schemeId, String version, ConceptScheme document) {
            Map<String, Concept> concepts = new LinkedHashMap<>();
            Map<String, List<String>> narrower = new HashMap<>();
            Map<String, List<String>> predecessors = new HashMap<>();
            Map<Key, List<Key>> exactInbound = new HashMap<>();
            Map<Key, List<Key>> closeInbound = new HashMap<>();
            for (Concept concept : document.getConceptsList()) {
                concepts.put(concept.getId(), concept);
                Key self = new Key(schemeId, concept.getId());
                for (String parent : concept.getBroaderList()) {
                    narrower.computeIfAbsent(parent, k -> new ArrayList<>()).add(concept.getId());
                }
                if (concept.getDeprecated()) {
                    for (String successor : concept.getReplacedByList()) {
                        predecessors.computeIfAbsent(successor, k -> new ArrayList<>())
                                .add(concept.getId());
                    }
                }
                for (ConceptRef target : concept.getExactMatchList()) {
                    exactInbound.computeIfAbsent(Key.of(target), k -> new ArrayList<>()).add(self);
                }
                for (ConceptRef target : concept.getCloseMatchList()) {
                    closeInbound.computeIfAbsent(Key.of(target), k -> new ArrayList<>()).add(self);
                }
            }
            return new Scheme(schemeId, version, concepts, narrower, predecessors,
                    exactInbound, closeInbound,
                    new ConceptSchemeCatalog.Mounted(schemeId, version, concepts.keySet()));
        }

        Map<Key, List<Key>> inbound(String relation) {
            return "exact_match".equals(relation) ? exactInbound : closeInbound;
        }
    }

    /** One expansion over one snapshot. */
    private static final class Expander {

        private final Snapshot snapshot;
        private final Key query;
        private final int bound;
        private final SortedSet<Key> concepts = new TreeSet<>();
        private final SortedMap<String, String> versions = new TreeMap<>();
        private final SortedMap<String, UnresolvedMapping> unresolved = new TreeMap<>();

        Expander(Snapshot snapshot, Key query, int bound) {
            this.snapshot = snapshot;
            this.query = query;
            this.bound = bound;
        }

        Expansion run(ConceptExpansion expansion) {
            Scheme home = snapshot.schemes().get(query.schemeId());
            if (home == null) {
                throw new IllegalStateException("concept scheme \"" + query.schemeId()
                        + "\" is not mounted; a query on " + query + " cannot be expanded");
            }
            versions.put(home.schemeId(), home.version());
            if (!home.concepts().containsKey(query.conceptId())) {
                throw new IllegalArgumentException("\"" + query.conceptId()
                        + "\" is not a concept of scheme \"" + query.schemeId()
                        + "\" at version " + home.version());
            }

            List<Key> exact = closure(query, "exact_match");
            for (Key member : exact) {
                add(member);
            }
            for (Key member : exact) {
                addPredecessors(member);
            }
            if (expansion.getNarrower()) {
                for (Key member : List.copyOf(concepts)) {
                    addDescendants(member);
                }
            }
            if (expansion.getCloseMatch()) {
                for (Key target : neighbours(query, "close_match")) {
                    add(target);
                }
            }
            return new Expansion(
                    concepts.stream().map(Key::ref).toList(),
                    versions,
                    List.copyOf(unresolved.values()));
        }

        /** The symmetric, transitive closure of {@code relation} from {@code start}. */
        private List<Key> closure(Key start, String relation) {
            List<Key> members = new ArrayList<>();
            Set<Key> seen = new HashSet<>();
            Deque<Key> pending = new ArrayDeque<>();
            seen.add(start);
            pending.add(start);
            while (!pending.isEmpty()) {
                Key member = pending.poll();
                members.add(member);
                if (members.size() > bound) {
                    throw overBound();
                }
                for (Key next : neighbours(member, relation)) {
                    if (seen.add(next)) {
                        pending.add(next);
                    }
                }
            }
            return members;
        }

        /**
         * The resolved one-hop {@code relation} neighbours of a mounted concept, both
         * directions, sorted. Unresolved outbound targets are recorded and skipped.
         */
        private SortedSet<Key> neighbours(Key from, String relation) {
            Scheme scheme = snapshot.schemes().get(from.schemeId());
            Concept concept = scheme.concepts().get(from.conceptId());
            List<ConceptRef> outbound = "exact_match".equals(relation)
                    ? concept.getExactMatchList() : concept.getCloseMatchList();
            SortedSet<Key> resolved = new TreeSet<>();
            for (ConceptRef ref : outbound) {
                Key target = Key.of(ref);
                Scheme targetScheme = snapshot.schemes().get(target.schemeId());
                if (targetScheme == null) {
                    unresolved(from, relation, target, "scheme \"" + target.schemeId()
                            + "\" is not mounted");
                    continue;
                }
                versions.put(targetScheme.schemeId(), targetScheme.version());
                if (!targetScheme.concepts().containsKey(target.conceptId())) {
                    unresolved(from, relation, target, "\"" + target.conceptId()
                            + "\" is not a concept of scheme \"" + target.schemeId()
                            + "\" at version " + targetScheme.version());
                    continue;
                }
                resolved.add(target);
            }
            // The reverse lookup reads every mounted scheme, and a scheme that names nothing
            // here still decides the result: a new version could add an inbound mapping. So
            // each one is a dependency of the expansion, reported with its version.
            for (Scheme consulted : snapshot.schemes().values()) {
                versions.put(consulted.schemeId(), consulted.version());
            }
            resolved.addAll(snapshot.reverseMappings(from, relation));
            return resolved;
        }

        private void unresolved(Key from, String relation, Key target, String reason) {
            unresolved.putIfAbsent(from + " " + relation + " " + target,
                    new UnresolvedMapping(from.ref(), relation, target.ref(), reason));
        }

        /** Deprecated concepts whose replaced_by chain reaches {@code successor}. */
        private void addPredecessors(Key successor) {
            Scheme scheme = snapshot.schemes().get(successor.schemeId());
            Deque<String> pending = new ArrayDeque<>();
            pending.add(successor.conceptId());
            while (!pending.isEmpty()) {
                for (String predecessor : scheme.predecessors().getOrDefault(pending.poll(), List.of())) {
                    if (add(new Key(scheme.schemeId(), predecessor))) {
                        pending.add(predecessor);
                    }
                }
            }
        }

        /** Every in-scheme descendant of {@code ancestor}, each once. */
        private void addDescendants(Key ancestor) {
            Scheme scheme = snapshot.schemes().get(ancestor.schemeId());
            Deque<String> pending = new ArrayDeque<>();
            Set<String> seen = new HashSet<>();
            pending.add(ancestor.conceptId());
            while (!pending.isEmpty()) {
                for (String child : scheme.narrower().getOrDefault(pending.poll(), List.of())) {
                    if (seen.add(child)) {
                        add(new Key(scheme.schemeId(), child));
                        pending.add(child);
                    }
                }
            }
        }

        private boolean add(Key key) {
            boolean added = concepts.add(key);
            if (concepts.size() > bound) {
                throw overBound();
            }
            return added;
        }

        private IllegalArgumentException overBound() {
            return new IllegalArgumentException("expanding " + query + " reaches more than "
                    + bound + " concepts (max_expanded_concepts); the query is refused, not"
                    + " truncated");
        }
    }
}
