package ai.protomolt.proto.types;

import ai.protomolt.proto.validate.ValidationResult;
import ai.protomolt.proto.validate.ValidationResult.Violation;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * The graph and label checks a {@link ConceptScheme} must pass before it mounts, beyond
 * the field rules its type declares. They need the whole document (and the scheme id its
 * config subject carries), so they cannot be field annotations:
 *
 * <ul>
 *   <li>concept ids are unique;</li>
 *   <li>label integrity (SKOS S13, S14): at most one {@code pref_label} per language on
 *       the scheme and on each concept, at most one {@code definition} per language, and
 *       within one language on a concept a text is in at most one of {@code pref_label},
 *       {@code alt_label} and {@code hidden_label};</li>
 *   <li>a cross-scheme mapping never names the concept's own scheme;</li>
 *   <li>every {@code broader}, {@code related} and {@code replaced_by} id names a concept
 *       of the scheme, and {@code related} never names the concept itself;</li>
 *   <li>the {@code broader} graph is acyclic, a violation naming one cycle in order. This
 *       is a ProtoMolt profile rule, stricter than SKOS, which permits {@code broader}
 *       cycles: expansion needs a well-defined set of descendants;</li>
 *   <li>{@code related} never connects a concept to one of its ancestors or descendants
 *       (SKOS S27), either side of the symmetric pair counting;</li>
 *   <li>{@code replaced_by} is set only on deprecated concepts, its graph is acyclic (a
 *       cycle is rejected at mount, named in order like a {@code broader} cycle), and
 *       it never names a deprecated concept that has no successor of its own, so every
 *       chain ends at a live concept.</li>
 * </ul>
 *
 * <p>The checks are deterministic: violations come in document order, and every walk
 * follows declared order. Graph walks are iterative, so a deep hierarchy cannot exhaust
 * the stack.</p>
 */
public final class ConceptSchemeRules {

    private ConceptSchemeRules() {
    }

    /**
     * Checks {@code scheme} as the document mounted under {@code schemeId}.
     *
     * @param schemeId the scheme's id, the remainder of its config subject
     * @param scheme the document
     * @return ok, or every violation found
     */
    public static ValidationResult check(String schemeId, ConceptScheme scheme) {
        Objects.requireNonNull(schemeId, "schemeId");
        Objects.requireNonNull(scheme, "scheme");
        List<Violation> violations = new ArrayList<>();
        singleLabelPerLanguage(scheme.getPrefLabelList(), "pref_label",
                "concept_scheme.pref_label_language", violations);

        // The first occurrence of an id owns it; later duplicates are reported and left out
        // of the graph so the remaining checks still say something precise.
        Map<String, Integer> indexById = new HashMap<>();
        List<Concept> concepts = scheme.getConceptsList();
        for (int i = 0; i < concepts.size(); i++) {
            String id = concepts.get(i).getId();
            Integer first = indexById.putIfAbsent(id, i);
            if (first != null) {
                violations.add(new Violation("concepts[" + i + "].id", "concept_scheme.id_unique",
                        "concept id \"" + id + "\" is already used by concepts[" + first + "]"));
            }
        }

        for (int i = 0; i < concepts.size(); i++) {
            checkConcept(schemeId, concepts.get(i), "concepts[" + i + "]", indexById, violations);
        }

        int[][] broader = edges(concepts, indexById, Concept::getBroaderList);
        boolean acyclic = reportCycles(concepts, broader, "broader",
                "concept_scheme.broader_cycle", violations);
        if (acyclic) {
            relatedOutsideHierarchy(concepts, indexById, broader, violations);
        }

        int[][] replacedBy = edges(concepts, indexById, Concept::getReplacedByList);
        if (reportCycles(concepts, replacedBy, "replaced_by",
                "concept_scheme.replaced_by_cycle", violations)) {
            deadEnds(concepts, indexById, violations);
        }
        return violations.isEmpty() ? ValidationResult.ok() : ValidationResult.failed(violations);
    }

    private static void checkConcept(
            String schemeId, Concept concept, String path, Map<String, Integer> indexById,
            List<Violation> violations) {
        singleLabelPerLanguage(concept.getPrefLabelList(), path + ".pref_label",
                "concept_scheme.pref_label_language", violations);
        singleLabelPerLanguage(concept.getDefinitionList(), path + ".definition",
                "concept_scheme.definition_language", violations);
        disjointLabels(concept, path, violations);

        mappingsLeaveScheme(schemeId, concept.getExactMatchList(), path + ".exact_match", violations);
        mappingsLeaveScheme(schemeId, concept.getCloseMatchList(), path + ".close_match", violations);
        mappingsLeaveScheme(schemeId, concept.getBroadMatchList(), path + ".broad_match", violations);
        mappingsLeaveScheme(schemeId, concept.getNarrowMatchList(), path + ".narrow_match", violations);
        mappingsLeaveScheme(schemeId, concept.getRelatedMatchList(), path + ".related_match", violations);

        resolves(concept.getBroaderList(), path + ".broader", indexById, violations);
        resolves(concept.getRelatedList(), path + ".related", indexById, violations);
        resolves(concept.getReplacedByList(), path + ".replaced_by", indexById, violations);
        List<String> related = concept.getRelatedList();
        for (int j = 0; j < related.size(); j++) {
            if (related.get(j).equals(concept.getId())) {
                violations.add(new Violation(path + ".related[" + j + "]",
                        "concept_scheme.related_self",
                        "concept \"" + concept.getId() + "\" is related to itself"));
            }
        }
        if (!concept.getDeprecated() && concept.getReplacedByCount() > 0) {
            violations.add(new Violation(path + ".replaced_by",
                    "concept_scheme.replaced_by_live",
                    "concept \"" + concept.getId() + "\" names successors but is not deprecated"));
        }

        if (concept.hasIri()) {
            requireIriScheme(concept.getIri(), path + ".iri", violations);
        }
        var notations = concept.getNotationList();
        for (int j = 0; j < notations.size(); j++) {
            String datatype = notations.get(j).getDatatypeIri();
            if (!datatype.isEmpty()) {
                requireIriScheme(datatype, path + ".notation[" + j + "].datatype_iri", violations);
            }
        }
    }

    /**
     * SKOS round-trip IRIs stay inert: only http, https, and urn schemes
     * (case-insensitive) are kept, so a scheme with side effects in a renderer
     * (javascript, data, vbscript, file) never rides along.
     */
    private static void requireIriScheme(String iri, String path, List<Violation> violations) {
        int colon = iri.indexOf(':');
        String scheme = colon < 0 ? "" : iri.substring(0, colon);
        if (!("http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme)
                || "urn".equalsIgnoreCase(scheme))) {
            violations.add(new Violation(path, "concept_scheme.iri_scheme",
                    "IRI \"" + iri + "\" must use the http, https, or urn scheme"));
        }
    }

    /** SKOS S13: at most one label of this kind per language. */
    private static void singleLabelPerLanguage(
            List<LangLabel> labels, String path, String ruleId, List<Violation> violations) {
        Map<String, Integer> firstByLanguage = new HashMap<>();
        for (int i = 0; i < labels.size(); i++) {
            String language = labels.get(i).getLanguage();
            Integer first = firstByLanguage.putIfAbsent(languageKey(language), i);
            if (first != null) {
                violations.add(new Violation(path + "[" + i + "]", ruleId,
                        "language \"" + language + "\" already has a label at " + path
                                + "[" + first + "]"));
            }
        }
    }

    /** SKOS S14: within one language a text is in at most one of the three label kinds. */
    private static void disjointLabels(Concept concept, String path, List<Violation> violations) {
        Map<String, String> owner = new HashMap<>();
        claimLabels(concept.getPrefLabelList(), path + ".pref_label", owner, violations);
        claimLabels(concept.getAltLabelList(), path + ".alt_label", owner, violations);
        claimLabels(concept.getHiddenLabelList(), path + ".hidden_label", owner, violations);
    }

    private static void claimLabels(
            List<LangLabel> labels, String path, Map<String, String> owner,
            List<Violation> violations) {
        String kind = path.substring(path.lastIndexOf('.') + 1);
        for (int i = 0; i < labels.size(); i++) {
            LangLabel label = labels.get(i);
            String key = languageKey(label.getLanguage()) + "\u0000" + label.getText();
            String claimed = owner.putIfAbsent(key, kind);
            if (claimed != null && !claimed.equals(kind)) {
                violations.add(new Violation(path + "[" + i + "]", "concept_scheme.label_disjoint",
                        "\"" + label.getText() + "\" (" + label.getLanguage() + ") is already a "
                                + claimed + " of this concept"));
            }
        }
    }

    /** Language tags compare case-insensitively (BCP 47); ASCII folding is enough. */
    private static String languageKey(String language) {
        StringBuilder folded = new StringBuilder(language.length());
        for (int i = 0; i < language.length(); i++) {
            char c = language.charAt(i);
            folded.append(c >= 'A' && c <= 'Z' ? (char) (c + ('a' - 'A')) : c);
        }
        return folded.toString();
    }

    private static void mappingsLeaveScheme(
            String schemeId, List<ConceptRef> mappings, String path, List<Violation> violations) {
        for (int i = 0; i < mappings.size(); i++) {
            if (mappings.get(i).getSchemeId().equals(schemeId)) {
                violations.add(new Violation(path + "[" + i + "]", "concept_scheme.mapping_scheme",
                        "a mapping names this concept's own scheme \"" + schemeId
                                + "\"; in-scheme relations use broader and related"));
            }
        }
    }

    private static void resolves(
            List<String> ids, String path, Map<String, Integer> indexById,
            List<Violation> violations) {
        for (int i = 0; i < ids.size(); i++) {
            if (!indexById.containsKey(ids.get(i))) {
                violations.add(new Violation(path + "[" + i + "]", "concept_scheme.reference",
                        "\"" + ids.get(i) + "\" is not a concept of this scheme"));
            }
        }
    }

    /** Resolved out-edges per owning concept; duplicate-id concepts and unknown ids drop out. */
    private static int[][] edges(
            List<Concept> concepts, Map<String, Integer> indexById,
            Function<Concept, List<String>> relation) {
        int[][] edges = new int[concepts.size()][];
        for (int i = 0; i < concepts.size(); i++) {
            if (indexById.get(concepts.get(i).getId()) != i) {
                edges[i] = new int[0];
                continue;
            }
            List<String> targets = relation.apply(concepts.get(i));
            int[] resolved = new int[targets.size()];
            int count = 0;
            for (String target : targets) {
                Integer index = indexById.get(target);
                if (index != null) {
                    resolved[count++] = index;
                }
            }
            edges[i] = Arrays.copyOf(resolved, count);
        }
        return edges;
    }

    /**
     * Depth-first walk over the whole graph in document order. Every back edge closes one
     * cycle, reported as its concept ids in walk order, ending where it started.
     *
     * @return whether the graph is acyclic
     */
    private static boolean reportCycles(
            List<Concept> concepts, int[][] edges, String relation, String ruleId,
            List<Violation> violations) {
        int n = concepts.size();
        byte[] state = new byte[n]; // 0 unvisited, 1 on the walk, 2 done
        int[] onStackAt = new int[n];
        int[] stackNode = new int[n];
        int[] stackEdge = new int[n];
        boolean acyclic = true;
        for (int root = 0; root < n; root++) {
            if (state[root] != 0) {
                continue;
            }
            int depth = 0;
            stackNode[0] = root;
            stackEdge[0] = 0;
            state[root] = 1;
            onStackAt[root] = 0;
            while (depth >= 0) {
                int node = stackNode[depth];
                if (stackEdge[depth] == edges[node].length) {
                    state[node] = 2;
                    depth--;
                    continue;
                }
                int next = edges[node][stackEdge[depth]++];
                if (state[next] == 0) {
                    depth++;
                    stackNode[depth] = next;
                    stackEdge[depth] = 0;
                    state[next] = 1;
                    onStackAt[next] = depth;
                } else if (state[next] == 1) {
                    acyclic = false;
                    StringBuilder cycle = new StringBuilder();
                    for (int k = onStackAt[next]; k <= depth; k++) {
                        cycle.append(concepts.get(stackNode[k]).getId()).append(" -> ");
                    }
                    cycle.append(concepts.get(next).getId());
                    violations.add(new Violation("concepts[" + next + "]." + relation, ruleId,
                            relation + " cycle: " + cycle));
                }
            }
        }
        return acyclic;
    }

    /** SKOS S27 over an acyclic hierarchy: no related pair is also an ancestor pair. */
    private static void relatedOutsideHierarchy(
            List<Concept> concepts, Map<String, Integer> indexById, int[][] broader,
            List<Violation> violations) {
        for (int i = 0; i < concepts.size(); i++) {
            Concept concept = concepts.get(i);
            if (indexById.get(concept.getId()) != i) {
                continue;
            }
            List<String> related = concept.getRelatedList();
            for (int j = 0; j < related.size(); j++) {
                Integer other = indexById.get(related.get(j));
                if (other == null || other == i) {
                    continue;
                }
                String relation = reaches(broader, i, other) ? "an ancestor"
                        : reaches(broader, other, i) ? "a descendant" : null;
                if (relation != null) {
                    violations.add(new Violation("concepts[" + i + "].related[" + j + "]",
                            "concept_scheme.related_hierarchy",
                            "\"" + related.get(j) + "\" is " + relation + " of \""
                                    + concept.getId() + "\"; related is not hierarchical"
                                    + " (SKOS S27)"));
                }
            }
        }
    }

    /** Whether {@code target} is reachable from {@code from} over one or more edges. */
    private static boolean reaches(int[][] edges, int from, int target) {
        Set<Integer> seen = new HashSet<>();
        Deque<Integer> pending = new ArrayDeque<>();
        pending.add(from);
        while (!pending.isEmpty()) {
            for (int next : edges[pending.poll()]) {
                if (next == target) {
                    return true;
                }
                if (seen.add(next)) {
                    pending.add(next);
                }
            }
        }
        return false;
    }

    /**
     * Over an acyclic replaced_by graph, a chain ends live unless it reaches a deprecated
     * concept with no successor; naming such a concept as a successor is the dead end.
     */
    private static void deadEnds(
            List<Concept> concepts, Map<String, Integer> indexById, List<Violation> violations) {
        for (int i = 0; i < concepts.size(); i++) {
            List<String> successors = concepts.get(i).getReplacedByList();
            for (int j = 0; j < successors.size(); j++) {
                Integer index = indexById.get(successors.get(j));
                if (index == null) {
                    continue;
                }
                Concept successor = concepts.get(index);
                if (successor.getDeprecated() && successor.getReplacedByCount() == 0) {
                    violations.add(new Violation("concepts[" + i + "].replaced_by[" + j + "]",
                            "concept_scheme.replaced_by_dead_end",
                            "\"" + successor.getId() + "\" is deprecated with no successor of"
                                    + " its own, so the chain from \"" + concepts.get(i).getId()
                                    + "\" ends at a retired concept"));
                }
            }
        }
    }
}
