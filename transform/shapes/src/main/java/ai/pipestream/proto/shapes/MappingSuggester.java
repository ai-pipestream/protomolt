package ai.pipestream.proto.shapes;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Descriptor-grounded mapping suggestions: given named sources and a target type, propose
 * candidate {@code target = source.path} rules an author (human or agent) can review, edit,
 * and adopt. Every emitted candidate has already passed {@link RuleChecker}, the same static
 * gate chains are verified with — a suggestion is never a rule the runtime would reject, and
 * nothing here executes or bypasses validation.
 *
 * <p>Candidates are found by name: a target field matches a source field whose normalized
 * name (lowercased, underscores stripped, so snake and camel spellings meet) is equal, either
 * directly on a source or one message level down. Type agreement is required before a
 * candidate is even offered to the checker: identical field types, identical repeated shape,
 * and identical message type for message fields. Map fields and {@code Struct}/{@code Any}
 * wildcards are never suggested — a suggestion must stay verifiable.</p>
 */
public final class MappingSuggester {

    /** One proposed mapping: the target path, the source path, and the ready-to-use rule. */
    public record Candidate(String targetPath, String sourcePath, String rule, String basis) {
    }

    /** The candidate rules for one target type, best basis first within each field. */
    public record Suggestions(String targetType, List<Candidate> candidates) {

        public Suggestions {
            candidates = List.copyOf(candidates);
        }

        /** The suggested rules alone, in candidate order. */
        public List<String> rules() {
            return candidates.stream().map(Candidate::rule).toList();
        }
    }

    private MappingSuggester() {
    }

    /**
     * Suggests mappings from {@code sources} (scope name to type) onto {@code target}'s
     * top-level fields.
     */
    public static Suggestions suggest(Map<String, Descriptor> sources, Descriptor target) {
        Objects.requireNonNull(sources, "sources");
        Objects.requireNonNull(target, "target");
        List<Candidate> candidates = new ArrayList<>();
        for (FieldDescriptor field : target.getFields()) {
            candidates.addAll(candidatesFor(sources, field));
        }
        candidates.sort(Comparator.comparing(Candidate::targetPath)
                .thenComparingInt(candidate -> rank(candidate.basis()))
                .thenComparing(Candidate::sourcePath));

        // The checker speaks last: nothing leaves here that the chain verifier would reject.
        RuleChecker checker = new RuleChecker();
        List<String> rules = new ArrayList<>(
                candidates.stream().map(Candidate::rule).toList());
        Set<String> rejected = new HashSet<>();
        for (RuleChecker.Finding finding
                : checker.checkScoped(sources, target, rules, List.of(), List.of())) {
            rejected.add(finding.rule());
        }
        List<Candidate> verified = candidates.stream()
                .filter(candidate -> !rejected.contains(candidate.rule()))
                .toList();
        return new Suggestions(target.getFullName(), verified);
    }

    private static List<Candidate> candidatesFor(Map<String, Descriptor> sources,
                                                 FieldDescriptor target) {
        List<Candidate> candidates = new ArrayList<>();
        String wanted = normalize(target.getName());
        for (Map.Entry<String, Descriptor> source : sources.entrySet()) {
            String scope = source.getKey();
            for (FieldDescriptor field : source.getValue().getFields()) {
                if (normalize(field.getName()).equals(wanted) && compatible(field, target)) {
                    String basis = field.getName().equals(target.getName())
                            ? "exact name" : "normalized name";
                    candidates.add(new Candidate(target.getName(),
                            scope + "." + field.getName(),
                            target.getName() + "=" + scope + "." + field.getName(), basis));
                }
                candidates.addAll(nested(scope, field, target, wanted, new HashSet<>()));
            }
        }
        return candidates;
    }

    /** One message level down: {@code scope.holder.field} matching the target's name. */
    private static List<Candidate> nested(String scope, FieldDescriptor holder,
                                          FieldDescriptor target, String wanted,
                                          Set<String> visiting) {
        if (holder.isRepeated() || holder.isMapField()
                || holder.getJavaType() != FieldDescriptor.JavaType.MESSAGE
                || !visiting.add(holder.getMessageType().getFullName())) {
            return List.of();
        }
        List<Candidate> candidates = new ArrayList<>();
        for (FieldDescriptor field : holder.getMessageType().getFields()) {
            if (normalize(field.getName()).equals(wanted) && compatible(field, target)) {
                String path = scope + "." + holder.getName() + "." + field.getName();
                candidates.add(new Candidate(target.getName(), path,
                        target.getName() + "=" + path, "nested name"));
            }
        }
        return candidates;
    }

    /**
     * Conservative type agreement: identical field type and repeated shape, and for message
     * fields the identical message type. Wildcard targets ({@code Struct}, {@code Any}) and
     * map fields are excluded — absorbing anything is the opposite of a grounded suggestion.
     */
    private static boolean compatible(FieldDescriptor source, FieldDescriptor target) {
        if (source.isMapField() || target.isMapField()
                || source.isRepeated() != target.isRepeated()
                || source.getType() != target.getType()) {
            return false;
        }
        if (source.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
            String type = target.getMessageType().getFullName();
            if (type.equals("google.protobuf.Struct") || type.equals("google.protobuf.Any")) {
                return false;
            }
            return source.getMessageType().getFullName().equals(type);
        }
        return true;
    }

    /** Exact spelling beats a normalized spelling beats a nested find. */
    private static int rank(String basis) {
        return switch (basis) {
            case "exact name" -> 0;
            case "normalized name" -> 1;
            default -> 2;
        };
    }

    /** Case- and underscore-insensitive spelling: snake_case and camelCase meet here. */
    private static String normalize(String fieldName) {
        return fieldName.replace("_", "").toLowerCase(java.util.Locale.ROOT);
    }
}
