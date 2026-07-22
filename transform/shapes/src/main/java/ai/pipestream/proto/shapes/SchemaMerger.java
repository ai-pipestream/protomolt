package ai.pipestream.proto.shapes;

import com.google.protobuf.Descriptors.FieldDescriptor;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Merges the top-level fields of two or more message types into one new type — the
 * schema-level join/union. The flow is validate, resolve, emit:
 *
 * <ol>
 *   <li><b>Validate</b>: clash analysis is pure descriptor work, so the report is available
 *       before anything is generated. Same name + same type is {@code COALESCED} (an info
 *       entry — these are the natural join keys); same name + different type or cardinality
 *       is a hard clash.</li>
 *   <li><b>Resolve</b>: hard clashes block emission until the caller decides —
 *       {@code rename} (each source's field kept under a new name, defaulting to
 *       {@code <source>_<field>}), {@code prefer} (one source's field wins), or, for
 *       coalesced entries only, an override of the default coalescing.</li>
 *   <li><b>Emit</b>: one move produces the linked shape (proto source, descriptor set) plus
 *       its mappings — {@code SynthesizedShape.impliedRules()} is the <em>defined join</em>
 *       (one ruleset reading every source at once; for coalesced singular fields, sources
 *       later in the list overwrite earlier ones and absent values skip), and
 *       {@link MergeResult#unionRules()} is the <em>defined union</em> (one ruleset per
 *       source, mapping it alone onto the merged shape).</li>
 * </ol>
 */
public final class SchemaMerger {

    /** How two same-named fields relate across sources. */
    public enum ClashKind {
        /** Same type and cardinality — merged into one field unless overridden. */
        COALESCED,
        /** Different types; a resolution is required. */
        TYPE_CLASH,
        /** Same type, repeated on one side and singular on the other; resolution required. */
        CARDINALITY_CLASH
    }

    /** One source's contribution to a (possibly clashing) field name. */
    public record Origin(String source, FieldDescriptor field) {
        /** The proto keyword or full type name, for reports. */
        public String display() {
            return (field.isRepeated() ? "repeated " : "")
                    + ShapeSynthesizer.typeKeyword(field);
        }
    }

    public record Clash(String field, ClashKind kind, List<Origin> origins,
                        Resolution suggested) {
    }

    /**
     * A caller's decision for a clashing field name: {@code action} is {@code rename},
     * {@code prefer}, or {@code coalesce}; {@code source} names the winner for
     * {@code prefer}; {@code names} maps source names to new field names for {@code rename}
     * (missing entries default to {@code <source>_<field>}).
     */
    public record Resolution(String action, String source, Map<String, String> names) {
        public Resolution {
            Objects.requireNonNull(action, "action");
            names = names == null ? Map.of() : Map.copyOf(names);
        }
    }

    /**
     * The merge outcome. Unresolved hard clashes leave {@code shape} null — the validate
     * step's answer; a resolved merge carries the shape (whose implied rules are the join)
     * and the per-source union rules.
     */
    public record MergeResult(List<Clash> clashes,
                              ShapeSynthesizer.SynthesizedShape shape,
                              Map<String, List<String>> unionRules) {

        public boolean resolved() {
            return shape != null;
        }
    }

    private final ShapeSynthesizer synthesizer = new ShapeSynthesizer();

    public MergeResult merge(String fullName, List<ShapeSynthesizer.NamedType> sources,
                             Map<String, Resolution> resolutions) {
        if (sources.size() < 2) {
            throw new IllegalArgumentException("A merge needs at least two sources");
        }
        rejectMapFields(sources);
        Map<String, List<Origin>> byName = groupByFieldName(sources);
        List<Clash> clashes = detectClashes(byName);
        validateResolutions(resolutions, byName, clashes);
        boolean blocked = clashes.stream().anyMatch(clash ->
                clash.kind() != ClashKind.COALESCED
                        && !resolutions.containsKey(clash.field()));
        if (blocked) {
            return new MergeResult(clashes, null, null);
        }

        // Plan the merged fields: sources in order, each source's fields in declaration
        // order; a clashing name is planned once, when its first contributor is reached.
        Map<String, List<Origin>> plan = new LinkedHashMap<>();
        for (ShapeSynthesizer.NamedType source : sources) {
            for (FieldDescriptor field : source.type().getFields()) {
                planField(plan, source.name(), field, byName, resolutions);
            }
        }

        List<ShapeSynthesizer.NamedField> fields = new ArrayList<>(plan.size());
        List<String> joinRules = new ArrayList<>();
        Map<String, List<String>> unionRules = new LinkedHashMap<>();
        sources.forEach(source -> unionRules.put(source.name(), new ArrayList<>()));
        for (Map.Entry<String, List<Origin>> planned : plan.entrySet()) {
            String finalName = planned.getKey();
            FieldDescriptor typedBy = planned.getValue().get(0).field();
            fields.add(new ShapeSynthesizer.NamedField(finalName, typedBy));
            boolean first = true;
            for (Origin origin : planned.getValue()) {
                // Coalesced repeated fields accumulate; coalesced singular fields let the
                // later source overwrite (absent values skip, so it is last-non-null-wins).
                String operator = typedBy.isRepeated() && !first ? " += " : " = ";
                String read = origin.source() + "." + origin.field().getName();
                joinRules.add(finalName + operator + read);
                unionRules.get(origin.source()).add(finalName + " = " + read);
                first = false;
            }
        }
        ShapeSynthesizer.SynthesizedShape shape;
        try {
            shape = synthesizer.fromFields(fullName, fields, joinRules);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Merged shape does not build: "
                    + e.getMessage(), e);
        }
        return new MergeResult(clashes, shape, unionRules);
    }

    private static void rejectMapFields(List<ShapeSynthesizer.NamedType> sources) {
        List<String> maps = new ArrayList<>();
        for (ShapeSynthesizer.NamedType source : sources) {
            for (FieldDescriptor field : source.type().getFields()) {
                if (field.isMapField()) {
                    maps.add(source.name() + "." + field.getName());
                }
            }
        }
        if (!maps.isEmpty()) {
            throw new IllegalArgumentException(
                    "Map fields are not yet mergeable: " + maps);
        }
    }

    private static Map<String, List<Origin>> groupByFieldName(
            List<ShapeSynthesizer.NamedType> sources) {
        Map<String, List<Origin>> byName = new LinkedHashMap<>();
        for (ShapeSynthesizer.NamedType source : sources) {
            for (FieldDescriptor field : source.type().getFields()) {
                byName.computeIfAbsent(field.getName(), name -> new ArrayList<>())
                        .add(new Origin(source.name(), field));
            }
        }
        return byName;
    }

    private static List<Clash> detectClashes(Map<String, List<Origin>> byName) {
        List<Clash> clashes = new ArrayList<>();
        for (Map.Entry<String, List<Origin>> entry : byName.entrySet()) {
            List<Origin> origins = entry.getValue();
            if (origins.size() < 2) {
                continue;
            }
            ClashKind kind = classify(origins);
            Resolution suggested = kind == ClashKind.COALESCED
                    ? new Resolution("coalesce", null, Map.of())
                    : new Resolution("rename", null, defaultNames(entry.getKey(), origins));
            clashes.add(new Clash(entry.getKey(), kind, List.copyOf(origins), suggested));
        }
        return clashes;
    }

    private static ClashKind classify(List<Origin> origins) {
        FieldDescriptor first = origins.get(0).field();
        boolean sameType = origins.stream().allMatch(origin ->
                typeKey(origin.field()).equals(typeKey(first)));
        if (!sameType) {
            return ClashKind.TYPE_CLASH;
        }
        boolean sameCardinality = origins.stream()
                .allMatch(origin -> origin.field().isRepeated() == first.isRepeated());
        return sameCardinality ? ClashKind.COALESCED : ClashKind.CARDINALITY_CLASH;
    }

    private static String typeKey(FieldDescriptor field) {
        return switch (field.getJavaType()) {
            case MESSAGE -> "message:" + field.getMessageType().getFullName();
            case ENUM -> "enum:" + field.getEnumType().getFullName();
            default -> field.getType().name();
        };
    }

    private static Map<String, String> defaultNames(String fieldName, List<Origin> origins) {
        Map<String, String> names = new LinkedHashMap<>();
        for (Origin origin : origins) {
            names.put(origin.source(), origin.source() + "_" + fieldName);
        }
        return names;
    }

    private static void validateResolutions(Map<String, Resolution> resolutions,
                                            Map<String, List<Origin>> byName,
                                            List<Clash> clashes) {
        Map<String, ClashKind> kinds = new LinkedHashMap<>();
        clashes.forEach(clash -> kinds.put(clash.field(), clash.kind()));
        for (Map.Entry<String, Resolution> entry : resolutions.entrySet()) {
            String field = entry.getKey();
            Resolution resolution = entry.getValue();
            ClashKind kind = kinds.get(field);
            if (kind == null) {
                throw new IllegalArgumentException("Resolution for '" + field
                        + "' names a field that does not clash");
            }
            switch (resolution.action()) {
                case "coalesce" -> {
                    if (kind != ClashKind.COALESCED) {
                        throw new IllegalArgumentException("'" + field + "' cannot coalesce: "
                                + "the sources disagree on its type or cardinality");
                    }
                }
                case "prefer" -> {
                    if (resolution.source() == null || byName.get(field).stream()
                            .noneMatch(origin -> origin.source().equals(resolution.source()))) {
                        throw new IllegalArgumentException("'prefer' for '" + field
                                + "' needs a 'source' among the field's contributors");
                    }
                }
                case "rename" -> {
                    // Names validate when the plan is built.
                }
                default -> throw new IllegalArgumentException("Unknown resolution action '"
                        + resolution.action() + "' for '" + field
                        + "'; use rename, prefer, or coalesce");
            }
        }
    }

    private static void planField(Map<String, List<Origin>> plan, String sourceName,
                                  FieldDescriptor field, Map<String, List<Origin>> byName,
                                  Map<String, Resolution> resolutions) {
        String name = field.getName();
        List<Origin> origins = byName.get(name);
        if (origins.size() == 1) {
            reserve(plan, name, List.of(origins.get(0)));
            return;
        }
        Resolution resolution = resolutions.get(name);
        String action = resolution != null
                ? resolution.action()
                : (classify(origins) == ClashKind.COALESCED ? "coalesce" : "rename");
        switch (action) {
            case "coalesce", "prefer" -> {
                if (plan.values().stream().flatMap(List::stream)
                        .anyMatch(origin -> origin.field().getName().equals(name))) {
                    return; // already planned when the first contributor was reached
                }
                List<Origin> contributors = action.equals("coalesce")
                        ? origins
                        : origins.stream()
                                .filter(origin -> origin.source().equals(resolution.source()))
                                .toList();
                reserve(plan, name, contributors);
            }
            case "rename" -> {
                String renamed = resolution != null
                        && resolution.names().containsKey(sourceName)
                        ? resolution.names().get(sourceName)
                        : sourceName + "_" + name;
                reserve(plan, renamed, List.of(new Origin(sourceName, field)));
            }
            default -> throw new IllegalStateException("unreachable: " + action);
        }
    }

    private static void reserve(Map<String, List<Origin>> plan, String name,
                                List<Origin> origins) {
        if (plan.putIfAbsent(name, origins) != null) {
            throw new IllegalArgumentException("Merged field name '" + name
                    + "' collides; pick different rename targets");
        }
    }
}
