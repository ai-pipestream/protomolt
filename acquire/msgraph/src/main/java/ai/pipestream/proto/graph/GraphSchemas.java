package ai.pipestream.proto.graph;

import ai.pipestream.proto.index.spi.IndexFieldKind;
import ai.pipestream.proto.index.spi.IndexingPlan;
import ai.pipestream.proto.index.spi.ResolvedFieldHint;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/**
 * Indexing hints to a Microsoft Graph external connection schema — the same
 * declare-once-in-the-contract story as the OpenSearch/Solr/Lucene generators, pointed at
 * Microsoft 365 Search and Copilot. TEXT fields become searchable strings, KEYWORD fields
 * exact-match queryables, sortable/facetable fields refinable (never searchable at the same
 * time — Graph forbids the combination), and repeated fields the collection types Graph
 * offers. Property names come from the plan's field name — the hint's name override when one
 * is set, the qualified path otherwise — made Graph-legal (alphanumeric, 32 chars) by
 * camel-casing at the separators, with collisions numbered.
 */
public final class GraphSchemas {

    /** The schema plus everything that could not be represented, named with a reason. */
    public record Rendered(ObjectNode schema, List<String> skipped) {
    }

    private GraphSchemas() {
    }

    public static Rendered connectionSchema(Descriptor descriptor, IndexingPlan plan) {
        Objects.requireNonNull(descriptor, "descriptor");
        Objects.requireNonNull(plan, "plan");
        ObjectNode schema = GraphClient.object();
        schema.put("baseType", "microsoft.graph.externalItem");
        ArrayNode properties = schema.putArray("properties");
        List<String> skipped = new ArrayList<>();
        Set<String> usedNames = new LinkedHashSet<>();

        for (IndexingPlan.IndexedField field : plan.fields()) {
            ResolvedFieldHint hint = field.hint();
            if (hint.type() == IndexFieldKind.SKIP) {
                continue;
            }
            boolean repeated = repeated(descriptor, field.path());
            String type = graphType(hint.type(), repeated);
            if (type == null) {
                skipped.add(field.path() + " ("
                        + (hint.type() == IndexFieldKind.OBJECT
                                || hint.type() == IndexFieldKind.NESTED
                        ? "Graph schemas are flat; hint the leaf fields or map them upstream"
                        : hint.type() + (repeated ? ", repeated" : "")
                                + " has no Graph property type") + ")");
                continue;
            }
            ObjectNode property = properties.addObject();
            property.put("name", propertyName(field.fieldName(), usedNames));
            property.put("type", type);
            boolean searchable = hint.type() == IndexFieldKind.TEXT;
            boolean refinable = !searchable && !repeated
                    && (hint.sortable() || hint.facetable());
            property.put("isSearchable", searchable);
            property.put("isQueryable", true);
            property.put("isRetrievable", hint.stored());
            property.put("isRefinable", refinable);
            if (hint.type() == IndexFieldKind.KEYWORD) {
                property.put("isExactMatchRequired", true);
            }
        }
        return new Rendered(schema, List.copyOf(skipped));
    }

    /** Graph offers no boolean/binary/vector collections; those skip with a reason. */
    private static String graphType(IndexFieldKind kind, boolean repeated) {
        String base = switch (kind) {
            case TEXT, KEYWORD -> "string";
            case INT32, INT64 -> "int64";
            case FLOAT, DOUBLE -> "double";
            case DATE -> "dateTime";
            case BOOLEAN -> repeated ? null : "boolean";
            default -> null;
        };
        if (base == null) {
            return null;
        }
        return repeated ? base + "Collection" : base;
    }

    /** Graph property names: letters and digits only, must start with a letter, max 32. */
    static String propertyName(String fieldName, Set<String> used) {
        StringBuilder name = new StringBuilder(fieldName.length());
        boolean capitalizeNext = false;
        for (char c : fieldName.toCharArray()) {
            if (Character.isLetterOrDigit(c)) {
                name.append(capitalizeNext && name.length() > 0
                        ? Character.toUpperCase(c) : c);
                capitalizeNext = false;
            } else {
                capitalizeNext = true;
            }
        }
        String candidate = name.isEmpty() ? "property" : name.toString();
        if (!Character.isLetter(candidate.charAt(0))) {
            candidate = "p" + candidate;
        }
        if (candidate.length() > 32) {
            candidate = candidate.substring(0, 32);
        }
        String unique = candidate;
        int suffix = 2;
        while (!used.add(unique.toLowerCase(Locale.ROOT))) {
            String tail = Integer.toString(suffix++);
            unique = candidate.substring(0, Math.min(candidate.length(), 32 - tail.length()))
                    + tail;
        }
        return unique;
    }

    /**
     * Whether any segment of the dotted proto path is repeated. A scalar flattened out of a
     * repeated parent message still contributes one value per parent entry, so it needs a
     * Graph collection type just as much as a repeated leaf does.
     */
    private static boolean repeated(Descriptor descriptor, String path) {
        Descriptor current = descriptor;
        for (String segment : path.split("\\.")) {
            if (current == null) {
                return false;
            }
            FieldDescriptor field = current.findFieldByName(segment);
            if (field == null) {
                return false;
            }
            if (field.isRepeated()) {
                return true;
            }
            current = field.getJavaType() == FieldDescriptor.JavaType.MESSAGE
                    ? field.getMessageType() : null;
        }
        return false;
    }
}
