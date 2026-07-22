package ai.pipestream.proto.index.spi;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Walks a message descriptor and builds an {@link IndexingPlan} using hint sources.
 *
 * <p>Nested messages expand into dotted paths unless the hint is {@link IndexFieldKind#OBJECT}
 * / {@link IndexFieldKind#NESTED} (engines that support real nesting keep a single entry).
 */
public final class IndexingPlanFactory {

    private final IndexingHintSource hints;
    private final boolean preservingProtoFieldNames;
    private final int maxDepth;

    public IndexingPlanFactory(IndexingHintSource hints) {
        this(hints, true, 8);
    }

    public IndexingPlanFactory(IndexingHintSource hints, boolean preservingProtoFieldNames, int maxDepth) {
        this.hints = Objects.requireNonNull(hints, "hints");
        this.preservingProtoFieldNames = preservingProtoFieldNames;
        this.maxDepth = maxDepth;
    }

    /** Catalog overrides → proto options → inference. */
    public static IndexingPlanFactory defaults(CatalogIndexingHintSource catalog) {
        IndexingHintSource chain = catalog
                .orElse(new ProtoOptionsIndexingHintSource())
                .orElse(new InferringIndexingHintSource());
        return new IndexingPlanFactory(chain);
    }

    public static IndexingPlanFactory inferringOnly() {
        return new IndexingPlanFactory(new InferringIndexingHintSource());
    }

    public IndexingPlan create(Descriptor descriptor) {
        List<IndexingPlan.IndexedField> fields = new ArrayList<>();
        walk(descriptor, "", "", 0, fields, new HashSet<>());
        return new IndexingPlan(descriptor.getFullName(), fields);
    }

    private void walk(
            Descriptor descriptor,
            String pathPrefix,
            String namePrefix,
            int depth,
            List<IndexingPlan.IndexedField> out,
            Set<String> visiting) {
        if (depth > maxDepth || !visiting.add(descriptor.getFullName())) {
            return;
        }
        for (FieldDescriptor field : descriptor.getFields()) {
            ResolvedFieldHint hint = hints.resolve(field)
                    .orElseGet(() -> InferringIndexingHintSource.infer(field));
            if (hint.type() == IndexFieldKind.UNSPECIFIED) {
                // Merge: take the type from inference but keep the hint's explicit attributes.
                hint = hint.toBuilder().type(InferringIndexingHintSource.infer(field).type()).build();
            }
            // Paths always use proto field names (the field-mapper vocabulary); engine field
            // names use one naming mode for every segment, prefix and leaf alike.
            String segment = preservingProtoFieldNames ? field.getName() : field.getJsonName();
            String path = pathPrefix.isEmpty() ? field.getName() : pathPrefix + "." + field.getName();
            validate(field, hint, path);
            String qualified = namePrefix.isEmpty() ? segment : namePrefix + "_" + segment;
            String fieldName = hint.nameOverride().orElse(qualified);

            if (shouldExpand(field, hint) && depth < maxDepth) {
                // A name override on an expanded message replaces that segment's contribution to
                // every child name, which is what an override does on a leaf as well: it stands in
                // for the qualified name the walk would otherwise have built.
                walk(field.getMessageType(), path, fieldName, depth + 1, out, new HashSet<>(visiting));
                continue;
            }
            out.add(new IndexingPlan.IndexedField(path, fieldName, hint, field.isRepeated()));
        }
        visiting.remove(descriptor.getFullName());
    }

    /** Hints that cannot possibly map surface here as planning errors, with path context. */
    private static void validate(FieldDescriptor field, ResolvedFieldHint hint, String path) {
        if (hint.type().isRange()) {
            if (field.isRepeated() || field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
                throw new IndexingPlanException(
                        hint.type() + " requires a singular message field with (gte,lte) or (min,max) bounds",
                        path);
            }
            if (RangeBounds.resolve(field.getMessageType(), hint.type()).isEmpty()) {
                throw new IndexingPlanException(
                        "Message " + field.getMessageType().getFullName()
                                + " declares no (gte,lte) or (min,max) pair matching " + hint.type(),
                        path);
            }
        }
        try {
            hint.missingSubstitute();
        } catch (NumberFormatException e) {
            throw new IndexingPlanException(
                    "null_value '" + hint.nullValue() + "' does not parse as " + hint.type(), path);
        }
    }

    private static boolean shouldExpand(FieldDescriptor field, ResolvedFieldHint hint) {
        if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
            return false;
        }
        return switch (hint.type()) {
            case DATE, SKIP, VECTOR, BINARY, OBJECT, NESTED,
                    INT_RANGE, LONG_RANGE, FLOAT_RANGE, DOUBLE_RANGE, DATE_RANGE -> false;
            default -> true;
        };
    }
}
