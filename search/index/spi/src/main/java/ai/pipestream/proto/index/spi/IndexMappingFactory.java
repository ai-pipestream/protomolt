package ai.pipestream.proto.index.spi;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Walks a message descriptor and builds an {@link IndexMapping} using hint sources.
 *
 * <p>Nested messages expand into dotted paths unless the hint is {@link IndexFieldKind#OBJECT}
 * / {@link IndexFieldKind#NESTED} (engines that support real nesting keep a single entry).
 */
public final class IndexMappingFactory {

    private final IndexingHintSource hints;
    private final boolean preservingProtoFieldNames;
    private final int maxDepth;

    public IndexMappingFactory(IndexingHintSource hints) {
        this(hints, true, 8);
    }

    public IndexMappingFactory(IndexingHintSource hints, boolean preservingProtoFieldNames, int maxDepth) {
        this.hints = Objects.requireNonNull(hints, "hints");
        this.preservingProtoFieldNames = preservingProtoFieldNames;
        this.maxDepth = maxDepth;
    }

    /** Catalog overrides → proto options → inference. */
    public static IndexMappingFactory defaults(CatalogIndexingHintSource catalog) {
        IndexingHintSource chain = catalog
                .orElse(new ProtoOptionsIndexingHintSource())
                .orElse(new InferringIndexingHintSource());
        return new IndexMappingFactory(chain);
    }

    public static IndexMappingFactory inferringOnly() {
        return new IndexMappingFactory(new InferringIndexingHintSource());
    }

    /**
     * The hint chain this factory resolves with, so companions resolve per-field settings
     * consistently with the mapping — {@link AnyPayloadGate} reads
     * {@code validate_payloads} opt-outs through it.
     */
    public IndexingHintSource hints() {
        return hints;
    }

    public IndexMapping create(Descriptor descriptor) {
        List<IndexMapping.IndexedField> fields = new ArrayList<>();
        walk(descriptor, "", "", 0, fields, new HashSet<>(), false);
        return new IndexMapping(descriptor.getFullName(), fields);
    }

    private void walk(
            Descriptor descriptor,
            String pathPrefix,
            String namePrefix,
            int depth,
            List<IndexMapping.IndexedField> out,
            Set<String> visiting,
            boolean underRepeated) {
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

            // A child under any repeated ancestor is multi-valued at write time (the engines
            // fan out over the elements), whatever the child's own cardinality.
            boolean childUnderRepeated = underRepeated || field.isRepeated();
            if (shouldExpand(field, hint) && depth < maxDepth) {
                // A name override on an expanded message replaces that segment's contribution to
                // every child name, which is what an override does on a leaf as well: it stands in
                // for the qualified name the walk would otherwise have built.
                walk(field.getMessageType(), path, fieldName, depth + 1, out,
                        new HashSet<>(visiting), childUnderRepeated);
                continue;
            }
            out.add(new IndexMapping.IndexedField(
                    path, fieldName, hint, field.isRepeated() || underRepeated));
            if (hint.blockRole() == BlockRole.CHUNKS && depth < maxDepth) {
                // The chunk scope keeps its container entry (block engines key on it) AND
                // expands its children into dotted paths, unlike plain NESTED which stays
                // a single entry. Child names are unprefixed: within a block the children
                // are their own documents, not properties of the parent.
                walk(field.getMessageType(), path, "", depth + 1, out,
                        new HashSet<>(visiting), childUnderRepeated);
            }
        }
        visiting.remove(descriptor.getFullName());
    }

    /** Hints that cannot possibly map surface here as mapping errors, with path context. */
    private static void validate(FieldDescriptor field, ResolvedFieldHint hint, String path) {
        if (hint.blockRole() == BlockRole.CHUNKS
                && (!field.isRepeated() || field.getJavaType() != FieldDescriptor.JavaType.MESSAGE)) {
            throw new IndexMappingException(
                    "BLOCK_ROLE_CHUNKS requires a repeated message field", path);
        }
        if (hint.chunkRecipe() != null
                && field.getJavaType() != FieldDescriptor.JavaType.STRING) {
            throw new IndexMappingException(
                    "chunk_recipe derives from text and requires a string field", path);
        }
        if (hint.type().isRange()) {
            if (field.isRepeated() || field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
                throw new IndexMappingException(
                        hint.type() + " requires a singular message field with (gte,lte) or (min,max) bounds",
                        path);
            }
            if (RangeBounds.resolve(field.getMessageType(), hint.type()).isEmpty()) {
                throw new IndexMappingException(
                        "Message " + field.getMessageType().getFullName()
                                + " declares no (gte,lte) or (min,max) pair matching " + hint.type(),
                        path);
            }
        }
        try {
            hint.missingSubstitute();
        } catch (NumberFormatException e) {
            throw new IndexMappingException(
                    "null_value '" + hint.nullValue() + "' does not parse as " + hint.type(), path);
        }
    }

    private static boolean shouldExpand(FieldDescriptor field, ResolvedFieldHint hint) {
        if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
            return false;
        }
        return switch (hint.type()) {
            case DATE, SKIP, VECTOR, BINARY, OBJECT, NESTED, ANY,
                    INT_RANGE, LONG_RANGE, FLOAT_RANGE, DOUBLE_RANGE, DATE_RANGE -> false;
            default -> true;
        };
    }
}
