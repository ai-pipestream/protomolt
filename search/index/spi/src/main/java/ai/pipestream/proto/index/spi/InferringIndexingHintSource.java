package ai.pipestream.proto.index.spi;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Timestamp;

import java.util.Locale;
import java.util.Optional;

/**
 * Infers Lucene-aligned hints from the field descriptor when no explicit tag exists.
 */
public final class InferringIndexingHintSource implements IndexingHintSource {

    @Override
    public Optional<ResolvedFieldHint> resolve(FieldDescriptor field) {
        return Optional.of(infer(field));
    }

    public static ResolvedFieldHint infer(FieldDescriptor field) {
        String name = field.getName().toLowerCase(Locale.ROOT);
        return switch (field.getJavaType()) {
            case STRING -> looksLikeKeyword(name)
                    ? ResolvedFieldHint.of(IndexFieldKind.KEYWORD)
                    : ResolvedFieldHint.of(IndexFieldKind.TEXT);
            case BOOLEAN -> ResolvedFieldHint.of(IndexFieldKind.BOOLEAN);
            case INT -> ResolvedFieldHint.of(IndexFieldKind.INT32);
            case LONG -> ResolvedFieldHint.of(IndexFieldKind.INT64);
            case FLOAT -> ResolvedFieldHint.of(IndexFieldKind.FLOAT);
            case DOUBLE -> ResolvedFieldHint.of(IndexFieldKind.DOUBLE);
            case BYTE_STRING -> ResolvedFieldHint.of(IndexFieldKind.BINARY);
            case ENUM -> ResolvedFieldHint.of(IndexFieldKind.KEYWORD);
            case MESSAGE -> inferMessage(field);
        };
    }

    private static ResolvedFieldHint inferMessage(FieldDescriptor field) {
        Descriptor type = field.getMessageType();
        if (Timestamp.getDescriptor().getFullName().equals(type.getFullName())) {
            return ResolvedFieldHint.of(IndexFieldKind.DATE);
        }
        if ("google.protobuf.Struct".equals(type.getFullName())
                || "google.protobuf.Value".equals(type.getFullName())) {
            return ResolvedFieldHint.of(IndexFieldKind.OBJECT);
        }
        if (field.isRepeated()) {
            return ResolvedFieldHint.of(IndexFieldKind.NESTED);
        }
        return ResolvedFieldHint.of(IndexFieldKind.OBJECT);
    }

    private static boolean looksLikeKeyword(String name) {
        return name.equals("id")
                || name.endsWith("_id")
                || name.endsWith("id") && name.length() <= 4
                || name.endsWith("_key")
                || name.endsWith("_code")
                || name.equals("uri")
                || name.endsWith("_uri")
                || name.equals("status")
                || name.equals("type")
                || name.endsWith("_type");
    }
}
