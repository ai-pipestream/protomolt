package ai.pipestream.proto.index.spi;

import com.google.protobuf.Descriptors.FieldDescriptor;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Programmatic / side-car hint tags keyed by {@code messageFullName.fieldName}
 * or bare {@code fieldName}.
 */
public final class CatalogIndexingHintSource implements IndexingHintSource {

    private final Map<String, ResolvedFieldHint> byKey = new ConcurrentHashMap<>();

    public CatalogIndexingHintSource put(String key, ResolvedFieldHint hint) {
        byKey.put(key, hint);
        return this;
    }

    public CatalogIndexingHintSource put(String messageFullName, String fieldName, ResolvedFieldHint hint) {
        return put(messageFullName + "." + fieldName, hint);
    }

    @Override
    public Optional<ResolvedFieldHint> resolve(FieldDescriptor field) {
        String qualified = field.getContainingType().getFullName() + "." + field.getName();
        ResolvedFieldHint hint = byKey.get(qualified);
        if (hint == null) {
            hint = byKey.get(field.getName());
        }
        return Optional.ofNullable(hint);
    }
}
