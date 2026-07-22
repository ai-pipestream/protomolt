package ai.pipestream.proto.index.spi;

import com.google.protobuf.Descriptors.FieldDescriptor;

import java.util.Optional;

/** Resolves an indexing hint for a protobuf field. */
@FunctionalInterface
public interface IndexingHintSource {
    Optional<ResolvedFieldHint> resolve(FieldDescriptor field);

    static IndexingHintSource empty() {
        return field -> Optional.empty();
    }

    default IndexingHintSource orElse(IndexingHintSource fallback) {
        return field -> resolve(field).or(() -> fallback.resolve(field));
    }
}
