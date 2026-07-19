package ai.pipestream.proto.projection;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import com.google.protobuf.Descriptors.Descriptor;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Resolves declared source type names to descriptors. Used only for eager
 * validation when a {@link MessageProjection} is built; at projection time the
 * descriptor is taken from the source message itself, so an unresolvable name
 * is not fatal — it only means CEL rules cannot be pre-checked against that type.
 */
@FunctionalInterface
public interface SourceResolver {

    /**
     * Resolves a fully-qualified message name.
     *
     * @param fullName e.g. {@code "acme.court.v1.Case"}
     * @return the descriptor, or empty when the type is not available
     */
    Optional<Descriptor> resolve(String fullName);

    /** Resolves names against a {@link DescriptorRegistry}. */
    static SourceResolver of(DescriptorRegistry registry) {
        Objects.requireNonNull(registry, "registry");
        return name -> Optional.ofNullable(registry.findDescriptorByFullName(name));
    }

    /** Resolves names against a fixed set of descriptors. */
    static SourceResolver of(Descriptor... sources) {
        return of(java.util.List.of(sources));
    }

    /** Resolves names against a fixed set of descriptors. */
    static SourceResolver of(Collection<Descriptor> sources) {
        Map<String, Descriptor> byName = sources.stream()
                .collect(Collectors.toMap(Descriptor::getFullName, Function.identity()));
        return name -> Optional.ofNullable(byName.get(name));
    }
}
