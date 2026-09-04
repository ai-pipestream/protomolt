package ai.protomolt.proto.projection;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.descriptors.DescriptorIdentity;
import com.google.protobuf.Descriptors.Descriptor;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Resolves declared source type names to descriptors when a {@link MessageProjection}
 * is compiled. Every declared source must resolve. The compiled projection records
 * its exact descriptor identity and rejects same-named schema drift at execution time.
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

    /**
     * Resolves names against a fixed set of descriptors.
     *
     * @throws IllegalArgumentException when two descriptors share a fully-qualified name but
     *         have different canonical descriptor identities
     */
    static SourceResolver of(Collection<Descriptor> sources) {
        Objects.requireNonNull(sources, "sources");
        Map<String, Descriptor> byName = new LinkedHashMap<>();
        for (Descriptor source : sources) {
            Objects.requireNonNull(source, "sources contains null");
            Descriptor previous = byName.putIfAbsent(source.getFullName(), source);
            if (previous != null
                    && !DescriptorIdentity.of(previous).equals(DescriptorIdentity.of(source))) {
                throw new IllegalArgumentException("Two different descriptors were given for "
                        + source.getFullName() + "; source types must be unique by exact "
                        + "descriptor identity");
            }
        }
        return name -> Optional.ofNullable(byName.get(name));
    }
}
