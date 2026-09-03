package ai.protomolt.proto.validate.spi;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * The validator's window onto mounted postal-code grammars: the runtime data
 * behind the {@code google.type.PostalAddress} postal-code check. Unlike the
 * taxonomy rule, nothing in the schema declares the binding — the type is the
 * binding — so an unmounted region leaves the postal code unchecked, the
 * data-free default: the pack is a per-region opt-in enhancement, never a
 * fail-closed gate. Given a mounted version the verdict is deterministic, and
 * the version rides every grammar violation as evidence.
 *
 * <p>Implementations must be thread-safe; the validator consults the catalog
 * on every PostalAddress validation.</p>
 */
public interface PostalCodeCatalog {

    /**
     * The grammar mounted for {@code regionCode}, or empty when none is.
     *
     * @param regionCode the ISO 3166-1 alpha-2 region
     * @return the mounted grammar, if any
     */
    Optional<Mounted> region(String regionCode);

    /** A catalog with nothing mounted: every postal code stays unchecked. */
    static PostalCodeCatalog empty() {
        return regionCode -> Optional.empty();
    }

    /**
     * One region's mounted grammar: postal-code masks in the UPU convention
     * ({@code N} a digit, {@code A} an uppercase letter, anything else
     * literal). A value must match at least one mask.
     *
     * @param regionCode the ISO 3166-1 alpha-2 region
     * @param version the config source's version of the pack, the evidence a
     *        grammar violation cites
     * @param masks the masks, at least one
     */
    record Mounted(String regionCode, String version, List<String> masks) {

        public Mounted {
            if (regionCode == null || regionCode.isBlank()) {
                throw new IllegalArgumentException("regionCode must not be blank");
            }
            Objects.requireNonNull(version, "version");
            masks = List.copyOf(Objects.requireNonNull(masks, "masks"));
            if (masks.isEmpty()) {
                throw new IllegalArgumentException("a mounted region needs at least one mask");
            }
        }
    }
}
