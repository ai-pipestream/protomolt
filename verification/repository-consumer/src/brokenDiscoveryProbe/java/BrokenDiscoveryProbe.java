import ai.protomolt.proto.repo.blob.spi.BlobStores;
import java.util.Map;
import java.util.ServiceConfigurationError;

/**
 * Probe launched against deliberately corrupted packaging fixtures (never
 * against real publications). Modes:
 * <ul>
 *   <li>{@code missing} — provider jar repackaged without META-INF/services:
 *       discovery must find no provider and selection must fail explicitly.</li>
 *   <li>{@code wrong-class} — service entry naming a class that does not exist:
 *       discovery must surface the ServiceLoader error, not skip silently.</li>
 * </ul>
 */
public final class BrokenDiscoveryProbe {
    public static void main(String[] args) {
        String mode = args.length == 1 ? args[0] : throwUsage();
        try {
            var providers = BlobStores.discover();
            if (!mode.equals("missing")) {
                throw new AssertionError("corrupt service registration must surface a ServiceConfigurationError, got "
                        + providers.providerIds());
            }
            if (providers.providerIds().contains("redis")) {
                throw new AssertionError("a jar without service registration must not yield a provider: "
                        + providers.providerIds());
            }
            try {
                providers.open("redis", Map.of());
                throw new AssertionError("an unregistered provider must not open");
            } catch (IllegalArgumentException expected) {
                if (!expected.getMessage().contains("not installed")) {
                    throw new AssertionError("unexpected refusal message: " + expected.getMessage());
                }
            }
            System.out.println("BROKEN-DISCOVERY-PROBE OK: missing service registration detected");
        } catch (ServiceConfigurationError corrupt) {
            if (!mode.equals("wrong-class")) {
                throw new AssertionError("unexpected ServiceConfigurationError in missing mode", corrupt);
            }
            if (corrupt.getMessage() == null || !corrupt.getMessage().contains("DeliberatelyMissingProvider")) {
                throw new AssertionError("ServiceConfigurationError must name the bogus registration", corrupt);
            }
            System.out.println("BROKEN-DISCOVERY-PROBE OK: corrupt service registration detected: "
                    + corrupt.getMessage());
        }
    }

    private static String throwUsage() {
        throw new IllegalArgumentException("usage: BrokenDiscoveryProbe missing|wrong-class");
    }
}
