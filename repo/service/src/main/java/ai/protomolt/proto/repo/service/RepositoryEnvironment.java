package ai.protomolt.proto.repo.service;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/** Environment defaults apply to omitted limits, never malformed configured values. */
final class RepositoryEnvironment {
    private final Map<String, String> values;

    RepositoryEnvironment(Map<String, String> values) { this.values = Objects.requireNonNull(values); }

    String text(String name, String fallback) {
        if (!values.containsKey(name)) return fallback;
        String value = values.get(name);
        if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " must not be blank when present");
        return value;
    }

    long number(String name, long fallback, long minimum, long maximum) {
        if (!values.containsKey(name)) return fallback;
        String value = values.get(name);
        try {
            if (value != null) {
                long parsed = Long.parseLong(value.trim());
                if (parsed >= minimum && parsed <= maximum) return parsed;
            }
        } catch (NumberFormatException invalid) {
            // Do not include raw environment values, which can be misassigned secrets.
            throw invalidNumber(name, minimum, maximum);
        }
        throw invalidNumber(name, minimum, maximum);
    }

    boolean flag(String name, boolean fallback) {
        if (!values.containsKey(name)) return fallback;
        String value = values.get(name);
        if (value != null) {
            switch (value.trim().toLowerCase(Locale.ROOT)) {
                case "true", "1", "yes", "on": return true;
                case "false", "0", "no", "off": return false;
            }
        }
        throw new IllegalArgumentException(name + " must be true/false, 1/0, yes/no or on/off");
    }

    int httpPort(String name, int fallback) {
        String value = values.get(name);
        if (value != null && value.trim().equalsIgnoreCase("off")) return 0;
        return (int) number(name, fallback, 0, 65535);
    }

    private static IllegalArgumentException invalidNumber(String name, long minimum, long maximum) {
        return new IllegalArgumentException(name + " must be an integer between " + minimum + " and " + maximum);
    }
}
