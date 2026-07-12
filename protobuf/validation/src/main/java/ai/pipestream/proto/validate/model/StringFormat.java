package ai.pipestream.proto.validate.model;

import java.util.List;

/**
 * Well-known string formats a {@link StringConstraints} can demand. Each format
 * carries its stable violation rule id and knows how to test a value; checks are
 * purely syntactic (no DNS lookups or network access).
 */
public enum StringFormat {
    EMAIL("string.email", "value must be a valid email address") {
        @Override
        public boolean matches(String value) {
            int at = value.indexOf('@');
            if (at <= 0 || at == value.length() - 1 || value.indexOf('@', at + 1) >= 0) {
                return false;
            }
            return isHostname(value.substring(at + 1));
        }
    },
    UUID("string.uuid", "value must be a valid UUID") {
        @Override
        public boolean matches(String value) {
            if (value.length() != 36) {
                return false;
            }
            for (int i = 0; i < 36; i++) {
                char c = value.charAt(i);
                if (i == 8 || i == 13 || i == 18 || i == 23) {
                    if (c != '-') {
                        return false;
                    }
                } else if (Character.digit(c, 16) < 0) {
                    return false;
                }
            }
            return true;
        }
    },
    HOSTNAME("string.hostname", "value must be a valid hostname") {
        @Override
        public boolean matches(String value) {
            return isHostname(value);
        }
    },
    URI("string.uri", "value must be an absolute URI") {
        @Override
        public boolean matches(String value) {
            try {
                return new java.net.URI(value).isAbsolute();
            } catch (java.net.URISyntaxException e) {
                return false;
            }
        }
    },
    IP("string.ip", "value must be a valid IP address") {
        @Override
        public boolean matches(String value) {
            return IPV4.matches(value) || IPV6.matches(value);
        }
    },
    IPV4("string.ipv4", "value must be a valid IPv4 address") {
        @Override
        public boolean matches(String value) {
            return isIpv4(value);
        }
    },
    IPV6("string.ipv6", "value must be a valid IPv6 address") {
        @Override
        public boolean matches(String value) {
            return isIpv6(value);
        }
    };

    private final String ruleId;
    private final String defaultMessage;

    StringFormat(String ruleId, String defaultMessage) {
        this.ruleId = ruleId;
        this.defaultMessage = defaultMessage;
    }

    /** Stable violation rule id, e.g. {@code string.email}. */
    public String ruleId() {
        return ruleId;
    }

    public String defaultMessage() {
        return defaultMessage;
    }

    /** True when {@code value} satisfies this format. */
    public abstract boolean matches(String value);

    /** RFC 1123 hostname: dot-separated alphanumeric/hyphen labels, ≤ 253 chars. */
    private static boolean isHostname(String value) {
        if (value.isEmpty() || value.length() > 253) {
            return false;
        }
        String host = value.endsWith(".") ? value.substring(0, value.length() - 1) : value;
        String[] labels = host.split("\\.", -1);
        for (String label : labels) {
            if (label.isEmpty() || label.length() > 63
                    || label.charAt(0) == '-' || label.charAt(label.length() - 1) == '-') {
                return false;
            }
            for (int i = 0; i < label.length(); i++) {
                char c = label.charAt(i);
                if (!Character.isLetterOrDigit(c) || c > 0x7f) {
                    if (c != '-') {
                        return false;
                    }
                }
            }
        }
        // The final label must not be purely numeric (it would parse as an IP).
        return !labels[labels.length - 1].chars().allMatch(Character::isDigit);
    }

    private static boolean isIpv4(String value) {
        String[] octets = value.split("\\.", -1);
        if (octets.length != 4) {
            return false;
        }
        for (String octet : octets) {
            if (octet.isEmpty() || octet.length() > 3) {
                return false;
            }
            if (octet.length() > 1 && octet.charAt(0) == '0') {
                return false;
            }
            int n = 0;
            for (int i = 0; i < octet.length(); i++) {
                char c = octet.charAt(i);
                if (c < '0' || c > '9') {
                    return false;
                }
                n = n * 10 + (c - '0');
            }
            if (n > 255) {
                return false;
            }
        }
        return true;
    }

    private static boolean isIpv6(String value) {
        int doubleColon = value.indexOf("::");
        if (doubleColon >= 0 && value.indexOf("::", doubleColon + 1) >= 0) {
            return false;
        }
        String head = doubleColon < 0 ? value : value.substring(0, doubleColon);
        String tail = doubleColon < 0 ? "" : value.substring(doubleColon + 2);
        List<String> groups = new java.util.ArrayList<>();
        if (!head.isEmpty()) {
            groups.addAll(List.of(head.split(":", -1)));
        }
        List<String> tailGroups = tail.isEmpty() ? List.of() : List.of(tail.split(":", -1));
        groups.addAll(tailGroups);
        if (doubleColon < 0 && value.isEmpty()) {
            return false;
        }
        int units = 0;
        for (int i = 0; i < groups.size(); i++) {
            String group = groups.get(i);
            if (group.isEmpty()) {
                return false;
            }
            // A trailing IPv4 suffix (e.g. ::ffff:1.2.3.4) counts as two groups.
            if (i == groups.size() - 1 && group.indexOf('.') >= 0) {
                if (!isIpv4(group)) {
                    return false;
                }
                units += 2;
                continue;
            }
            if (group.length() > 4) {
                return false;
            }
            for (int j = 0; j < group.length(); j++) {
                if (Character.digit(group.charAt(j), 16) < 0) {
                    return false;
                }
            }
            units++;
        }
        return doubleColon < 0 ? units == 8 : units <= 7;
    }
}
