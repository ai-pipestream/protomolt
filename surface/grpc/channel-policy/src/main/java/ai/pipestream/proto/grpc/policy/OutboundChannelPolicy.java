package ai.pipestream.proto.grpc.policy;

import io.grpc.ManagedChannel;

import java.time.Duration;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * Host-owned guardrail for every outbound gRPC channel.
 *
 * <p>The policy validates the target without resolving or contacting the host, checks the
 * transport and deadline, and leases one bounded channel slot until the returned channel is shut
 * down. Credential, trust, and client-certificate references are deliberately not accepted here:
 * a host must resolve those references in its own channel factory before it can open a channel.</p>
 */
public final class OutboundChannelPolicy {

    private static final Pattern DNS_LABEL = Pattern.compile("[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?");

    private final Set<String> allowedSchemes;
    private final Set<String> allowedHosts;
    private final Set<Integer> allowedPorts;
    private final boolean allowPlaintext;
    private final boolean allowTls;
    private final long maxDeadlineMillis;
    private final Semaphore channelSlots;

    private OutboundChannelPolicy(Builder builder) {
        allowedSchemes = Set.copyOf(builder.allowedSchemes);
        allowedHosts = Set.copyOf(builder.allowedHosts);
        allowedPorts = Set.copyOf(builder.allowedPorts);
        allowPlaintext = builder.allowPlaintext;
        allowTls = builder.allowTls;
        maxDeadlineMillis = builder.maxDeadline.toMillis();
        channelSlots = new Semaphore(builder.maxActiveChannels, true);
    }

    /** Returns a policy suitable for existing callers, with strict target parsing and broad hosts. */
    public static OutboundChannelPolicy defaults() {
        return builder().build();
    }

    /** Starts a policy builder with DNS/IP targets, both transports, and a 60-second deadline. */
    public static Builder builder() {
        return new Builder();
    }

    /** Validates a raw gRPC target without DNS resolution or network access. */
    public ValidatedTarget validateTarget(String rawTarget, boolean tls) {
        ParsedTarget parsed = parse(rawTarget);
        if (!allowedSchemes.contains(parsed.scheme())) {
            throw new OutboundChannelPolicyException("gRPC target scheme is not allowed: "
                    + parsed.scheme());
        }
        if (!allowedHosts.isEmpty() && allowedHosts.stream().noneMatch(
                allowed -> hostMatches(allowed, parsed.host()))) {
            throw new OutboundChannelPolicyException("gRPC target host is not allowed: "
                    + parsed.host());
        }
        if (!allowedPorts.isEmpty() && !allowedPorts.contains(parsed.port())) {
            throw new OutboundChannelPolicyException("gRPC target port is not allowed: "
                    + parsed.port());
        }
        if (tls && !allowTls) {
            throw new OutboundChannelPolicyException("TLS outbound channels are disabled by policy");
        }
        if (!tls && !allowPlaintext) {
            throw new OutboundChannelPolicyException(
                    "plaintext outbound channels are disabled by policy");
        }
        return new ValidatedTarget(parsed.original(), parsed.scheme(), parsed.host(), parsed.port(),
                parsed.canonicalTarget(), tls);
    }

    /** Validates a per-call deadline before opening a channel or issuing a call. */
    public void validateDeadline(long deadlineMillis) {
        if (deadlineMillis <= 0) {
            throw new OutboundChannelPolicyException("gRPC deadline must be positive");
        }
        if (deadlineMillis > maxDeadlineMillis) {
            throw new OutboundChannelPolicyException("gRPC deadline exceeds policy maximum of "
                    + maxDeadlineMillis + "ms");
        }
    }

    /**
     * Validates and opens a channel through the supplied host factory. The factory receives the
     * canonical validated target, never the unparsed caller string.
     */
    public ManagedChannel open(String rawTarget, boolean tls,
                               Function<String, ManagedChannel> opener) {
        if (opener == null) {
            throw new IllegalArgumentException("channel opener must not be null");
        }
        ValidatedTarget target = validateTarget(rawTarget, tls);
        if (!channelSlots.tryAcquire()) {
            throw new OutboundChannelPolicyException(
                    "outbound channel concurrency limit is exhausted");
        }
        try {
            ManagedChannel delegate = opener.apply(target.canonicalTarget());
            if (delegate == null) {
                throw new IllegalStateException("channel opener returned null");
            }
            return new LeasedChannel(delegate, channelSlots);
        } catch (RuntimeException e) {
            channelSlots.release();
            throw e;
        }
    }

    /** A parsed target after policy validation. */
    public record ValidatedTarget(String original, String scheme, String host, int port,
                                  String canonicalTarget, boolean tls) {
    }

    /** Builder for host configuration. Empty host/port allowlists mean any valid value. */
    public static final class Builder {
        private final Set<String> allowedSchemes = new HashSet<>(Set.of("dns", "ipv4", "ipv6"));
        private final Set<String> allowedHosts = new HashSet<>();
        private final Set<Integer> allowedPorts = new HashSet<>();
        private boolean allowPlaintext = true;
        private boolean allowTls = true;
        private Duration maxDeadline = Duration.ofSeconds(60);
        private int maxActiveChannels = 64;

        public Builder allowedSchemes(Set<String> schemes) {
            allowedSchemes.clear();
            if (schemes != null) {
                schemes.forEach(scheme -> allowedSchemes.add(normalizeScheme(scheme)));
            }
            return this;
        }

        public Builder allowScheme(String scheme) {
            allowedSchemes.add(normalizeScheme(scheme));
            return this;
        }

        public Builder allowedHosts(Set<String> hosts) {
            allowedHosts.clear();
            if (hosts != null) {
                hosts.forEach(host -> allowedHosts.add(normalizeHostRule(host)));
            }
            return this;
        }

        public Builder allowHost(String host) {
            allowedHosts.add(normalizeHostRule(host));
            return this;
        }

        public Builder allowedPorts(Set<Integer> ports) {
            allowedPorts.clear();
            if (ports != null) {
                ports.forEach(this::addPort);
            }
            return this;
        }

        public Builder allowPort(int port) {
            addPort(port);
            return this;
        }

        public Builder allowPlaintext(boolean allowed) {
            allowPlaintext = allowed;
            return this;
        }

        public Builder allowTls(boolean allowed) {
            allowTls = allowed;
            return this;
        }

        public Builder maxDeadline(Duration deadline) {
            if (deadline == null || deadline.isZero() || deadline.isNegative()) {
                throw new IllegalArgumentException("max deadline must be positive");
            }
            try {
                if (deadline.toMillis() <= 0) {
                    throw new IllegalArgumentException("max deadline must be at least 1ms");
                }
            } catch (ArithmeticException e) {
                throw new IllegalArgumentException("max deadline is too large", e);
            }
            maxDeadline = deadline;
            return this;
        }

        public Builder maxActiveChannels(int count) {
            if (count <= 0) {
                throw new IllegalArgumentException("max active channels must be positive");
            }
            maxActiveChannels = count;
            return this;
        }

        public OutboundChannelPolicy build() {
            if (allowedSchemes.isEmpty()) {
                throw new IllegalArgumentException("at least one target scheme is required");
            }
            return new OutboundChannelPolicy(this);
        }

        private void addPort(int port) {
            if (port < 1 || port > 65535) {
                throw new IllegalArgumentException("allowed port must be between 1 and 65535");
            }
            allowedPorts.add(port);
        }
    }

    private record ParsedTarget(String original, String scheme, String host, int port,
                                String canonicalTarget) {
    }

    private static ParsedTarget parse(String rawTarget) {
        if (rawTarget == null || rawTarget.isBlank() || !rawTarget.equals(rawTarget.trim())) {
            throw new OutboundChannelPolicyException("gRPC target must be a non-blank value without surrounding whitespace");
        }
        String value = rawTarget;
        String scheme = "dns";
        String authority = value;
        boolean explicitScheme = false;
        if (value.startsWith("dns:")) {
            scheme = "dns";
            explicitScheme = true;
            authority = value.substring(4);
            if (!authority.startsWith("///")) {
                throw new OutboundChannelPolicyException(
                        "DNS targets must use dns:///host:port form");
            }
            authority = authority.substring(3);
        } else if (value.startsWith("ipv4:")) {
            scheme = "ipv4";
            explicitScheme = true;
            authority = value.substring(5);
        } else if (value.startsWith("ipv6:")) {
            scheme = "ipv6";
            explicitScheme = true;
            authority = value.substring(5);
        } else if (value.startsWith("[")) {
            scheme = "ipv6";
        }
        if (authority.indexOf('/') >= 0 || authority.indexOf('?') >= 0
                || authority.indexOf('#') >= 0 || authority.indexOf('@') >= 0
                || authority.indexOf('%') >= 0) {
            throw new OutboundChannelPolicyException("gRPC target contains a forbidden URI component");
        }
        HostPort hostPort = splitHostPort(authority);
        if (!explicitScheme && looksLikeIpv4Literal(hostPort.host())) {
            scheme = "ipv4";
        }
        validateHost(scheme, hostPort.host());
        String canonicalHost = scheme.equals("ipv6") ? "[" + hostPort.host() + "]" : hostPort.host();
        // gRPC Java's built-in resolver is the DNS resolver; keep the validated IP
        // classification for policy allowlists while emitting a target syntax that the
        // standard ManagedChannelBuilder can resolve without a custom ipv4/ipv6 provider.
        String canonical = "dns:///" + canonicalHost + ":" + hostPort.port();
        return new ParsedTarget(rawTarget, scheme, hostPort.host(), hostPort.port(), canonical);
    }

    private static boolean looksLikeIpv4Literal(String host) {
        return host.matches("\\d+(?:\\.\\d+){3}");
    }

    private static HostPort splitHostPort(String authority) {
        if (authority.startsWith("[")) {
            int close = authority.indexOf(']');
            if (close < 0 || close + 2 > authority.length() || authority.charAt(close + 1) != ':') {
                throw new OutboundChannelPolicyException("IPv6 target must use [address]:port form");
            }
            return new HostPort(authority.substring(1, close), parsePort(authority.substring(close + 2)));
        }
        int separator = authority.lastIndexOf(':');
        if (separator <= 0 || separator == authority.length() - 1
                || authority.substring(0, separator).indexOf(':') >= 0) {
            throw new OutboundChannelPolicyException("gRPC target must use host:port form");
        }
        return new HostPort(authority.substring(0, separator),
                parsePort(authority.substring(separator + 1)));
    }

    private static int parsePort(String value) {
        try {
            int port = Integer.parseInt(value);
            if (port < 1 || port > 65535) {
                throw new NumberFormatException();
            }
            return port;
        } catch (NumberFormatException e) {
            throw new OutboundChannelPolicyException("gRPC target port must be between 1 and 65535");
        }
    }

    private static void validateHost(String scheme, String host) {
        if (host.isBlank() || host.codePoints().anyMatch(Character::isISOControl)) {
            throw new OutboundChannelPolicyException("gRPC target host is invalid");
        }
        if (scheme.equals("ipv6")) {
            if (!isValidIpv6(host)) {
                throw new OutboundChannelPolicyException("IPv6 target host is invalid");
            }
            return;
        }
        if (scheme.equals("ipv4")) {
            String[] octets = host.split("\\.", -1);
            if (octets.length != 4) {
                throw new OutboundChannelPolicyException("IPv4 target host is invalid");
            }
            for (String octet : octets) {
                try {
                    if (octet.isEmpty() || octet.length() > 3
                            || !octet.chars().allMatch(Character::isDigit)
                            || Integer.parseInt(octet) > 255) {
                        throw new NumberFormatException();
                    }
                } catch (NumberFormatException e) {
                    throw new OutboundChannelPolicyException("IPv4 target host is invalid");
                }
            }
            return;
        }
        if (host.length() > 253) {
            throw new OutboundChannelPolicyException("DNS target host is too long");
        }
        for (String label : host.split("\\.", -1)) {
            if (label.isEmpty() || !DNS_LABEL.matcher(label).matches()) {
                throw new OutboundChannelPolicyException("DNS target host is invalid");
            }
        }
    }

    private static boolean isValidIpv6(String host) {
        int compression = host.indexOf("::");
        if (compression != host.lastIndexOf("::")) {
            return false;
        }
        if (compression < 0) {
            return validIpv6Groups(host) == 8;
        }
        int left = validIpv6Groups(host.substring(0, compression));
        int right = validIpv6Groups(host.substring(compression + 2));
        return left >= 0 && right >= 0 && left + right < 8;
    }

    private static int validIpv6Groups(String section) {
        if (section.isEmpty()) {
            return 0;
        }
        String[] groups = section.split(":", -1);
        for (String group : groups) {
            if (group.isEmpty() || group.length() > 4
                    || !group.chars().allMatch(value -> Character.digit(value, 16) >= 0)) {
                return -1;
            }
        }
        return groups.length;
    }

    private static boolean hostMatches(String rule, String host) {
        host = host.toLowerCase(Locale.ROOT);
        if (rule.startsWith("*.")) {
            return host.endsWith(rule.substring(1)) && !host.equals(rule.substring(2));
        }
        return rule.equals(host);
    }

    private static String normalizeScheme(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("target scheme must not be blank");
        }
        return value.toLowerCase(Locale.ROOT);
    }

    private static String normalizeHostRule(String value) {
        if (value == null || value.isBlank() || value.codePoints().anyMatch(Character::isWhitespace)) {
            throw new IllegalArgumentException("allowed host must be a non-blank host");
        }
        String normalized = value.toLowerCase(Locale.ROOT);
        if (normalized.startsWith("*.") && normalized.length() == 2) {
            throw new IllegalArgumentException("allowed host wildcard must name a suffix");
        }
        return normalized;
    }

    private record HostPort(String host, int port) {
    }

    private static final class LeasedChannel extends ManagedChannel {
        private final ManagedChannel delegate;
        private final Semaphore slots;
        private final AtomicBoolean released = new AtomicBoolean();

        private LeasedChannel(ManagedChannel delegate, Semaphore slots) {
            this.delegate = delegate;
            this.slots = slots;
        }

        @Override
        public <RequestT, ResponseT> io.grpc.ClientCall<RequestT, ResponseT> newCall(
                io.grpc.MethodDescriptor<RequestT, ResponseT> methodDescriptor,
                io.grpc.CallOptions callOptions) {
            return delegate.newCall(methodDescriptor, callOptions);
        }

        @Override
        public String authority() {
            return delegate.authority();
        }

        @Override
        public io.grpc.ConnectivityState getState(boolean requestConnection) {
            return delegate.getState(requestConnection);
        }

        @Override
        public void notifyWhenStateChanged(io.grpc.ConnectivityState source,
                                           Runnable callback) {
            delegate.notifyWhenStateChanged(source, callback);
        }

        @Override
        public void resetConnectBackoff() {
            delegate.resetConnectBackoff();
        }

        @Override
        public void enterIdle() {
            delegate.enterIdle();
        }

        @Override
        public ManagedChannel shutdown() {
            release();
            delegate.shutdown();
            return this;
        }

        @Override
        public boolean isShutdown() {
            return delegate.isShutdown();
        }

        @Override
        public ManagedChannel shutdownNow() {
            release();
            delegate.shutdownNow();
            return this;
        }

        @Override
        public boolean isTerminated() {
            boolean terminated = delegate.isTerminated();
            if (terminated) {
                release();
            }
            return terminated;
        }

        @Override
        public boolean awaitTermination(long timeout, java.util.concurrent.TimeUnit unit)
                throws InterruptedException {
            boolean terminated = delegate.awaitTermination(timeout, unit);
            if (terminated) {
                release();
            }
            return terminated;
        }

        private void release() {
            if (released.compareAndSet(false, true)) {
                slots.release();
            }
        }
    }
}
