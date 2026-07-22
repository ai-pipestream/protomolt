/**
 * RFC-accurate validators for the common string formats, with no runtime dependencies.
 *
 * <p>{@link Formats} is the entry point: a facade exposing the validators under
 * the names protovalidate's CEL standard library uses, so a value that passes
 * here passes there. The individual validators are also public —
 * {@link Hostnames} for RFC 1034 hostnames, {@link Emails} for the WHATWG
 * email-address production, {@link IpAddresses} for IPv4/IPv6 addresses and
 * CIDR prefixes, {@link Rfc3986} for URIs and URI references,
 * {@link HostAndPort} for {@code host:port} authorities, and
 * {@link Identifiers} for UUID, ULID and protobuf fully-qualified names.</p>
 *
 * <p>Every check is purely syntactic: no DNS resolution, no network access, no
 * normalization. The validators are implemented by direct character scanning
 * rather than regular expressions, so they run in linear time and carry no
 * catastrophic-backtracking risk. The module depends on nothing at runtime —
 * not protobuf — and is usable on its own; ProtoMolt's validation engine calls
 * into it for the format rules.</p>
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/validation.md">Validation
 * guide</a>.</p>
 */
package ai.pipestream.format;
