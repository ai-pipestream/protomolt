package ai.protomolt.proto.http.connect;

import com.sun.net.httpserver.Headers;
import io.grpc.Metadata;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Header and trailer passing between a Connect request and the gRPC call behind it. Custom
 * request headers become call metadata; the call's response headers and trailers come back as
 * HTTP headers ({@code trailer-} prefixed for unary trailers) or, on a stream, as the
 * end-of-stream record's {@code metadata}. Keys ending in {@code -bin} carry base64 on the HTTP
 * side and bytes in the metadata.
 *
 * <p>Credentials never reach a service this way: {@code authorization}, {@code api_token} and
 * {@code cookie} are read by the gateway's caller resolver and stop there. Protocol, transport
 * and fetch-metadata headers stop at the gateway too, and {@code grpc-} keys are the transport's
 * own in both directions.
 */
final class ConnectMetadata {

    private static final Set<String> GATEWAY_ONLY = Set.of(
            "accept", "accept-encoding", "api_token", "authorization", "connection", "content-encoding",
            "content-length", "content-type", "cookie", "host", "keep-alive", "origin", "proxy-connection",
            "referer", "te", "trailer", "transfer-encoding", "upgrade", "user-agent");
    private static final String CONNECT_PREFIX = "connect-";
    private static final String FETCH_PREFIX = "sec-";
    private static final String GRPC_PREFIX = "grpc-";

    private ConnectMetadata() {
    }

    /**
     * The call metadata for a request's custom headers.
     *
     * @param headers the request headers
     * @param reserved a metadata key the gateway sets itself and a client may not send
     * @return the metadata to start the call with
     * @throws IllegalArgumentException when a forwarded header has a name gRPC metadata cannot
     *         carry, or a {@code -bin} header is not base64
     */
    static Metadata requestMetadata(Headers headers, String reserved) {
        Metadata metadata = new Metadata();
        for (Map.Entry<String, List<String>> header : headers.entrySet()) {
            String name = header.getKey().toLowerCase(Locale.ROOT);
            if (GATEWAY_ONLY.contains(name) || name.startsWith(CONNECT_PREFIX) || name.startsWith(FETCH_PREFIX)
                    || name.startsWith(GRPC_PREFIX)) {
                continue;
            }
            if (name.equals(reserved)) {
                throw new IllegalArgumentException("request header " + name + " is reserved for the gateway");
            }
            for (String value : header.getValue()) {
                if (name.endsWith(Metadata.BINARY_HEADER_SUFFIX)) {
                    metadata.put(Metadata.Key.of(name, Metadata.BINARY_BYTE_MARSHALLER), base64(name, value));
                } else {
                    metadata.put(key(name), value);
                }
            }
        }
        return metadata;
    }

    /**
     * Copies call metadata onto HTTP response headers.
     *
     * @param metadata the call's headers or trailers, or null when there are none
     * @param out the response headers
     * @param prefix prepended to each name: empty for headers, {@code trailer-} for unary trailers
     */
    static void writeHeaders(Metadata metadata, Headers out, String prefix) {
        if (metadata == null) {
            return;
        }
        for (Map.Entry<String, List<String>> entry : values(metadata).entrySet()) {
            for (String value : entry.getValue()) {
                out.add(prefix + entry.getKey(), value);
            }
        }
    }

    /**
     * The values of call metadata as the HTTP side carries them, by key, with transport keys
     * left out.
     *
     * @param metadata the metadata, or null
     * @return key to values, sorted by key
     */
    static Map<String, List<String>> values(Metadata metadata) {
        Map<String, List<String>> out = new TreeMap<>();
        if (metadata == null) {
            return out;
        }
        Base64.Encoder base64 = Base64.getEncoder().withoutPadding();
        for (String name : metadata.keys()) {
            if (name.startsWith(GRPC_PREFIX) || name.startsWith(":")) {
                continue;
            }
            if (name.endsWith(Metadata.BINARY_HEADER_SUFFIX)) {
                Iterable<byte[]> raw = metadata.getAll(Metadata.Key.of(name, Metadata.BINARY_BYTE_MARSHALLER));
                if (raw != null) {
                    for (byte[] value : raw) {
                        out.computeIfAbsent(name, k -> new ArrayList<>()).add(base64.encodeToString(value));
                    }
                }
            } else {
                Iterable<String> text = metadata.getAll(key(name));
                if (text != null) {
                    for (String value : text) {
                        out.computeIfAbsent(name, k -> new ArrayList<>()).add(value);
                    }
                }
            }
        }
        return out;
    }

    private static Metadata.Key<String> key(String name) {
        return Metadata.Key.of(name, Metadata.ASCII_STRING_MARSHALLER);
    }

    private static byte[] base64(String name, String value) {
        String trimmed = value.trim();
        try {
            return Base64.getDecoder().decode(trimmed);
        } catch (IllegalArgumentException standard) {
            try {
                return Base64.getUrlDecoder().decode(trimmed);
            } catch (IllegalArgumentException url) {
                url.addSuppressed(standard);
                throw new IllegalArgumentException("header " + name + " is not base64", url);
            }
        }
    }
}
