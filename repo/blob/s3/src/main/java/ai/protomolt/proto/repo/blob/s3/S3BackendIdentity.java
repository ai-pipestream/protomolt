package ai.protomolt.proto.repo.blob.s3;

import ai.protomolt.proto.repo.blob.spi.BackendIdentity;
import java.net.URI;
import java.util.Locale;
import java.util.Map;

/** S3 physical location identity, independent of credentials and client lifetime. */
public final class S3BackendIdentity {
    public static final String SDK_DEFAULT = "SDK_DEFAULT";
    private S3BackendIdentity() {}

    public static BackendIdentity of(String endpoint, String region, boolean pathStyle) {
        if (!isValidRegion(region))
            throw new IllegalArgumentException("Invalid S3 identity region");
        if (!SDK_DEFAULT.equals(endpoint)) {
            URI uri;
            try { uri = URI.create(endpoint); }
            catch (RuntimeException invalid) { throw new IllegalArgumentException("Invalid S3 identity endpoint"); }
            if (!("http".equals(uri.getScheme()) || "https".equals(uri.getScheme()))
                    || uri.getHost() == null || uri.getUserInfo() != null || uri.getQuery() != null
                    || uri.getFragment() != null || (uri.getRawPath() != null
                    && !uri.getRawPath().isEmpty() && !"/".equals(uri.getRawPath())))
                throw new IllegalArgumentException("S3 identity endpoint must be an HTTP origin without credentials, query or fragment");
            int port = uri.getPort();
            if (port > 65535) throw new IllegalArgumentException("Invalid S3 identity endpoint port");
            if (("https".equals(uri.getScheme()) && port == 443) || ("http".equals(uri.getScheme()) && port == 80)) port = -1;
            endpoint = uri.getScheme() + "://" + uri.getHost().toLowerCase(Locale.ROOT) + (port == -1 ? "" : ":" + port);
        }
        return new BackendIdentity("s3", "s3/v1", Map.of("endpoint", endpoint, "region", region,
                "path-style", Boolean.toString(pathStyle)));
    }

    private static boolean isValidRegion(String region) {
        if (region == null || region.isEmpty() || region.length() > 128) return false;
        if (!isAsciiAlphanumeric(region.charAt(0))) return false;
        for (int i = 1; i < region.length(); i++) {
            char c = region.charAt(i);
            if (!isAsciiAlphanumeric(c) && c != '.' && c != '_' && c != ':' && c != '-') {
                return false;
            }
        }
        return true;
    }

    private static boolean isAsciiAlphanumeric(char c) {
        return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9');
    }
}
