package ai.protomolt.proto.acquire.confluence;

import com.fasterxml.jackson.databind.JsonNode;

import java.net.URI;
import java.util.Objects;

import static ai.protomolt.proto.acquire.confluence.ConfluenceJsonFields.text;

/**
 * Link composition for one Confluence tenant: server-relative paths from the
 * REST JSON become absolute {@code web_url} and {@code download_url} values
 * against the tenant base URL.
 */
final class ConfluenceLinks {

    private final String baseUrl;
    private final String basePath;
    private final String origin;

    /**
     * @param baseUrl the tenant base URL including {@code /wiki}; trailing
     *        slashes are dropped
     */
    ConfluenceLinks(String baseUrl) {
        this.baseUrl = stripTrailingSlashes(Objects.requireNonNull(baseUrl, "baseUrl"));
        URI uri = URI.create(this.baseUrl);
        this.basePath = uri.getPath() == null ? "" : uri.getPath();
        this.origin = uri.getScheme() + "://" + uri.getAuthority();
    }

    /**
     * Composes an absolute URL from a server-relative path. Confluence Cloud
     * returns {@code webui} links without the {@code /wiki} prefix and
     * {@code _links.next} with it, so both forms are handled: paths already
     * carrying the base path resolve against the origin, bare paths against
     * the full base URL. Values starting with {@code http} pass through, and
     * blank input yields the empty string.
     */
    String absolute(String url) {
        if (url == null || url.isBlank()) {
            return "";
        }
        if (url.startsWith("http")) {
            return url;
        }
        String path = url.startsWith("/") ? url : "/" + url;
        if (!basePath.isEmpty() && path.startsWith(basePath + "/")) {
            return origin + path;
        }
        return baseUrl + path;
    }

    /** The server-relative {@code _links.webui} path, or the empty string. */
    static String webui(JsonNode node) {
        return text(node.path("_links"), "webui");
    }

    private static String stripTrailingSlashes(String value) {
        int end = value.length();
        while (end > 0 && value.charAt(end - 1) == '/') {
            end--;
        }
        return value.substring(0, end);
    }
}
