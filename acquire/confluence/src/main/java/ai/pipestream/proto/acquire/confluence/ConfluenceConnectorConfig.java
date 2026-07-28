package ai.pipestream.proto.acquire.confluence;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Everything the Confluence crawler needs from the outside world, in one value
 * object - no framework configuration binding. Production uses
 * {@link #fromEnvironment()}; tests use {@link #builder()}. The API token is
 * never included in {@link #toString()}.
 *
 * @param baseUrl the Confluence Cloud base URL including the {@code /wiki}
 *        suffix ({@code CONFLUENCE_BASE_URL}, e.g.
 *        {@code https://pipestreamai.atlassian.net/wiki}); required
 * @param email the Atlassian account email for basic auth
 *        ({@code CONFLUENCE_EMAIL}, or the {@code CONFLUENCE_USER} alias;
 *        the canonical name wins when both are set); required
 * @param apiToken the Atlassian API token for basic auth
 *        ({@code CONFLUENCE_API_TOKEN}, or the {@code CONFLUENCE_TOKEN} alias;
 *        the canonical name wins when both are set); required, redacted
 *        everywhere
 * @param spaces space keys to crawl ({@code CONFLUENCE_SPACES},
 *        comma-separated); empty = every space the credentials can see
 * @param pageSize the page size for list endpoints
 *        ({@code CONFLUENCE_PAGE_SIZE}, default 100, capped at the API's 250)
 * @param bodyFormat the body representation to fetch for pages and blog posts
 *        ({@code CONFLUENCE_BODY_FORMAT}, default {@code "storage"}; the v2
 *        list endpoints accept {@code storage} or {@code atlas_doc_format})
 */
public record ConfluenceConnectorConfig(
        String baseUrl,
        String email,
        String apiToken,
        List<String> spaces,
        int pageSize,
        String bodyFormat) {

    /** Environment variable for the Confluence Cloud base URL (with /wiki). */
    public static final String ENV_BASE_URL = "CONFLUENCE_BASE_URL";
    /** Environment variable for the basic-auth account email. */
    public static final String ENV_EMAIL = "CONFLUENCE_EMAIL";
    /** Alias for {@link #ENV_EMAIL}; used only when the canonical name is unset. */
    public static final String ENV_EMAIL_ALIAS = "CONFLUENCE_USER";
    /** Environment variable for the basic-auth API token. */
    public static final String ENV_API_TOKEN = "CONFLUENCE_API_TOKEN";
    /** Alias for {@link #ENV_API_TOKEN}; used only when the canonical name is unset. */
    public static final String ENV_API_TOKEN_ALIAS = "CONFLUENCE_TOKEN";
    /** Environment variable for the space-key allowlist (comma-separated). */
    public static final String ENV_SPACES = "CONFLUENCE_SPACES";
    /** Environment variable for the list-endpoint page size. */
    public static final String ENV_PAGE_SIZE = "CONFLUENCE_PAGE_SIZE";
    /** Environment variable for the body representation to fetch. */
    public static final String ENV_BODY_FORMAT = "CONFLUENCE_BODY_FORMAT";

    /** Default page size for list endpoints. */
    public static final int DEFAULT_PAGE_SIZE = 100;
    /** The v2 list endpoints cap page size at 250. */
    public static final int MAX_PAGE_SIZE = 250;
    /** Default body representation: Confluence storage format (XHTML). */
    public static final String DEFAULT_BODY_FORMAT = "storage";

    public ConfluenceConnectorConfig {
        if (baseUrl == null || baseUrl.isBlank()) {
            throw new IllegalArgumentException(ENV_BASE_URL + " is required"
                    + " (e.g. https://example.atlassian.net/wiki)");
        }
        baseUrl = baseUrl.trim().replaceAll("/+$", "");
        if (email == null || email.isBlank()) {
            throw new IllegalArgumentException(ENV_EMAIL + " is required");
        }
        email = email.trim();
        if (apiToken == null || apiToken.isBlank()) {
            throw new IllegalArgumentException(ENV_API_TOKEN + " is required");
        }
        if (spaces == null) {
            spaces = List.of();
        } else {
            spaces = List.copyOf(spaces);
        }
        if (pageSize <= 0) {
            pageSize = DEFAULT_PAGE_SIZE;
        }
        pageSize = Math.min(pageSize, MAX_PAGE_SIZE);
        if (bodyFormat == null || bodyFormat.isBlank()) {
            bodyFormat = DEFAULT_BODY_FORMAT;
        }
        bodyFormat = bodyFormat.trim().toLowerCase(Locale.ROOT);
        if (!bodyFormat.equals("storage") && !bodyFormat.equals("atlas_doc_format")) {
            throw new IllegalArgumentException(ENV_BODY_FORMAT
                    + " must be storage or atlas_doc_format (got \"" + bodyFormat + "\")");
        }
    }

    /**
     * Whether the crawl is restricted to a space-key allowlist.
     *
     * @return true when specific space keys are configured
     */
    public boolean hasSpaceAllowlist() {
        return !spaces.isEmpty();
    }

    /**
     * Build the config from the process environment, using the
     * {@code CONFLUENCE_*} variables documented on this record.
     *
     * @return the resolved config
     */
    public static ConfluenceConnectorConfig fromEnvironment() {
        return fromEnvironment(System.getenv());
    }

    /**
     * Build the config from an explicit environment map; production calls
     * {@link #fromEnvironment()}, tests call this. {@code CONFLUENCE_USER}
     * and {@code CONFLUENCE_TOKEN} act as aliases for the canonical
     * credential variables; the canonical names take precedence when both
     * forms are set.
     *
     * @param env the environment to read
     * @return the resolved config
     */
    static ConfluenceConnectorConfig fromEnvironment(Map<String, String> env) {
        return builder()
                .baseUrl(env.get(ENV_BASE_URL))
                .email(firstNonBlank(env.get(ENV_EMAIL), env.get(ENV_EMAIL_ALIAS)))
                .apiToken(firstNonBlank(env.get(ENV_API_TOKEN), env.get(ENV_API_TOKEN_ALIAS)))
                .spaces(parseSpaces(env.get(ENV_SPACES)))
                .pageSize(parseIntOrDefault(env.get(ENV_PAGE_SIZE), DEFAULT_PAGE_SIZE))
                .bodyFormat(env.get(ENV_BODY_FORMAT))
                .build();
    }

    public static Builder builder() {
        return new Builder();
    }

    private static List<String> parseSpaces(String value) {
        if (value == null || value.isBlank()) {
            return List.of();
        }
        return java.util.Arrays.stream(value.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toList();
    }

    private static String firstNonBlank(String canonical, String alias) {
        return canonical == null || canonical.isBlank() ? alias : canonical;
    }

    private static int parseIntOrDefault(String value, int fallback) {
        if (value == null || value.isBlank()) {
            return fallback;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    /** Secrets stay out of any log line this record lands in. */
    @Override
    public String toString() {
        return "ConfluenceConnectorConfig{baseUrl=" + baseUrl
                + ", email=" + email
                + ", apiToken=***"
                + ", spaces=" + spaces
                + ", pageSize=" + pageSize
                + ", bodyFormat=" + bodyFormat + "}";
    }

    /** Test-friendly builder; every field the record validates is optional here. */
    public static final class Builder {
        private String baseUrl;
        private String email;
        private String apiToken;
        private List<String> spaces = List.of();
        private int pageSize = DEFAULT_PAGE_SIZE;
        private String bodyFormat = DEFAULT_BODY_FORMAT;

        private Builder() {
        }

        public Builder baseUrl(String baseUrl) {
            this.baseUrl = baseUrl;
            return this;
        }

        public Builder email(String email) {
            this.email = email;
            return this;
        }

        public Builder apiToken(String apiToken) {
            this.apiToken = apiToken;
            return this;
        }

        public Builder spaces(List<String> spaces) {
            this.spaces = Objects.requireNonNullElse(spaces, List.of());
            return this;
        }

        public Builder spaces(String... spaces) {
            this.spaces = List.of(spaces);
            return this;
        }

        public Builder pageSize(int pageSize) {
            this.pageSize = pageSize;
            return this;
        }

        public Builder bodyFormat(String bodyFormat) {
            this.bodyFormat = bodyFormat;
            return this;
        }

        public ConfluenceConnectorConfig build() {
            return new ConfluenceConnectorConfig(baseUrl, email, apiToken, spaces, pageSize,
                    bodyFormat);
        }
    }
}
