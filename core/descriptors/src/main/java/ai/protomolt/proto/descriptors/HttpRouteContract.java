package ai.protomolt.proto.descriptors;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * Declarative HTTP route contract served by a protomolt HTTP server, shared between live
 * request dispatch and release-baseline compatibility snapshots.
 *
 * <p>The schema registry, repository upload and intake HTTP servers each declare their routes
 * as instances of this record. The compatibility baseline reads the same instances, so a
 * route that is served is a route that is recorded.</p>
 *
 * @param routeId                 canonical route identifier
 * @param pathTemplate            URI path template; {@code {name}} segments are path parameters
 * @param httpMethods             allowed uppercase HTTP methods
 * @param pathParameters          ordered path parameter names
 * @param requiredQueryParameters required query parameter names
 * @param requestMediaType        expected request media type (or empty when bodyless)
 * @param responseMediaType       produced response media type
 * @param requestType             logical request payload contract name
 * @param responseType            logical response payload contract name
 * @param successStatusCodes      documented HTTP status codes for the endpoint
 * @param authRequired            whether authentication is required on this server configuration
 * @param stability               declared stability tier
 */
@ApiStability(StabilityTier.STABLE)
public record HttpRouteContract(
        String routeId,
        String pathTemplate,
        List<String> httpMethods,
        List<String> pathParameters,
        List<String> requiredQueryParameters,
        String requestMediaType,
        String responseMediaType,
        String requestType,
        String responseType,
        List<Integer> successStatusCodes,
        boolean authRequired,
        StabilityTier stability) {

    public HttpRouteContract {
        Objects.requireNonNull(routeId, "routeId");
        Objects.requireNonNull(pathTemplate, "pathTemplate");
        httpMethods = List.copyOf(Objects.requireNonNull(httpMethods, "httpMethods"));
        pathParameters = List.copyOf(Objects.requireNonNull(pathParameters, "pathParameters"));
        requiredQueryParameters = List.copyOf(
                Objects.requireNonNull(requiredQueryParameters, "requiredQueryParameters"));
        requestMediaType = Objects.requireNonNull(requestMediaType, "requestMediaType");
        responseMediaType = Objects.requireNonNull(responseMediaType, "responseMediaType");
        requestType = Objects.requireNonNull(requestType, "requestType");
        responseType = Objects.requireNonNull(responseType, "responseType");
        successStatusCodes = List.copyOf(
                Objects.requireNonNull(successStatusCodes, "successStatusCodes"));
        Objects.requireNonNull(stability, "stability");
    }

    /**
     * Returns whether {@code method}, upper-cased with {@link Locale#ROOT}, is one of the
     * declared methods.
     *
     * @param method request method, may be {@code null}
     * @return {@code true} when the route declares this method; {@code false} for {@code null}
     */
    public boolean allowsMethod(String method) {
        return method != null && httpMethods.contains(method.toUpperCase(Locale.ROOT));
    }

    /**
     * Returns the value of an {@code Allow} response header listing the declared methods.
     *
     * @return declared methods joined with {@code ", "}
     */
    public String allowHeader() {
        return String.join(", ", httpMethods);
    }

    /**
     * Returns whether the non-empty request path segments match {@link #pathTemplate()},
     * treating each {@code {name}} template segment as matching any single segment.
     *
     * @param requestSegments non-empty, already-split request path segments
     * @return {@code true} when every literal segment matches and the counts agree
     */
    public boolean matchesSegments(List<String> requestSegments) {
        Objects.requireNonNull(requestSegments, "requestSegments");
        List<String> templateSegments = nonEmptySegments(pathTemplate);
        if (templateSegments.size() != requestSegments.size()) {
            return false;
        }
        for (int i = 0; i < templateSegments.size(); i++) {
            String segment = templateSegments.get(i);
            if (isTemplateParameter(segment)) {
                continue;
            }
            if (!segment.equals(requestSegments.get(i))) {
                return false;
            }
        }
        return true;
    }

    private static List<String> nonEmptySegments(String template) {
        List<String> segments = new ArrayList<>();
        int start = 0;
        while (start <= template.length()) {
            int next = template.indexOf('/', start);
            int end = next < 0 ? template.length() : next;
            if (end > start) {
                segments.add(template.substring(start, end));
            }
            if (next < 0) {
                break;
            }
            start = next + 1;
        }
        return segments;
    }

    private static boolean isTemplateParameter(String segment) {
        return segment.length() > 2
                && segment.charAt(0) == '{'
                && segment.charAt(segment.length() - 1) == '}';
    }
}
