package ai.protomolt.proto.compat;

import ai.protomolt.proto.descriptors.ApiStability;
import ai.protomolt.proto.descriptors.StabilityTier;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Deterministic snapshot of HTTP/REST routes, their allowed verbs, path/query parameters,
 * status codes, media types, request/response schemas, and stability tiers.
 *
 * @param routes sorted list of REST route contract entries
 */
@ApiStability(StabilityTier.STABLE)
public record RestRouteSnapshot(List<RestRouteEntry> routes) {

    public RestRouteSnapshot {
        Objects.requireNonNull(routes, "routes");
        List<RestRouteEntry> copy = new ArrayList<>(routes);
        copy.sort(Comparator.comparing(RestRouteEntry::routeId));
        routes = List.copyOf(copy);
    }

    /**
     * Indexes the captured routes by their stable {@code routeId}.
     *
     * @return immutable map of routeId to {@link RestRouteEntry}
     */
    public Map<String, RestRouteEntry> routesById() {
        Map<String, RestRouteEntry> map = new LinkedHashMap<>();
        for (RestRouteEntry entry : routes) {
            map.put(entry.routeId(), entry);
        }
        return Map.copyOf(map);
    }

    /**
     * Extracts ordered path parameter names enclosed in {@code {...}} from a path template
     * using linear character scanning.
     *
     * @param pathTemplate the URL path template (e.g. {@code "/subjects/{subject}/versions/{version}"})
     * @return immutable list of parameter names in path order
     */
    public static List<String> extractPathParameters(String pathTemplate) {
        if (pathTemplate == null || pathTemplate.isEmpty()) {
            return List.of();
        }
        List<String> params = new ArrayList<>();
        int index = 0;
        while (index < pathTemplate.length()) {
            int open = pathTemplate.indexOf('{', index);
            if (open < 0) {
                break;
            }
            int close = pathTemplate.indexOf('}', open + 1);
            if (close < 0) {
                break;
            }
            String param = pathTemplate.substring(open + 1, close).trim();
            if (!param.isEmpty()) {
                params.add(param);
            }
            index = close + 1;
        }
        return List.copyOf(params);
    }

    /**
     * Contract snapshot of a single REST route.
     *
     * @param routeId                 canonical route identifier
     * @param pathTemplate            HTTP path template (e.g. {@code "/Greeter/SayHello"} or {@code "/subjects/{subject}/versions"})
     * @param httpMethods             uppercase HTTP verbs accepted on this route
     * @param pathParameters          ordered path parameters extracted from {@code pathTemplate}
     * @param requiredQueryParameters sorted required query parameter names
     * @param requestContentType      expected request content type (or empty for bodyless routes)
     * @param responseContentType     expected response content type
     * @param requestTypeName         fully-qualified protobuf request message or schema name
     * @param responseTypeName        fully-qualified protobuf response message or schema name
     * @param statusCodes             sorted HTTP status codes part of the route contract
     * @param authRequired            whether authentication/token is required
     * @param stability               stability tier of this route
     */
    @ApiStability(StabilityTier.STABLE)
    public record RestRouteEntry(
            String routeId,
            String pathTemplate,
            List<String> httpMethods,
            List<String> pathParameters,
            List<String> requiredQueryParameters,
            String requestContentType,
            String responseContentType,
            String requestTypeName,
            String responseTypeName,
            List<Integer> statusCodes,
            boolean authRequired,
            StabilityTier stability) {

        public RestRouteEntry {
            if (routeId == null || routeId.trim().isEmpty()) {
                throw new IllegalArgumentException("routeId must not be blank");
            }
            routeId = routeId.trim();
            if (pathTemplate == null || pathTemplate.trim().isEmpty()) {
                throw new IllegalArgumentException("pathTemplate must not be blank");
            }
            pathTemplate = pathTemplate.trim();

            List<String> normalizedVerbs = new ArrayList<>();
            if (httpMethods != null) {
                for (String verb : httpMethods) {
                    if (verb != null && !verb.trim().isEmpty()) {
                        normalizedVerbs.add(verb.trim().toUpperCase(Locale.ROOT));
                    }
                }
            }
            if (normalizedVerbs.isEmpty()) {
                normalizedVerbs.add("POST");
            }
            normalizedVerbs.sort(Comparator.naturalOrder());
            httpMethods = List.copyOf(normalizedVerbs);

            pathParameters = (pathParameters == null || pathParameters.isEmpty())
                    ? extractPathParameters(pathTemplate)
                    : List.copyOf(pathParameters);

            List<String> queryCopy = new ArrayList<>(
                    requiredQueryParameters == null ? List.of() : requiredQueryParameters);
            queryCopy.sort(Comparator.naturalOrder());
            requiredQueryParameters = List.copyOf(queryCopy);

            requestContentType = requestContentType == null ? "" : requestContentType.trim();
            responseContentType = responseContentType == null ? "application/json" : responseContentType.trim();
            requestTypeName = requestTypeName == null ? "" : requestTypeName.trim();
            responseTypeName = responseTypeName == null ? "" : responseTypeName.trim();

            List<Integer> statusCopy = new ArrayList<>(
                    statusCodes == null || statusCodes.isEmpty() ? List.of(200) : statusCodes);
            statusCopy.sort(Comparator.naturalOrder());
            statusCodes = List.copyOf(statusCopy);

            Objects.requireNonNull(stability, "stability");
        }
    }
}
