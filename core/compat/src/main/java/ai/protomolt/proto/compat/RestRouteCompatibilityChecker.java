package ai.protomolt.proto.compat;

import ai.protomolt.proto.compat.RestRouteSnapshot.RestRouteEntry;
import ai.protomolt.proto.descriptors.ApiStability;
import ai.protomolt.proto.descriptors.StabilityTier;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Evaluates compatibility between a baseline {@link RestRouteSnapshot} and a current
 * {@link RestRouteSnapshot}.
 */
@ApiStability(StabilityTier.STABLE)
public final class RestRouteCompatibilityChecker {

    public static final String REST_ROUTE_ADDED = "REST_ROUTE_ADDED";
    public static final String REST_ROUTE_REMOVED = "REST_ROUTE_REMOVED";
    public static final String REST_PATH_TEMPLATE_CHANGED = "REST_PATH_TEMPLATE_CHANGED";
    public static final String REST_HTTP_METHOD_REMOVED = "REST_HTTP_METHOD_REMOVED";
    public static final String REST_HTTP_METHOD_ADDED = "REST_HTTP_METHOD_ADDED";
    public static final String REST_PATH_PARAMETERS_CHANGED = "REST_PATH_PARAMETERS_CHANGED";
    public static final String REST_REQUIRED_QUERY_PARAM_ADDED = "REST_REQUIRED_QUERY_PARAM_ADDED";
    public static final String REST_CONTENT_TYPE_CHANGED = "REST_CONTENT_TYPE_CHANGED";
    public static final String REST_REQUEST_TYPE_CHANGED = "REST_REQUEST_TYPE_CHANGED";
    public static final String REST_RESPONSE_TYPE_CHANGED = "REST_RESPONSE_TYPE_CHANGED";
    public static final String REST_SUCCESS_STATUS_REMOVED = "REST_SUCCESS_STATUS_REMOVED";
    public static final String REST_AUTH_REQUIREMENT_ADDED = "REST_AUTH_REQUIREMENT_ADDED";
    public static final String REST_STABILITY_DOWNGRADED = "REST_STABILITY_DOWNGRADED";

    private RestRouteCompatibilityChecker() {
    }

    /**
     * Compares {@code current} against {@code baseline} and returns all detected
     * {@link SurfaceChange}s in deterministic order.
     *
     * @param baseline the baseline REST route snapshot
     * @param current  the current REST route snapshot
     * @return immutable list of detected changes
     */
    public static List<SurfaceChange> compare(
            RestRouteSnapshot baseline,
            RestRouteSnapshot current) {
        Objects.requireNonNull(baseline, "baseline");
        Objects.requireNonNull(current, "current");

        Map<String, RestRouteEntry> oldRoutes = baseline.routesById();
        Map<String, RestRouteEntry> newRoutes = current.routesById();
        List<SurfaceChange> changes = new ArrayList<>();

        for (RestRouteEntry oldRoute : baseline.routes()) {
            String routeId = oldRoute.routeId();
            RestRouteEntry newRoute = newRoutes.get(routeId);
            if (newRoute == null) {
                changes.add(change(
                        REST_ROUTE_REMOVED,
                        routeId,
                        oldRoute.stability(),
                        String.join(",", oldRoute.httpMethods()) + " " + oldRoute.pathTemplate(),
                        "",
                        "REST route removed: " + routeId + " (" + oldRoute.pathTemplate() + ")",
                        true));
                continue;
            }
            compareRoute(oldRoute, newRoute, changes);
        }

        for (RestRouteEntry newRoute : current.routes()) {
            if (!oldRoutes.containsKey(newRoute.routeId())) {
                changes.add(change(
                        REST_ROUTE_ADDED,
                        newRoute.routeId(),
                        newRoute.stability(),
                        "",
                        String.join(",", newRoute.httpMethods()) + " " + newRoute.pathTemplate(),
                        "REST route added: " + newRoute.routeId() + " (" + newRoute.pathTemplate() + ")",
                        false));
            }
        }

        return List.copyOf(changes);
    }

    private static void compareRoute(
            RestRouteEntry oldRoute,
            RestRouteEntry newRoute,
            List<SurfaceChange> changes) {
        String routeId = oldRoute.routeId();
        StabilityTier tier = oldRoute.stability();

        if (!oldRoute.pathTemplate().equals(newRoute.pathTemplate())) {
            changes.add(change(
                    REST_PATH_TEMPLATE_CHANGED,
                    routeId,
                    tier,
                    oldRoute.pathTemplate(),
                    newRoute.pathTemplate(),
                    "REST route path template changed on " + routeId + ": "
                            + oldRoute.pathTemplate() + " -> " + newRoute.pathTemplate(),
                    true));
        } else if (!oldRoute.pathParameters().equals(newRoute.pathParameters())) {
            changes.add(change(
                    REST_PATH_PARAMETERS_CHANGED,
                    routeId,
                    tier,
                    String.join(",", oldRoute.pathParameters()),
                    String.join(",", newRoute.pathParameters()),
                    "REST route path parameters changed on " + routeId,
                    true));
        }

        Set<String> oldVerbs = new LinkedHashSet<>(oldRoute.httpMethods());
        Set<String> newVerbs = new LinkedHashSet<>(newRoute.httpMethods());
        for (String oldVerb : oldVerbs) {
            if (!newVerbs.contains(oldVerb)) {
                changes.add(change(
                        REST_HTTP_METHOD_REMOVED,
                        routeId + ":" + oldVerb,
                        tier,
                        oldVerb,
                        "",
                        "HTTP verb " + oldVerb + " removed from REST route " + routeId,
                        true));
            }
        }
        for (String newVerb : newVerbs) {
            if (!oldVerbs.contains(newVerb)) {
                changes.add(change(
                        REST_HTTP_METHOD_ADDED,
                        routeId + ":" + newVerb,
                        newRoute.stability(),
                        "",
                        newVerb,
                        "HTTP verb " + newVerb + " added to REST route " + routeId,
                        false));
            }
        }

        Set<String> oldQuery = new LinkedHashSet<>(oldRoute.requiredQueryParameters());
        for (String newQueryParam : newRoute.requiredQueryParameters()) {
            if (!oldQuery.contains(newQueryParam)) {
                changes.add(change(
                        REST_REQUIRED_QUERY_PARAM_ADDED,
                        routeId + "?" + newQueryParam,
                        tier,
                        "",
                        newQueryParam,
                        "Required query parameter '" + newQueryParam + "' added to REST route " + routeId,
                        true));
            }
        }

        if (!oldRoute.requestContentType().isEmpty()
                && !oldRoute.requestContentType().equals(newRoute.requestContentType())) {
            changes.add(change(
                    REST_CONTENT_TYPE_CHANGED,
                    routeId + "#requestContentType",
                    tier,
                    oldRoute.requestContentType(),
                    newRoute.requestContentType(),
                    "Request content-type changed on REST route " + routeId + ": "
                            + oldRoute.requestContentType() + " -> " + newRoute.requestContentType(),
                    true));
        }

        if (!oldRoute.responseContentType().isEmpty()
                && !oldRoute.responseContentType().equals(newRoute.responseContentType())) {
            changes.add(change(
                    REST_CONTENT_TYPE_CHANGED,
                    routeId + "#responseContentType",
                    tier,
                    oldRoute.responseContentType(),
                    newRoute.responseContentType(),
                    "Response content-type changed on REST route " + routeId + ": "
                            + oldRoute.responseContentType() + " -> " + newRoute.responseContentType(),
                    true));
        }

        if (!oldRoute.requestTypeName().isEmpty()
                && !oldRoute.requestTypeName().equals(newRoute.requestTypeName())) {
            changes.add(change(
                    REST_REQUEST_TYPE_CHANGED,
                    routeId + "#requestType",
                    tier,
                    oldRoute.requestTypeName(),
                    newRoute.requestTypeName(),
                    "Request message contract changed on REST route " + routeId + ": "
                            + oldRoute.requestTypeName() + " -> " + newRoute.requestTypeName(),
                    true));
        }

        if (!oldRoute.responseTypeName().isEmpty()
                && !oldRoute.responseTypeName().equals(newRoute.responseTypeName())) {
            changes.add(change(
                    REST_RESPONSE_TYPE_CHANGED,
                    routeId + "#responseType",
                    tier,
                    oldRoute.responseTypeName(),
                    newRoute.responseTypeName(),
                    "Response message contract changed on REST route " + routeId + ": "
                            + oldRoute.responseTypeName() + " -> " + newRoute.responseTypeName(),
                    true));
        }

        Set<Integer> newStatuses = new LinkedHashSet<>(newRoute.statusCodes());
        for (Integer status : oldRoute.statusCodes()) {
            if (status >= 200 && status < 300 && !newStatuses.contains(status)) {
                changes.add(change(
                        REST_SUCCESS_STATUS_REMOVED,
                        routeId + "#status:" + status,
                        tier,
                        String.valueOf(status),
                        newRoute.statusCodes().toString(),
                        "Success HTTP status code " + status + " removed from REST route " + routeId,
                        true));
            }
        }

        if (!oldRoute.authRequired() && newRoute.authRequired()) {
            changes.add(change(
                    REST_AUTH_REQUIREMENT_ADDED,
                    routeId,
                    tier,
                    "unauthenticated",
                    "authRequired",
                    "Authentication requirement added to previously unauthenticated REST route " + routeId,
                    true));
        }

        if (tier.isDowngradeTo(newRoute.stability())) {
            changes.add(change(
                    REST_STABILITY_DOWNGRADED,
                    routeId,
                    tier,
                    tier.name(),
                    newRoute.stability().name(),
                    "REST route " + routeId + " downgraded stability from "
                            + tier + " to " + newRoute.stability(),
                    true));
        }
    }

    private static SurfaceChange change(
            String ruleId,
            String path,
            StabilityTier stability,
            String before,
            String after,
            String message,
            boolean breaking) {
        return new SurfaceChange(
                CompatibilitySurface.REST_ROUTE,
                ruleId,
                path,
                stability,
                before,
                after,
                message,
                breaking);
    }
}
