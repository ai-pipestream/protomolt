package ai.protomolt.proto.serve;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.authz.ConsoleSessions;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/** Fixed workflow entry and launch verbs over the authenticated task-console cookie. */
final class WorkflowLaunchConsoleApiHandler implements HttpHandler {
    private static final String PREFIX = "/api/workflow-launch";
    private static final int MAX_BODY_BYTES = 8 * 1024 * 1024;
    private static final ObjectMapper JSON = new ObjectMapper();

    private final ActionCatalog catalog;
    private final ConsoleSessions sessions;
    private final boolean authoringEntry;

    WorkflowLaunchConsoleApiHandler(ActionCatalog catalog, ConsoleSessions sessions) {
        this(catalog, sessions, false);
    }

    static WorkflowLaunchConsoleApiHandler authoringEntry(ActionCatalog catalog, ConsoleSessions sessions) {
        return new WorkflowLaunchConsoleApiHandler(catalog, sessions, true);
    }

    private WorkflowLaunchConsoleApiHandler(ActionCatalog catalog, ConsoleSessions sessions, boolean authoringEntry) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.sessions = Objects.requireNonNull(sessions, "sessions");
        this.authoringEntry = authoringEntry;
        if (!sessions.requiresLogin()) {
            throw new IllegalArgumentException("workflow launch requires authenticated console sessions");
        }
    }

    @Override public void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            exchange.getResponseHeaders().set("Cache-Control", "no-store");
            String path = exchange.getRequestURI().getPath();
            String action = authoringEntry ? switch (path) {
                case "/api/workflow-authoring/template" -> "get-workflow-authoring-template";
                case "/api/workflow-authoring/start" -> "start-workflow-authoring";
                default -> null;
            } : switch (path) {
                case PREFIX + "/accepted" -> "get-accepted-workflow";
                case PREFIX + "/contract" -> "get-workflow-launch-input-contract";
                case PREFIX + "/prepare" -> "prepare-workflow-launch-input";
                case PREFIX + "/launch" -> "launch-accepted-workflow";
                case PREFIX + "/status" -> "get-workflow-launch-status";
                default -> null;
            };
            if (action == null) { error(exchange, 404, "not-found"); return; }
            if (!"POST".equals(exchange.getRequestMethod())) {
                error(exchange, 405, "method-not-allowed"); return;
            }
            Caller caller = sessions.caller(exchange).orElse(null);
            if (caller == null) { error(exchange, 401, "authentication-required"); return; }
            if (caller.unrestricted() || !caller.holds(Scopes.WORKER_COORDINATE)
                    || !authoringEntry && !caller.holds(Scopes.WORKFLOW_LAUNCH)) {
                error(exchange, 403, "permission-denied"); return;
            }
            if (!sameOrigin(exchange)) { error(exchange, 403, "origin-denied"); return; }
            String contentType = exchange.getRequestHeaders().getFirst("Content-Type");
            if (contentType == null || !contentType.toLowerCase(Locale.ROOT).matches(
                    "application/json(?:\\s*;.*)?")) {
                error(exchange, 415, "json-required"); return;
            }
            byte[] bytes = BoundedBodies.read(exchange.getRequestBody(), MAX_BODY_BYTES);
            if (bytes == null) { error(exchange, 413, "request-too-large"); return; }
            ObjectNode request;
            try (JsonParser parser = JSON.createParser(bytes)) {
                if (!(JSON.readTree(parser) instanceof ObjectNode object)
                        || parser.nextToken() != null) {
                    error(exchange, 400, "object-required"); return;
                }
                request = object;
            } catch (IOException malformed) {
                error(exchange, 400, "invalid-json"); return;
            }
            try {
                respond(exchange, 200, catalog.execute(action, request, caller));
            } catch (ActionException failure) {
                error(exchange, status(failure.code()), failure.code());
            } catch (RuntimeException unexpected) {
                error(exchange, 500, "internal-error");
            }
        }
    }

    private static int status(String code) {
        return switch (code) {
            case "invalid-input" -> 400;
            case "permission-denied" -> 403;
            case "resource-exhausted" -> 429;
            case "workflow-launch-conflict", "workflow-authoring-conflict" -> 409;
            case "workflow-authoring-rejected" -> 412;
            case "workflow-authoring-deadline" -> 504;
            case "workflow-authoring-unavailable" -> 503;
            case "workflow-authoring-storage-failed" -> 503;
            case "invalid-upstream-response" -> 502;
            default -> 500;
        };
    }

    private static boolean sameOrigin(HttpExchange exchange) {
        List<String> fetchSites = exchange.getRequestHeaders().get("Sec-Fetch-Site");
        if (fetchSites != null && (fetchSites.size() != 1
                || !("same-origin".equalsIgnoreCase(fetchSites.getFirst())
                || "none".equalsIgnoreCase(fetchSites.getFirst())))) return false;
        List<String> origins = exchange.getRequestHeaders().get("Origin");
        if (origins == null) return true;
        List<String> hosts = exchange.getRequestHeaders().get("Host");
        if (origins.size() != 1 || hosts == null || hosts.size() != 1) return false;
        try {
            URI origin = URI.create(origins.getFirst());
            String scheme = origin.getScheme();
            if (!("http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme))
                    || origin.getHost() == null || origin.getRawUserInfo() != null
                    || origin.getRawPath() != null && !origin.getRawPath().isEmpty()
                    || origin.getRawQuery() != null || origin.getRawFragment() != null) return false;
            // Compare the browser's Origin to the actual Host header. A TLS reverse proxy
            // preserves Host; forwarded host/proto headers are never treated as authority.
            URI host = URI.create(scheme + "://" + hosts.getFirst());
            return origin.getHost().equalsIgnoreCase(host.getHost())
                    && port(origin) == port(host) && host.getRawUserInfo() == null
                    && (host.getRawPath() == null || host.getRawPath().isEmpty())
                    && host.getRawQuery() == null && host.getRawFragment() == null;
        } catch (IllegalArgumentException malformed) {
            return false;
        }
    }

    private static int port(URI uri) {
        if (uri.getPort() >= 0) return uri.getPort();
        return "https".equalsIgnoreCase(uri.getScheme()) ? 443 : 80;
    }

    private static void error(HttpExchange exchange, int status, String code) throws IOException {
        respond(exchange, status, JSON.createObjectNode().put("error", code));
    }

    private static void respond(HttpExchange exchange, int status, ObjectNode body) throws IOException {
        byte[] bytes = JSON.writeValueAsBytes(body);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
    }
}
