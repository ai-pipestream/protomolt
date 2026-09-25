package ai.protomolt.proto.serve;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.authz.ConsoleSessions;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.util.Locale;
import java.util.Set;

/** Scoped browser access to two fixed correction RPCs, never an arbitrary service proxy. */
final class CorrectionConsoleApiHandler implements HttpHandler {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String PREFIX = "/api/correction";
    private final ActionCatalog catalog;
    private final ConsoleSessions sessions;

    CorrectionConsoleApiHandler(ActionCatalog catalog, ConsoleSessions sessions) {
        if (!sessions.requiresLogin()) {
            throw new IllegalArgumentException("correction console requires authenticated sessions");
        }
        this.catalog = catalog;
        this.sessions = sessions;
    }

    @Override public void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            exchange.getResponseHeaders().set("Cache-Control", "no-store");
            Caller caller = sessions.caller(exchange).orElse(null);
            if (caller == null) { error(exchange, 401, "authentication-required"); return; }
            if (!caller.holds(Scopes.WORKER_COORDINATE)) {
                error(exchange, 403, "permission-denied"); return;
            }
            String path = exchange.getRequestURI().getPath();
            String method = exchange.getRequestMethod();
            if (path.equals(PREFIX) && method.equals("GET")) {
                respond(exchange, 200, JSON.createObjectNode()
                        .put("workflow", "correct-contact")
                        .put("profile", "correction")
                        .put("authentication", "console-session")
                        .put("receiptNote", "Receipts bind recorded evidence and policy; they do not prove semantic truth."));
                return;
            }
            ObjectNode request;
            String rpc;
            if (path.equals(PREFIX + "/runs") && method.equals("POST")) {
                String type = exchange.getRequestHeaders().getFirst("Content-Type");
                if (type == null || !type.toLowerCase(Locale.ROOT).startsWith("application/json")) {
                    error(exchange, 415, "json-required"); return;
                }
                byte[] body = BoundedBodies.read(exchange.getRequestBody(), 64 * 1024);
                if (body == null) { error(exchange, 413, "request-too-large"); return; }
                JsonNode parsed;
                try { parsed = JSON.readTree(body); }
                catch (IOException invalid) { error(exchange, 400, "invalid-json"); return; }
                if (!(parsed instanceof ObjectNode object)) { error(exchange, 400, "object-required"); return; }
                request = object;
                rpc = "RunCorrection";
            } else if (path.startsWith(PREFIX + "/runs/") && method.equals("GET")) {
                String id = path.substring((PREFIX + "/runs/").length());
                request = JSON.createObjectNode().put("runId", id);
                rpc = "GetCorrection";
            } else { error(exchange, 404, "not-found"); return; }

            ObjectNode invocation = JSON.createObjectNode().put("name", "correction")
                    .put("endpoint", "default").put("method", CorrectionStarter.SERVICE + rpc);
            invocation.set("request", request);
            try {
                // The browser principal can only choose the request to these two RPCs.
                // General service-invoke remains inaccessible through this HTTP handler.
                JsonNode result = catalog.execute("service-invoke", invocation,
                        Caller.scoped(caller.name() + ":correction", Set.of(Scopes.SERVICE_INVOKE)));
                if (!result.path("ok").asBoolean()) {
                    String status = result.path("status").asText("UPSTREAM_FAILURE");
                    int code = switch (status) {
                        case "INVALID_ARGUMENT" -> 400;
                        case "ALREADY_EXISTS" -> 409;
                        case "NOT_FOUND" -> 404;
                        case "RESOURCE_EXHAUSTED" -> 429;
                        default -> 502;
                    };
                    error(exchange, code, status);
                    return;
                }
                JsonNode responses = result.path("responses");
                if (!responses.isArray() || responses.size() != 1) {
                    error(exchange, 502, "invalid-upstream-response"); return;
                }
                JsonNode response = responses.get(0);
                respond(exchange, 200, response.isTextual() ? JSON.readTree(response.asText()) : response);
            } catch (ActionException rejected) {
                error(exchange, rejected.code().equals("invalid-input") ? 400 : 502, rejected.code());
            }
        }
    }

    private static void error(HttpExchange exchange, int status, String code) throws IOException {
        respond(exchange, status, JSON.createObjectNode().put("error", code));
    }

    private static void respond(HttpExchange exchange, int status, JsonNode body) throws IOException {
        byte[] bytes = JSON.writeValueAsBytes(body);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
    }
}
