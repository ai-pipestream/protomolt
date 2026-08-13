package ai.pipestream.proto.serve;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/** Establishes and revokes the task console's scoped HttpOnly browser session. */
final class TaskConsoleSessionHandler implements HttpHandler {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final int MAX_BODY_BYTES = 4 * 1024;
    private final TaskConsoleSessions sessions;

    TaskConsoleSessionHandler(TaskConsoleSessions sessions) {
        this.sessions = sessions;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            exchange.getResponseHeaders().set("Cache-Control", "no-store");
            if (!"/api/task-session".equals(exchange.getRequestURI().getPath())) {
                exchange.sendResponseHeaders(404, -1);
                return;
            }
            switch (exchange.getRequestMethod().toUpperCase(java.util.Locale.ROOT)) {
                case "GET" -> status(exchange);
                case "POST" -> login(exchange);
                case "DELETE" -> logout(exchange);
                default -> exchange.sendResponseHeaders(405, -1);
            }
        }
    }

    private void status(HttpExchange exchange) throws IOException {
        boolean authenticated = sessions.authorized(exchange);
        respondJson(exchange, authenticated ? 200 : 401,
                "{\"authenticated\":" + authenticated
                        + ",\"loginRequired\":" + sessions.requiresLogin() + "}");
    }

    private void login(HttpExchange exchange) throws IOException {
        if (!sessions.requiresLogin()) {
            respondJson(exchange, 200,
                    "{\"authenticated\":true,\"loginRequired\":false}");
            return;
        }
        byte[] body = BoundedBodies.read(exchange.getRequestBody(), MAX_BODY_BYTES);
        if (body == null) {
            exchange.sendResponseHeaders(413, -1);
            return;
        }
        JsonNode parsed;
        try {
            parsed = JSON.readTree(body);
        } catch (IOException e) {
            respondJson(exchange, 400, "{\"error\":\"invalid JSON\"}");
            return;
        }
        String token = parsed == null ? "" : parsed.path("token").asText("");
        if (!sessions.acceptsLogin(token)) {
            respondJson(exchange, 401, "{\"error\":\"invalid credentials\"}");
            return;
        }
        String session = sessions.issue();
        exchange.getResponseHeaders().add("Set-Cookie", TaskConsoleSessions.COOKIE + "="
                + session + "; Path=/; Max-Age=" + sessions.maxAgeSeconds()
                + "; HttpOnly; Secure; SameSite=Strict");
        respondJson(exchange, 200,
                "{\"authenticated\":true,\"loginRequired\":true}");
    }

    private void logout(HttpExchange exchange) throws IOException {
        sessions.revoke(exchange);
        exchange.getResponseHeaders().add("Set-Cookie", TaskConsoleSessions.COOKIE
                + "=; Path=/; Max-Age=0; HttpOnly; Secure; SameSite=Strict");
        exchange.sendResponseHeaders(204, -1);
    }

    private static void respondJson(HttpExchange exchange, int status, String json)
            throws IOException {
        byte[] body = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(status, body.length);
        exchange.getResponseBody().write(body);
    }
}
