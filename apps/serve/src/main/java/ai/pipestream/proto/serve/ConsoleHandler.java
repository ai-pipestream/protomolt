package ai.pipestream.proto.serve;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Serves the bundled console (a single-page app) at {@code /console} from classpath
 * resources: hashed assets get immutable caching, unknown extensionless paths fall back to
 * {@code index.html} (the router owns them), and a build without the console bundled
 * explains how to add it rather than 404ing.
 */
final class ConsoleHandler implements HttpHandler {

    private static final String PREFIX = "/console";
    private static final String RESOURCES = "ai/pipestream/proto/serve/console";

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())
                    && !"HEAD".equalsIgnoreCase(exchange.getRequestMethod())) {
                respond(exchange, 405, "text/plain", "method not allowed".getBytes(StandardCharsets.UTF_8));
                return;
            }
            String path = exchange.getRequestURI().getPath();
            if (path.equals(PREFIX)) {
                exchange.getResponseHeaders().set("Location", PREFIX + "/");
                exchange.sendResponseHeaders(308, -1);
                return;
            }
            String relative = path.substring(PREFIX.length() + 1);
            if (relative.isEmpty()) {
                relative = "index.html";
            }
            byte[] body = resource(relative);
            String served = relative;
            // SPA fallback: anything that is not a bundled file belongs to the router —
            // including subject paths like .../subjects/demo/shop/v1/shop.proto, so a file
            // extension is no signal. Only misses under assets/ (vite's hashed output) 404.
            if (body == null && !relative.startsWith("assets/")) {
                served = "index.html";
                body = resource(served);
            }
            if (body == null) {
                if (resource("index.html") == null) {
                    respond(exchange, 503, "text/plain; charset=utf-8",
                            ("The console is not bundled in this build. Build it with "
                                    + "'cd console && npm ci && npm run build', then rebuild "
                                    + "protomolt-serve.").getBytes(StandardCharsets.UTF_8));
                    return;
                }
                respond(exchange, 404, "text/plain", "not found".getBytes(StandardCharsets.UTF_8));
                return;
            }
            if (served.startsWith("assets/")) {
                // Vite content-hashes asset names; they are immutable by construction.
                exchange.getResponseHeaders().set("Cache-Control", "public, max-age=31536000, immutable");
            } else {
                exchange.getResponseHeaders().set("Cache-Control", "no-cache");
            }
            respond(exchange, 200, contentType(served), body);
        }
    }

    private static byte[] resource(String relative) throws IOException {
        if (relative.contains("..")) {
            return null;
        }
        try (InputStream in = ConsoleHandler.class.getClassLoader()
                .getResourceAsStream(RESOURCES + "/" + relative)) {
            return in == null ? null : in.readAllBytes();
        }
    }

    private static void respond(HttpExchange exchange, int status, String type, byte[] body)
            throws IOException {
        exchange.getResponseHeaders().set("Content-Type", type);
        if ("HEAD".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(status, -1);
            return;
        }
        exchange.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
        if (body.length > 0) {
            exchange.getResponseBody().write(body);
        }
    }

    private static String contentType(String path) {
        int dot = path.lastIndexOf('.');
        String extension = dot < 0 ? "" : path.substring(dot + 1);
        return switch (extension) {
            case "html" -> "text/html; charset=utf-8";
            case "js", "mjs" -> "text/javascript; charset=utf-8";
            case "css" -> "text/css; charset=utf-8";
            case "svg" -> "image/svg+xml";
            case "png" -> "image/png";
            case "ico" -> "image/x-icon";
            case "json", "map" -> "application/json";
            case "woff2" -> "font/woff2";
            case "woff" -> "font/woff";
            case "ttf" -> "font/ttf";
            case "eot" -> "application/vnd.ms-fontobject";
            case "txt" -> "text/plain; charset=utf-8";
            default -> "application/octet-stream";
        };
    }
}
