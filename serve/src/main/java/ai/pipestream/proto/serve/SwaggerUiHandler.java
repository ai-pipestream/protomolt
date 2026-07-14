package ai.pipestream.proto.serve;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;

/**
 * Serves Swagger UI at a path prefix (default {@code /docs}), wired to the gateway's
 * {@code /openapi.json}. Assets come from the bundled swagger-ui webjar; the only page
 * authored here is the initializer.
 */
public final class SwaggerUiHandler implements HttpHandler {

    private static final Map<String, String> CONTENT_TYPES = Map.of(
            "html", "text/html; charset=utf-8",
            "css", "text/css",
            "js", "application/javascript",
            "png", "image/png",
            "map", "application/json",
            "json", "application/json");

    private final String prefix;
    private final String webjarRoot;
    private final String indexHtml;

    /**
     * @param prefix       mount path, e.g. {@code /docs}
     * @param openApiPath  the OpenAPI document the UI loads, e.g. {@code /openapi.json}
     */
    public SwaggerUiHandler(String prefix, String openApiPath) {
        this.prefix = prefix.endsWith("/") ? prefix.substring(0, prefix.length() - 1) : prefix;
        this.webjarRoot = "META-INF/resources/webjars/swagger-ui/" + webjarVersion() + "/";
        this.indexHtml = """
                <!DOCTYPE html>
                <html lang="en">
                <head>
                  <meta charset="utf-8">
                  <meta name="viewport" content="width=device-width, initial-scale=1">
                  <title>ProtoMolt API</title>
                  <link rel="stylesheet" href="%1$s/swagger-ui.css">
                </head>
                <body>
                  <div id="swagger-ui"></div>
                  <script src="%1$s/swagger-ui-bundle.js"></script>
                  <script src="%1$s/swagger-ui-standalone-preset.js"></script>
                  <script>
                    window.onload = () => {
                      window.ui = SwaggerUIBundle({
                        url: "%2$s",
                        dom_id: "#swagger-ui",
                        presets: [SwaggerUIBundle.presets.apis, SwaggerUIStandalonePreset],
                        layout: "StandaloneLayout"
                      });
                    };
                  </script>
                </body>
                </html>
                """.formatted(this.prefix, openApiPath);
    }

    private static String webjarVersion() {
        try (InputStream in = SwaggerUiHandler.class.getClassLoader()
                .getResourceAsStream("META-INF/maven/org.webjars/swagger-ui/pom.properties")) {
            if (in != null) {
                Properties props = new Properties();
                props.load(in);
                String version = props.getProperty("version");
                if (version != null && !version.isBlank()) {
                    return version;
                }
            }
        } catch (IOException ignored) {
            // fall through to the error below
        }
        throw new IllegalStateException("swagger-ui webjar not found on the classpath");
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            write(exchange, 405, "text/plain", "Method not allowed".getBytes(StandardCharsets.UTF_8));
            return;
        }
        String path = exchange.getRequestURI().getPath();
        String rest = path.length() > prefix.length() ? path.substring(prefix.length() + 1) : "";
        if (rest.isEmpty() || "index.html".equals(rest)) {
            write(exchange, 200, CONTENT_TYPES.get("html"), indexHtml.getBytes(StandardCharsets.UTF_8));
            return;
        }
        // Asset names are flat within the webjar; reject anything that isn't.
        if (rest.contains("/") || rest.contains("..")) {
            write(exchange, 404, "text/plain", "Not found".getBytes(StandardCharsets.UTF_8));
            return;
        }
        try (InputStream in = getClass().getClassLoader().getResourceAsStream(webjarRoot + rest)) {
            if (in == null) {
                write(exchange, 404, "text/plain", "Not found".getBytes(StandardCharsets.UTF_8));
                return;
            }
            String ext = rest.substring(rest.lastIndexOf('.') + 1);
            write(exchange, 200, CONTENT_TYPES.getOrDefault(ext, "application/octet-stream"),
                    in.readAllBytes());
        }
    }

    private static void write(HttpExchange exchange, int status, String contentType, byte[] body)
            throws IOException {
        exchange.getResponseHeaders().set("content-type", contentType);
        exchange.sendResponseHeaders(status, body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
        }
    }
}
