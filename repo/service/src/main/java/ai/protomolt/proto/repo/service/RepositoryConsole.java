package ai.protomolt.proto.repo.service;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;

/**
 * The console bundle the repository host serves at {@link #PATH}: the production build of
 * {@code apps/console} (a single-page app built with {@code base: '/console/'}), read from a
 * directory on disk.
 *
 * <p>The bundle is configured explicitly. {@link #fromEnvironment} reads
 * {@link #ENV_CONSOLE_DIR}: absent means {@link #notConfigured()}, whose route answers 503 and
 * names the variable instead of serving an empty page; present means {@link #directory(Path)},
 * which rejects at boot a directory without an {@code index.html}. The distributions built by
 * Gradle contain the bundle under {@code console/} when {@code apps/console/dist} was built first,
 * so an image points the variable at that directory.
 *
 * <p>The route serves static files only and needs no credential: the page has to load before
 * anyone can sign in, and it contains no repository data. Hashed assets under {@code assets/}
 * are cached as immutable; any other path that is not a bundled file falls back to
 * {@code index.html}, because the console router handles it.
 */
public final class RepositoryConsole {

    /** The route the console is served under; matches the bundle's vite {@code base}. */
    public static final String PATH = "/console";

    /** Environment variable naming the directory of the built console bundle. */
    public static final String ENV_CONSOLE_DIR = "DOCUMENT_PLATFORM_CONSOLE_DIR";

    private static final Logger LOG = LoggerFactory.getLogger(RepositoryConsole.class);
    private static final String INDEX = "index.html";
    private static final String ASSETS = "assets/";

    /** The bundle root as a real path, or null when no bundle is configured. */
    private final Path root;

    private RepositoryConsole(Path root) {
        this.root = root;
    }

    /**
     * A host that serves no console bundle: {@link #PATH} answers 503 and explains how to
     * configure one.
     *
     * @return the unconfigured console
     */
    public static RepositoryConsole notConfigured() {
        return new RepositoryConsole(null);
    }

    /**
     * Serves the bundle in {@code directory}.
     *
     * @param directory the built console bundle (the contents of {@code apps/console/dist})
     * @return the console over that directory
     * @throws IllegalArgumentException when the directory does not exist or has no
     *         {@code index.html}
     */
    public static RepositoryConsole directory(Path directory) {
        Objects.requireNonNull(directory, "directory");
        if (!Files.isDirectory(directory)) {
            throw new IllegalArgumentException("console bundle directory " + directory
                    + " does not exist or is not a directory");
        }
        Path real;
        try {
            real = directory.toRealPath();
        } catch (IOException e) {
            throw new IllegalArgumentException("console bundle directory " + directory
                    + " cannot be resolved", e);
        }
        if (!Files.isRegularFile(real.resolve(INDEX))) {
            throw new IllegalArgumentException("console bundle directory " + directory
                    + " has no " + INDEX + "; build it with 'npm ci && npm run build' in"
                    + " apps/console and point at apps/console/dist");
        }
        return new RepositoryConsole(real);
    }

    /**
     * Reads {@link #ENV_CONSOLE_DIR} from {@code environment}.
     *
     * @param environment the process environment
     * @return {@link #notConfigured()} when the variable is absent, otherwise
     *         {@link #directory(Path)} over its value
     * @throws IllegalArgumentException when the variable is present but blank, is not a
     *         path, or names a directory without a built bundle
     */
    public static RepositoryConsole fromEnvironment(Map<String, String> environment) {
        Objects.requireNonNull(environment, "environment");
        if (!environment.containsKey(ENV_CONSOLE_DIR)) {
            return notConfigured();
        }
        String value = environment.get(ENV_CONSOLE_DIR);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(ENV_CONSOLE_DIR + " must not be blank when present");
        }
        Path directory;
        try {
            directory = Path.of(value.trim());
        } catch (InvalidPathException e) {
            throw new IllegalArgumentException(ENV_CONSOLE_DIR + " is not a valid path", e);
        }
        try {
            return directory(directory);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(ENV_CONSOLE_DIR + ": " + e.getMessage(), e);
        }
    }

    /**
     * Whether a bundle is configured.
     *
     * @return true when this console serves a directory
     */
    public boolean configured() {
        return root != null;
    }

    /** The handler mounted at {@link #PATH}. */
    HttpHandler handler() {
        return this::handle;
    }

    private void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            String method = exchange.getRequestMethod();
            if (!"GET".equalsIgnoreCase(method) && !"HEAD".equalsIgnoreCase(method)) {
                exchange.getResponseHeaders().set("Allow", "GET, HEAD");
                text(exchange, 405, "method not allowed: " + method);
                return;
            }
            String path = exchange.getRequestURI().getPath();
            if (path.equals(PATH)) {
                exchange.getResponseHeaders().set("Location", PATH + "/");
                exchange.sendResponseHeaders(308, -1);
                return;
            }
            if (!path.startsWith(PATH + "/")) {
                text(exchange, 404, "not found: " + path);
                return;
            }
            if (root == null) {
                text(exchange, 503, "The repository console is not configured on this host: set "
                        + ENV_CONSOLE_DIR + " to a built apps/console bundle.");
                return;
            }
            String relative = path.substring(PATH.length() + 1);
            if (relative.isEmpty()) {
                relative = INDEX;
            }
            Path file = bundled(relative);
            if (file == null && !relative.startsWith(ASSETS)) {
                relative = INDEX;
                file = bundled(INDEX);
            }
            if (file == null) {
                if (bundled(INDEX) == null) {
                    LOG.error("console bundle at {} lost its {}", root, INDEX);
                    text(exchange, 503, "The console bundle on this host has no " + INDEX + ".");
                    return;
                }
                text(exchange, 404, "not found: " + path);
                return;
            }
            exchange.getResponseHeaders().set("Cache-Control", relative.startsWith(ASSETS)
                    ? "public, max-age=31536000, immutable" : "no-cache");
            exchange.getResponseHeaders().set("Content-Type", contentType(relative));
            exchange.getResponseHeaders().set("X-Content-Type-Options", "nosniff");
            long size = Files.size(file);
            if ("HEAD".equalsIgnoreCase(method)) {
                exchange.getResponseHeaders().set("Content-Length", Long.toString(size));
                exchange.sendResponseHeaders(200, -1);
                return;
            }
            exchange.sendResponseHeaders(200, size == 0 ? -1 : size);
            if (size > 0) {
                try (InputStream in = Files.newInputStream(file);
                     OutputStream out = exchange.getResponseBody()) {
                    in.transferTo(out);
                }
            }
        }
    }

    /**
     * The bundled regular file at {@code relative}, or null when there is none. A path that
     * resolves outside the bundle root, directly or through a link, is not bundled.
     */
    private Path bundled(String relative) throws IOException {
        Path candidate;
        try {
            candidate = root.resolve(relative).normalize();
        } catch (InvalidPathException e) {
            // A request path the file system cannot name (a NUL byte, say) cannot be a
            // bundled file; the caller answers it like any other miss.
            return null;
        }
        if (!candidate.startsWith(root) || !Files.isRegularFile(candidate)) {
            return null;
        }
        Path real = candidate.toRealPath();
        if (!real.startsWith(root) || !Files.isRegularFile(real, LinkOption.NOFOLLOW_LINKS)) {
            return null;
        }
        return real;
    }

    private static void text(HttpExchange exchange, int status, String message) throws IOException {
        byte[] body = message.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
        if ("HEAD".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(status, -1);
            return;
        }
        exchange.sendResponseHeaders(status, body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
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
            case "map" -> "application/json";
            case "woff2" -> "font/woff2";
            case "woff" -> "font/woff";
            case "ttf" -> "font/ttf";
            case "eot" -> "application/vnd.ms-fontobject";
            case "txt" -> "text/plain; charset=utf-8";
            case "proto" -> "text/plain; charset=utf-8";
            case "binpb" -> "application/x-protobuf";
            default -> "application/octet-stream";
        };
    }
}
