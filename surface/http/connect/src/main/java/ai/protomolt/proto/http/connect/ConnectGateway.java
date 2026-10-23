package ai.protomolt.proto.http.connect;

import ai.protomolt.proto.authz.AuthenticatedCaller;
import ai.protomolt.proto.authz.RequestCallerResolver;
import ai.protomolt.proto.authz.RequestHeaders;
import ai.protomolt.proto.authz.grpc.CallerContexts;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Deadline;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServerMethodDefinition;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Serves the Connect protocol, binary protobuf only, for a set of gRPC services on the JDK
 * {@code HttpServer}: mount it at {@link #PATH} and a Connect client calls
 * {@code POST /connect/<package.Service>/<Method>}.
 *
 * <ul>
 *   <li>Only the procedures the gateway is built with are reachable, even when a registered
 *       service has more methods; any other path answers {@code unimplemented} without
 *       reaching a service.</li>
 *   <li>A browser request whose {@code Sec-Fetch-Site} is not {@code same-origin} (or
 *       {@code none}) is refused with {@code permission_denied} before any credential is
 *       read. The gateway sends no CORS headers.</li>
 *   <li>Unary calls take and answer {@value #UNARY_CONTENT_TYPE}; server-streaming calls take
 *       and answer {@value #STREAM_CONTENT_TYPE} with enveloped messages. Client-streaming and
 *       bidirectional methods are refused by name. A JSON content type is refused with 415 and
 *       never transcoded. Compression other than {@code identity} is refused.</li>
 *   <li>The caller of every call comes from the {@link RequestCallerResolver} the gateway is
 *       built with. No credential means {@code unauthenticated}, a store outage means
 *       {@code unavailable}, and a resolved caller runs under the same
 *       {@link CallerContexts} keys the gRPC credential interceptor sets, so service-side scope
 *       and account checks apply unchanged.</li>
 *   <li>{@code connect-timeout-ms} becomes the call's deadline; a client that goes away
 *       mid-stream cancels the call; gRPC status codes map to Connect codes and HTTP statuses
 *       by the protocol's table.</li>
 * </ul>
 *
 * <p>Dispatch goes through a private in-process gRPC server holding the registered service
 * definitions, reached only by this gateway, so each service's own interceptors, validation
 * and status mapping run as they do on any gRPC transport. Messages cross as bytes: the
 * gateway parses no message and needs no descriptor. The Connect error record and the
 * end-of-stream record are JSON by the protocol's definition; they are the only JSON written,
 * see {@link ConnectErrors}.
 */
public final class ConnectGateway implements HttpHandler, AutoCloseable {

    /** The context path the gateway is mounted at. */
    public static final String PATH = "/connect";

    /** The content type of a unary request and its answer. */
    public static final String UNARY_CONTENT_TYPE = "application/proto";

    /** The content type of a server-streaming request and its answer. */
    public static final String STREAM_CONTENT_TYPE = "application/connect+proto";

    private static final Logger LOG = LoggerFactory.getLogger(ConnectGateway.class);
    private static final String ERROR_CONTENT_TYPE = "application/json";
    private static final Metadata.Key<String> CALL_KEY =
            Metadata.Key.of("protomolt-connect-call", Metadata.ASCII_STRING_MARSHALLER);
    private static final int FLAG_COMPRESSED = 0x01;
    private static final int FLAG_END_STREAM = 0x02;
    private static final MethodDescriptor.Marshaller<byte[]> BYTES = new MethodDescriptor.Marshaller<>() {
        @Override
        public InputStream stream(byte[] value) {
            return new ByteArrayInputStream(value);
        }

        @Override
        public byte[] parse(InputStream stream) {
            try {
                return stream.readAllBytes();
            } catch (IOException e) {
                throw new UncheckedIOException("reading a message from the in-process transport", e);
            }
        }
    };

    private final String basePath;
    private final RequestCallerResolver callers;
    private final ConnectLimits limits;
    private final Map<String, MethodDescriptor<byte[], byte[]>> methods;
    private final ConcurrentMap<String, AuthenticatedCaller> handoffs = new ConcurrentHashMap<>();
    private final ExecutorService executor;
    private final Server server;
    private final ManagedChannel channel;
    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * Builds the gateway and starts its private dispatch server.
     *
     * @param services the services the exposed procedures belong to; at least one, no two
     *        registering the same method. Pass the definitions without a transport credential
     *        interceptor: the gateway resolves the caller itself
     * @param exposed the procedures reachable through the gateway, as
     *        {@code package.Service/Method}; at least one, each a method of {@code services}
     * @param callers resolves the caller of every request
     * @param limits the request and response message bounds
     * @throws IllegalArgumentException when no service or procedure is given, two services
     *         share a method name, or an exposed procedure is not a registered method
     */
    public ConnectGateway(List<ServerServiceDefinition> services, Set<String> exposed,
            RequestCallerResolver callers, ConnectLimits limits) {
        this(services, exposed, callers, limits, PATH);
    }

    /**
     * Builds the gateway for a context other than {@link #PATH}; a Connect client then calls
     * {@code POST <basePath>/<package.Service>/<Method>}.
     *
     * @param services the services the exposed procedures belong to, as in the four-argument form
     * @param exposed the procedures reachable through the gateway
     * @param callers resolves the caller of every request
     * @param limits the request and response message bounds
     * @param basePath the context path the gateway is mounted at: empty for the server root,
     *        otherwise starting with {@code /} and not ending with one
     * @throws IllegalArgumentException as the four-argument form, or for a malformed base path
     */
    public ConnectGateway(List<ServerServiceDefinition> services, Set<String> exposed,
            RequestCallerResolver callers, ConnectLimits limits, String basePath) {
        Objects.requireNonNull(basePath, "basePath");
        if (!basePath.isEmpty() && (!basePath.startsWith("/") || basePath.endsWith("/"))) {
            throw new IllegalArgumentException("basePath must be empty or start with / and not end with /: "
                    + basePath);
        }
        this.basePath = basePath;
        Objects.requireNonNull(services, "services");
        Objects.requireNonNull(exposed, "exposed");
        if (exposed.isEmpty()) {
            throw new IllegalArgumentException("a Connect gateway needs at least one exposed procedure");
        }
        this.callers = Objects.requireNonNull(callers, "callers");
        this.limits = Objects.requireNonNull(limits, "limits");
        if (services.isEmpty()) {
            throw new IllegalArgumentException("a Connect gateway needs at least one service");
        }
        Map<String, MethodDescriptor<byte[], byte[]>> byName = new HashMap<>();
        String name = "protomolt-connect-" + UUID.randomUUID();
        InProcessServerBuilder builder = InProcessServerBuilder.forName(name)
                .maxInboundMessageSize(limits.maxRequestBytes());
        ServerInterceptor handoff = new CallerHandoff();
        for (ServerServiceDefinition service : services) {
            Objects.requireNonNull(service, "service");
            for (ServerMethodDefinition<?, ?> method : service.getMethods()) {
                MethodDescriptor<?, ?> descriptor = method.getMethodDescriptor();
                String fullName = descriptor.getFullMethodName();
                MethodDescriptor<byte[], byte[]> raw = MethodDescriptor.newBuilder(BYTES, BYTES)
                        .setFullMethodName(fullName)
                        .setType(descriptor.getType())
                        .build();
                if (byName.putIfAbsent(fullName, raw) != null) {
                    throw new IllegalArgumentException("two services register " + fullName);
                }
            }
            builder.addService(ServerInterceptors.intercept(service, handoff));
        }
        Map<String, MethodDescriptor<byte[], byte[]>> reachable = new HashMap<>();
        for (String procedure : exposed) {
            MethodDescriptor<byte[], byte[]> method = byName.get(Objects.requireNonNull(procedure, "procedure"));
            if (method == null) {
                throw new IllegalArgumentException("exposed procedure " + procedure
                        + " is not a method of the registered services");
            }
            reachable.put(procedure, method);
        }
        this.methods = Map.copyOf(reachable);
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        builder.executor(executor);
        try {
            this.server = builder.build().start();
        } catch (IOException e) {
            executor.close();
            throw new UncheckedIOException("starting the Connect dispatch server", e);
        }
        this.channel = InProcessChannelBuilder.forName(name)
                .executor(executor)
                .maxInboundMessageSize(limits.maxResponseMessageBytes())
                .build();
    }

    /**
     * The procedures this gateway exposes, as {@code package.Service/Method}.
     *
     * @return the procedure names
     */
    public Set<String> procedures() {
        return methods.keySet();
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            serve(exchange);
        }
    }

    private void serve(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getRawPath();
        if (!path.startsWith(basePath + "/")) {
            unaryError(exchange, Status.NOT_FOUND.withDescription("no Connect procedure at " + path), null);
            return;
        }
        String procedure = path.substring(basePath.length() + 1);
        MethodDescriptor<byte[], byte[]> method = methods.get(procedure);
        if (method == null) {
            // The protocol answers an unknown procedure with 404 and code unimplemented.
            sendError(exchange, 404, Status.UNIMPLEMENTED.withDescription("no procedure " + procedure), null);
            return;
        }
        MethodDescriptor.MethodType type = method.getType();
        if (type != MethodDescriptor.MethodType.UNARY && type != MethodDescriptor.MethodType.SERVER_STREAMING) {
            unaryError(exchange, Status.UNIMPLEMENTED.withDescription(procedure + " is a "
                    + type.name().toLowerCase(Locale.ROOT).replace('_', '-')
                    + " method; the gateway serves unary and server-streaming calls only"
                    + " (uploads use the HTTP upload route)"), null);
            return;
        }
        boolean streaming = type == MethodDescriptor.MethodType.SERVER_STREAMING;
        if (!"POST".equals(exchange.getRequestMethod())) {
            exchange.getResponseHeaders().set("Allow", "POST");
            // A protocol-level refusal: plain HTTP, not a Connect error record.
            sendPlain(exchange, 405, "Connect calls are POST; got " + exchange.getRequestMethod());
            return;
        }
        String expected = streaming ? STREAM_CONTENT_TYPE : UNARY_CONTENT_TYPE;
        String contentType = mediaType(exchange.getRequestHeaders().getFirst("Content-Type"));
        if (!expected.equals(contentType)) {
            exchange.getResponseHeaders().set("Accept-Post", expected);
            // A protocol-level refusal: plain HTTP 415, as the protocol specifies for an
            // unsupported codec, not a Connect error record.
            sendPlain(exchange, 415, procedure + " takes " + expected + "; got "
                    + (contentType == null ? "no content type" : contentType)
                    + ". The gateway carries binary protobuf only and does not transcode JSON");
            return;
        }
        Headers headers = exchange.getRequestHeaders();
        Status crossSite = crossSiteRefusal(headers);
        if (crossSite != null) {
            reject(exchange, streaming, crossSite);
            return;
        }
        Status refused = protocolRefusal(headers, streaming);
        if (refused != null) {
            if (refused.getCode() == Status.Code.UNIMPLEMENTED) {
                exchange.getResponseHeaders().set(streaming ? "Connect-Accept-Encoding" : "Accept-Encoding",
                        "identity");
            }
            reject(exchange, streaming, refused);
            return;
        }
        Deadline deadline;
        try {
            deadline = deadline(headers);
        } catch (IllegalArgumentException e) {
            reject(exchange, streaming, Status.INVALID_ARGUMENT.withDescription(e.getMessage()));
            return;
        }
        Metadata forwarded;
        try {
            forwarded = ConnectMetadata.requestMetadata(headers, CALL_KEY.name());
        } catch (IllegalArgumentException e) {
            reject(exchange, streaming, Status.INVALID_ARGUMENT.withDescription(e.getMessage()));
            return;
        }
        // Resolve the caller from headers alone before touching the body, so an
        // unauthenticated request is rejected without the gateway reading (and
        // buffering) anything it sent.
        AuthenticatedCaller caller;
        try {
            caller = callers.resolve(view(headers)).orElse(null);
        } catch (IllegalStateException e) {
            LOG.warn("Connect call {} refused: the credential store is unavailable", procedure, e);
            reject(exchange, streaming, Status.UNAVAILABLE.withDescription(
                    "credential store unavailable").withCause(e));
            return;
        }
        if (caller == null) {
            reject(exchange, streaming, Status.UNAUTHENTICATED.withDescription(
                    "missing, unknown or expired credential"));
            return;
        }
        byte[] request;
        try {
            request = streaming ? readEnvelope(exchange.getRequestBody()) : readBounded(exchange.getRequestBody());
        } catch (RequestRefused e) {
            reject(exchange, streaming, e.status);
            return;
        }
        CallOptions options = deadline == null ? CallOptions.DEFAULT : CallOptions.DEFAULT.withDeadline(deadline);
        if (streaming) {
            serverStream(exchange, method, options, caller, forwarded, request);
        } else {
            unary(exchange, method, options, caller, forwarded, request);
        }
    }

    // ------------------------------------------------------------------ calls

    private void unary(HttpExchange exchange, MethodDescriptor<byte[], byte[]> method, CallOptions options,
            AuthenticatedCaller caller, Metadata forwarded, byte[] request) throws IOException {
        CompletableFuture<Outcome> outcome = new CompletableFuture<>();
        AtomicReference<byte[]> response = new AtomicReference<>();
        AtomicReference<Metadata> responseHeaders = new AtomicReference<>();
        String id = UUID.randomUUID().toString();
        handoffs.put(id, caller);
        ClientCall<byte[], byte[]> call = channel.newCall(method, options);
        try {
            call.start(new ClientCall.Listener<>() {
                @Override
                public void onHeaders(Metadata metadata) {
                    responseHeaders.set(metadata);
                }

                @Override
                public void onMessage(byte[] message) {
                    response.set(message);
                }

                @Override
                public void onClose(Status status, Metadata trailers) {
                    outcome.complete(new Outcome(status, trailers));
                }
            }, handoff(forwarded, id));
            call.sendMessage(request);
            call.halfClose();
            call.request(2);
            Outcome done = await(outcome, call);
            // Connect carries unary trailers as headers with a trailer- prefix, on success and on error.
            ConnectMetadata.writeHeaders(responseHeaders.get(), exchange.getResponseHeaders(), "");
            ConnectMetadata.writeHeaders(done.trailers, exchange.getResponseHeaders(), "trailer-");
            if (!done.status.isOk()) {
                unaryError(exchange, done.status, done.trailers);
                return;
            }
            byte[] answer = response.get();
            if (answer == null) {
                unaryError(exchange, Status.INTERNAL.withDescription(
                        method.getFullMethodName() + " completed without a response message"), null);
                return;
            }
            exchange.getResponseHeaders().set("Content-Type", UNARY_CONTENT_TYPE);
            exchange.sendResponseHeaders(200, answer.length == 0 ? -1 : answer.length);
            if (answer.length > 0) {
                try (OutputStream out = exchange.getResponseBody()) {
                    out.write(answer);
                }
            }
        } finally {
            handoffs.remove(id);
        }
    }

    private void serverStream(HttpExchange exchange, MethodDescriptor<byte[], byte[]> method,
            CallOptions options, AuthenticatedCaller caller, Metadata forwarded, byte[] request) throws IOException {
        BlockingQueue<Object> events = new LinkedBlockingQueue<>();
        String id = UUID.randomUUID().toString();
        handoffs.put(id, caller);
        ClientCall<byte[], byte[]> call = channel.newCall(method, options);
        try {
            call.start(new ClientCall.Listener<>() {
                @Override
                public void onHeaders(Metadata metadata) {
                    events.add(new ResponseHeaders(metadata));
                }

                @Override
                public void onMessage(byte[] message) {
                    events.add(message);
                }

                @Override
                public void onClose(Status status, Metadata trailers) {
                    events.add(new Outcome(status, trailers));
                }
            }, handoff(forwarded, id));
            call.sendMessage(request);
            call.halfClose();
            call.request(1);
            OutputStream out = null;
            while (true) {
                Object event;
                try {
                    event = events.take();
                } catch (InterruptedException e) {
                    call.cancel("the gateway was interrupted", e);
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted while streaming " + method.getFullMethodName(), e);
                }
                try {
                    if (event instanceof ResponseHeaders responseHeaders) {
                        if (out != null) {
                            throw new IllegalStateException(method.getFullMethodName()
                                    + " sent response headers after its first message");
                        }
                        ConnectMetadata.writeHeaders(responseHeaders.metadata(), exchange.getResponseHeaders(), "");
                        out = startStream(exchange);
                    } else if (event instanceof byte[] message) {
                        if (out == null) {
                            out = startStream(exchange);
                        }
                        writeEnvelope(out, 0, message);
                        out.flush();
                        call.request(1);
                    } else {
                        Outcome done = (Outcome) event;
                        if (out == null) {
                            out = startStream(exchange);
                        }
                        writeEnvelope(out, FLAG_END_STREAM, ConnectErrors.endOfStream(done.status, done.trailers));
                        out.close();
                        return;
                    }
                } catch (IOException e) {
                    // The client went away: stop the service's work instead of streaming into nothing.
                    call.cancel("the Connect client disconnected", e);
                    throw e;
                }
            }
        } finally {
            handoffs.remove(id);
        }
    }

    /** Sends the stream's HTTP status and headers; the body follows chunked. */
    private static OutputStream startStream(HttpExchange exchange) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", STREAM_CONTENT_TYPE);
        exchange.sendResponseHeaders(200, 0);
        return exchange.getResponseBody();
    }

    /** The response headers a stream's service sent before its first message. */
    private record ResponseHeaders(Metadata metadata) {
    }

    /** The forwarded request metadata plus the key the dispatch server finds the caller by. */
    private static Metadata handoff(Metadata forwarded, String id) {
        forwarded.put(CALL_KEY, id);
        return forwarded;
    }

    private static Outcome await(CompletableFuture<Outcome> outcome, ClientCall<byte[], byte[]> call)
            throws IOException {
        try {
            return outcome.get();
        } catch (InterruptedException e) {
            call.cancel("the gateway was interrupted", e);
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while awaiting a unary answer", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("unary outcome failed", e.getCause());
        }
    }

    /** A finished call: its status and trailers. */
    private record Outcome(Status status, Metadata trailers) {
    }

    /** Puts the caller the gateway resolved for a call onto that call's gRPC context. */
    private final class CallerHandoff implements ServerInterceptor {
        @Override
        public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
                Metadata headers, ServerCallHandler<ReqT, RespT> next) {
            String id = headers.get(CALL_KEY);
            AuthenticatedCaller caller = id == null ? null : handoffs.remove(id);
            if (caller == null) {
                call.close(Status.UNAUTHENTICATED.withDescription("no caller was resolved for this call"),
                        new Metadata());
                return new ServerCall.Listener<>() { };
            }
            Context context = Context.current().withValues(
                    CallerContexts.CALLER, caller.caller(),
                    CallerContexts.AUTHENTICATED_CALLER, caller);
            return Contexts.interceptCall(context, call, headers, next);
        }
    }

    // ------------------------------------------------------------------ protocol

    /**
     * Refuses a browser request sent from another site. Browsers stamp {@code Sec-Fetch-Site}
     * on every request; {@code same-origin} is the console calling its own host and
     * {@code none} a user-initiated navigation. Clients that are not browsers send no such
     * header and pass.
     */
    private static Status crossSiteRefusal(Headers headers) {
        List<String> sites = headers.get("Sec-Fetch-Site");
        if (sites == null) {
            return null;
        }
        if (sites.size() == 1 && ("same-origin".equals(sites.getFirst()) || "none".equals(sites.getFirst()))) {
            return null;
        }
        return Status.PERMISSION_DENIED.withDescription("cross-site requests are refused");
    }

    /** The protocol version and compression checks; null when the request passes. */
    private static Status protocolRefusal(Headers headers, boolean streaming) {
        List<String> versions = headers.get("Connect-Protocol-Version");
        if (versions != null && (versions.size() != 1 || !"1".equals(versions.getFirst().trim()))) {
            return Status.INVALID_ARGUMENT.withDescription("connect-protocol-version must be 1");
        }
        List<String> encodings = headers.get(streaming ? "Connect-Content-Encoding" : "Content-Encoding");
        if (encodings != null && (encodings.size() != 1 || !"identity".equals(encodings.getFirst().trim()))) {
            return Status.UNIMPLEMENTED.withDescription("compressed requests are not accepted;"
                    + " send identity encoding");
        }
        return null;
    }

    /** The deadline named by {@code connect-timeout-ms}, or null when the request names none. */
    private static Deadline deadline(Headers headers) {
        List<String> values = headers.get("Connect-Timeout-Ms");
        if (values == null) {
            return null;
        }
        String value = values.size() == 1 ? values.getFirst().trim() : "";
        if (value.isEmpty() || value.length() > 10 || !value.chars().allMatch(c -> c >= '0' && c <= '9')) {
            throw new IllegalArgumentException("connect-timeout-ms must be a positive integer of at most 10 digits");
        }
        long millis = Long.parseLong(value);
        if (millis < 1) {
            throw new IllegalArgumentException("connect-timeout-ms must be a positive integer of at most 10 digits");
        }
        return Deadline.after(millis, TimeUnit.MILLISECONDS);
    }

    private byte[] readBounded(InputStream body) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buffer = new byte[8192];
        int read;
        while ((read = body.read(buffer)) != -1) {
            if (out.size() + read > limits.maxRequestBytes()) {
                throw new RequestRefused(Status.RESOURCE_EXHAUSTED.withDescription(
                        "request message exceeds " + limits.maxRequestBytes() + " bytes"));
            }
            out.write(buffer, 0, read);
        }
        return out.toByteArray();
    }

    /** The one request message of a server-streaming call, unwrapped from its envelope. */
    private byte[] readEnvelope(InputStream body) throws IOException {
        byte[] prefix = body.readNBytes(5);
        if (prefix.length < 5) {
            throw new RequestRefused(Status.UNIMPLEMENTED.withDescription(
                    "a server-streaming request is one enveloped message"));
        }
        int flags = prefix[0] & 0xff;
        if ((flags & FLAG_COMPRESSED) != 0) {
            // No compression was negotiated, so a compressed message is a protocol violation.
            throw new RequestRefused(Status.INTERNAL.withDescription(
                    "a compressed message arrived without a negotiated encoding"));
        }
        if (flags != 0) {
            throw new RequestRefused(Status.INVALID_ARGUMENT.withDescription(
                    "request envelope flags must be 0, got " + flags));
        }
        long length = ((prefix[1] & 0xffL) << 24) | ((prefix[2] & 0xff) << 16)
                | ((prefix[3] & 0xff) << 8) | (prefix[4] & 0xff);
        if (length > limits.maxRequestBytes()) {
            throw new RequestRefused(Status.RESOURCE_EXHAUSTED.withDescription(
                    "request message exceeds " + limits.maxRequestBytes() + " bytes"));
        }
        byte[] message = body.readNBytes((int) length);
        if (message.length < length) {
            throw new RequestRefused(Status.INVALID_ARGUMENT.withDescription(
                    "request envelope declares " + length + " bytes but carries " + message.length));
        }
        if (body.read() != -1) {
            throw new RequestRefused(Status.UNIMPLEMENTED.withDescription(
                    "a server-streaming request carries exactly one message"));
        }
        return message;
    }

    private static void writeEnvelope(OutputStream out, int flags, byte[] payload) throws IOException {
        int length = payload.length;
        out.write(new byte[] {(byte) flags, (byte) (length >>> 24), (byte) (length >>> 16),
                (byte) (length >>> 8), (byte) length});
        out.write(payload);
    }

    private static RequestHeaders view(Headers headers) {
        return name -> {
            List<String> values = headers.get(name);
            return values == null ? List.of() : List.copyOf(values);
        };
    }

    private static String mediaType(String contentType) {
        if (contentType == null) {
            return null;
        }
        int semicolon = contentType.indexOf(';');
        String base = semicolon < 0 ? contentType : contentType.substring(0, semicolon);
        return base.trim().toLowerCase(Locale.ROOT);
    }

    /** A refusal before dispatch: unary as an HTTP error, streaming as an end-of-stream error. */
    private static void reject(HttpExchange exchange, boolean streaming, Status status) throws IOException {
        if (!streaming) {
            unaryError(exchange, status, null);
            return;
        }
        exchange.getResponseHeaders().set("Content-Type", STREAM_CONTENT_TYPE);
        exchange.sendResponseHeaders(200, 0);
        try (OutputStream out = exchange.getResponseBody()) {
            writeEnvelope(out, FLAG_END_STREAM, ConnectErrors.endOfStream(status, null));
        }
    }

    private static void unaryError(HttpExchange exchange, Status status, Metadata trailers) throws IOException {
        sendError(exchange, ConnectErrors.httpStatus(status.getCode()), status, trailers);
    }

    private static void sendPlain(HttpExchange exchange, int httpStatus, String message) throws IOException {
        byte[] body = message.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
        exchange.sendResponseHeaders(httpStatus, body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
        }
    }

    private static void sendError(HttpExchange exchange, int httpStatus, Status status, Metadata trailers)
            throws IOException {
        byte[] body = ConnectErrors.errorRecord(status, trailers);
        exchange.getResponseHeaders().set("Content-Type", ERROR_CONTENT_TYPE);
        exchange.sendResponseHeaders(httpStatus, body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
        }
    }

    /** A request refused while it is read, carrying the status to answer with. */
    private static final class RequestRefused extends IOException {
        private final transient Status status;

        private RequestRefused(Status status) {
            super(status.getDescription());
            this.status = status;
        }
    }

    /** Stops the dispatch server and cancels any call still running; a second close is a no-op. */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        channel.shutdownNow();
        server.shutdownNow();
        try {
            if (!channel.awaitTermination(10, TimeUnit.SECONDS) || !server.awaitTermination(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("the Connect dispatch server did not stop within 10 seconds");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while stopping the Connect dispatch server", e);
        } finally {
            executor.close();
        }
    }
}
