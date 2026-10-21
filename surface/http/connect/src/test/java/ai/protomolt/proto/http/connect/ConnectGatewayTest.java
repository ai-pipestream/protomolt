package ai.protomolt.proto.http.connect;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.authz.AuthenticatedCaller;
import ai.protomolt.proto.authz.RequestCallerResolver;
import ai.protomolt.proto.authz.grpc.CallerContexts;
import com.google.protobuf.Any;
import com.google.protobuf.Int32Value;
import com.google.protobuf.StringValue;
import com.sun.net.httpserver.HttpServer;
import io.grpc.ForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The gateway over a real JDK {@code HttpServer}, driven byte for byte the way a Connect client
 * speaks: binary protobuf bodies, enveloped streams, the protocol's headers and error records.
 */
class ConnectGatewayTest {

    private static final String OPERATOR = "synthetic-operator-token";
    private static final String SCOPED = "synthetic-scoped-token";
    private static final String OUTAGE = "synthetic-outage-token";
    private static final String SERVICE = "test.v1.Echo";

    private static final MethodDescriptor<StringValue, StringValue> SAY =
            MethodDescriptor.newBuilder(ProtoUtils.marshaller(StringValue.getDefaultInstance()),
                            ProtoUtils.marshaller(StringValue.getDefaultInstance()))
                    .setFullMethodName(SERVICE + "/Say").setType(MethodDescriptor.MethodType.UNARY).build();
    private static final MethodDescriptor<Int32Value, Int32Value> COUNT =
            MethodDescriptor.newBuilder(ProtoUtils.marshaller(Int32Value.getDefaultInstance()),
                            ProtoUtils.marshaller(Int32Value.getDefaultInstance()))
                    .setFullMethodName(SERVICE + "/Count").setType(MethodDescriptor.MethodType.SERVER_STREAMING)
                    .build();
    private static final MethodDescriptor<StringValue, StringValue> COLLECT =
            MethodDescriptor.newBuilder(ProtoUtils.marshaller(StringValue.getDefaultInstance()),
                            ProtoUtils.marshaller(StringValue.getDefaultInstance()))
                    .setFullMethodName(SERVICE + "/Collect").setType(MethodDescriptor.MethodType.CLIENT_STREAMING)
                    .build();

    private final AtomicInteger invocations = new AtomicInteger();
    private final AtomicReference<Metadata> seen = new AtomicReference<>();
    private static final Metadata.Key<String> WANT = Metadata.Key.of("x-want-metadata", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> ANSWER = Metadata.Key.of("x-answer", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> DONE = Metadata.Key.of("x-done", Metadata.ASCII_STRING_MARSHALLER);

    /**
     * Records each call's request metadata, and when asked by {@code x-want-metadata} answers a
     * response header and a trailer, as a service with its own metadata would.
     */
    private final class MetadataProbe implements ServerInterceptor {
        @Override
        public <Q, R> ServerCall.Listener<Q> interceptCall(ServerCall<Q, R> call, Metadata headers,
                ServerCallHandler<Q, R> next) {
            seen.set(headers);
            if (!headers.containsKey(WANT)) {
                return next.startCall(call, headers);
            }
            return next.startCall(new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
                @Override
                public void sendHeaders(Metadata response) {
                    response.put(ANSWER, "42");
                    super.sendHeaders(response);
                }

                @Override
                public void close(Status status, Metadata trailers) {
                    trailers.put(DONE, "yes");
                    super.close(status, trailers);
                }
            }, headers);
        }
    }
    private final CountDownLatch cancelled = new CountDownLatch(1);

    private ConnectGateway gateway;
    private HttpServer http;
    private String base;
    private final HttpClient client = HttpClient.newHttpClient();

    private ServerServiceDefinition echo() {
        return ServerServiceDefinition.builder(SERVICE)
                .addMethod(SAY, ServerCalls.asyncUnaryCall((request, response) -> {
                    invocations.incrementAndGet();
                    String value = request.getValue();
                    if (value.equals("fail")) {
                        com.google.rpc.Status rich = com.google.rpc.Status.newBuilder()
                                .setCode(Status.Code.FAILED_PRECONDITION.value())
                                .setMessage("drive \"intake\" is sealed")
                                .addDetails(Any.pack(StringValue.of("detail-bytes")))
                                .build();
                        response.onError(StatusProto.toStatusRuntimeException(rich));
                        return;
                    }
                    if (value.equals("slow")) {
                        ((ServerCallStreamObserver<StringValue>) response).setOnCancelHandler(cancelled::countDown);
                        return;
                    }
                    response.onNext(StringValue.of("hello " + value + " as " + CallerContexts.current().name()));
                    response.onCompleted();
                }))
                .addMethod(COUNT, ServerCalls.asyncServerStreamingCall((request, response) -> {
                    invocations.incrementAndGet();
                    int n = request.getValue();
                    if (n == -1) {
                        forever((ServerCallStreamObserver<Int32Value>) response);
                        return;
                    }
                    if (n == -3) {
                        response.onError(StatusProto.toStatusRuntimeException(com.google.rpc.Status.newBuilder()
                                .setCode(Status.Code.ABORTED.value())
                                .setMessage("revision moved")
                                .addDetails(Any.pack(Int32Value.of(7)))
                                .build()));
                        return;
                    }
                    if (n == -2) {
                        response.onNext(Int32Value.of(1));
                        response.onError(Status.INTERNAL.withDescription("index went away").asRuntimeException());
                        return;
                    }
                    for (int i = 1; i <= n; i++) {
                        response.onNext(Int32Value.of(i));
                    }
                    response.onCompleted();
                }))
                .addMethod(COLLECT, ServerCalls.asyncClientStreamingCall(response -> new StreamObserver<>() {
                    @Override public void onNext(StringValue value) { }
                    @Override public void onError(Throwable t) { }
                    @Override public void onCompleted() { response.onCompleted(); }
                }))
                .build();
    }

    private void forever(ServerCallStreamObserver<Int32Value> response) {
        response.setOnCancelHandler(cancelled::countDown);
        Thread.ofVirtual().start(() -> {
            int i = 0;
            while (!response.isCancelled()) {
                response.onNext(Int32Value.of(++i));
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    return;
                }
            }
        });
    }

    private static RequestCallerResolver resolver() {
        RequestCallerResolver outage = headers -> {
            if (headers.values("api_token").contains(OUTAGE)) {
                throw new IllegalStateException("synthetic identity provider outage");
            }
            return Optional.empty();
        };
        return RequestCallerResolver.firstOf(List.of(outage, RequestCallerResolver.bearer(OPERATOR,
                token -> SCOPED.equals(token)
                        ? Optional.of(Caller.scoped("alice", Set.of()))
                        : Optional.empty())));
    }

    @BeforeEach
    void start() throws IOException {
        gateway = new ConnectGateway(List.of(ServerInterceptors.intercept(echo(), new MetadataProbe())),
                Set.of(SERVICE + "/Say", SERVICE + "/Count", SERVICE + "/Collect"),
                resolver(), new ConnectLimits(64, 1 << 20));
        http = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        http.createContext(ConnectGateway.PATH, gateway);
        http.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        http.start();
        base = "http://127.0.0.1:" + http.getAddress().getPort();
    }

    @AfterEach
    void stop() {
        http.stop(0);
        gateway.close();
    }

    private HttpResponse<byte[]> post(String procedure, String contentType, byte[] body, String... headers)
            throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(base + "/connect/" + procedure))
                .header("Content-Type", contentType)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body));
        for (int i = 0; i < headers.length; i += 2) {
            request.header(headers[i], headers[i + 1]);
        }
        return client.send(request.build(), HttpResponse.BodyHandlers.ofByteArray());
    }

    private HttpResponse<byte[]> say(String value, String... headers) throws Exception {
        return post(SERVICE + "/Say", ConnectGateway.UNARY_CONTENT_TYPE,
                StringValue.of(value).toByteArray(), headers);
    }

    private static String text(HttpResponse<byte[]> response) {
        return new String(response.body(), StandardCharsets.UTF_8);
    }

    private static byte[] envelope(int flags, byte[] payload) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(flags);
        out.write(payload.length >>> 24);
        out.write(payload.length >>> 16);
        out.write(payload.length >>> 8);
        out.write(payload.length);
        out.writeBytes(payload);
        return out.toByteArray();
    }

    private record Frame(int flags, byte[] payload) {
        String text() {
            return new String(payload, StandardCharsets.UTF_8);
        }
    }

    private static List<Frame> frames(byte[] body) throws IOException {
        List<Frame> frames = new ArrayList<>();
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(body));
        while (in.available() > 0) {
            int flags = in.readUnsignedByte();
            byte[] payload = new byte[in.readInt()];
            in.readFully(payload);
            frames.add(new Frame(flags, payload));
        }
        return frames;
    }

    // ------------------------------------------------------------------ unary

    @Test
    void aUnaryCallAnswersBinaryProtobufAsTheResolvedCaller() throws Exception {
        HttpResponse<byte[]> operator = say("x", "api_token", OPERATOR, "Connect-Protocol-Version", "1");
        assertThat(operator.statusCode()).isEqualTo(200);
        assertThat(operator.headers().firstValue("Content-Type")).hasValue("application/proto");
        assertThat(StringValue.parseFrom(operator.body()).getValue())
                .isEqualTo("hello x as " + Caller.operator().name());

        HttpResponse<byte[]> scoped = say("x", "Authorization", "Bearer " + SCOPED);
        assertThat(StringValue.parseFrom(scoped.body()).getValue()).isEqualTo("hello x as alice");
    }

    @Test
    void aRequestWithoutAUsableCredentialIsUnauthenticatedAndReachesNoService() throws Exception {
        for (HttpResponse<byte[]> response : List.of(say("x"), say("x", "api_token", "unknown-token"))) {
            assertThat(response.statusCode()).isEqualTo(401);
            assertThat(response.headers().firstValue("Content-Type")).hasValue("application/json");
            assertThat(text(response)).isEqualTo(
                    "{\"code\":\"unauthenticated\",\"message\":\"missing, unknown or expired credential\"}");
        }
        assertThat(invocations).hasValue(0);
    }

    @Test
    void aCredentialStoreOutageIsUnavailableNotUnauthenticated() throws Exception {
        HttpResponse<byte[]> response = say("x", "api_token", OUTAGE);

        assertThat(response.statusCode()).isEqualTo(503);
        assertThat(text(response)).contains("\"code\":\"unavailable\"");
        assertThat(invocations).hasValue(0);
    }

    @Test
    void jsonIsRefusedNotTranscoded() throws Exception {
        for (String json : List.of("application/json", "application/connect+json", "application/json; charset=utf-8")) {
            HttpResponse<byte[]> response = post(SERVICE + "/Say", json,
                    "{\"value\":\"x\"}".getBytes(StandardCharsets.UTF_8), "api_token", OPERATOR);
            assertThat(response.statusCode()).as(json).isEqualTo(415);
            assertThat(response.headers().firstValue("Accept-Post")).hasValue("application/proto");
            assertThat(text(response)).contains("does not transcode JSON");
        }
        HttpResponse<byte[]> wrongFraming = post(SERVICE + "/Count", "application/proto",
                Int32Value.of(1).toByteArray(), "api_token", OPERATOR);
        assertThat(wrongFraming.statusCode()).isEqualTo(415);
        assertThat(wrongFraming.headers().firstValue("Accept-Post")).hasValue("application/connect+proto");
        assertThat(invocations).hasValue(0);
    }

    @Test
    void unknownProceduresAndClientStreamingAreRefusedByName() throws Exception {
        HttpResponse<byte[]> unknown = post(SERVICE + "/Nope", "application/proto", new byte[0], "api_token", OPERATOR);
        assertThat(unknown.statusCode()).isEqualTo(404);
        assertThat(text(unknown)).isEqualTo(
                "{\"code\":\"unimplemented\",\"message\":\"no procedure test.v1.Echo/Nope\"}");

        HttpResponse<byte[]> collect = post(SERVICE + "/Collect", "application/proto", new byte[0],
                "api_token", OPERATOR);
        assertThat(collect.statusCode()).isEqualTo(501);
        assertThat(text(collect)).contains("test.v1.Echo/Collect is a client-streaming method");

        HttpResponse<byte[]> get = client.send(HttpRequest.newBuilder(
                URI.create(base + "/connect/" + SERVICE + "/Say")).GET().build(), HttpResponse.BodyHandlers.ofByteArray());
        assertThat(get.statusCode()).isEqualTo(405);
        assertThat(get.headers().firstValue("Allow")).hasValue("POST");
    }

    @Test
    void serviceErrorsMapToConnectCodesWithTheirDetails() throws Exception {
        HttpResponse<byte[]> response = say("fail", "api_token", OPERATOR);

        assertThat(response.statusCode()).isEqualTo(400);
        String body = text(response);
        String value = Base64.getEncoder().withoutPadding().encodeToString(
                StringValue.of("detail-bytes").toByteArray());
        assertThat(body).isEqualTo("{\"code\":\"failed_precondition\",\"message\":\"drive \\\"intake\\\" is sealed\","
                + "\"details\":[{\"type\":\"google.protobuf.StringValue\",\"value\":\"" + value + "\"}]}");
    }

    @Test
    void connectTimeoutBecomesTheDeadlineAndCancelsTheService() throws Exception {
        HttpResponse<byte[]> response = say("slow", "api_token", OPERATOR, "Connect-Timeout-Ms", "100");

        assertThat(response.statusCode()).isEqualTo(504);
        assertThat(text(response)).contains("\"code\":\"deadline_exceeded\"");
        assertThat(cancelled.await(10, TimeUnit.SECONDS)).isTrue();

        for (String bad : List.of("abc", "0", "-5", "12345678901")) {
            HttpResponse<byte[]> refused = say("x", "api_token", OPERATOR, "Connect-Timeout-Ms", bad);
            assertThat(refused.statusCode()).as(bad).isEqualTo(400);
            assertThat(text(refused)).contains("invalid_argument", "connect-timeout-ms");
        }
    }

    @Test
    void oversizedCompressedOrWrongVersionRequestsAreRefused() throws Exception {
        HttpResponse<byte[]> large = say("x".repeat(100), "api_token", OPERATOR);
        assertThat(large.statusCode()).isEqualTo(429);
        assertThat(text(large)).contains("resource_exhausted", "64 bytes");

        HttpResponse<byte[]> gzip = say("x", "api_token", OPERATOR, "Content-Encoding", "gzip");
        assertThat(gzip.statusCode()).isEqualTo(501);
        assertThat(gzip.headers().firstValue("Accept-Encoding")).hasValue("identity");

        HttpResponse<byte[]> version = say("x", "api_token", OPERATOR, "Connect-Protocol-Version", "2");
        assertThat(version.statusCode()).isEqualTo(400);
        assertThat(invocations).hasValue(0);
    }

    @Test
    void aMethodLeftOffTheExposedListIsUnreachable() throws Exception {
        try (ConnectGateway narrow = new ConnectGateway(List.of(echo()), Set.of(SERVICE + "/Count"),
                resolver(), new ConnectLimits(64, 1 << 20))) {
            http.removeContext(ConnectGateway.PATH);
            http.createContext(ConnectGateway.PATH, narrow);
            HttpResponse<byte[]> response = say("x", "api_token", OPERATOR);

            assertThat(response.statusCode()).isEqualTo(404);
            assertThat(text(response)).contains("unimplemented", "no procedure test.v1.Echo/Say");
            assertThat(invocations).hasValue(0);
            assertThat(narrow.procedures()).containsExactly(SERVICE + "/Count");
        }
    }

    @Test
    void crossSiteBrowserRequestsAreRefusedBeforeAnyCredentialIsRead() throws Exception {
        for (String site : List.of("cross-site", "same-site")) {
            HttpResponse<byte[]> refused = say("x", "api_token", OPERATOR, "Sec-Fetch-Site", site);
            assertThat(refused.statusCode()).as(site).isEqualTo(403);
            assertThat(text(refused)).contains("permission_denied", "cross-site");
        }
        assertThat(frames(count(1, "api_token", OPERATOR, "Sec-Fetch-Site", "cross-site").body()))
                .singleElement().satisfies(frame -> assertThat(frame.text()).contains("permission_denied"));
        assertThat(invocations).hasValue(0);

        assertThat(say("x", "api_token", OPERATOR, "Sec-Fetch-Site", "same-origin").statusCode()).isEqualTo(200);
        assertThat(say("x", "api_token", OPERATOR, "Sec-Fetch-Site", "none").statusCode()).isEqualTo(200);
    }

    @Test
    void customHeadersReachTheServiceButCredentialsAndProtocolHeadersDoNot() throws Exception {
        byte[] token = {1, 2, 3, (byte) 0xff};
        HttpResponse<byte[]> response = say("x", "api_token", OPERATOR, "Cookie", "session=secret",
                "X-Trace", "abc", "X-Token-Bin", Base64.getEncoder().encodeToString(token),
                "Connect-Timeout-Ms", "5000", "Sec-Fetch-Site", "same-origin");
        assertThat(response.statusCode()).isEqualTo(200);
        Metadata headers = seen.get();
        assertThat(headers.get(Metadata.Key.of("x-trace", Metadata.ASCII_STRING_MARSHALLER))).isEqualTo("abc");
        assertThat(headers.get(Metadata.Key.of("x-token-bin", Metadata.BINARY_BYTE_MARSHALLER))).isEqualTo(token);
        assertThat(headers.keys()).doesNotContain("api_token", "authorization", "cookie", "content-type",
                "connect-timeout-ms", "sec-fetch-site");
    }

    @Test
    void responseHeadersAndTrailersComeBackOnUnaryAndStreams() throws Exception {
        HttpResponse<byte[]> unary = say("x", "api_token", OPERATOR, "X-Want-Metadata", "1");
        assertThat(unary.statusCode()).isEqualTo(200);
        assertThat(unary.headers().firstValue("x-answer")).hasValue("42");
        assertThat(unary.headers().firstValue("trailer-x-done")).hasValue("yes");

        HttpResponse<byte[]> failed = say("fail", "api_token", OPERATOR, "X-Want-Metadata", "1");
        assertThat(failed.statusCode()).isEqualTo(400);
        assertThat(failed.headers().firstValue("trailer-x-done")).hasValue("yes");
        assertThat(failed.headers().map().keySet()).noneMatch(name -> name.toLowerCase(Locale.ROOT)
                .startsWith("trailer-grpc"));

        HttpResponse<byte[]> stream = count(2, "api_token", OPERATOR, "X-Want-Metadata", "1");
        assertThat(stream.headers().firstValue("x-answer")).hasValue("42");
        assertThat(frames(stream.body()).getLast().text()).isEqualTo("{\"metadata\":{\"x-done\":[\"yes\"]}}");
    }

    @Test
    void reservedOrMalformedForwardedHeadersAreRefused() throws Exception {
        HttpResponse<byte[]> reserved = say("x", "api_token", OPERATOR, "protomolt-connect-call", "forged");
        assertThat(reserved.statusCode()).isEqualTo(400);
        assertThat(text(reserved)).contains("invalid_argument", "reserved");

        HttpResponse<byte[]> notBase64 = say("x", "api_token", OPERATOR, "X-Token-Bin", "!!!");
        assertThat(notBase64.statusCode()).isEqualTo(400);
        assertThat(text(notBase64)).contains("invalid_argument", "x-token-bin");
        assertThat(invocations).hasValue(0);
    }

    // ------------------------------------------------------------------ server streaming

    private HttpResponse<byte[]> count(int n, String... headers) throws Exception {
        return post(SERVICE + "/Count", ConnectGateway.STREAM_CONTENT_TYPE,
                envelope(0, Int32Value.of(n).toByteArray()), headers);
    }

    @Test
    void aServerStreamCarriesEnvelopedMessagesAndAnEmptyEndRecord() throws Exception {
        HttpResponse<byte[]> response = count(3, "api_token", OPERATOR);

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.headers().firstValue("Content-Type")).hasValue("application/connect+proto");
        List<Frame> frames = frames(response.body());
        assertThat(frames).hasSize(4);
        for (int i = 0; i < 3; i++) {
            assertThat(frames.get(i).flags()).isZero();
            assertThat(Int32Value.parseFrom(frames.get(i).payload()).getValue()).isEqualTo(i + 1);
        }
        assertThat(frames.get(3).flags()).isEqualTo(2);
        assertThat(frames.get(3).text()).isEqualTo("{}");
    }

    @Test
    void streamFailuresArriveInTheEndRecord() throws Exception {
        List<Frame> midStream = frames(count(-2, "api_token", OPERATOR).body());
        assertThat(midStream).hasSize(2);
        assertThat(Int32Value.parseFrom(midStream.get(0).payload()).getValue()).isEqualTo(1);
        assertThat(midStream.get(1).text())
                .isEqualTo("{\"error\":{\"code\":\"internal\",\"message\":\"index went away\"}}");

        String detail = Base64.getEncoder().withoutPadding().encodeToString(Int32Value.of(7).toByteArray());
        assertThat(frames(count(-3, "api_token", OPERATOR).body())).singleElement()
                .satisfies(frame -> assertThat(frame.text()).isEqualTo(
                        "{\"error\":{\"code\":\"aborted\",\"message\":\"revision moved\","
                                + "\"details\":[{\"type\":\"google.protobuf.Int32Value\",\"value\":\""
                                + detail + "\"}]}}"));

        HttpResponse<byte[]> anonymous = count(3);
        assertThat(anonymous.statusCode()).isEqualTo(200);
        assertThat(frames(anonymous.body())).singleElement()
                .satisfies(frame -> assertThat(frame.text()).contains("\"code\":\"unauthenticated\""));

        byte[] trailing = envelope(0, Int32Value.of(1).toByteArray());
        HttpResponse<byte[]> extra = post(SERVICE + "/Count", ConnectGateway.STREAM_CONTENT_TYPE,
                Arrays.copyOf(trailing, trailing.length + 1), "api_token", OPERATOR);
        assertThat(frames(extra.body())).singleElement()
                .satisfies(frame -> assertThat(frame.text()).contains("unimplemented", "exactly one message"));

        HttpResponse<byte[]> compressed = post(SERVICE + "/Count", ConnectGateway.STREAM_CONTENT_TYPE,
                envelope(1, Int32Value.of(1).toByteArray()), "api_token", OPERATOR);
        assertThat(frames(compressed.body())).singleElement()
                .satisfies(frame -> assertThat(frame.text()).contains("internal", "without a negotiated encoding"));
        assertThat(invocations).hasValue(2);
    }

    @Test
    void aClientThatGoesAwayCancelsTheStream() throws Exception {
        byte[] body = envelope(0, Int32Value.of(-1).toByteArray());
        try (Socket socket = new Socket("127.0.0.1", http.getAddress().getPort())) {
            OutputStream out = socket.getOutputStream();
            out.write(("POST /connect/" + SERVICE + "/Count HTTP/1.1\r\n"
                    + "Host: 127.0.0.1\r\n"
                    + "Content-Type: application/connect+proto\r\n"
                    + "api_token: " + OPERATOR + "\r\n"
                    + "Content-Length: " + body.length + "\r\n\r\n").getBytes(StandardCharsets.US_ASCII));
            out.write(body);
            out.flush();
            InputStream in = socket.getInputStream();
            byte[] seen = in.readNBytes(256);
            assertThat(new String(seen, StandardCharsets.US_ASCII)).startsWith("HTTP/1.1 200");
        }

        assertThat(cancelled.await(10, TimeUnit.SECONDS)).as("service saw the cancellation").isTrue();
    }

    // ------------------------------------------------------------------ construction

    @Test
    void theGatewayRefusesNoServicesDuplicateMethodsAndNonPositiveLimits() {
        RequestCallerResolver callers = headers -> Optional.of(AuthenticatedCaller.unbound(Caller.operator()));
        ConnectLimits limits = new ConnectLimits(1, 1);

        Set<String> say = Set.of(SERVICE + "/Say");
        assertThatThrownBy(() -> new ConnectGateway(List.of(), say, callers, limits))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ConnectGateway(List.of(echo(), echo()), say, callers, limits))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("two services register test.v1.Echo/");
        assertThatThrownBy(() -> new ConnectGateway(List.of(echo()), Set.of(), callers, limits))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ConnectGateway(List.of(echo()), Set.of(SERVICE + "/Missing"), callers, limits))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("test.v1.Echo/Missing");
        assertThatThrownBy(() -> new ConnectLimits(0, 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ConnectLimits(1, 0)).isInstanceOf(IllegalArgumentException.class);
        assertThat(gateway.procedures()).containsExactlyInAnyOrder(
                "test.v1.Echo/Say", "test.v1.Echo/Count", "test.v1.Echo/Collect");
    }
}
