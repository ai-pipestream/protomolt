package ai.protomolt.proto.http.connect.conformance;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import connectrpc.conformance.v1.ConformanceServiceGrpc;
import connectrpc.conformance.v1.Service.ConformancePayload;
import connectrpc.conformance.v1.Service.Error;
import connectrpc.conformance.v1.Service.Header;
import connectrpc.conformance.v1.Service.IdempotentUnaryRequest;
import connectrpc.conformance.v1.Service.IdempotentUnaryResponse;
import connectrpc.conformance.v1.Service.ServerStreamRequest;
import connectrpc.conformance.v1.Service.ServerStreamResponse;
import connectrpc.conformance.v1.Service.StreamResponseDefinition;
import connectrpc.conformance.v1.Service.UnaryRequest;
import connectrpc.conformance.v1.Service.UnaryResponse;
import connectrpc.conformance.v1.Service.UnaryResponseDefinition;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Deadline;
import io.grpc.ForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;

import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * {@code connectrpc.conformance.v1.ConformanceService} as the conformance suite specifies it
 * for a server under test: each call answers what its response definition asks for (headers,
 * trailers, data or an error, a delay) and reports back the request as the service received it
 * (headers, deadline, the request message). Client-streaming and bidirectional calls stay
 * unimplemented; the gateway refuses them before they get here.
 */
final class ConformanceService extends ConformanceServiceGrpc.ConformanceServiceImplBase {

    private static final Context.Key<Metadata> REQUEST_HEADERS = Context.key("conformance-request-headers");
    private static final Context.Key<Metadata> RESPONSE_HEADERS = Context.key("conformance-response-headers");
    private static final Context.Key<Metadata> RESPONSE_TRAILERS = Context.key("conformance-response-trailers");

    @Override
    public void unary(UnaryRequest request, StreamObserver<UnaryResponse> response) {
        answerUnary(request.getResponseDefinition(), request, request.hasResponseDefinition(),
                payload -> UnaryResponse.newBuilder().setPayload(payload).build(), response);
    }

    @Override
    public void idempotentUnary(IdempotentUnaryRequest request, StreamObserver<IdempotentUnaryResponse> response) {
        answerUnary(request.getResponseDefinition(), request, request.hasResponseDefinition(),
                payload -> IdempotentUnaryResponse.newBuilder().setPayload(payload).build(), response);
    }

    @Override
    public void serverStream(ServerStreamRequest request, StreamObserver<ServerStreamResponse> response) {
        StreamResponseDefinition definition = request.getResponseDefinition();
        respondWith(definition.getResponseHeadersList(), definition.getResponseTrailersList());
        ConformancePayload.RequestInfo info = requestInfo(request);
        boolean first = true;
        for (ByteString data : definition.getResponseDataList()) {
            pause(definition.getResponseDelayMs());
            ConformancePayload.Builder payload = ConformancePayload.newBuilder().setData(data);
            if (first) {
                payload.setRequestInfo(info);
            }
            response.onNext(ServerStreamResponse.newBuilder().setPayload(payload).build());
            first = false;
        }
        if (definition.hasError()) {
            response.onError(failure(definition.getError(), first ? info : null));
            return;
        }
        response.onCompleted();
    }

    private interface Wrap<R> {
        R of(ConformancePayload payload);
    }

    private static <R> void answerUnary(UnaryResponseDefinition definition, Message request, boolean defined,
            Wrap<R> wrap, StreamObserver<R> response) {
        respondWith(definition.getResponseHeadersList(), definition.getResponseTrailersList());
        ConformancePayload.RequestInfo info = requestInfo(request);
        pause(definition.getResponseDelayMs());
        if (defined && definition.hasError()) {
            response.onError(failure(definition.getError(), info));
            return;
        }
        response.onNext(wrap.of(ConformancePayload.newBuilder()
                .setData(definition.getResponseData())
                .setRequestInfo(info)
                .build()));
        response.onCompleted();
    }

    private static void respondWith(List<Header> headers, List<Header> trailers) {
        copy(headers, RESPONSE_HEADERS.get());
        copy(trailers, RESPONSE_TRAILERS.get());
    }

    private static void copy(List<Header> from, Metadata to) {
        for (Header header : from) {
            String name = header.getName().toLowerCase(Locale.ROOT);
            for (String value : header.getValueList()) {
                if (name.endsWith(Metadata.BINARY_HEADER_SUFFIX)) {
                    to.put(Metadata.Key.of(name, Metadata.BINARY_BYTE_MARSHALLER), Base64.getDecoder().decode(value));
                } else {
                    to.put(Metadata.Key.of(name, Metadata.ASCII_STRING_MARSHALLER), value);
                }
            }
        }
    }

    /** What the service saw of the call: headers, the deadline and the request. */
    private static ConformancePayload.RequestInfo requestInfo(Message request) {
        ConformancePayload.RequestInfo.Builder info = ConformancePayload.RequestInfo.newBuilder()
                .addRequests(Any.pack(request));
        Metadata headers = REQUEST_HEADERS.get();
        Base64.Encoder base64 = Base64.getEncoder().withoutPadding();
        for (String name : headers.keys()) {
            Header.Builder header = Header.newBuilder().setName(name);
            if (name.endsWith(Metadata.BINARY_HEADER_SUFFIX)) {
                for (byte[] value : headers.getAll(Metadata.Key.of(name, Metadata.BINARY_BYTE_MARSHALLER))) {
                    header.addValue(base64.encodeToString(value));
                }
            } else {
                for (String value : headers.getAll(Metadata.Key.of(name, Metadata.ASCII_STRING_MARSHALLER))) {
                    header.addValue(value);
                }
            }
            info.addRequestHeaders(header);
        }
        Deadline deadline = Context.current().getDeadline();
        if (deadline != null) {
            info.setTimeoutMs(deadline.timeRemaining(TimeUnit.MILLISECONDS));
        }
        return info.build();
    }

    private static StatusRuntimeException failure(Error error, ConformancePayload.RequestInfo info) {
        com.google.rpc.Status.Builder status = com.google.rpc.Status.newBuilder()
                .setCode(error.getCodeValue())
                .setMessage(error.getMessage())
                .addAllDetails(error.getDetailsList());
        if (info != null) {
            status.addDetails(Any.pack(info));
        }
        return StatusProto.toStatusRuntimeException(status.build());
    }

    private static void pause(int millis) {
        if (millis <= 0) {
            return;
        }
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Status.CANCELLED.withDescription("interrupted during the response delay").withCause(e)
                    .asRuntimeException();
        }
    }

    /**
     * Captures each call's request headers for {@link #requestInfo} and sends the response
     * headers and trailers a definition asks for, headers included when a call fails before its
     * first message.
     */
    static final class CallMetadata implements ServerInterceptor {
        @Override
        public <Q, R> ServerCall.Listener<Q> interceptCall(ServerCall<Q, R> call, Metadata headers,
                ServerCallHandler<Q, R> next) {
            Metadata responseHeaders = new Metadata();
            Metadata responseTrailers = new Metadata();
            ServerCall<Q, R> forwarding = new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
                private boolean headersSent;

                @Override
                public void sendHeaders(Metadata metadata) {
                    metadata.merge(responseHeaders);
                    headersSent = true;
                    super.sendHeaders(metadata);
                }

                @Override
                public void close(Status status, Metadata trailers) {
                    if (!headersSent && responseHeaders.keys().size() > 0) {
                        sendHeaders(new Metadata());
                    }
                    trailers.merge(responseTrailers);
                    super.close(status, trailers);
                }
            };
            Context context = Context.current().withValues(REQUEST_HEADERS, headers,
                    RESPONSE_HEADERS, responseHeaders, RESPONSE_TRAILERS, responseTrailers);
            return Contexts.interceptCall(context, forwarding, headers, next);
        }
    }
}
