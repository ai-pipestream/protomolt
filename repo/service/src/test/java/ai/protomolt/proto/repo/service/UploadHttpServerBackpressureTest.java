package ai.protomolt.proto.repo.service;

import static org.assertj.core.api.Assertions.assertThat;
import ai.protomolt.proto.repo.archive.ArchiveBinding;
import ai.protomolt.proto.repo.archive.DocumentArchive;
import ai.protomolt.proto.repo.archive.v1.VersioningPolicy;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v2.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v2.GetOperationOutcomeRequest;
import ai.protomolt.proto.repo.v2.GetOperationOutcomeResponse;
import ai.protomolt.proto.repo.v2.GetDocumentRequest;
import ai.protomolt.proto.repo.v2.GetDocumentResponse;
import ai.protomolt.proto.repo.v2.ListRevisionsRequest;
import ai.protomolt.proto.repo.v2.ListRevisionsResponse;
import ai.protomolt.proto.repo.v2.UploadDocumentRequest;
import ai.protomolt.proto.repo.v2.UploadDocumentResponse;
import io.grpc.Channel;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.ForwardingClientCall;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class UploadHttpServerBackpressureTest {
    @Test void httpMustNotQueueWholeBodyWhenDocumentConsumerStopsRequesting() throws Exception {
        int bodySize = 8 * 1024 * 1024;
        var queuedBytes = new AtomicLong();
        var allQueued = new CountDownLatch(1);
        var headerReceived = new CountDownLatch(1);
        var downstream = new java.util.concurrent.atomic.AtomicReference<StreamObserver<UploadDocumentResponse>>();
        String name = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder.forName(name).addService(new DocumentServiceGrpc.DocumentServiceImplBase() {
            @Override public void getOperationOutcome(GetOperationOutcomeRequest q, StreamObserver<GetOperationOutcomeResponse> r) {
                r.onNext(GetOperationOutcomeResponse.getDefaultInstance()); r.onCompleted();
            }
            @Override public void getDocument(GetDocumentRequest q, StreamObserver<GetDocumentResponse> r) {
                r.onError(Status.NOT_FOUND.asRuntimeException());
            }
            @Override public void listRevisions(ListRevisionsRequest q, StreamObserver<ListRevisionsResponse> r) {
                r.onNext(ListRevisionsResponse.getDefaultInstance()); r.onCompleted();
            }
            @Override public StreamObserver<UploadDocumentRequest> uploadDocument(StreamObserver<UploadDocumentResponse> r) {
                var control = (ServerCallStreamObserver<UploadDocumentResponse>) r;
                control.disableAutoRequest(); control.request(1); downstream.set(r);
                return new StreamObserver<>() {
                    @Override public void onNext(UploadDocumentRequest q) {
                        if (!q.hasHeader()) return;
                        // Accept the header so data may flow; then stop requesting frames.
                        r.onNext(UploadDocumentResponse.newBuilder()
                                .setAccepted(ai.protomolt.proto.repo.v2.UploadAccepted.getDefaultInstance()).build());
                        headerReceived.countDown();
                    }
                    @Override public void onError(Throwable t) {}
                    @Override public void onCompleted() {}
                };
            }
        }).build().start();
        ManagedChannel channel = InProcessChannelBuilder.forName(name).build();
        Channel counted = ClientInterceptors.intercept(channel, new ClientInterceptor() {
            @Override public <Q,S> ClientCall<Q,S> interceptCall(MethodDescriptor<Q,S> method, CallOptions options, Channel next) {
                return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, options)) {
                    @Override public void sendMessage(Q value) {
                        super.sendMessage(value);
                        if (value instanceof UploadDocumentRequest q && q.hasData()
                                && queuedBytes.addAndGet(q.getData().size()) >= bodySize) allQueued.countDown();
                    }
                };
            }
        });
        var archive = new DocumentArchive(counted, (account, key) -> Optional.of(new ArchiveBinding(
                account, key, "ds-test", "", VersioningPolicy.VERSIONING_POLICY_RETAINED)), Clock.systemUTC());
        CompletableFuture<HttpResponse<String>> response = null;
        try (var http = new UploadHttpServer(null, () -> new RepositoryCaller("operator", true), "test-key", archive);
                var client = HttpClient.newHttpClient()) {
            int port = http.start(0);
            var request = HttpRequest.newBuilder(URI.create("http://localhost:" + port
                    + UploadHttpServer.ARCHIVE_UPLOAD_PATH
                    + "?account_id=acct-test&archive=reports&entry_id=test&operation_id=" + UUID.randomUUID()))
                    .header("api_token", "test-key").header("Content-Type", "application/octet-stream")
                    .POST(HttpRequest.BodyPublishers.ofByteArray(new byte[bodySize])).build();
            response = client.sendAsync(request, HttpResponse.BodyHandlers.ofString());
            try {
                assertThat(headerReceived.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(allQueued.await(3, TimeUnit.SECONDS))
                        .as("all %s bytes queued while downstream requested only the header", queuedBytes.get()).isFalse();
                assertThat(queuedBytes.get()).isLessThanOrEqualTo(64 * 1024);
                downstream.get().onError(Status.UNAVAILABLE.withDescription("store stopped").asRuntimeException());
                assertThat(response.get(5, TimeUnit.SECONDS).statusCode()).isEqualTo(503);
            } finally {
                response.cancel(true);
                channel.shutdownNow(); server.shutdownNow();
            }
        } finally {
            if (response != null) response.cancel(true);
            channel.shutdownNow(); server.shutdownNow();
        }
    }
}
