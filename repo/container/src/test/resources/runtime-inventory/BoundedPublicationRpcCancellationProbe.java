package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.container.ledger.Tx;
import ai.protomolt.proto.repo.v1.*;
import io.grpc.*;
import io.grpc.stub.*;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** Operator-token transport cancellation; scoped-key authorization is a separate qualification. */
final class BoundedPublicationRpcCancellationProbe {
    static void run(RepoServices host,Tx tx,BoundedPublicationShutdownProbe.PutGate gate,
            AtomicInteger closes,CountDownLatch serverCancelled) throws Exception {
        String name="bounded-publication-cancellation-"+UUID.randomUUID();
        host.startInProcess(name,"publication-cancellation-fixture-token",null);
        var channel=io.grpc.inprocess.InProcessChannelBuilder.forName(name).build();
        var headers=new Metadata();
        headers.put(Metadata.Key.of("api_token",Metadata.ASCII_STRING_MARSHALLER),"publication-cancellation-fixture-token");
        var authenticated=ClientInterceptors.intercept(channel,MetadataUtils.newAttachHeadersInterceptor(headers));
        var service=(DocumentPublicationGrpcService)host.services().stream()
                .filter(value -> value instanceof DocumentPublicationGrpcService).findFirst().orElseThrow();
        var fixture=BoundedDocumentHostProbe.prepare(host,tx);
        var next=BoundedDocumentHostProbe.prepare(host,tx);
        var cancelled=new CountDownLatch(1);
        var failure=new AtomicReference<Throwable>();
        var response=new AtomicReference<PublishDocumentResponse>();
        var call=new AtomicReference<ClientCallStreamObserver<PublishDocumentRequest>>();
        gate.armed.set(true);
        try {
            DocumentPublicationServiceGrpc.newStub(authenticated).withDeadlineAfter(30,TimeUnit.SECONDS)
                    .publishDocument(fixture.request(),new ClientResponseObserver<PublishDocumentRequest,PublishDocumentResponse>() {
                        public void beforeStart(ClientCallStreamObserver<PublishDocumentRequest> request) { call.set(request); }
                        public void onNext(PublishDocumentResponse value) { response.set(value); }
                        public void onError(Throwable error) { failure.set(error); cancelled.countDown(); }
                        public void onCompleted() { cancelled.countDown(); }
                    });
            require(gate.entered.await(10,TimeUnit.SECONDS),"authenticated call reached real Redis PUT");
            gate.verifyBytes();
            call.get().cancel("fixture cancels accepted publication",null);
            require(cancelled.await(5,TimeUnit.SECONDS) && response.get()==null
                    && failure.get()!=null && Status.fromThrowable(failure.get()).getCode()==Status.Code.CANCELLED,
                    "client receives cancellation without a receipt");
            require(serverCancelled.await(5,TimeUnit.SECONDS),"cancellation reached server context");
            require(gate.exited.getCount()==1 && !service.awaitIdle(Duration.ZERO) && closes.get()==0,
                    "cancelled RPC retains active producer and host resources");
            var blocking=DocumentPublicationServiceGrpc.newBlockingStub(authenticated).withDeadlineAfter(10,TimeUnit.SECONDS);
            try { blocking.publishDocument(next.request()); throw new AssertionError("RPC capacity released before producer exit"); }
            catch (StatusRuntimeException expected) { require(expected.getStatus().getCode()==Status.Code.RESOURCE_EXHAUSTED,
                    "second call refused by occupied transport capacity: "+expected); }
            BoundedPublicationShutdownProbe.assertUncommitted(tx,fixture.request());
            BoundedPublicationShutdownProbe.assertUncommitted(tx,next.request());
            gate.release.countDown();
            require(gate.exited.await(5,TimeUnit.SECONDS) && service.awaitIdle(Duration.ofSeconds(10)),
                    "producer and transport release after actual provider exit");
            BoundedPublicationShutdownProbe.assertUncommitted(tx,fixture.request());
            gate.verifyBytes();
            var committed=blocking.publishDocument(next.request());
            require(committed.hasCommitted(),"new operation succeeds after cancelled call drains");
            require(blocking.publishDocument(next.request()).equals(committed),"new operation replays exact receipt");
            require(service.awaitIdle(Duration.ofSeconds(5)),"successful replay delivery finished");
            host.close(Duration.ofSeconds(5));
            require(closes.get()==1,"RPC host releases provider once after all calls finish");
        } finally {
            gate.release.countDown();
            channel.shutdownNow();
            require(channel.awaitTermination(5,TimeUnit.SECONDS),"cancellation channel stopped");
        }
        System.out.println("BOUNDED_PUBLICATION_RPC_CANCELLATION_OK");
    }
    private static void require(boolean condition,String message) { if (!condition) throw new AssertionError(message); }
}
