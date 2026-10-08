package ai.protomolt.repo.consumer;

import ai.protomolt.proto.asset.bridge.BridgeEngine;
import ai.protomolt.proto.repo.service.*;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import io.grpc.*;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** Out-of-package consumer: public assembly, discovered provider, actual typed publication and history. */
public final class BoundedDocumentPublicConsumer {
    public record Fixture(PublishDocumentRequest request,Document expected) {}
    @FunctionalInterface public interface Bootstrap { Fixture prepare(RepoServices host) throws Exception; }
    public static void run(RepoServiceConfig config,ManagedSchemaAccess schemas,ManagedPublicationOptions publication,
            RepositoryCaller caller,Bootstrap bootstrap,boolean rpc) throws Exception {
        long started = System.nanoTime();
        phase(rpc, "host-build-start", started);
        var history=rpc ? new HistoricalReadAccess(auth -> caller,32L*1024*1024,2) : null;
        try (var host=RepoServices.buildBoundedDocuments(config,BridgeEngine.standard(),history,schemas,publication,
                new BoundedDocumentOptions(1024*1024,64L*1024*1024))) {
            phase(rpc, "host-built", started);
            require(host.services().size()==(rpc ? 2 : 0),"only explicitly selected RPCs mounted");
            unavailable(host::repository); unavailable(host::archiveRepository); unavailable(host::archiveMutationRepository);
            unavailable(host::driveRepository); unavailable(() -> host.startHttp(0,"fixture-token"));
            phase(rpc, "fixture-prepare-start", started);
            var fixture=bootstrap.prepare(host);
            phase(rpc, "fixture-prepared", started);
            ManagedChannel channel=null;
            try {
                PublishDocumentResponse result;
                Channel authenticated=null;
                if (rpc) {
                    String name="public-bounded-"+UUID.randomUUID();
                    host.startInProcess(name,"public-bounded-fixture-token",null);
                    channel=io.grpc.inprocess.InProcessChannelBuilder.forName(name).build();
                    var raw=DocumentPublicationServiceGrpc.newBlockingStub(channel).withDeadlineAfter(10,TimeUnit.SECONDS);
                    unauthenticated(() -> raw.publishDocument(fixture.request()));
                    var wrong=DocumentPublicationServiceGrpc.newBlockingStub(withToken(channel,"wrong-fixture-token"))
                            .withDeadlineAfter(10,TimeUnit.SECONDS);
                    unauthenticated(() -> wrong.publishDocument(fixture.request()));
                    authenticated=withToken(channel,"public-bounded-fixture-token");
                    phase(rpc, "typed-publication-start", started);
                    result=DocumentPublicationServiceGrpc.newBlockingStub(authenticated).withDeadlineAfter(10,TimeUnit.SECONDS)
                            .publishDocument(fixture.request());
                    phase(rpc, "typed-publication-complete", started);
                } else {
                    phase(rpc, "typed-publication-start", started);
                    result=host.publicationRepository().publishDocument(caller,fixture.request(),RepositoryReadControl.NONE);
                    phase(rpc, "typed-publication-complete", started);
                }
                require(result.hasCommitted() && result.getCommitted().getMembersCount()==1,"public consumer committed typed document");
                phase(rpc, "receipt-replay-start", started);
                require(host.publicationRepository().publishDocument(caller,fixture.request(),RepositoryReadControl.NONE).equals(result),
                        "library returns exact committed receipt");
                phase(rpc, "receipt-replay-complete", started);
                var revision=result.getCommitted().getMembers(0);
                phase(rpc, "history-read-start", started);
                try (var read=host.historicalRepository().readValidated(caller,revision.getAddress(),
                        UUID.fromString(revision.getRevisionId()),RepositoryReadControl.NONE)) {
                    require(read.document().equals(fixture.expected()),"library decodes retained typed history");
                    read.authorizeDelivery(RepositoryReadControl.NONE);
                }
                phase(rpc, "history-read-complete", started);
                if (rpc) {
                    phase(rpc, "rpc-replay-start", started);
                    require(DocumentPublicationServiceGrpc.newBlockingStub(authenticated).withDeadlineAfter(10,TimeUnit.SECONDS)
                            .publishDocument(fixture.request()).equals(result),"remote exact receipt replay");
                    var read=DocumentHistoryServiceGrpc.newBlockingStub(authenticated).withDeadlineAfter(10,TimeUnit.SECONDS)
                            .readRevision(ReadRevisionRequest.newBuilder().setAddress(revision.getAddress()).setRevisionId(revision.getRevisionId())
                                    .setMode(HistoricalDocumentReadMode.HISTORICAL_DOCUMENT_READ_MODE_VALIDATED).build());
                    require(read.getValidated().getDocument().equals(fixture.expected()),"RPC decodes retained typed history");
                    phase(rpc, "rpc-history-read-complete", started);
                }
            } finally {
                if (channel!=null) {
                    channel.shutdownNow();
                    require(channel.awaitTermination(5,TimeUnit.SECONDS),"public consumer channel drained");
                }
            }
            phase(rpc, "host-close-start", started);
        }
        phase(rpc, "host-closed", started);
        System.out.println(rpc ? "BOUNDED_PUBLIC_CONSUMER_RPC_OK" : "BOUNDED_PUBLIC_CONSUMER_LIBRARY_OK");
    }
    private static void phase(boolean rpc, String name, long started) {
        System.out.printf("PHASE name=consumer-%s-%s elapsed_ms=%d%n", rpc ? "rpc" : "library", name,
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started));
        System.out.flush();
    }
    private static Channel withToken(Channel channel,String token) {
        var headers=new Metadata();
        headers.put(Metadata.Key.of("api_token",Metadata.ASCII_STRING_MARSHALLER),token);
        return ClientInterceptors.intercept(channel,io.grpc.stub.MetadataUtils.newAttachHeadersInterceptor(headers));
    }
    private static void unauthenticated(Runnable call) {
        try { call.run(); throw new AssertionError("Unauthenticated consumer publication accepted"); }
        catch (StatusRuntimeException failure) { require(failure.getStatus().getCode()==Status.Code.UNAUTHENTICATED,"authentication refused"); }
    }
    private static void unavailable(Runnable call) {
        try { call.run(); throw new AssertionError("Unbounded API exposed to consumer"); }
        catch (UnsupportedOperationException expected) { }
    }
    private static void require(boolean condition,String message) { if (!condition) throw new AssertionError(message); }
}
