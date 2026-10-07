package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.asset.bridge.BridgeEngine;
import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.blob.spi.BlobStores;
import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import java.nio.file.*;
import java.time.Duration;

/** Fresh JVM after a real Redis restart; no live schema registry is available. */
public final class BoundedDocumentRestartProbe {
    public static void main(String[] args) throws Exception {
        Path saved=Path.of(args[1]);
        String generation=Files.readString(saved.resolve("generation"));
        var request=PublishDocumentRequest.parseFrom(Files.readAllBytes(saved.resolve("request.pb")));
        var receipt=PublishDocumentResponse.parseFrom(Files.readAllBytes(saved.resolve("receipt.pb")));
        var document=Document.parseFrom(Files.readAllBytes(saved.resolve("document.pb")));
        var config=new RepoServiceConfig(0,new LedgerConfig(System.getenv("PROTOMOLT_TEST_JDBC"),
                System.getenv("PROTOMOLT_TEST_USER"),System.getenv("PROTOMOLT_TEST_PASSWORD")),
                "http://127.0.0.1:1","us-east-1","unused","unused","bounded-document",0,
                "redis",null,null,System.getenv("PROTOMOLT_TEST_REDIS_URI"),0,1024*1024)
                .withManagedStorage(new ManagedStoragePolicy(generation,"bounded-document-realm",true));
        var caller=new RepositoryCaller("operator",true);
        ManagedSchemaAccess offline=new ManagedSchemaAccess() {
            public DocumentSchemaAdmission.Resolution open(RepositoryCaller caller,DocumentPublicationMember member,RepositoryReadControl control) {
                throw new AssertionError("Restart replay attempted live schema resolution");
            }
            public void close() { }
            public boolean awaitIdle(Duration timeout) { return true; }
        };
        var options=new ManagedPublicationOptions(Path.of(args[0]),Duration.ofMinutes(5),Duration.ofSeconds(5),
                (account,principal,operation) -> caller).withTransport(
                new ManagedPublicationOptions.Transport(auth -> caller,32L*1024*1024,2));
        try (var host=new RepoServices(config,BridgeEngine.standard(),BlobStores.discover(),
                new HistoricalReadAccess(auth -> caller,32L*1024*1024,2),offline,null,options.journaled(),
                new BoundedDocumentProfile(1024*1024,64L*1024*1024))) {
            if (!host.publicationRepository().publishDocument(caller,request,RepositoryReadControl.NONE).equals(receipt))
                throw new AssertionError("Restart changed durable receipt");
            BoundedDocumentHostProbe.verifyHistoryTransport(host,caller,document,receipt.getCommitted().getMembers(0));
        }
        System.out.println("BOUNDED_DOCUMENT_RESTART_OK");
    }
}
