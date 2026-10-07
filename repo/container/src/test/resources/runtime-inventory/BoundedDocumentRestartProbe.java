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
        try (var database=new ai.protomolt.proto.repo.container.ledger.LedgerDatabase(config.ledger());
             var host=new RepoServices(config,BridgeEngine.standard(),BlobStores.discover(),
                new HistoricalReadAccess(auth -> caller,32L*1024*1024,2),offline,null,options.journaled(),
                new BoundedDocumentProfile(1024*1024,64L*1024*1024))) {
            host.services();
            awaitCleanup(host,new ai.protomolt.proto.repo.container.ledger.Tx(database.entityManagerFactory()),saved);
            ai.protomolt.proto.repo.container.ledger.DocumentCleanupRetentionProbe.run(
                    new ai.protomolt.proto.repo.container.ledger.Tx(database.entityManagerFactory()),generation);
            for (String suffix:java.util.List.of("","-next")) {
            var request=PublishDocumentRequest.parseFrom(Files.readAllBytes(saved.resolve("request"+suffix+".pb")));
            var receipt=PublishDocumentResponse.parseFrom(Files.readAllBytes(saved.resolve("receipt"+suffix+".pb")));
            var document=Document.parseFrom(Files.readAllBytes(saved.resolve("document"+suffix+".pb")));
            if (!host.publicationRepository().publishDocument(caller,request,RepositoryReadControl.NONE).equals(receipt))
                throw new AssertionError("Restart changed durable receipt");
            BoundedDocumentHostProbe.verifyHistoryTransport(host,caller,document,receipt.getCommitted().getMembers(0));
            }
        }
        System.out.println("BOUNDED_DOCUMENT_RESTART_OK");
    }
    private static void awaitCleanup(RepoServices host,ai.protomolt.proto.repo.container.ledger.Tx tx,Path saved) throws Exception {
        var coordinates=new java.util.Properties();
        try (var input=Files.newInputStream(saved.resolve("lost-put.properties"))) { coordinates.load(input); }
        String namespace=coordinates.getProperty("namespace"),key=coordinates.getProperty("key");
        long deadline=System.nanoTime()+java.util.concurrent.TimeUnit.SECONDS.toNanos(420);
        long report=0;
        while (true) {
            var row=tx.readOnly(em -> (Object[])em.createNativeQuery("""
                    SELECT a.attempt_id,c.state,a.lease_until > clock_timestamp(),
                        EXISTS(SELECT 1 FROM document_part_publication_history h WHERE h.attempt_id=a.attempt_id)
                    FROM document_part_attempts a
                    JOIN document_part_attempt_objects o ON o.attempt_id=a.attempt_id
                    LEFT JOIN document_part_attempt_cleanup c ON c.attempt_id=a.attempt_id
                    WHERE a.storage_namespace=:namespace AND o.object_key=:key
                    """).setParameter("namespace",namespace).setParameter("key",key).getSingleResult());
            if ((Boolean)row[3]) throw new AssertionError("Failed upload entered publication history");
            if ("ABSENT".equals(row[1])) break;
            if (System.nanoTime()>=deadline) throw new AssertionError("Host did not reclaim failed upload after lease expiry: "+row[1]);
            if (System.nanoTime()>=report) {
                System.out.println("BOUNDED_CLEANUP_WAIT lease_live="+row[2]+" state="+row[1]);
                report=System.nanoTime()+java.util.concurrent.TimeUnit.SECONDS.toNanos(30);
            }
            Thread.sleep(1000);
        }
        try {
            host.blobStore().getBounded(namespace,key,null,1024*1024);
            throw new AssertionError("Cleanup left uncommitted Redis bytes");
        } catch (ai.protomolt.proto.repo.blob.spi.BlobStore.BlobNotFoundException expected) { }
        System.out.println("BOUNDED_DOCUMENT_ORPHAN_CLEANUP_OK");
    }
}
