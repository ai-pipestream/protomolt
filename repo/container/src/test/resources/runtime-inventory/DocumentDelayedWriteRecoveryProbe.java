package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.*;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** Delivers the parent's original queued request only after durable attempt cleanup. */
public final class DocumentDelayedWriteRecoveryProbe {
    public static void run(Tx tx,String generation,BlobStore store,ObjectReclaimer real,Path saved) throws Exception {
        var coordinates=new Properties();
        try (var input=Files.newInputStream(saved.resolve("delayed-put.properties"))) { coordinates.load(input); }
        String namespace=coordinates.getProperty("namespace"),key=coordinates.getProperty("key");
        UUID attempt=tx.readOnly(em -> (UUID)em.createNativeQuery("""
                SELECT a.attempt_id FROM document_part_attempts a
                JOIN document_part_attempt_objects o ON o.attempt_id=a.attempt_id
                WHERE a.backend_generation=:generation AND a.storage_namespace=:namespace AND o.object_key=:key
                """).setParameter("generation",generation).setParameter("namespace",namespace).setParameter("key",key).getSingleResult());
        var current=ai.protomolt.proto.repo.v1.PublishDocumentResponse.parseFrom(Files.readAllBytes(saved.resolve("receipt-next.pb")))
                .getCommitted().getMembers(0);
        var previous=ai.protomolt.proto.repo.v1.PublishDocumentResponse.parseFrom(Files.readAllBytes(saved.resolve("receipt.pb")))
                .getCommitted().getMembers(0);
        long overlap=tx.readOnly(em -> ((Number)em.createNativeQuery("""
                SELECT count(*) FROM document_part_attempt_objects o
                JOIN document_revision_parts p ON p.object_id=o.physical_object_id
                WHERE p.revision_id IN (:previous,:current) AND o.storage_namespace=:namespace AND o.object_key=:key
                """).setParameter("previous",UUID.fromString(previous.getRevisionId()))
                .setParameter("current",UUID.fromString(current.getRevisionId()))
                .setParameter("namespace",namespace).setParameter("key",key).getSingleResult()).longValue());
        require(overlap==0,"delayed key differs from both committed revisions");
        requireCurrent(tx,attempt,current);
        var profile=new ManagedBackendLedger(tx).find(generation).orElseThrow();
        var recovery=new DocumentAttemptRecovery(new DocumentAttemptCleanupLedger(tx),(original,retained) -> {
            require(original.equals(generation) && retained.equals(profile),"original delayed-write backend identity");
            return real;
        });
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(420);
        while (true) {
            var result=recovery.recover(attempt,Duration.ofSeconds(10));
            if (result.outcome()==DocumentAttemptRecovery.Outcome.ABSENT) break;
            require(result.outcome()==DocumentAttemptRecovery.Outcome.NOT_CLAIMED,"unexpected initial cleanup outcome: "+result);
            if (System.nanoTime()>=deadline) throw new AssertionError("Delayed attempt did not become reclaimable");
            Thread.sleep(1000);
        }
        requireAbsent(store,namespace,key);
        requireUnpublished(tx,attempt,key);
        requireDurableAbsence(tx,attempt);
        requireCurrent(tx,attempt,current);
        Path control=Path.of(System.getenv("PROTOMOLT_TEST_DELAYED_CONTROL"));
        Files.createFile(control.resolve("deliver"));
        long deliveryDeadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(15);
        while (!Files.exists(control.resolve("delivered"))) {
            if (System.nanoTime()>=deliveryDeadline) throw new AssertionError("Parent did not deliver original queued Redis request");
            Thread.sleep(25);
        }
        byte[] expected=Files.readAllBytes(saved.resolve("delayed-put-body.bin"));
        require(DocumentPartCodec.sha256Hex(expected).equals(coordinates.getProperty("sha256")),"original delayed body identity");
        require(Arrays.equals(store.getBounded(namespace,key,null,1024*1024).data(),expected),"original queued request reached Redis after ABSENT");
        requireDurableAbsence(tx,attempt);
        requireUnpublished(tx,attempt,key);
        requireCurrent(tx,attempt,current);
        var retried=recovery.recover(attempt,Duration.ofSeconds(10));
        require(retried.outcome()==DocumentAttemptRecovery.Outcome.ABSENT && retried.failure()==null,"late-write reclamation");
        requireAbsent(store,namespace,key);
        requireDurableAbsence(tx,attempt);
        requireUnpublished(tx,attempt,key);
        requireCurrent(tx,attempt,current);
        System.out.println("DOCUMENT_DELAYED_WRITE_RECOVERY_OK");
    }
    private static void requireUnpublished(Tx tx,UUID attempt,String key) {
        var row=tx.readOnly(em -> (Object[])em.createNativeQuery("""
                SELECT o.verified,
                    EXISTS(SELECT 1 FROM repository_object_references r WHERE r.object_id=o.physical_object_id),
                    EXISTS(SELECT 1 FROM document_part_publication_history h WHERE h.attempt_id=o.attempt_id),
                    o.etag,o.provider_version
                FROM document_part_attempt_objects o WHERE o.attempt_id=:attempt AND o.object_key=:key
                """).setParameter("attempt",attempt).setParameter("key",key).getSingleResult());
        require(Boolean.FALSE.equals(row[0]) && Boolean.FALSE.equals(row[1]) && Boolean.FALSE.equals(row[2])
                && row[3]==null && row[4]==null,
                "late bytes remain unverified and unreferenced");
    }
    private static void requireCurrent(Tx tx,UUID attempt,ai.protomolt.proto.repo.v1.DocumentPublishedRevision expected) {
        var row=tx.readOnly(em -> (Object[])em.createNativeQuery("""
                SELECT p.revision_id,d.mutation_revision FROM document_part_attempts a
                JOIN document_revision_current p ON p.node_id=a.node_id
                JOIN documents d ON d.node_id=a.node_id WHERE a.attempt_id=:attempt
                """).setParameter("attempt",attempt).getSingleResult());
        require(UUID.fromString(expected.getRevisionId()).equals(row[0])
                && ((Number)row[1]).longValue()==expected.getMutationRevision(),"current revision unchanged by delayed write and recovery");
    }
    private static void requireDurableAbsence(Tx tx,UUID attempt) {
        var row=tx.readOnly(em -> (Object[])em.createNativeQuery("""
                SELECT state,absence_observed_at,last_error FROM document_part_attempt_cleanup WHERE attempt_id=:attempt
                """).setParameter("attempt",attempt).getSingleResult());
        require("ABSENT".equals(row[0]) && row[1]!=null && row[2]==null,"durable absence receipt");
    }
    private static void requireAbsent(BlobStore store,String namespace,String key) {
        try { store.getBounded(namespace,key,null,1024*1024); throw new AssertionError("Unpublished bytes remain"); }
        catch (BlobStore.BlobNotFoundException expected) { }
    }
    private static void require(boolean condition,String message) { if (!condition) throw new AssertionError(message); }
}
