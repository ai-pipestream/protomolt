package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.blob.spi.ObjectReclaimer;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/** Recreates real provider bytes after cleanup, then faults the next exact-key cleanup call. */
public final class DocumentCleanupRetryProbe {
    private static final class InjectedCleanupFailure extends RuntimeException { }
    public static void run(Tx tx,String generation,BlobStore store,ObjectReclaimer real,Path saved) throws Exception {
        var coordinates=new Properties();
        try (var input=Files.newInputStream(saved.resolve("lost-put.properties"))) { coordinates.load(input); }
        String namespace=coordinates.getProperty("namespace"),key=coordinates.getProperty("key");
        UUID attempt=tx.readOnly(em -> (UUID)em.createNativeQuery("""
                SELECT a.attempt_id FROM document_part_attempts a
                JOIN document_part_attempt_objects o ON o.attempt_id=a.attempt_id
                WHERE a.backend_generation=:generation AND a.storage_namespace=:namespace AND o.object_key=:key
                """).setParameter("generation",generation).setParameter("namespace",namespace).setParameter("key",key).getSingleResult());
        require("ABSENT".equals(state(tx,attempt)[0]),"initial durable absence");
        byte[] bytes=Files.readAllBytes(saved.resolve("lost-put-body.bin"));
        require(DocumentPartCodec.sha256Hex(bytes).equals(coordinates.getProperty("sha256")),"saved original payload digest");
        // Models provider reappearance; this is not a claim of an in-flight network PUT surviving JVM exit.
        store.put(new BlobStore.PutSpec(namespace,key,coordinates.getProperty("contentType"),Map.of(),
                coordinates.getProperty("sha256")),bytes);
        require(Arrays.equals(store.getBounded(namespace,key,null,1024*1024).data(),bytes),"real bytes reappeared");
        var failure=new InjectedCleanupFailure(); var failOnce=new AtomicBoolean(true);
        var profile=new ManagedBackendLedger(tx).find(generation).orElseThrow();
        var recovery=new DocumentAttemptRecovery(new DocumentAttemptCleanupLedger(tx),(original,retained) -> {
            require(original.equals(generation) && retained.equals(profile),"original cleanup backend identity");
            return (bucket,objectKey) -> {
                require(bucket.equals(namespace),"original namespace");
                if (objectKey.equals(key) && failOnce.compareAndSet(true,false)) throw failure;
                return real.reclaim(bucket,objectKey);
            };
        });
        var failed=recovery.recover(attempt,Duration.ofSeconds(10));
        require(failed.outcome()==DocumentAttemptRecovery.Outcome.RETRY && failed.failure()==failure,"provider failure surfaced");
        var pending=state(tx,attempt);
        require("DELETING".equals(pending[0]) && "Cleanup failed: InjectedCleanupFailure".equals(pending[1])
                && pending[2]==null,"durable retry without false absence");
        require(Arrays.equals(store.getBounded(namespace,key,null,1024*1024).data(),bytes),"failed cleanup preserves uncertain bytes");
        var retried=recovery.recover(attempt,Duration.ofSeconds(10));
        require(retried.outcome()==DocumentAttemptRecovery.Outcome.ABSENT && retried.failure()==null,"real cleanup retry succeeds");
        var completed=state(tx,attempt);
        require("ABSENT".equals(completed[0]) && completed[1]==null && completed[2]!=null,"durable confirmed absence");
        try { store.getBounded(namespace,key,null,1024*1024); throw new AssertionError("Reappeared bytes remain"); }
        catch (BlobStore.BlobNotFoundException expected) { }
        System.out.println("DOCUMENT_CLEANUP_PROVIDER_RETRY_OK");
    }
    private static Object[] state(Tx tx,UUID attempt) {
        return tx.readOnly(em -> (Object[])em.createNativeQuery("""
                SELECT state,last_error,absence_observed_at FROM document_part_attempt_cleanup WHERE attempt_id=:attempt
                """).setParameter("attempt",attempt).getSingleResult());
    }
    private static void require(boolean condition,String message) { if (!condition) throw new AssertionError(message); }
}
