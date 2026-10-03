package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.*;
import ai.protomolt.proto.repo.blob.s3.S3BackendIdentity;
import ai.protomolt.proto.repo.codec.*;
import ai.protomolt.proto.repo.v1.Document;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;

/** Forked fault worker. halt deliberately bypasses every finally block and shutdown hook. */
public final class DocumentCrashWorker {
    public static void main(String[] args) throws Exception {
        String mode = args[0];
        UUID attempt = UUID.fromString(args[1]), node = UUID.fromString(args[2]);
        var env = System.getenv();
        try (var database = new LedgerDatabase(new LedgerConfig(env.get("TEST_DB_URL"), env.get("TEST_DB_USER"), env.get("TEST_DB_PASSWORD")));
             var opened = BlobStores.discover().open("s3", Map.of("endpoint", env.get("TEST_ENDPOINT"), "region", env.get("TEST_REGION"),
                     "access-key", env.get("TEST_ACCESS"), "secret-key", env.get("TEST_SECRET"), "path-style", "true", "conditional-writes", "false"))) {
            var tx = new Tx(database.entityManagerFactory());
            String generation = env.get("TEST_GENERATION"), namespace = env.get("TEST_NAMESPACE");
            var identity = S3BackendIdentity.of(env.get("TEST_ENDPOINT"), env.get("TEST_REGION"), true);
            var parts = DocumentPartCodec.split(Document.newBuilder().setDocId(node.toString())
                    .setSearchMetadata(ai.protomolt.proto.repo.v1.SearchMetadata.newBuilder().addSemanticResults(
                            ai.protomolt.proto.repo.v1.SemanticProcessingResult.newBuilder().setResultId("crash-chunks"))).build(), PartLayouts.document());
            String prefix = "documents/account/" + node + "/attempts/" + attempt + "/";
            var objects = parts.stream().map(p -> new DocumentPartAttemptLedger.PlannedObject(p.part(), p.subKey(),
                    DocumentPartCodec.objectKey(prefix, p.part(), p.subKey()), p.bytes().length, p.sha256(), "application/protobuf")).toList();
            var plan = new DocumentPartAttemptLedger.Plan(attempt,
                    new DocumentPartAttemptLedger.Location(node, "account", generation, namespace), 0, Map.of(), objects);
            BlobStore intercepted = (BlobStore) java.lang.reflect.Proxy.newProxyInstance(BlobStore.class.getClassLoader(),
                    new Class<?>[] {BlobStore.class}, (proxy, method, arguments) -> {
                        Object result;
                        try { result = method.invoke(opened.store(), arguments); }
                        catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
                        if (mode.equals("put") && method.getName().equals("put")) Runtime.getRuntime().halt(71);
                        return result;
                    });
            var borrowed = new OpenedBlobStore(intercepted, () -> {}, opened.capabilities(), opened::ensureNamespace, opened.reclaimer());
            try (var stager = new DocumentPartStager(tx, generation, identity, borrowed, 256L * 1024 * 1024, 1)) {
                stager.stage(plan, parts, Duration.ofSeconds(1), Map.of());
            }
            if (!mode.equals("cleanup")) throw new IllegalArgumentException("Unknown crash point");
            tx.readOnly(em -> em.createNativeQuery("""
                    SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM lease_until-clock_timestamp()))+0.05)
                    FROM document_part_attempts WHERE attempt_id=:id
                    """).setParameter("id", attempt).getSingleResult());
            var claim = new DocumentAttemptCleanupLedger(tx).claim(attempt, Duration.ofSeconds(1)).orElseThrow();
            if (!opened.reclaimer().reclaim(namespace, claim.keys().getFirst())) throw new IllegalStateException("Deletion did not confirm absence");
            Runtime.getRuntime().halt(73);
        }
    }
}
