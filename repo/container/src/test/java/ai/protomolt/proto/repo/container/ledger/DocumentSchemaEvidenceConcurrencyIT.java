package ai.protomolt.proto.repo.container.ledger;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** Real SQL concurrency over synthetic evidence, not a typed-admission proof. */
@Testcontainers
class DocumentSchemaEvidenceConcurrencyIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Test void anotherConnectionCannotExtendEvidenceDuringOrAfterPublication() throws Exception {
        try (var c = context(POSTGRES); var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var p = prepare(c, 1);
            var ready = new CountDownLatch(1);
            var release = new CompletableFuture<Void>();
            var holder = new AtomicInteger();
            var contender = new AtomicInteger();
            var publication = executor.submit(() -> publish(c, p, Fault.NONE, false, (em, revision) -> {
                // One exact CORE identity, with explicitly synthetic bytes and locator.
                em.createNativeQuery("""
                        INSERT INTO document_revision_schema_evidence
                        (revision_id,revision_ordinal,account_id,principal,operation_id,owner_generation,
                         root_locator_sha256,fragment_sha256,fragment_size,evidence_codec,evidence_version,
                         evidence_bytes,evidence_sha256)
                        SELECT :revision,p.revision_ordinal,:account,:principal,:operation,:generation,
                         sha256(convert_to('locator','UTF8')),decode(o.expected_sha256,'hex'),o.expected_size,
                         'document-root-schema-evidence',1,convert_to('synthetic','UTF8'),sha256(convert_to('synthetic','UTF8'))
                        FROM document_revision_parts p JOIN repository_physical_locations l USING(object_id)
                        JOIN document_part_attempt_objects o ON o.physical_object_id=l.object_id
                         AND o.attempt_id=l.source_id AND o.ordinal=l.source_ordinal
                        WHERE p.revision_id=:revision AND p.part=1
                        """).setParameter("revision", revision).setParameter("account", p.owner().key().account())
                        .setParameter("principal", p.owner().key().principal()).setParameter("operation", p.owner().key().operationId())
                        .setParameter("generation", p.owner().generation()).executeUpdate();
                holder.set(((Number) em.createNativeQuery("SELECT pg_backend_pid()").getSingleResult()).intValue());
                ready.countDown();
                release.join();
            }, em -> {}));
            try {
                assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
                var extension = executor.submit(() -> c.tx().inTransaction((java.util.function.Consumer<jakarta.persistence.EntityManager>) em -> {
                    contender.set(((Number) em.createNativeQuery("SELECT pg_backend_pid()").getSingleResult()).intValue());
                    RepositoryOperationLedger.fenceLiveOwner(em, p.owner());
                    throw new AssertionError("A second writer must not pass the completed operation fence");
                }));
                boolean blocked = false;
                long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
                while (!blocked && System.nanoTime() < deadline) {
                    if (contender.get() > 0) blocked = c.tx().readOnly(em -> (Boolean) em.createNativeQuery(
                            "SELECT :holder=ANY(pg_blocking_pids(:contender))")
                            .setParameter("holder", holder.get()).setParameter("contender", contender.get()).getSingleResult());
                    if (!blocked) Thread.sleep(10);
                }
                assertThat(blocked).as("the second connection waits on the same operation owner").isTrue();
                release.complete(null);
                assertThat(publication.get(10, TimeUnit.SECONDS).getMembersCount()).isEqualTo(1);
                assertThatThrownBy(() -> extension.get(10, TimeUnit.SECONDS)).hasStackTraceContaining("terminal");
                assertThat(count(c, "document_revision_schema_evidence")).isEqualTo(1);
            } finally { release.complete(null); }
        }
    }
}
