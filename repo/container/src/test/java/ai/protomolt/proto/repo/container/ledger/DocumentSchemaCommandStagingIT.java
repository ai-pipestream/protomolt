package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** Real command and owner fencing. Staged fixture bytes do not claim schema validity. */
@Testcontainers
class DocumentSchemaCommandStagingIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final ByteString BYTES = ByteString.copyFromUtf8("schema staging fixture");

    @Test void exactCommandStagesAndCanReadTheClaimWithoutPublishing() {
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 1);
            var storage = new RepositorySchemaArtifacts(c.tx());
            var hashes = storage.stage(p.owner(), p.command(), List.of(BYTES), () -> {});
            assertThat(hashes).hasSize(1);
            assertThat(storage.readRetained(p.owner(), hashes.getFirst(), () -> {})).isEqualTo(BYTES);
            assertThat(count(c, "repository_schema_artifact_claims")).isEqualTo(1);
            assertThat(count(c, "document_revision_commits")).isZero();
        }
    }

    @Test void changedCommandUnderTheSameOperationCannotStageAnyBytes() {
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 1);
            var changed = new DocumentPublicationCommand(p.command().intent().toBuilder()
                    .setMembers(0, p.command().intent().getMembers(0).toBuilder().setMemberId("changed-member")).build());
            assertThatThrownBy(() -> new RepositorySchemaArtifacts(c.tx()).stage(p.owner(), changed, List.of(BYTES), () -> {}))
                    .isInstanceOf(RepositoryOperationLedger.CommandConflictException.class);
            assertThat(count(c, "repository_schema_artifacts")).isZero();
            assertThat(count(c, "repository_schema_artifact_claims")).isZero();
        }
    }

    @Test void semanticCommandDigestDoesNotReplaceOperationScopeOrOwnerToken() {
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 1);
            var anotherOperation = new DocumentPublicationCommand(p.command().intent().toBuilder()
                    .setOperationId(UUID.randomUUID().toString()).build());
            assertThat(anotherOperation.sha256()).isEqualTo(p.command().sha256());
            var storage = new RepositorySchemaArtifacts(c.tx());
            assertThatThrownBy(() -> storage.stage(p.owner(), anotherOperation, List.of(BYTES), () -> {}))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("owner scope");
            var wrongToken = new RepositoryOperationLedger.Owner(p.owner().key(), p.owner().generation(),
                    UUID.randomUUID(), p.owner().leaseUntil());
            assertThatThrownBy(() -> storage.stage(wrongToken, p.command(), List.of(BYTES), () -> {}))
                    .isInstanceOf(RepositoryOperationLedger.OwnerFencedException.class);
            assertThat(count(c, "repository_schema_artifacts")).isZero();
        }
    }

    @Test void cancellationAfterStagingRollsBackArtifactsAndClaims() {
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 1);
            var cancelled = new CancellationException("cancel after artifact insert");
            var visits = new java.util.concurrent.atomic.AtomicInteger();
            assertThatThrownBy(() -> new RepositorySchemaArtifacts(c.tx()).stage(p.owner(), p.command(), List.of(BYTES), () -> {
                // The fifth checkpoint follows the actual SQL staging call.
                if (visits.incrementAndGet() == 5) throw cancelled;
            })).isSameAs(cancelled);
            assertThat(visits).hasValue(5);
            assertThat(count(c, "repository_schema_artifacts")).isZero();
            assertThat(count(c, "repository_schema_artifact_claims")).isZero();
        }
    }

    private static long count(Context c, String table) {
        return c.tx().readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table).getSingleResult()).longValue());
    }
}
