package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.container.blob.DocumentIds;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import java.time.Duration;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** Real current authorization locks. Does not bypass the public historical-command guard. */
@Testcontainers
class DocumentHistoricalAuthorizationIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller SCOPED = new RepositoryCaller("principal", false, Set.of("account"), Set.of());
    private static final String ALLOW = "{\"permissions\":[{\"identityType\":\"public\",\"identity\":\"public\",\"access\":\"ACCESS_READ\"},{\"identityType\":\"public\",\"identity\":\"public\",\"access\":\"ACCESS_WRITE\"}]}";

    @ParameterizedTest @ValueSource(strings = {"allowed", "source-denied", "destination-denied", "stale-destination"})
    void historicalReadsUseCurrentPolicyButNotOldRevisionCas(String scenario) throws Exception {
        try (var c = context(POSTGRES)) {
            var f = prepare(c, 2); var published = publish(c, f, Fault.NONE, em -> {});
            var source = published.getMembers(1).getAddress(); var destination = published.getMembers(0).getAddress();
            policy(c, source, ALLOW); policy(c, destination, ALLOW);
            var plan = target(c, f);
            var base = DocumentAdmissionAuthorization.prepare(plan);
            // Directly test the final authorization lockset, independently of still-disabled command activation.
            var authorization = new DocumentAdmissionAuthorization.Prepared(base.destinations(), base.sources(),
                    Map.of(DocumentIds.nodeId(source), source));
            long currentSource = new DocumentLedger(c.tx()).findByNodeId(DocumentIds.nodeId(source)).orElseThrow().mutationRevision;
            assertThat(currentSource).isGreaterThan(published.getMembers(1).getMutationRevision());
            if (!scenario.equals("allowed")) c.tx().inTransaction(em -> { em.createNativeQuery(
                    "UPDATE documents SET filename='changed' WHERE node_id=:node")
                    .setParameter("node", DocumentIds.nodeId(destination)).executeUpdate(); });
            if (scenario.equals("source-denied")) policy(c, source, "{}");
            if (scenario.equals("destination-denied")) policy(c, destination, "{}");
            if (scenario.equals("allowed")) {
                var result = c.tx().inTransaction(em -> { return DocumentAdmissionAuthorization.lockAndAuthorize(em, SCOPED, plan, authorization); });
                assertThat(result).containsKey(DocumentIds.nodeId(destination));
            } else {
                var failure = catchThrowable(() -> c.tx().inTransaction(em -> {
                    DocumentAdmissionAuthorization.lockAndAuthorize(em, SCOPED, plan, authorization);
                }));
                if (scenario.equals("stale-destination")) assertThat(failure).isInstanceOf(DocumentLedger.RevisionConflictException.class);
                else assertThat(failure).isInstanceOfSatisfying(RepositoryException.class,
                        error -> assertThat(error.code()).isEqualTo(RepositoryException.Code.NOT_FOUND));
            }
        }
    }

    @Test void preparationRefusesHistoricalSourcesNotDeclaredByTheCommandAndClosedUses() throws Exception {
        try (var c = context(POSTGRES)) {
            var f = prepare(c, 2); var published = publish(c, f, Fault.NONE, em -> {});
            var source = published.getMembers(1).getAddress();
            policy(c, source, ALLOW); policy(c, published.getMembers(0).getAddress(), ALLOW);
            var plan = target(c, f); var ledger = new DocumentReadLedger(c.tx(), UUID.randomUUID());
            var history = ledger.captureHistorical(SCOPED, source, UUID.fromString(published.getMembers(1).getRevisionId()));
            try (var use = history.use()) {
                var selector = PublicationHistoricalReuse.newBuilder().setSource(source)
                        .setRevisionId(published.getMembers(1).getRevisionId()).setRevisionOrdinal(0)
                        .setSourceSlot(f.sources().get(1).slots().get(0)).setObject(f.sources().get(1).identities().get(0)).build();
                var reference = DocumentHistoricalReferenceAdmission.prepare(history, use, List.of(selector), RepositoryReadControl.NONE);
                assertThatThrownBy(() -> DocumentAdmissionAuthorization.prepare(plan, List.of(reference)))
                        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("complete command");
                use.close();
                assertThatThrownBy(() -> DocumentAdmissionAuthorization.prepare(plan, List.of(reference)))
                        .isInstanceOf(IllegalStateException.class).hasMessageContaining("use has ended");
            }
            history.close(); assertThat(history.awaitDrained(Duration.ofSeconds(1))).isTrue(); history.release();
            ledger.fence(); ledger.attestLocalQuiescence();
        }
    }

    private static DocumentUploadPlan.Prepared target(Context c, DocumentNativePublicationFixture.Prepared f) {
        var original = f.command().intent().getMembers(0);
        var row = new DocumentLedger(c.tx()).findByNodeId(DocumentIds.nodeId(original.getDestination().getAddress())).orElseThrow();
        var condition = original.getDestination().toBuilder().setExpectedMutationRevision(row.mutationRevision).build();
        var member = original.toBuilder().setDestination(condition)
                .setOwnership(original.getOwnership().toBuilder().setSecurity(row.readSecurity()));
        for (int i = 0; i < member.getPartsCount(); i++) member.setParts(i,
                member.getParts(i).toBuilder().setReuse(member.getParts(i).getReuse().toBuilder().setSource(condition)));
        var command = new DocumentPublicationCommand(f.command().intent().toBuilder().setOperationId(UUID.randomUUID().toString())
                .clearMembers().addMembers(member).build());
        var drive = new DriveLedger(c.tx()).findById(UUID.fromString(member.getDriveId())).orElseThrow();
        var profile = new ManagedBackendLedger(c.tx()).find("native-test").orElseThrow();
        return DocumentUploadPlan.prepare(command, Map.of(drive.driveId,
                DocumentUploadPlan.Placement.sample(drive, "native-test", profile)), Map.of());
    }
    private static void policy(Context c, NodeAddress address, String json) {
        c.tx().inTransaction(em -> { em.createNativeQuery("UPDATE documents SET security=CAST(:policy AS jsonb) WHERE node_id=:node")
                .setParameter("policy", json).setParameter("node", DocumentIds.nodeId(address)).executeUpdate(); });
    }
}
