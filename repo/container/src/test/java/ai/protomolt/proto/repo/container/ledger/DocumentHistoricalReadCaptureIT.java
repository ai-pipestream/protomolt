package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** Historical capture and pin lifetime over real PostgreSQL; provider observations are fixture data. */
@Testcontainers
class DocumentHistoricalReadCaptureIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller ADMIN = new RepositoryCaller("historical-reader", true);

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"revoke", "rotate"})
    void unchangedPublicReadPolicyDoesNotPreserveExpiredCredentialAuthority(String change) {
        try (var c = context(POSTGRES)) {
            var prepared = prepare(c, 1);
            var published = publish(c, prepared, Fault.NONE, em -> {});
            var revision = UUID.fromString(published.getMembers(0).getRevisionId());
            var address = prepared.command().intent().getMembers(0).getDestination().getAddress();
            // Change current READ policy deliberately; the archived snapshot remains unchanged.
            c.tx().inTransaction(em -> { em.createNativeQuery(
                    "UPDATE documents SET security=CAST(:policy AS jsonb) WHERE node_id=:node")
                    .setParameter("node", prepared.sources().getFirst().row().nodeId)
                    .setParameter("policy", "{\"permissions\":[{\"identityType\":\"public\",\"identity\":\"public\",\"access\":\"ACCESS_READ\"}]}")
                    .executeUpdate(); });
            var key = new ai.protomolt.proto.repo.spi.RepositoryCredentialBinding("history-read", UUID.randomUUID(), 1);
            var caller = new RepositoryCaller("scoped-reader", false, java.util.Set.of("account"), java.util.Set.of(), java.util.Optional.of(key));
            var credentials = new RepositoryCredentialAuthorities(c.tx());
            credentials.register(ADMIN, key, caller.principalName());
            var ledger = new DocumentReadLedger(c.tx(), UUID.randomUUID());
            var captured = ledger.captureHistorical(caller, address, revision);
            try {
                captured.authorizeDelivery(ai.protomolt.proto.repo.spi.RepositoryReadControl.NONE);
                long pins = count(c, "document_read_pins");
                if (change.equals("rotate")) credentials.rotate(ADMIN, key, caller.principalName());
                else credentials.revoke(ADMIN, key, caller.principalName());
                assertThatThrownBy(() -> captured.authorizeDelivery(ai.protomolt.proto.repo.spi.RepositoryReadControl.NONE))
                        .isInstanceOfSatisfying(RepositoryException.class,
                                failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.UNAUTHENTICATED));
                assertThatThrownBy(() -> ledger.captureHistorical(caller, address, revision))
                        .isInstanceOfSatisfying(RepositoryException.class,
                                failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.UNAUTHENTICATED));
                assertThat(count(c, "document_read_pins")).isEqualTo(pins);
                // Principal-only hosts remain a separate supported authentication model.
                for (var control : java.util.List.of(ADMIN, new RepositoryCaller("external-reader", false,
                        java.util.Set.of("account"), java.util.Set.of()))) {
                    var allowed = ledger.captureHistorical(control, address, revision);
                    try { allowed.authorizeDelivery(ai.protomolt.proto.repo.spi.RepositoryReadControl.NONE); }
                    finally { allowed.close(); allowed.release(); }
                }
            } finally {
                captured.close(); captured.release(); ledger.fence(); ledger.attestLocalQuiescence();
            }
            assertThat(count(c, "document_read_pins")).isZero();
        }
    }

    @Test void capturesExactNativeBindingsAndDrainsTransferredUseBeforeRelease() throws Exception {
        try (var c = context(POSTGRES)) {
            var prepared = prepare(c, 1);
            var published = publish(c, prepared, Fault.NONE, em -> {});
            UUID revision = UUID.fromString(published.getMembers(0).getRevisionId());
            var address = prepared.command().intent().getMembers(0).getDestination().getAddress();
            var ledger = new DocumentReadLedger(new Tx(c.emf()), UUID.randomUUID());
            c.tx().inTransaction(em -> {
                em.createNativeQuery("UPDATE documents SET security=CAST(:policy AS jsonb) WHERE node_id=:node")
                        .setParameter("node", prepared.sources().getFirst().row().nodeId)
                        .setParameter("policy", "{\"permissions\":[{\"identityType\":\"public\",\"identity\":\"public\",\"access\":\"ACCESS_READ\"}]}")
                        .executeUpdate();
            });
            var caller = new RepositoryCaller("different-reader", false, java.util.Set.of("account"), java.util.Set.of());
            var captured = ledger.captureHistorical(caller, address, revision);
            var setupUse = captured.use();
            var plan = setupUse.plan();
            assertThat(plan.address()).isEqualTo(address);
            assertThat(plan.revision()).isEqualTo(revision);
            assertThat(plan.publicationRevision()).isPositive();
            assertThat(plan.manifest().getAddress()).isEqualTo(address);
            assertThat(plan.entries()).hasSize(2);
            var source = prepared.sources().getFirst();
            for (int ordinal = 0; ordinal < plan.entries().size(); ordinal++) {
                var entry = plan.entries().get(ordinal);
                var expected = source.identities().get(ordinal);
                assertThat(entry.revisionOrdinal()).isEqualTo(ordinal);
                assertThat(entry.objectId()).isEqualTo(UUID.fromString(expected.getObjectId()));
                assertThat(entry.part().part().part()).isEqualTo(source.slots().get(ordinal).getPart());
                assertThat(entry.part().part().subKey()).isEqualTo(source.slots().get(ordinal).getSubKey());
                assertThat(entry.part().part().key()).isEqualTo(expected.getObjectKey());
                assertThat(entry.part().binding().generation()).isEqualTo("native-test");
                assertThat(entry.part().binding().namespace()).isNotBlank();
            }
            assertThat(count(c, "document_read_pins")).isEqualTo(2);

            var batchUse = setupUse.transfer();
            setupUse.close(); // transfer ended this owner; the batch remains protected
            ledger.fence();
            assertThat(batchUse.plan()).isSameAs(plan);
            captured.close();
            assertThat(captured.awaitDrained(Duration.ZERO)).isFalse();
            batchUse.close();
            assertThat(captured.awaitDrained(Duration.ofSeconds(1))).isTrue();
            captured.release();
            ledger.attestLocalQuiescence();
            assertThat(count(c, "document_read_pins")).isZero();
        }
    }

    @Test void deniedAndCrossAccountCaptureHideExistingAndRandomRevisionsWithoutPins() {
        try (var c = context(POSTGRES)) {
            var prepared = prepare(c, 1);
            var published = publish(c, prepared, Fault.NONE, em -> {});
            UUID revision = UUID.fromString(published.getMembers(0).getRevisionId());
            var address = prepared.command().intent().getMembers(0).getDestination().getAddress();
            var denied = new RepositoryCaller("denied", false, java.util.Set.of("account"), java.util.Set.of());
            var wrongAccount = new RepositoryCaller("other-account", false, java.util.Set.of("elsewhere"), java.util.Set.of());
            var deniedLedger = new DocumentReadLedger(new Tx(c.emf()), UUID.randomUUID());
            var wrongAccountLedger = new DocumentReadLedger(new Tx(c.emf()), UUID.randomUUID());

            assertSameNotFound(deniedLedger, denied, address, revision);
            assertSameNotFound(deniedLedger, denied, address, UUID.randomUUID());
            assertSameNotFound(wrongAccountLedger, wrongAccount, address, revision);
            assertSameNotFound(wrongAccountLedger, wrongAccount, address, UUID.randomUUID());
            var elsewhere = address.toBuilder().setAccountId("elsewhere").build();
            assertSameNotFound(wrongAccountLedger, wrongAccount, elsewhere, revision);
            assertSameNotFound(wrongAccountLedger, wrongAccount, elsewhere, UUID.randomUUID());
            assertThat(count(c, "document_read_pins")).isZero();

            // Failed capture must relinquish its local lifetime so normal fencing can quiesce.
            deniedLedger.fence();
            deniedLedger.attestLocalQuiescence();
            wrongAccountLedger.fence();
            wrongAccountLedger.attestLocalQuiescence();
        }
    }

    @Test void capturesTypedNativeRevisionWithItsRetainedParts() throws Exception {
        try (var c = context(POSTGRES)) {
            var fixture = DocumentSchemaRetentionFixture.prepare(c, true);
            new DocumentSchemaPolicies(c.tx()).activate(fixture.batch().policy().policy(), 0, () -> {});
            UUID revision = DocumentSchemaRetentionFixture.publishBound(c, fixture, (em, candidate) -> {},
                    (em, id, manifest) -> fixture.retention().write(em, fixture.owner(), id, () -> {}));
            var address = fixture.command().intent().getMembers(0).getDestination().getAddress();
            var ledger = new DocumentReadLedger(new Tx(c.emf()), UUID.randomUUID());

            var captured = ledger.captureHistorical(ADMIN, address, revision);
            try (var use = captured.use()) {
                var plan = use.plan();
                assertThat(plan.revision()).isEqualTo(revision);
                assertThat(plan.entries()).isNotEmpty();
                assertThat(plan.entries()).allSatisfy(entry -> {
                    assertThat(entry.revisionOrdinal()).isBetween(0, plan.manifest().getPartsCount() - 1);
                    assertThat(plan.manifest().getParts(entry.revisionOrdinal()).getState())
                            .isEqualTo(ai.protomolt.proto.repo.v1.PartState.PART_STATE_PRESENT);
                    assertThat(entry.part().part().contentType()).isEqualTo("application/protobuf");
                });
            }
            captured.close();
            assertThat(captured.awaitDrained(Duration.ofSeconds(1))).isTrue();
            captured.release();
            ledger.fence();
            ledger.attestLocalQuiescence();
            assertThat(count(c, "document_read_pins")).isZero();
        }
    }

    @Test void refusesLegacyRevisionBeforeCreatingPins() {
        try (var c = context(POSTGRES, "58")) {
            var prepared = prepare(c, 1);
            UUID revision = prepared.sources().getFirst().attempt();
            String schema = c.tx().readOnly(em -> (String) em.createNativeQuery("SELECT current_schema()").getSingleResult());
            org.flywaydb.core.Flyway.configure().dataSource(c.pool().getJdbcUrl(), c.pool().getUsername(), c.pool().getPassword())
                    .schemas(schema).defaultSchema(schema).locations("classpath:db/migration/repo").load().migrate();
            var address = prepared.command().intent().getMembers(0).getDestination().getAddress();
            var ledger = new DocumentReadLedger(new Tx(c.emf()), UUID.randomUUID());

            assertThatThrownBy(() -> ledger.captureHistorical(ADMIN, address, revision))
                    .isInstanceOfSatisfying(RepositoryException.class,
                            error -> assertThat(error.code()).isEqualTo(RepositoryException.Code.FAILED_PRECONDITION));
            assertThat(count(c, "document_read_pins")).isZero();
            ledger.fence();
            ledger.attestLocalQuiescence();
        }
    }

    @Test void laterRetiringObjectRollsBackWholeHistoricalPinSetAndLocalLifetime() {
        try (var c = context(POSTGRES)) {
            var prepared = prepare(c, 1);
            var published = publish(c, prepared, Fault.NONE, em -> {});
            UUID revision = UUID.fromString(published.getMembers(0).getRevisionId());
            var address = prepared.command().intent().getMembers(0).getDestination().getAddress();
            var objectIds = prepared.sources().getFirst().identities().stream()
                    .map(identity -> UUID.fromString(identity.getObjectId())).sorted().toList();
            UUID retiring = c.tx().readOnly(em -> (UUID) em.createNativeQuery(
                    "SELECT object_id FROM repository_object_retention WHERE object_id IN (:first,:second) ORDER BY object_id DESC LIMIT 1")
                    .setParameter("first", objectIds.get(0)).setParameter("second", objectIds.get(1)).getSingleResult());
            c.tx().inTransaction(em -> {
                em.createNativeQuery("UPDATE repository_object_retention SET retiring=true WHERE object_id=:object")
                        .setParameter("object", retiring).executeUpdate();
            });
            var ledger = new DocumentReadLedger(new Tx(c.emf()), UUID.randomUUID());

            assertThatThrownBy(() -> ledger.captureHistorical(ADMIN, address, revision))
                    .hasStackTraceContaining("Document read pin requires an open retained historical source");
            assertThat(count(c, "document_read_pins")).isZero();
            long readerMirrors = c.tx().readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT count(*) FROM repository_object_references WHERE owner_kind='DOCUMENT_READER'")
                    .getSingleResult()).longValue());
            assertThat(readerMirrors).isZero();
            ledger.fence();
            ledger.attestLocalQuiescence();
        }
    }

    private static void assertSameNotFound(DocumentReadLedger ledger, RepositoryCaller caller,
            ai.protomolt.proto.repo.v1.NodeAddress address, UUID revision) {
        var existing = catchThrowable(() -> ledger.captureHistorical(caller, address, revision));
        var random = catchThrowable(() -> ledger.captureHistorical(caller, address, UUID.randomUUID()));
        assertThat(existing).isInstanceOfSatisfying(RepositoryException.class,
                error -> assertThat(error.code()).isEqualTo(RepositoryException.Code.NOT_FOUND));
        assertThat(random).isInstanceOfSatisfying(RepositoryException.class,
                error -> assertThat(error.code()).isEqualTo(RepositoryException.Code.NOT_FOUND));
        assertThat(((RepositoryException) existing).getMessage()).isEqualTo(((RepositoryException) random).getMessage());
    }
}
