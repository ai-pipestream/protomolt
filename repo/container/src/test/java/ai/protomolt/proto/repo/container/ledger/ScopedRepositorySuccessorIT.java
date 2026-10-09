package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.PublicationUpload;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentPublicationRegistrationInspectionIT.*;
import static org.assertj.core.api.Assertions.*;

/** Real SQL handoff and execution checks. This does not claim provider publication after recovery. */
@Testcontainers
class ScopedRepositorySuccessorIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @ParameterizedTest
    @ValueSource(strings = {"allowed", "revoked-before", "revoked-after", "wrong-key", "host-denied"})
    void successorUsesOriginalLiveGrantAndHostGate(String scenario) {
        try (var c = context(POSTGRES)) {
            var source = input(c);
            var original = source.command().intent().getMembers(0);
            var member = original.toBuilder().clearSources().clearParts()
                    .setDestination(original.getDestination().toBuilder().setIfAbsent(true)
                            .setAddress(original.getDestination().getAddress().toBuilder().setDocId("scoped-successor")));
            for (var part : original.getPartsList()) member.addParts(part.toBuilder().clearReuse().setUpload(
                    PublicationUpload.newBuilder().setSizeBytes(1).setSha256("a".repeat(64)).setContentType("application/protobuf")));
            var command = new DocumentPublicationCommand(source.command().intent().toBuilder().setMembers(0, member).build());
            var key = source.key();
            var previous = new DocumentPublicationPreparationRecord(key, command, DocumentPublicationSeeds.mint(key, command),
                    source.placements(), LEASE, 0);
            var binding = new RepositoryCredentialBinding("successor-test", UUID.randomUUID(), 1);
            var caller = new RepositoryCaller(key.principal(), false, Set.of(key.account()), Set.of(), Optional.of(binding));
            var credentials = new RepositoryCredentialAuthorities(c.tx()); credentials.register(CALLER, binding, key.principal());
            var gates = new java.util.concurrent.atomic.AtomicInteger();
            var drives = new DriveLedger(c.tx(), drive -> {
                gates.incrementAndGet();
                if (scenario.equals("host-denied")) throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                        "Successor host refused backend");
            });
            // Provision before first claim; the tested host gate applies to execution, not this setup.
            var grants = new RepositoryCreationGrants(c.tx(), new DriveLedger(c.tx()));
            var grant = RepositoryCreationGrants.prepare(caller, command, source.placements(),
                    (System.currentTimeMillis()+300_000)*1000);
            grants.install(CALLER, grant);
            var plan = RepositorySuccessorInstallIT.plan(c, previous, LEASE);
            var budget = new PayloadBudget(64_000_000);
            RepositorySuccessorInstall.install(c.tx(), budget, CALLER, plan, NONE);
            if (scenario.equals("revoked-before")) grants.revoke(CALLER, key);
            RepositoryCaller executionCaller = caller;
            if (scenario.equals("wrong-key")) {
                var other = new RepositoryCredentialBinding(binding.issuer(), UUID.randomUUID(), 1);
                credentials.register(CALLER, other, key.principal());
                executionCaller = new RepositoryCaller(key.principal(), false, Set.of(key.account()), Set.of(), Optional.of(other));
            }
            var authenticated = executionCaller;
            if (Set.of("revoked-before", "wrong-key", "host-denied").contains(scenario)) {
                assertThatThrownBy(() -> RepositorySuccessorExecution.activate(c.tx(), budget, CALLER, authenticated, plan, NONE, drives))
                        .isInstanceOf(RepositoryException.class)
                        .hasMessageContaining(scenario.equals("host-denied") ? "Successor host refused" : "Creation grant is unavailable");
                assertThat(count(c, "repository_successor_executions")).isZero();
            } else {
                RepositorySuccessorExecution.activate(c.tx(), budget, CALLER, authenticated, plan, NONE, drives);
                assertThat(gates.get()).isPositive();
                if (scenario.equals("revoked-after")) {
                    grants.revoke(CALLER, key);
                    // Confirmation of an immutable activation does not reissue execution authority.
                    RepositorySuccessorExecution.activate(c.tx(), budget, CALLER, authenticated, plan, NONE, drives);
                    assertThatThrownBy(() -> RepositorySuccessorExecution.attach(c.tx(), budget, authenticated, plan, NONE, drives))
                            .isInstanceOf(RepositoryException.class).hasMessageContaining("Creation grant is unavailable");
                } else {
                    var attached = RepositorySuccessorExecution.attach(c.tx(), budget, authenticated, plan, NONE, drives);
                    assertThat(attached.owner().generation()).isEqualTo(2);
                    assertThat(new DocumentOperationUploadAdmission(c.tx(), drives)
                            .admit(authenticated, attached.owner(), plan.next().prepare())).hasSize(1);
                }
            }
            assertThat(budget.reservedBytes()).isZero();
        }
    }
}
