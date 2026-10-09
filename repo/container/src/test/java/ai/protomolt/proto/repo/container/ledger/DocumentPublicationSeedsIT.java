package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** Stable private identities over real SQL; this is not a durable-session or multi-host recovery test. */
@Testcontainers
class DocumentPublicationSeedsIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);
    private static final Duration LEASE = Duration.ofMinutes(5);

    @Test void reconstructedPreparationUsesTheSameOwnerAttemptAndLeaseTokensInSql() {
        try (var c = context(POSTGRES)) {
            var input = input(c, true);
            var session = new DocumentPublicationSession(c.tx(), CALLER, input.command, input.placements, LEASE);
            var seeds = session.seeds();
            assertThat(new RepositoryOperationLedger(c.tx()).find(key(input.command))).isEmpty();
            var attempts = new HashMap<>(seeds.attempts()); var tokens = new HashMap<>(seeds.uploadTokens());
            var restored = DocumentPublicationSeeds.restore(key(input.command), input.command, seeds.ownerNonce(), attempts, tokens);
            attempts.clear(); tokens.clear();
            restored.requireCommand(key(input.command), input.command);
            var rebuilt = DocumentOperationUploadAdmission.prepare(input.command, input.placements, restored.attempts(), LEASE, restored.uploadTokens());
            assertThat(rebuilt.members()).isEqualTo(session.prepared().members());
            assertThat(rebuilt.uploadTokens()).isEqualTo(session.prepared().uploadTokens());
            assertThat(restored.attempts()).hasSize(1); // Reuse-only member gets no fabricated upload identities.
            assertThatThrownBy(() -> restored.attempts().clear()).isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> restored.uploadTokens().clear()).isInstanceOf(UnsupportedOperationException.class);
            var owner = session.admit(CALLER, RepositoryReadControl.NONE).orElseThrow();
            assertThat(owner.token()).isEqualTo(restored.ownerNonce());
            var admitted = new DocumentOperationUploadAdmission(c.tx(), new DriveLedger(c.tx())).admit(CALLER, owner, rebuilt);
            assertThat(admitted).hasSize(1);
            assertThat(admitted.getFirst().id()).isIn(restored.attempts().values());
            assertThat(admitted.getFirst().token()).isIn(restored.uploadTokens().values());
            assertThat(restored.toString()).isEqualTo("DocumentPublicationSeeds[private]");
        }
    }

    @Test void commandAndCallerScopeArePartOfTheIdentityBinding() {
        try (var c = context(POSTGRES)) {
            var input = input(c, true); var key = key(input.command);
            var seeds = DocumentPublicationSeeds.mint(key, input.command);
            var otherId = new DocumentPublicationCommand(input.command.intent().toBuilder().setOperationId(UUID.randomUUID().toString()).build());
            assertThat(otherId.sha256()).isEqualTo(input.command.sha256()); // Canonical command deliberately excludes operation ID.
            assertThatThrownBy(() -> seeds.requireCommand(key, otherId)).isInstanceOf(IllegalArgumentException.class);
            var changed = new DocumentPublicationCommand(input.command.intent().toBuilder()
                    .setMembers(0, input.command.intent().getMembers(0).toBuilder().setClusterId("different-routing")).build());
            assertThatThrownBy(() -> seeds.requireCommand(key, changed)).isInstanceOf(IllegalArgumentException.class);
            var otherPrincipal = new RepositoryOperationLedger.Key(key.account(), "another", key.operationId());
            assertThatThrownBy(() -> seeds.requireCommand(otherPrincipal, input.command)).isInstanceOf(IllegalArgumentException.class);
            var otherAccount = new RepositoryOperationLedger.Key("another", key.principal(), key.operationId());
            assertThatThrownBy(() -> DocumentPublicationSeeds.mint(otherAccount, input.command)).isInstanceOf(IllegalArgumentException.class);
            assertThat(new RepositoryOperationLedger(c.tx()).find(key)).isEmpty();
        }
    }

    @Test void missingExtraAndAliasedCapabilitiesCannotBeRestored() {
        try (var c = context(POSTGRES)) {
            var input = input(c, true); var key = key(input.command); var seeds = DocumentPublicationSeeds.mint(key, input.command);
            assertThatThrownBy(() -> DocumentPublicationSeeds.restore(key, input.command, seeds.ownerNonce(), Map.of(), seeds.uploadTokens()))
                    .isInstanceOf(IllegalArgumentException.class);
            var extra = new HashMap<>(seeds.uploadTokens()); extra.put("reuse-only-or-absent", UUID.randomUUID());
            assertThatThrownBy(() -> DocumentPublicationSeeds.restore(key, input.command, seeds.ownerNonce(), seeds.attempts(), extra))
                    .isInstanceOf(IllegalArgumentException.class);
            var alias = Map.of(seeds.uploadTokens().keySet().iterator().next(), seeds.ownerNonce());
            assertThatThrownBy(() -> DocumentPublicationSeeds.restore(key, input.command, seeds.ownerNonce(), seeds.attempts(), alias))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("distinct");
            assertThat(new RepositoryOperationLedger(c.tx()).find(key)).isEmpty();
        }
    }

    @Test void explicitPreparationRefusesMissingExtraOrSharedLeaseTokens() {
        try (var c = context(POSTGRES)) {
            var mixed = input(c, true);
            var intent = mixed.command.intent().toBuilder();
            var upload = intent.getMembers(0).getParts(1).getUpload();
            intent.getMembersBuilder(1).getPartsBuilder(1).setUpload(upload);
            var input = new Input(new DocumentPublicationCommand(intent.build()), mixed.placements);
            var seeds = DocumentPublicationSeeds.mint(key(input.command), input.command);
            assertThat(seeds.attempts()).hasSize(2);
            var duplicate = new HashMap<String, UUID>(); var token = UUID.randomUUID();
            seeds.attempts().keySet().forEach(member -> duplicate.put(member, token));
            var extra = new HashMap<>(seeds.uploadTokens()); extra.put("absent", UUID.randomUUID());
            for (var invalid : java.util.List.of(Map.<String, UUID>of(), duplicate, extra))
                assertThatThrownBy(() -> DocumentOperationUploadAdmission.prepare(input.command, input.placements,
                        seeds.attempts(), LEASE, invalid)).isInstanceOf(IllegalArgumentException.class);
            assertThat(new RepositoryOperationLedger(c.tx()).find(key(input.command))).isEmpty();
        }
    }

    private record Input(DocumentPublicationCommand command, Map<UUID, DocumentUploadPlan.Placement> placements) {}
    private static Input input(Context c, boolean reuseSecond) {
        var seed = prepare(c, 2, reuseSecond);
        var command = new DocumentPublicationCommand(seed.command().intent().toBuilder().setOperationId(UUID.randomUUID().toString()).build());
        var id = UUID.fromString(command.intent().getMembers(0).getDriveId());
        var drive = new DriveLedger(c.tx()).findById(id).orElseThrow();
        return new Input(command, Map.of(id, DocumentUploadPlan.Placement.sample(drive, "native-test",
                new ManagedBackendLedger(c.tx()).find("native-test").orElseThrow())));
    }
    private static RepositoryOperationLedger.Key key(DocumentPublicationCommand command) {
        return new RepositoryOperationLedger.Key(command.intent().getAccountId(), CALLER.principalName(), command.operationId());
    }
}
