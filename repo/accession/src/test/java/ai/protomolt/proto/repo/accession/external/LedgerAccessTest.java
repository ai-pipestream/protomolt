package ai.protomolt.proto.repo.accession.external;

import static org.assertj.core.api.Assertions.assertThat;

import ai.protomolt.proto.repo.external.v1.ExternalPublicationLedger;
import ai.protomolt.proto.repo.external.v1.ExternalPurgePreparation;
import ai.protomolt.proto.repo.v1.Access;
import ai.protomolt.proto.repo.v1.AccessRule;
import ai.protomolt.proto.repo.v1.DocumentSecurity;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** Which access rules a ledger save writes, for each source of rules. */
class LedgerAccessTest {

    private static final DocumentSecurity ALICE_AND_BOB = DocumentSecurity.newBuilder()
            .addPermissions(rule("alice")).addPermissions(rule("bob")).build();
    private static final DocumentSecurity ALICE = DocumentSecurity.newBuilder()
            .addPermissions(rule("alice")).build();
    private static final DocumentSecurity NO_GRANT = DocumentSecurity.getDefaultInstance();

    private static AccessRule rule(String who) {
        return AccessRule.newBuilder().setIdentityType("service").setIdentity(who)
                .setAccess(Access.ACCESS_READ).build();
    }

    private static Optional<ExternalLedgerStore.LoadedLedger> ledgerWith(DocumentSecurity rules, boolean prepared) {
        ExternalPublicationLedger.Builder ledger = ExternalPublicationLedger.newBuilder();
        if (prepared) {
            ledger.setPurgePreparation(ExternalPurgePreparation.newBuilder().setPurgeId("purge-1"));
        }
        return Optional.of(new ExternalLedgerStore.LoadedLedger(ledger.build(), 3L, rules));
    }

    @Test
    void aLiveSourceWithNoGrantLeftRevokesTheLedgersGrants() {
        assertThat(ExternalLedgerStore.securityToSave(LedgerAccess.liveSource(NO_GRANT), ledgerWith(ALICE_AND_BOB, false)))
                .isEqualTo(NO_GRANT);
        assertThat(ExternalLedgerStore.securityToSave(LedgerAccess.liveSource(ALICE), ledgerWith(ALICE_AND_BOB, false)))
                .isEqualTo(ALICE);
    }

    @Test
    void withNoLiveRulesTheLedgerKeepsItsOwn() {
        assertThat(ExternalLedgerStore.securityToSave(LedgerAccess.keep(), ledgerWith(ALICE, false)))
                .isEqualTo(ALICE);
        assertThat(ExternalLedgerStore.securityToSave(LedgerAccess.keep(), Optional.empty()))
                .isEqualTo(NO_GRANT);
    }

    @Test
    void aPreparedPurgePinsTheLedgersRulesAgainstEveryOtherSave() {
        assertThat(ExternalLedgerStore.securityToSave(LedgerAccess.liveSource(ALICE_AND_BOB), ledgerWith(ALICE, true)))
                .isEqualTo(ALICE);
        assertThat(ExternalLedgerStore.securityToSave(LedgerAccess.purgePreparation(NO_GRANT), ledgerWith(ALICE, true)))
                .isEqualTo(NO_GRANT);
    }
}
