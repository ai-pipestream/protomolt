package ai.protomolt.proto.repo.fixtures;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.repo.accession.RepositoryCorpus;
import ai.protomolt.proto.repo.archive.ArchiveCoordinates;
import ai.protomolt.proto.repo.archive.DocumentArchiveBindings;
import ai.protomolt.proto.repo.v1.NodeAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** The seeder's refusal rules, without a store. */
class CorpusAccountGuardTest {
    static final RepositoryCorpus CORPUS = RepositoryCorpus.build();
    static final String ALPHA = RepositoryCorpus.ACCOUNTS.get(0);

    @Test void anEmptyAccountIsWritten() {
        assertThat(CorpusAccountGuard.check(CORPUS, ALPHA, List.of())).isEqualTo(CorpusAccountGuard.Decision.WRITE);
    }

    @Test void anAccountHoldingTheWholeCorpusIsLeftAlone() {
        assertThat(CorpusAccountGuard.check(CORPUS, ALPHA, CorpusSeeder.addresses(CORPUS, ALPHA)))
                .isEqualTo(CorpusAccountGuard.Decision.SEEDED);
    }

    @Test void aForeignDocumentRefusesTheAccountByName() {
        var existing = new ArrayList<>(CorpusSeeder.addresses(CORPUS, ALPHA));
        existing.add(NodeAddress.newBuilder().setAccountId(ALPHA).setGraphId("intake:" + ALPHA)
                .setGraphAddressId("ds-crawler").setDocId("someone-elses-report").build());
        existing.add(NodeAddress.newBuilder().setAccountId(ALPHA).setGraphId("intake:" + ALPHA)
                .setGraphAddressId("ds-other").setDocId("another").build());
        assertThatThrownBy(() -> CorpusAccountGuard.check(CORPUS, ALPHA, existing))
                .isInstanceOfSatisfying(CorpusAccountGuard.ForeignDocuments.class, refused -> {
                    assertThat(refused.accountId()).isEqualTo(ALPHA);
                    assertThat(refused.documentIds()).containsExactly(
                            "intake:" + ALPHA + "/ds-crawler/someone-elses-report", "intake:" + ALPHA + "/ds-other/another");
                    assertThat(refused.getMessage()).contains(ALPHA).contains("someone-elses-report").contains("another");
                });
    }

    @Test void aCorpusDocumentIdUnderAnotherGraphIsForeign() {
        var elsewhere = CorpusSeeder.addresses(CORPUS, ALPHA).getFirst().toBuilder().setGraphId("pipeline:other").build();
        assertThatThrownBy(() -> CorpusAccountGuard.check(CORPUS, ALPHA, List.of(elsewhere)))
                .isInstanceOf(CorpusAccountGuard.ForeignDocuments.class)
                .hasMessageContaining(elsewhere.getDocId());
    }

    @Test void aPartialLoadIsNotResumed() {
        var some = CorpusSeeder.addresses(CORPUS, ALPHA).subList(0, 2);
        assertThatThrownBy(() -> CorpusAccountGuard.check(CORPUS, ALPHA, some))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("2 of the corpus's");
    }

    @Test void aCorpusDocumentIdUnderAnotherDatasourceIsForeign() {
        var elsewhere = CORPUS.documents(ALPHA).getFirst().address().toBuilder()
                .setGraphAddressId("someone-elses-datasource").build();
        assertThatThrownBy(() -> CorpusAccountGuard.check(CORPUS, ALPHA, List.of(elsewhere)))
                .isInstanceOf(CorpusAccountGuard.ForeignDocuments.class)
                .hasMessageContaining("someone-elses-datasource");
    }

    @Test void archivePrefixesDoNotAdmitUnrelatedDefinitionsOrEntries() {
        var script = CORPUS.archives().getFirst();
        var unknownDefinition = DocumentArchiveBindings.definitionAddress(ALPHA, "foreign-archive");
        var unknownEntry = ArchiveCoordinates.address(script.entries().getFirst().getAddress().toBuilder()
                .setEntryId("foreign-entry").build());
        for (var foreign : List.of(unknownDefinition, unknownEntry)) {
            var existing = new ArrayList<>(CorpusSeeder.addresses(CORPUS, ALPHA));
            existing.add(foreign);
            assertThatThrownBy(() -> CorpusAccountGuard.check(CORPUS, ALPHA, existing))
                    .isInstanceOf(CorpusAccountGuard.ForeignDocuments.class)
                    .hasMessageContaining(foreign.getDocId());
        }
    }

    @Test void missingAnArchiveDefinitionOrEntryIsAPartialLoad() {
        var script = CORPUS.archives().getFirst();
        var definition = DocumentArchiveBindings.definitionAddress(ALPHA, script.archive().getName());
        var entry = ArchiveCoordinates.address(script.entries().getFirst().getAddress());
        for (var missing : List.of(definition, entry)) {
            var existing = new ArrayList<>(CorpusSeeder.addresses(CORPUS, ALPHA));
            assertThat(existing.remove(missing)).isTrue();
            assertThatThrownBy(() -> CorpusAccountGuard.check(CORPUS, ALPHA, existing))
                    .isInstanceOf(IllegalStateException.class)
                    .isNotInstanceOf(CorpusAccountGuard.ForeignDocuments.class)
                    .hasMessageContaining("partial load");
        }
    }

    @Test void aListingOfAnotherAccountIsAProgrammingError() {
        var other = CorpusSeeder.addresses(CORPUS, RepositoryCorpus.ACCOUNTS.get(1));
        assertThatThrownBy(() -> CorpusAccountGuard.check(CORPUS, ALPHA, other)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test void theSeederRunsOnlyInTheDevProfile() {
        assertThat(CorpusSeederMain.run(Map.of())).isEqualTo(2);
        assertThat(CorpusSeederMain.run(Map.of(CorpusSeederMain.ENV_PROFILE, "prod"))).isEqualTo(2);
        assertThat(CorpusSeederMain.run(Map.of(CorpusSeederMain.ENV_PROFILE, "dev", CorpusSeederMain.ENV_SEED, "x")))
                .as("a malformed seed is refused before any store is opened").isEqualTo(2);
        assertThat(CorpusSeederMain.parameters(Map.of())).isEqualTo(RepositoryCorpus.Parameters.defaults());
        assertThat(CorpusSeederMain.parameters(Map.of(CorpusSeederMain.ENV_SEED, "7", CorpusSeederMain.ENV_LARGE_BLOB_BYTES, "4096")))
                .isEqualTo(new RepositoryCorpus.Parameters(7L, 4096));
    }
}
