package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.accession.CatalogEntryRebuild;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RepoCatalogMaintenanceTest {
    @Test void malformedCommandsNeverOpenTheRepository() {
        var output = new ByteArrayOutputStream();
        for (var args : List.of(new String[0], new String[] {"serve"},
                new String[] {"rebuild-entry", "a", "g", "ds", "d", "0"},
                new String[] {"rebuild-entry", "a", "g", "ds", "d", "latest"},
                new String[] {"verify-core", "a", "", "ds", "d", "1"},
                new String[] {"verify-core", "a", "g", "ds", "d", "9223372036854775808"})) {
            assertThatThrownBy(() -> RepoCatalogMaintenance.run(args, () -> {
                throw new AssertionError("Invalid command opened the repository");
            }, new PrintStream(output))).isInstanceOf(IllegalArgumentException.class);
        }
        assertThat(output.size()).isZero();
    }

    @Test void aPurgeBetweenTheRepairTransactionsIsReportedAsPurgedNotNotCurrent() {
        var purged = new CatalogEntryRebuild.RevisionResult(false, true, false, true);
        var historical = new CatalogEntryRebuild.RevisionResult(false, true, false, false);
        var audit = new CatalogEntryRebuild.RevisionResult(false, false, false, false);

        assertThat(RepoCatalogMaintenance.report(7, true, purged))
                .isEqualTo("revision=7 core=VERIFIED_WITH_RETAINED_SCHEMAS revision_type=REBUILT current_entry=PURGED")
                .doesNotContain("NOT_CURRENT");
        assertThat(RepoCatalogMaintenance.report(7, true, historical)).endsWith("current_entry=NOT_CURRENT");
        assertThat(RepoCatalogMaintenance.report(7, false, audit))
                .isEqualTo("revision=7 core=VERIFIED_WITH_RETAINED_SCHEMAS revision_type=NOT_REQUESTED current_entry=NOT_REQUESTED");
    }

    @Test void repositoryFailureIsPropagatedWithoutSuccessOutput() {
        var output = new ByteArrayOutputStream();
        var failure = new IllegalStateException("catalog unavailable");
        assertThatThrownBy(() -> RepoCatalogMaintenance.run(
                new String[] {"verify-core", "a", "g", "ds", "d", "1"},
                () -> { throw failure; }, new PrintStream(output))).isSameAs(failure);
        assertThat(output.size()).isZero();
    }
}
