package ai.protomolt.proto.repo.catalog;

import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.repo.catalog.CatalogRecords.Commit;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v2.Accession;
import ai.protomolt.proto.repo.v2.DocumentEvent;
import ai.protomolt.proto.repo.v2.DocumentLookup;
import ai.protomolt.proto.repo.v2.ExpectedRevision;
import ai.protomolt.proto.repo.v2.IntakeOrigin;
import ai.protomolt.proto.repo.v2.RecordedOutcome;
import ai.protomolt.proto.repo.v2.StoredManifest;
import ai.protomolt.proto.repo.v2.WriterCredential;
import ai.protomolt.proto.repo.v2.WriterGrant;
import com.google.protobuf.Timestamp;
import java.sql.DriverManager;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * A commit rechecks every writer grant that authorizes it (#314, pre-settled for
 * #316): the IDs are deduplicated and locked in unsigned UTF-8 order, and one
 * revoked grant among several denies the write without committing anything.
 */
@Testcontainers
class CatalogGrantRecheckIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Test void grantIdsAreDeduplicatedAndSortedByUnsignedUtf8() {
        var commit = commit(address("doc"), "op", List.of("grant-é", "grant-b", "grant-Z", "grant-b", "grant-😀"));
        // UTF-16 order would put the surrogate pair before U+E9; code point order puts it last.
        assertThat(commit.grantIds()).containsExactly("grant-Z", "grant-b", "grant-é", "grant-😀");
    }

    @Test void everyGrantIsRecheckedAndOneRevokedGrantDeniesTheCommit() throws Exception {
        try (var catalog = new PostgresCatalog(new CatalogConfig(database("grants"), POSTGRES.getUsername(),
                POSTGRES.getPassword(), 2))) {
            var credential = UUID.randomUUID();
            var first = install(catalog, "grant-a", credential);
            var second = install(catalog, "grant-b", credential);
            catalog.revokeGrant(second.getGrantId());

            var denied = catalog.commit(commit(address("doc-1"), "op-1", List.of(second.getGrantId(), first.getGrantId())));
            assertThat(denied).isInstanceOfSatisfying(CatalogRecords.WriteDenied.class,
                    write -> assertThat(write.reason()).contains("grant-b").contains("revoked"));
            assertThat(catalog.current(address("doc-1"))).isEmpty();
            assertThat(catalog.outcome("acct", "op-1")).isEmpty();

            var missing = catalog.commit(commit(address("doc-1"), "op-1", List.of(first.getGrantId(), "grant-gone")));
            assertThat(missing).isInstanceOfSatisfying(CatalogRecords.WriteDenied.class,
                    write -> assertThat(write.reason()).contains("grant-gone").contains("no longer exists"));

            var third = install(catalog, "grant-c", credential);
            assertThat(catalog.commit(commit(address("doc-1"), "op-1", List.of(third.getGrantId(), first.getGrantId()))))
                    .isInstanceOf(CatalogRecords.Committed.class);
            assertThat(catalog.current(address("doc-1"))).contains(new CatalogRecords.Current(1, false));
        }
    }

    private static WriterGrant install(PostgresCatalog catalog, String id, UUID credential) {
        var grant = WriterGrant.newBuilder().setGrantId(id).setAccountId("acct")
                .setCredential(WriterCredential.newBuilder().setIssuer("issuer").setCredentialId(credential.toString()))
                .setIntake(IntakeOrigin.newBuilder().setDatasourceId("ds")).setAllowCreate(true).build();
        catalog.installGrant(grant);
        return grant;
    }

    private static NodeAddress address(String doc) {
        return NodeAddress.newBuilder().setAccountId("acct").setGraphId("intake:acct").setGraphAddressId("ds")
                .setDocId(doc).build();
    }

    /** The smallest records the catalog accepts for revision 1; only numbering is checked here. */
    private static Commit commit(NodeAddress address, String operation, List<String> grantIds) {
        var intent = "0".repeat(64);
        var now = Timestamp.newBuilder().setSeconds(1_800_000_000L).build();
        var manifest = StoredManifest.newBuilder().setAddress(address).setRevision(1).setOperationId(operation)
                .setIntentSha256(intent).setCommittedAt(now).build();
        var outcome = RecordedOutcome.newBuilder().setAccession(Accession.newBuilder().setAddress(address).setRevision(1))
                .setRecordedAt(now).build();
        var event = DocumentEvent.newBuilder().setEventId(UUID.randomUUID().toString()).setAddress(address).setRevision(1)
                .build();
        return new Commit(address, operation, intent, ExpectedRevision.newBuilder().setNone(true).build(), manifest,
                outcome, event, List.of(), List.of(), Map.of(), grantIds, Optional.empty(),
                Optional.of(DocumentLookup.getDefaultInstance()));
    }

    private static String database(String name) throws Exception {
        try (var connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())) {
            connection.createStatement().execute("CREATE DATABASE " + name);
        }
        return POSTGRES.getJdbcUrl().replaceFirst("/[^/?]+(\\?|$)", "/" + name + "$1");
    }
}
