package ai.protomolt.proto.repo.accession;

import static ai.protomolt.proto.repo.accession.AccessionFixture.*;
import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.repo.catalog.Catalog;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v2.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.DriverManager;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Catalog write and read amplification (#327): a host sends each schema artifact
 * once, and a history page reads only its own revisions' events.
 */
class CatalogWorkIT {
    private AccessionFixture f;
    private String account;

    @BeforeEach void open() throws Exception {
        f = new AccessionFixture();
        account = "acct-" + UUID.randomUUID();
    }

    @AfterEach void close() { f.close(); }

    @Test void aHostSendsEachSchemaArtifactOnceAndTheDatabaseStaysTheTruth() throws Exception {
        var address = address(account, "doc-artifacts");
        long before = f.catalog.artifactsSent();
        f.saves.save(processAuthority(), create("op-1", address, document(account, "doc-artifacts", "v1")).build());
        long first = f.catalog.artifactsSent() - before;
        assertThat(first).as("the first commit sends the container, payload and grant schemas").isPositive();
        int rows = artifactRows();
        for (int revision = 2; revision <= 4; revision++) {
            long sent = f.catalog.artifactsSent();
            f.saves.save(processAuthority(), titleSave("op-" + revision, address, revision - 1, "v" + revision));
            assertThat(f.catalog.artifactsSent()).as("commit %d sends no artifact", revision).isEqualTo(sent);
        }
        assertThat(artifactRows()).isEqualTo(rows);

        // A new process starts cold and sends again; the rows already exist and do not change.
        try (var restarted = f.restarted()) {
            restarted.saves.save(processAuthority(), titleSave("op-5", address, 4, "v5"));
            assertThat(restarted.catalog.artifactsSent()).isPositive();
        }
        assertThat(artifactRows()).isEqualTo(rows);
    }

    @Test void aColdHostOnAFreshDatabasePersistsWhatTheDatabaseLacks() throws Exception {
        // The first fixture's process has seen these artifacts committed elsewhere; a fresh
        // catalog behind a new host must still receive them, or retained reads would fail.
        var address = address(account, "doc-fresh");
        f.saves.save(processAuthority(), create("op-1", address, document(account, "doc-fresh", "v1")).build());
        try (var other = new AccessionFixture()) {
            other.saves.save(processAuthority(), create("op-1", address, document(account, "doc-fresh", "v1")).build());
            assertThat(other.catalog.artifactsSent()).isPositive();
            var read = other.reads.getDocument(processAuthority(), GetDocumentRequest.newBuilder().setAddress(address).build());
            assertThat(read.getDocument().getSearchMetadata().getTitle()).isEqualTo("v1");
        }
    }

    @Test void historyPagesReadOneBoundedEventQueryEach() {
        var address = address(account, "doc-pages");
        f.saves.save(processAuthority(), create("op-1", address, document(account, "doc-pages", "v1")).build());
        for (int revision = 2; revision <= 50; revision++)
            f.saves.save(processAuthority(), titleSave("op-" + revision, address, revision - 1, "v" + revision));

        var queries = new ArrayList<List<Long>>();
        var whole = new int[1];
        var spied = (Catalog) Proxy.newProxyInstance(Catalog.class.getClassLoader(), new Class<?>[] {Catalog.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("events")) {
                        if (arguments.length == 1) whole[0]++;
                        else queries.add(List.copyOf(castLongs(arguments[1])));
                    }
                    try {
                        return method.invoke(f.catalog, arguments);
                    } catch (InvocationTargetException failure) {
                        throw failure.getCause();
                    }
                });
        var reads = new DocumentReads(spied, f.drives, limits(), bytes -> () -> {}, Clock.systemUTC());
        var token = "";
        var seen = new ArrayList<Long>();
        int pages = 0;
        do {
            var page = reads.listRevisions(processAuthority(), ListRevisionsRequest.newBuilder().setAddress(address)
                    .setPageSize(5).setPageToken(token).build());
            pages++;
            var revisions = page.getRevisionsList().stream().map(RevisionSummary::getRevision).toList();
            assertThat(queries.getLast()).isEqualTo(revisions);
            assertThat(page.getRevisionsList()).allSatisfy(summary -> assertThat(summary.getChangesList()).isNotEmpty());
            seen.addAll(revisions);
            token = page.getNextPageToken();
            if (!token.isEmpty()) assertThat(token).isEqualTo(Long.toString(revisions.getLast()));
        } while (!token.isEmpty());
        assertThat(pages).isEqualTo(10);
        assertThat(queries).hasSize(10).allSatisfy(query -> assertThat(query).hasSize(5));
        assertThat(whole[0]).as("no page reads the whole outbox").isZero();
        var expected = new ArrayList<Long>();
        for (long revision = 50; revision >= 1; revision--) expected.add(revision);
        assertThat(seen).isEqualTo(expected);
    }

    @SuppressWarnings("unchecked")
    private static List<Long> castLongs(Object value) { return (List<Long>) value; }

    private int artifactRows() throws Exception {
        try (var connection = DriverManager.getConnection(f.url, POSTGRES.getUsername(), POSTGRES.getPassword());
             var rows = connection.createStatement().executeQuery("SELECT count(*) FROM catalog_schema_artifacts")) {
            rows.next();
            return rows.getInt(1);
        }
    }

    private static SaveDocumentRequest titleSave(String operation, NodeAddress address, long expected, String title) {
        var document = document(address.getAccountId(), address.getDocId(), title);
        return SaveDocumentRequest.newBuilder().setOperationId(operation)
                .setTarget(DocumentTarget.newBuilder().setAddress(address)).setExpected(at(expected))
                .setDocument(Document.newBuilder().setDocId(address.getDocId()).setOwnership(document.getOwnership())
                        .setSearchMetadata(document.getSearchMetadata()))
                .addWrites(put(DocumentPart.DOCUMENT_PART_CORE)).setOwner(intakeOwner()).build();
    }
}
