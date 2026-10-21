package ai.protomolt.proto.repo.fixtures;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.repo.accession.CorpusLoader;
import ai.protomolt.proto.repo.accession.DocumentSaves;
import ai.protomolt.proto.repo.accession.RepositoryCorpus;
import ai.protomolt.proto.repo.accession.SyntheticBytes;
import ai.protomolt.proto.repo.archive.v1.GetEntryRequest;
import ai.protomolt.proto.repo.archive.v1.ListVersionsRequest;
import ai.protomolt.proto.repo.archive.v1.RenditionContent;
import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import ai.protomolt.proto.repo.service.RepoServiceConfig;
import ai.protomolt.proto.repo.service.RepoServices;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.GetDriveRequest;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.OwnershipContext;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.repo.v2.DocumentPart;
import ai.protomolt.proto.repo.v2.DocumentTarget;
import ai.protomolt.proto.repo.v2.ExpectedRevision;
import ai.protomolt.proto.repo.v2.IntakeOrigin;
import ai.protomolt.proto.repo.v2.PartSelector;
import ai.protomolt.proto.repo.v2.PartWrite;
import ai.protomolt.proto.repo.v2.SaveDocumentRequest;
import ai.protomolt.proto.repo.v2.SaveOwner;
import ai.protomolt.proto.repo.v2.WriteAction;
import java.time.Clock;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * The seeder against the production {@link RepoServices} composition over
 * PostgreSQL and LocalStack S3: one run provisions the drives and writes the
 * documents and the archives, a second run finds the corpus and writes nothing,
 * and an account holding a document the corpus did not create refuses the run
 * by name. The archive entries read back with their renditions and retained
 * versions. Opt-in: {@code PROTOMOLT_CORPUS_IT=1}.
 */
@Testcontainers
class CorpusSeederIT {
    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Container
    static final LocalStackContainer LOCALSTACK = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
            .withServices("s3");

    static RepoServices services;
    static RepositoryCorpus corpus;

    @BeforeAll static void boot() {
        var config = new RepoServiceConfig(0,
                new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
                LOCALSTACK.getEndpoint().toString(), LOCALSTACK.getRegion(), LOCALSTACK.getAccessKey(),
                LOCALSTACK.getSecretKey(), "it-corpus", 0, null, null, 0, 0L);
        services = RepoServices.build(config);
        corpus = RepositoryCorpus.build();
    }

    @AfterAll static void close() {
        if (services != null) services.close();
    }

    @Test void seedsOnceThenLeavesTheCorpusAloneAndRefusesForeignDocumentsByName() throws Exception {
        var operator = new RepositoryCaller("it-operator", true);
        var first = CorpusSeeder.seed(services, corpus);
        assertThat(first.written()).isTrue();
        assertThat(first.decisions().values()).containsOnly(CorpusAccountGuard.Decision.WRITE);
        assertThat(first.saves()).isEqualTo(corpus.documents().stream().mapToInt(script -> (int) script.steps().stream()
                .filter(step -> step instanceof RepositoryCorpus.Save).count()).sum());
        assertThat(first.archiveEntries()).isEqualTo(corpus.archives().stream().mapToInt(script -> script.entries().size()).sum());
        for (var account : RepositoryCorpus.ACCOUNTS) {
            for (var drive : java.util.List.of(RepositoryCorpus.INTAKE_DRIVE, RepositoryCorpus.PIPELINE_DRIVE, RepositoryCorpus.ARCHIVE_DRIVE)) {
                assertThat(services.driveRepository().getDrive(operator, GetDriveRequest.newBuilder()
                        .setAccountId(account).setName(drive).build()).getDrive().getName()).isEqualTo(drive);
            }
            assertThat(CorpusLoader.listAll(services.v2Composition().catalog(), account)).hasSize(corpus.documents(account).size());
        }

        var second = CorpusSeeder.seed(services, corpus);
        assertThat(second.written()).isFalse();
        assertThat(second.decisions().values()).containsOnly(CorpusAccountGuard.Decision.SEEDED);
        assertThat(CorpusLoader.listAll(services.v2Composition().catalog(), RepositoryCorpus.ACCOUNTS.get(0)))
                .hasSize(corpus.documents(RepositoryCorpus.ACCOUNTS.get(0)).size());

        // Archives: renditions read back byte for byte; the retained entry has two versions, the replaced one has one.
        var archives = services.archiveRepository();
        var docs = corpus.archives().getFirst();
        var report = docs.entries().getFirst().getAddress();
        var entry = archives.getEntry(operator, GetEntryRequest.newBuilder().setAddress(report).build());
        assertThat(entry.getManifest().getVersion()).isEqualTo(2);
        // Version 2 re-put original and markdown; the thumbnail is carried forward from version 1.
        assertThat(entry.getRenditionsList()).extracting(content -> content.getRendition().getName())
                .containsExactlyInAnyOrder("original", "markdown", "thumbnail");
        var expectedMarkdown = docs.entries().get(1).getRenditionsList().stream()
                .filter(content -> content.getRendition().getName().equals("markdown")).findFirst().orElseThrow();
        assertThat(rendition(entry.getRenditionsList(), "markdown").getData()).isEqualTo(expectedMarkdown.getData());
        var versions = archives.listVersions(operator, ListVersionsRequest.newBuilder().setAddress(report).setLimit(10).build());
        assertThat(versions.getVersionsList()).extracting(manifest -> manifest.getVersion()).containsExactlyInAnyOrder(1L, 2L);
        var v1 = archives.getEntry(operator, GetEntryRequest.newBuilder().setAddress(report).setVersion(1).build());
        assertThat(v1.getRenditionsList()).extracting(content -> content.getRendition().getName())
                .containsExactlyInAnyOrder("original", "markdown", "thumbnail");
        assertThat(SyntheticBytes.sha256(rendition(v1.getRenditionsList(), "thumbnail").getData().toByteArray()))
                .isEqualTo(SyntheticBytes.sha256(rendition(docs.entries().getFirst().getRenditionsList(), "thumbnail").getData().toByteArray()));
        var scan = docs.entries().get(2).getAddress();
        var pages = archives.getEntry(operator, GetEntryRequest.newBuilder().setAddress(scan).build());
        assertThat(pages.getRenditionsList()).extracting(content -> content.getRendition().getName() + "/" + content.getRendition().getSubKey())
                .containsExactlyInAnyOrder("original/", "pages/p1", "pages/p2");
        var scans = corpus.archives().get(1);
        var invoice = scans.entries().getFirst().getAddress();
        var replaced = archives.listVersions(operator, ListVersionsRequest.newBuilder().setAddress(invoice).setLimit(10).build());
        assertThat(replaced.getVersionsList()).hasSize(1);
        assertThat(archives.getEntry(operator, GetEntryRequest.newBuilder().setAddress(invoice).build()).getRenditionsList())
                .extracting(content -> content.getRendition().getName()).containsExactlyInAnyOrder("original", "ocr");

        // A document the corpus did not create, in a corpus account, refuses the next run by name.
        var gamma = RepositoryCorpus.ACCOUNTS.get(2);
        var v2 = services.v2Composition();
        var saves = new DocumentSaves(v2.catalog(), v2.drives(), v2.resolver(), v2.limits(), bytes -> () -> {},
                Clock.systemUTC(), v2.schemas());
        var foreign = NodeAddress.newBuilder().setAccountId(gamma).setGraphId("intake:" + gamma)
                .setGraphAddressId("ds-crawler").setDocId("not-from-the-corpus").build();
        saves.save(operator, SaveDocumentRequest.newBuilder().setOperationId("foreign-1")
                .setTarget(DocumentTarget.newBuilder().setAddress(foreign))
                .setExpected(ExpectedRevision.newBuilder().setNone(true))
                .setDocument(Document.newBuilder().setDocId("not-from-the-corpus")
                        .setOwnership(OwnershipContext.newBuilder().setAccountId(gamma).setDatasourceId("ds-crawler"))
                        .setSearchMetadata(SearchMetadata.newBuilder().setTitle("Someone else's")))
                .setOwner(SaveOwner.newBuilder().setIntake(IntakeOrigin.newBuilder().setDatasourceId("ds-crawler")))
                .addWrites(PartWrite.newBuilder().setSelector(PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE))
                        .setAction(WriteAction.WRITE_ACTION_PUT)).build());
        assertThatThrownBy(() -> CorpusSeeder.seed(services, corpus))
                .isInstanceOfSatisfying(CorpusAccountGuard.ForeignDocuments.class, refused -> {
                    assertThat(refused.accountId()).isEqualTo(gamma);
                    assertThat(refused.documentIds()).containsExactly("intake:" + gamma + "/ds-crawler/not-from-the-corpus");
                });
        assertThat(CorpusLoader.listAll(v2.catalog(), gamma)).as("the refusal wrote nothing")
                .hasSize(corpus.documents(gamma).size() + 1);
    }

    @Test void theMainRefusesOutsideTheDevProfileWithoutOpeningTheStore() {
        assertThat(CorpusSeederMain.run(Map.of(
                "DOCUMENT_PLATFORM_JDBC_URL", POSTGRES.getJdbcUrl(),
                "DOCUMENT_PLATFORM_USERNAME", POSTGRES.getUsername(),
                "DOCUMENT_PLATFORM_PASSWORD", POSTGRES.getPassword()))).isEqualTo(2);
    }

    private static RenditionContent rendition(java.util.List<RenditionContent> renditions, String name) {
        return renditions.stream().filter(content -> content.getRendition().getName().equals(name)).findFirst().orElseThrow();
    }
}
