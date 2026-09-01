package ai.pipestream.proto.repo.service;

import ai.pipestream.proto.asset.v1.Classification;
import ai.pipestream.proto.asset.v1.ClassificationState;
import ai.pipestream.proto.asset.v1.FormatFact;
import ai.pipestream.proto.asset.v1.ObjectStoreOrigin;
import ai.pipestream.proto.asset.v1.PdfDocument;
import ai.pipestream.proto.asset.v1.TarArchive;
import ai.pipestream.proto.repo.archive.v1.Archive;
import ai.pipestream.proto.repo.archive.v1.ArchiveServiceGrpc;
import ai.pipestream.proto.repo.archive.v1.ClassificationStateCount;
import ai.pipestream.proto.repo.archive.v1.ClassifyEntryRequest;
import ai.pipestream.proto.repo.archive.v1.CreateArchiveRequest;
import ai.pipestream.proto.repo.archive.v1.EntryAddress;
import ai.pipestream.proto.repo.archive.v1.GetArchiveStatsRequest;
import ai.pipestream.proto.repo.archive.v1.GetEntryManifestRequest;
import ai.pipestream.proto.repo.archive.v1.ListEntriesRequest;
import ai.pipestream.proto.repo.archive.v1.PutEntryRequest;
import ai.pipestream.proto.repo.archive.v1.RenditionContent;
import ai.pipestream.proto.repo.archive.v1.RenditionDescriptor;
import ai.pipestream.proto.repo.archive.v1.VersioningPolicy;
import ai.pipestream.proto.repo.v1.CreateDriveRequest;
import ai.pipestream.proto.repo.v1.DriveServiceGrpc;
import ai.pipestream.proto.repo.v1.DriveType;
import ai.pipestream.proto.repo.container.ledger.LedgerConfig;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The classification state machine behind the archive's doors, end to end
 * against real infrastructure: declarations validate against the contract's
 * own rules at the door, verification and conflict resolve from real bytes,
 * the HTTP door identifies without any declaration, ClassifyEntry
 * re-resolves after the fact, and the per-state counts are exact.
 */
@Testcontainers(disabledWithoutDocker = true)
class ArchiveClassificationIT {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Container
    static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
                    .withServices("s3");

    static RepoServices services;
    static ManagedChannel channel;
    static ArchiveServiceGrpc.ArchiveServiceBlockingStub archives;
    static UploadHttpServer http;

    private static final String ACCOUNT = "acct-classify";

    @BeforeAll
    static void boot() {
        RepoServiceConfig config = new RepoServiceConfig(
                0,
                new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(),
                        POSTGRES.getPassword()),
                LOCALSTACK.getEndpoint().toString(),
                LOCALSTACK.getRegion(),
                LOCALSTACK.getAccessKey(),
                LOCALSTACK.getSecretKey(),
                "it-classify",
                0,
                null, null, null, null, 0, 0L);
        services = RepoServices.build(config);
        services.startInProcess("classify-it");
        channel = InProcessChannelBuilder.forName("classify-it").build();
        archives = ArchiveServiceGrpc.newBlockingStub(channel);
        DriveServiceGrpc.newBlockingStub(channel).createDrive(CreateDriveRequest.newBuilder()
                .setName("classify-drive")
                .setAccountId(ACCOUNT)
                .setDriveType(DriveType.DRIVE_TYPE_CUSTOM)
                .build());
        archives.createArchive(CreateArchiveRequest.newBuilder()
                .setArchive(Archive.newBuilder()
                        .setName("classified")
                        .setAccountId(ACCOUNT)
                        .setDriveName("classify-drive")
                        .setVersioning(VersioningPolicy.VERSIONING_POLICY_RETAINED))
                .build());
        http = services.startHttp(0);
    }

    @AfterAll
    static void tearDown() {
        channel.shutdownNow();
        services.close();
    }

    private static EntryAddress address(String entryId) {
        return EntryAddress.newBuilder()
                .setAccountId(ACCOUNT)
                .setArchive("classified")
                .setEntryId(entryId)
                .build();
    }

    private static byte[] tarBytes() {
        byte[] head = new byte[1024];
        byte[] magic = "ustar".getBytes(StandardCharsets.ISO_8859_1);
        System.arraycopy(magic, 0, head, 257, magic.length);
        return head;
    }

    private static FormatFact tarClaim(String filename) {
        return FormatFact.newBuilder()
                .setTar(TarArchive.newBuilder().setFilename(filename)).build();
    }

    @Test
    void aDeclarationTheBytesConfirmIsVerified() {
        archives.putEntry(PutEntryRequest.newBuilder()
                .setAddress(address("bundle"))
                .setFilename("bundle.tar")
                .setDeclared(tarClaim("bundle.tar"))
                .setOrigin(ObjectStoreOrigin.newBuilder()
                        .setBucket("landing").setObjectKey("in/bundle.tar"))
                .addRenditions(RenditionContent.newBuilder()
                        .setRendition(RenditionDescriptor.newBuilder().setName("original"))
                        .setData(ByteString.copyFrom(tarBytes())))
                .build());
        Classification c = archives.getEntryManifest(GetEntryManifestRequest.newBuilder()
                        .setAddress(address("bundle")).build())
                .getInfo().getClassification();
        assertThat(c.getState()).isEqualTo(ClassificationState.CLASSIFICATION_STATE_VERIFIED);
        assertThat(c.getDeclared().getTar().getFilename()).isEqualTo("bundle.tar");
        assertThat(c.getIdentified().getFormatCase()).isEqualTo(FormatFact.FormatCase.TAR);
        assertThat(c.getOrigin().getBucket()).isEqualTo("landing");
        assertThat(c.getEvidenceList()).isNotEmpty();
    }

    @Test
    void aDeclarationTheBytesContradictIsConflictedAndBothFactsAreKept() {
        archives.putEntry(PutEntryRequest.newBuilder()
                .setAddress(address("liar"))
                .setFilename("liar.tar")
                .setDeclared(tarClaim("liar.tar"))
                .addRenditions(RenditionContent.newBuilder()
                        .setRendition(RenditionDescriptor.newBuilder().setName("original"))
                        .setData(ByteString.copyFromUtf8("%PDF-1.7 not a tar at all")))
                .build());
        Classification c = archives.getEntryManifest(GetEntryManifestRequest.newBuilder()
                        .setAddress(address("liar")).build())
                .getInfo().getClassification();
        assertThat(c.getState()).isEqualTo(ClassificationState.CLASSIFICATION_STATE_CONFLICTED);
        assertThat(c.getDeclared().getFormatCase()).isEqualTo(FormatFact.FormatCase.TAR);
        assertThat(c.getIdentified().getFormatCase()).isEqualTo(FormatFact.FormatCase.PDF);
    }

    @Test
    void aGrammarInvalidDeclarationRefusesAtTheDoorNamingTheRule() {
        assertThatThrownBy(() -> archives.putEntry(PutEntryRequest.newBuilder()
                .setAddress(address("wrong"))
                .setDeclared(tarClaim("wrong.zip"))
                .build()))
                .isInstanceOf(StatusRuntimeException.class)
                .satisfies(t -> {
                    StatusRuntimeException e = (StatusRuntimeException) t;
                    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
                    assertThat(e.getStatus().getDescription()).contains("filename");
                });
    }

    @Test
    void aDeclarationWithoutItsFilenameIsRefusedTheClaimIsAboutANamedFile() {
        assertThatThrownBy(() -> archives.putEntry(PutEntryRequest.newBuilder()
                .setAddress(address("nameless"))
                .setDeclared(FormatFact.newBuilder()
                        .setPdf(PdfDocument.newBuilder()).build())
                .build()))
                .isInstanceOf(StatusRuntimeException.class)
                .satisfies(t -> assertThat(
                        ((StatusRuntimeException) t).getStatus().getDescription())
                        .contains("declaration names its file"));
    }

    @Test
    void anOriginWithoutItsCoordinatesIsRefusedByName() {
        assertThatThrownBy(() -> archives.putEntry(PutEntryRequest.newBuilder()
                .setAddress(address("rootless"))
                .setOrigin(ObjectStoreOrigin.newBuilder().setBucket("landing"))
                .build()))
                .isInstanceOf(StatusRuntimeException.class)
                .satisfies(t -> assertThat(
                        ((StatusRuntimeException) t).getStatus().getDescription())
                        .contains("object_key"));
    }

    @Test
    void theHttpDoorIdentifiesWithoutAnyDeclaration() throws Exception {
        byte[] pdf = "%PDF-1.4 minimal".getBytes(StandardCharsets.ISO_8859_1);
        HttpClient client = HttpClient.newHttpClient();
        HttpResponse<String> response = client.send(HttpRequest.newBuilder(URI.create(
                        "http://127.0.0.1:" + http.port() + UploadHttpServer.ARCHIVE_UPLOAD_PATH
                                + "?account_id=" + ACCOUNT + "&archive=classified"
                                + "&entry_id=scan&rendition=original&filename=scan.pdf"))
                        .header("Content-Type", "application/pdf")
                        .POST(HttpRequest.BodyPublishers.ofByteArray(pdf))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);

        Classification c = archives.getEntryManifest(GetEntryManifestRequest.newBuilder()
                        .setAddress(address("scan")).build())
                .getInfo().getClassification();
        assertThat(c.getState()).isEqualTo(ClassificationState.CLASSIFICATION_STATE_IDENTIFIED);
        assertThat(c.getIdentified().getFormatCase()).isEqualTo(FormatFact.FormatCase.PDF);
        assertThat(c.getIdentified().getPdf().getFilename()).isEqualTo("scan.pdf");
        assertThat(c.hasDeclared()).isFalse();
    }

    @Test
    void classifyEntryDeclaresAfterTheFactAndRereadsTheBytes() {
        archives.putEntry(PutEntryRequest.newBuilder()
                .setAddress(address("late"))
                .setFilename("late.tar")
                .addRenditions(RenditionContent.newBuilder()
                        .setRendition(RenditionDescriptor.newBuilder().setName("original"))
                        .setData(ByteString.copyFrom(tarBytes())))
                .build());
        Classification before = archives.getEntryManifest(GetEntryManifestRequest.newBuilder()
                        .setAddress(address("late")).build())
                .getInfo().getClassification();
        assertThat(before.getState())
                .isEqualTo(ClassificationState.CLASSIFICATION_STATE_IDENTIFIED);

        Classification after = archives.classifyEntry(ClassifyEntryRequest.newBuilder()
                        .setAddress(address("late"))
                        .setDeclared(tarClaim("late.tar"))
                        .build())
                .getClassification();
        assertThat(after.getState())
                .isEqualTo(ClassificationState.CLASSIFICATION_STATE_VERIFIED);
    }

    @Test
    void listingsFilterByStateAndTheCountsAreExact() {
        archives.putEntry(PutEntryRequest.newBuilder()
                .setAddress(address("mystery"))
                .addRenditions(RenditionContent.newBuilder()
                        .setRendition(RenditionDescriptor.newBuilder().setName("notes"))
                        .setData(ByteString.copyFrom(new byte[] {0x00, 0x01, 0x02})))
                .build());
        var unclassified = archives.listEntries(ListEntriesRequest.newBuilder()
                .setAccountId(ACCOUNT)
                .setArchive("classified")
                .setClassificationState(ClassificationState.CLASSIFICATION_STATE_UNCLASSIFIED)
                .build());
        assertThat(unclassified.getEntriesList())
                .anySatisfy(e -> assertThat(e.getAddress().getEntryId()).isEqualTo("mystery"));

        var stats = archives.getArchiveStats(GetArchiveStatsRequest.newBuilder()
                .setAccountId(ACCOUNT).setArchive("classified").build()).getStats();
        long counted = stats.getClassificationStatesList().stream()
                .mapToLong(ClassificationStateCount::getCount).sum();
        assertThat(counted).isEqualTo(stats.getEntries());
        assertThat(stats.getClassificationStatesList())
                .anySatisfy(s -> assertThat(s.getState())
                        .isEqualTo(ClassificationState.CLASSIFICATION_STATE_UNCLASSIFIED));
    }
}
