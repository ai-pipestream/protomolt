package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.asset.catalog.AssetCatalogRows;
import ai.protomolt.proto.asset.v1.AssetCatalogRow;
import ai.protomolt.proto.asset.v1.FormatFact;
import ai.protomolt.proto.asset.v1.PdfDocument;
import ai.protomolt.proto.asset.v1.TarArchive;
import ai.protomolt.proto.repo.archive.v1.Archive;
import ai.protomolt.proto.repo.archive.v1.ArchiveServiceGrpc;
import ai.protomolt.proto.repo.archive.v1.BridgeEntryRequest;
import ai.protomolt.proto.repo.archive.v1.CreateArchiveRequest;
import ai.protomolt.proto.repo.archive.v1.EntryAddress;
import ai.protomolt.proto.repo.archive.v1.ListEntriesRequest;
import ai.protomolt.proto.repo.archive.v1.ListEntriesResponse;
import ai.protomolt.proto.repo.archive.v1.PutEntryRequest;
import ai.protomolt.proto.repo.archive.v1.RenditionContent;
import ai.protomolt.proto.repo.archive.v1.RenditionDescriptor;
import ai.protomolt.proto.repo.archive.v1.VersioningPolicy;
import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import ai.protomolt.proto.repo.v1.CreateDriveRequest;
import ai.protomolt.proto.repo.v1.DriveServiceGrpc;
import ai.protomolt.proto.repo.v1.DriveType;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The catalog view over a real archive: one listing carries the manifests a
 * projection needs, and the rows it produces are the facts the archive
 * already stored — no second pass over the objects, no second store.
 */
@Testcontainers(disabledWithoutDocker = true)
class ArchiveCatalogIT {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Container
    static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
                    .withServices("s3");

    static RepoServices services;
    static ManagedChannel channel;
    static ArchiveServiceGrpc.ArchiveServiceBlockingStub archives;

    private static final String ACCOUNT = "acct-catalog";
    private static final String ARCHIVE = "catalogued";

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
                "it-catalog",
                0,
                null, null, null, null, 0, 0L);
        services = RepoServices.build(config);
        services.startInProcess("catalog-it");
        channel = InProcessChannelBuilder.forName("catalog-it").build();
        archives = ArchiveServiceGrpc.newBlockingStub(channel);
        DriveServiceGrpc.newBlockingStub(channel).createDrive(CreateDriveRequest.newBuilder()
                .setName("catalog-drive")
                .setAccountId(ACCOUNT)
                .setDriveType(DriveType.DRIVE_TYPE_CUSTOM)
                .build());
        archives.createArchive(CreateArchiveRequest.newBuilder()
                .setArchive(Archive.newBuilder()
                        .setName(ARCHIVE)
                        .setAccountId(ACCOUNT)
                        .setDriveName("catalog-drive")
                        .setVersioning(VersioningPolicy.VERSIONING_POLICY_RETAINED))
                .build());

        // A verified tar that gets bridged, a declared pdf nobody bridges,
        // and an asset nobody classified.
        put("bundle", "bundle.tar", tarClaim(), tar("inner.txt", "inner"));
        archives.bridgeEntry(BridgeEntryRequest.newBuilder()
                .setAddress(address("bundle")).build());
        put("paper", "paper.pdf", FormatFact.newBuilder()
                        .setPdf(PdfDocument.newBuilder().setFilename("paper.pdf")).build(),
                "%PDF-1.7 body".getBytes(StandardCharsets.UTF_8));
        archives.putEntry(PutEntryRequest.newBuilder()
                .setAddress(address("mystery"))
                .addRenditions(RenditionContent.newBuilder()
                        .setRendition(RenditionDescriptor.newBuilder().setName("original"))
                        .setData(ByteString.copyFrom(new byte[] {0x00, 0x01, 0x02, 0x03})))
                .build());
    }

    @AfterAll
    static void tearDown() {
        channel.shutdownNow();
        services.close();
    }

    @Test
    void oneListingCarriesEverythingAProjectionNeeds() {
        ListEntriesResponse listing = archives.listEntries(ListEntriesRequest.newBuilder()
                .setAccountId(ACCOUNT)
                .setArchive(ARCHIVE)
                .setIncludeManifests(true)
                .build());

        assertThat(listing.getEntriesCount()).isEqualTo(3);
        // The two lists zip: a projection walks them by index.
        assertThat(listing.getManifestsCount()).isEqualTo(listing.getEntriesCount());
        assertThat(listing.getManifestsList()).allSatisfy(manifest ->
                assertThat(manifest.getRenditionsCount()).isPositive());
    }

    @Test
    void aListingThatDidNotAskForManifestsDoesNotPayForThem() {
        ListEntriesResponse listing = archives.listEntries(ListEntriesRequest.newBuilder()
                .setAccountId(ACCOUNT).setArchive(ARCHIVE).build());

        assertThat(listing.getEntriesCount()).isEqualTo(3);
        assertThat(listing.getManifestsList()).isEmpty();
    }

    @Test
    void theRowsAreTheFactsTheArchiveAlreadyStored() {
        Map<String, AssetCatalogRow> rows = catalog();

        AssetCatalogRow bundle = rows.get("bundle");
        assertThat(bundle.getAccountId()).isEqualTo(ACCOUNT);
        assertThat(bundle.getArchive()).isEqualTo(ARCHIVE);
        assertThat(bundle.getClassificationState()).isEqualTo("VERIFIED");
        assertThat(bundle.getFormatKind()).isEqualTo("tar");
        assertThat(bundle.getVerified()).isTrue();
        // The bridge landed "members" beside "original".
        assertThat(bundle.getRenditionCount()).isEqualTo(2);
        assertThat(bundle.getDerivedRenditions()).isEqualTo(1);
        assertThat(bundle.getBridged()).isTrue();
        assertThat(bundle.getSizeBytes()).isPositive();
        assertThat(bundle.getClassifiedAt().getSeconds()).isPositive();

        AssetCatalogRow paper = rows.get("paper");
        assertThat(paper.getClassificationState()).isEqualTo("VERIFIED");
        assertThat(paper.getFormatKind()).isEqualTo("pdf");
        assertThat(paper.getBridged()).isFalse();
        assertThat(paper.getDerivedRenditions()).isZero();

        AssetCatalogRow mystery = rows.get("mystery");
        assertThat(mystery.getClassificationState()).isEqualTo("UNCLASSIFIED");
        assertThat(mystery.getFormatKind()).isEmpty();
        assertThat(mystery.getVerified()).isFalse();
        assertThat(mystery.getQualityMeasured()).isFalse();
        assertThat(mystery.getContentQuality())
                .isEqualTo(AssetCatalogRows.UNMEASURED_QUALITY);
    }

    @Test
    void aStateFilteredListingProjectsTheSameWay() {
        ListEntriesResponse listing = archives.listEntries(ListEntriesRequest.newBuilder()
                .setAccountId(ACCOUNT)
                .setArchive(ARCHIVE)
                .setIncludeManifests(true)
                .setClassificationState(
                        ai.protomolt.proto.asset.v1.ClassificationState
                                .CLASSIFICATION_STATE_UNCLASSIFIED)
                .build());

        assertThat(listing.getEntriesCount()).isEqualTo(1);
        assertThat(listing.getManifestsCount()).isEqualTo(1);
        assertThat(AssetCatalogRows.of(listing.getEntries(0), listing.getManifests(0))
                .getClassificationState()).isEqualTo("UNCLASSIFIED");
    }

    // ------------------------------------------------------------------

    private static Map<String, AssetCatalogRow> catalog() {
        ListEntriesResponse listing = archives.listEntries(ListEntriesRequest.newBuilder()
                .setAccountId(ACCOUNT)
                .setArchive(ARCHIVE)
                .setIncludeManifests(true)
                .build());
        List<AssetCatalogRow> rows = IntStream.range(0, listing.getEntriesCount())
                .mapToObj(i -> AssetCatalogRows.of(
                        listing.getEntries(i), listing.getManifests(i)))
                .toList();
        return rows.stream().collect(Collectors.toMap(
                AssetCatalogRow::getEntryId, Function.identity()));
    }

    private static void put(String entryId, String filename, FormatFact declared,
                            byte[] bytes) {
        archives.putEntry(PutEntryRequest.newBuilder()
                .setAddress(address(entryId))
                .setFilename(filename)
                .setDeclared(declared)
                .addRenditions(RenditionContent.newBuilder()
                        .setRendition(RenditionDescriptor.newBuilder().setName("original"))
                        .setData(ByteString.copyFrom(bytes)))
                .build());
    }

    private static EntryAddress address(String entryId) {
        return EntryAddress.newBuilder()
                .setAccountId(ACCOUNT).setArchive(ARCHIVE).setEntryId(entryId).build();
    }

    private static FormatFact tarClaim() {
        return FormatFact.newBuilder()
                .setTar(TarArchive.newBuilder().setFilename("bundle.tar")).build();
    }

    private static byte[] tar(String path, String content) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] body = content.getBytes(StandardCharsets.UTF_8);
        byte[] header = new byte[512];
        write(header, 0, path);
        write(header, 100, "0000644");
        write(header, 108, "0000000");
        write(header, 116, "0000000");
        write(header, 124, String.format("%011o", body.length));
        write(header, 136, String.format("%011o", 1_700_000_000L));
        Arrays.fill(header, 148, 156, (byte) ' ');
        header[156] = '0';
        write(header, 257, "ustar");
        write(header, 263, "00");
        int sum = 0;
        for (byte b : header) {
            sum += b & 0xFF;
        }
        write(header, 148, String.format("%06o", sum) + "\0");
        out.writeBytes(header);
        out.writeBytes(body);
        out.writeBytes(new byte[512 - body.length % 512]);
        out.writeBytes(new byte[1024]);
        return out.toByteArray();
    }

    private static void write(byte[] target, int offset, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        System.arraycopy(bytes, 0, target, offset, bytes.length);
    }
}
