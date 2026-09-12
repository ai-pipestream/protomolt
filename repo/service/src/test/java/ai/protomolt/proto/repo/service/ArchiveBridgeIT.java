package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.asset.v1.BridgeKind;
import ai.protomolt.proto.asset.v1.BridgeStatus;
import ai.protomolt.proto.asset.v1.ContainerMember;
import ai.protomolt.proto.asset.v1.ContainerMembers;
import ai.protomolt.proto.asset.v1.DatasetField;
import ai.protomolt.proto.asset.v1.DatasetSchema;
import ai.protomolt.proto.asset.v1.DelimitedTable;
import ai.protomolt.proto.asset.v1.FieldKind;
import ai.protomolt.proto.asset.v1.HeaderPresence;
import ai.protomolt.proto.asset.v1.ParquetDataset;
import ai.protomolt.proto.asset.v1.FormatFact;
import ai.protomolt.proto.asset.v1.PdfDocument;
import ai.protomolt.proto.asset.v1.PlainText;
import ai.protomolt.proto.asset.v1.TarArchive;
import ai.protomolt.proto.asset.v1.ZipArchive;
import ai.protomolt.proto.repo.archive.v1.Archive;
import ai.protomolt.proto.repo.archive.v1.ArchiveServiceGrpc;
import ai.protomolt.proto.repo.archive.v1.BridgeEntryRequest;
import ai.protomolt.proto.repo.archive.v1.BridgeEntryResponse;
import ai.protomolt.proto.repo.archive.v1.ClassifyEntryRequest;
import ai.protomolt.proto.repo.archive.v1.CreateArchiveRequest;
import ai.protomolt.proto.repo.archive.v1.EntryAddress;
import ai.protomolt.proto.repo.archive.v1.GetEntryRequest;
import ai.protomolt.proto.repo.archive.v1.GetEntryResponse;
import ai.protomolt.proto.repo.archive.v1.PutEntryRequest;
import ai.protomolt.proto.repo.archive.v1.RenditionContent;
import ai.protomolt.proto.repo.archive.v1.RenditionDescriptor;
import ai.protomolt.proto.repo.archive.v1.RenditionManifestEntry;
import ai.protomolt.proto.repo.archive.v1.VersioningPolicy;
import ai.protomolt.proto.repo.archive.v1.WriteAttribution;
import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import ai.protomolt.proto.repo.v1.CreateDriveRequest;
import ai.protomolt.proto.repo.v1.DriveServiceGrpc;
import ai.protomolt.proto.repo.v1.DriveType;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Bridging end to end against real infrastructure: a classified container
 * derives its member listing beside the untouched original, re-running is
 * idempotent by content, a bridge the host does not run is reported as
 * deferred rather than silently skipped, and an asset whose classification
 * names no single format is refused.
 */
@Testcontainers(disabledWithoutDocker = true)
class ArchiveBridgeIT {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Container
    static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
                    .withServices("s3");

    static RepoServices services;
    static ManagedChannel channel;
    static ArchiveServiceGrpc.ArchiveServiceBlockingStub archives;

    private static final String ACCOUNT = "acct-bridge";
    private static final String ARCHIVE = "bridged";

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
                "it-bridge",
                0,
                null, null, null, null, 0, 0L);
        services = RepoServices.build(config);
        services.startInProcess("bridge-it");
        channel = InProcessChannelBuilder.forName("bridge-it").build();
        archives = ArchiveServiceGrpc.newBlockingStub(channel);
        DriveServiceGrpc.newBlockingStub(channel).createDrive(CreateDriveRequest.newBuilder()
                .setName("bridge-drive")
                .setAccountId(ACCOUNT)
                .setDriveType(DriveType.DRIVE_TYPE_CUSTOM)
                .build());
        archives.createArchive(CreateArchiveRequest.newBuilder()
                .setArchive(Archive.newBuilder()
                        .setName(ARCHIVE)
                        .setAccountId(ACCOUNT)
                        .setDriveName("bridge-drive")
                        .setVersioning(VersioningPolicy.VERSIONING_POLICY_RETAINED))
                .build());
    }

    @AfterAll
    static void tearDown() {
        channel.shutdownNow();
        services.close();
    }

    @Test
    void aClassifiedTarDerivesItsMemberListingBesideTheUntouchedOriginal()
            throws InvalidProtocolBufferException {
        byte[] tar = tar("docs/readme.md", "# hello", "data.csv", "a,b\n1,2\n");
        put("bundle", "bundle.tar", tarClaim("bundle.tar"), tar);

        BridgeEntryResponse response = archives.bridgeEntry(BridgeEntryRequest.newBuilder()
                .setAddress(address("bundle"))
                .setBridgedBy(WriteAttribution.newBuilder().setModule("bridge-it"))
                .build());

        assertThat(response.getOutcomesList()).singleElement().satisfies(outcome -> {
            assertThat(outcome.getBridge()).isEqualTo(BridgeKind.BRIDGE_KIND_CONTAINER_MEMBERS);
            assertThat(outcome.getRendition()).isEqualTo("members");
            assertThat(outcome.getStatus()).isEqualTo(BridgeStatus.BRIDGE_STATUS_PRODUCED);
            assertThat(outcome.getDetail()).isEmpty();
        });
        assertThat(response.getVersion()).isEqualTo(2);

        GetEntryResponse entry = archives.getEntry(GetEntryRequest.newBuilder()
                .setAddress(address("bundle")).build());
        RenditionManifestEntry derived = renditionNamed(entry, "members");

        // The derived rendition pins its own shape, so the bytes are
        // schema-validated data rather than an opaque blob.
        assertThat(derived.getRendition().getMediaType()).isEqualTo("application/x-protobuf");
        assertThat(derived.getRendition().getSchemaSubject())
                .isEqualTo("ai.protomolt.proto.asset.v1.ContainerMembers");
        assertThat(derived.getWrittenBy().getModule()).isEqualTo("bridge-it");

        ContainerMembers members = ContainerMembers.parseFrom(bytesOf(entry, "members"));
        assertThat(members.getMembersList()).extracting(ContainerMember::getPath)
                .containsExactly("docs/readme.md", "data.csv");
        assertThat(members.getMembers(0).getSizeBytes()).isEqualTo(7);
        assertThat(members.getTruncated()).isFalse();

        // The original is carried by reference: same bytes, same object key,
        // and the write that produced it keeps its own stamp.
        RenditionManifestEntry original = renditionNamed(entry, "original");
        assertThat(bytesOf(entry, "original").toByteArray()).isEqualTo(tar);
        assertThat(original.getWrittenBy().getModule()).isEqualTo("uploader");
    }

    @Test
    void reRunningABridgeIsIdempotentByContent() {
        put("stable", "stable.tar", tarClaim("stable.tar"), tar("only.txt", "same"));

        BridgeEntryResponse first = archives.bridgeEntry(bridge("stable"));
        assertThat(first.getOutcomes(0).getStatus()).isEqualTo(BridgeStatus.BRIDGE_STATUS_PRODUCED);

        BridgeEntryResponse again = archives.bridgeEntry(bridge("stable"));

        // Identical output hashes to the same key, the root checksum does not
        // move, and no version lands.
        assertThat(again.getOutcomes(0).getStatus())
                .isEqualTo(BridgeStatus.BRIDGE_STATUS_UNCHANGED);
        assertThat(again.getVersion()).isEqualTo(first.getVersion());
    }

    @Test
    void aZipIsBridgedFromItsBytesTheSameWay() throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try (ZipOutputStream zip = new ZipOutputStream(buffer)) {
            zip.putNextEntry(new ZipEntry("report.txt"));
            zip.write("zipped content".getBytes(StandardCharsets.UTF_8));
            zip.closeEntry();
        }
        put("archive-zip", "archive.zip", FormatFact.newBuilder()
                .setZip(ZipArchive.newBuilder().setFilename("archive.zip")).build(),
                buffer.toByteArray());

        archives.bridgeEntry(bridge("archive-zip"));

        GetEntryResponse entry = archives.getEntry(GetEntryRequest.newBuilder()
                .setAddress(address("archive-zip")).build());
        assertThat(renditionNames(entry)).contains("members");
    }

    @Test
    void aDeclaredCsvDerivesItsSchemaFromTheDeclarationsOwnParsingRules()
            throws InvalidProtocolBufferException {
        // Characterization never identifies a delimited table, because the
        // delimiter and header presence are part of the producer's claim.
        // The bridge reads them straight off that claim.
        put("rows", "rows.csv", FormatFact.newBuilder()
                        .setDelimited(DelimitedTable.newBuilder()
                                .setFilename("rows.csv")
                                .setDelimiter(";")
                                .setHeader(HeaderPresence.HEADER_PRESENCE_PRESENT)).build(),
                "id;label;ratio\n1;first;0.5\n2;second;1.5\n"
                        .getBytes(StandardCharsets.UTF_8));

        BridgeEntryResponse response = archives.bridgeEntry(bridge("rows"));

        assertThat(response.getOutcomesList()).extracting(
                        outcome -> outcome.getBridge() + ":" + outcome.getStatus())
                .containsExactly(
                        "BRIDGE_KIND_DATASET_SCHEMA:BRIDGE_STATUS_PRODUCED",
                        "BRIDGE_KIND_TABULAR_DATASET:BRIDGE_STATUS_DEFERRED");

        GetEntryResponse entry = archives.getEntry(GetEntryRequest.newBuilder()
                .setAddress(address("rows")).build());
        assertThat(renditionNamed(entry, "schema").getRendition().getSchemaSubject())
                .isEqualTo("ai.protomolt.proto.asset.v1.DatasetSchema");

        DatasetSchema schema = DatasetSchema.parseFrom(bytesOf(entry, "schema"));
        assertThat(schema.getFieldsList()).extracting(DatasetField::getName)
                .containsExactly("id", "label", "ratio");
        assertThat(schema.getFieldsList()).extracting(DatasetField::getKind)
                .containsExactly(FieldKind.FIELD_KIND_INTEGER,
                        FieldKind.FIELD_KIND_STRING,
                        FieldKind.FIELD_KIND_DOUBLE);
        assertThat(schema.getRowCount()).isEqualTo(2);
        assertThat(schema.getDeclaredByFormat()).isFalse();
    }

    @Test
    void aParquetAssetDefersItsSchemaWithTheReasonNamed() {
        put("columns", "columns.parquet", FormatFact.newBuilder()
                        .setParquet(ParquetDataset.newBuilder()
                                .setFilename("columns.parquet")).build(),
                "PAR1 not really a parquet file".getBytes(StandardCharsets.UTF_8));

        BridgeEntryResponse response = archives.bridgeEntry(bridge("columns"));

        // One kind, two formats, two answers: the schema bridge reads a CSV
        // and does not read a Parquet footer, and the caller is told which.
        assertThat(response.getOutcomesList()).singleElement().satisfies(outcome -> {
            assertThat(outcome.getBridge()).isEqualTo(BridgeKind.BRIDGE_KIND_DATASET_SCHEMA);
            assertThat(outcome.getStatus()).isEqualTo(BridgeStatus.BRIDGE_STATUS_DEFERRED);
            assertThat(outcome.getDetail()).contains("Parquet reader");
        });
        assertThat(response.getVersion()).isEqualTo(1);
    }

    @Test
    void aBridgeThisHostDoesNotRunIsDeferredNotSkipped() {
        // A PDF applies the text bridge, whose extraction rides a parser
        // service. The archive says so by name instead of reporting success.
        put("paper", "paper.pdf", FormatFact.newBuilder()
                        .setPdf(PdfDocument.newBuilder().setFilename("paper.pdf")).build(),
                "%PDF-1.7\nnot really a pdf".getBytes(StandardCharsets.UTF_8));

        BridgeEntryResponse response = archives.bridgeEntry(bridge("paper"));

        assertThat(response.getOutcomesList()).singleElement().satisfies(outcome -> {
            assertThat(outcome.getBridge()).isEqualTo(BridgeKind.BRIDGE_KIND_DOCUMENT_TEXT);
            assertThat(outcome.getRendition()).isEqualTo("text");
            assertThat(outcome.getStatus()).isEqualTo(BridgeStatus.BRIDGE_STATUS_DEFERRED);
            assertThat(outcome.getDetail()).contains("parser service");
        });
        // Nothing landed, so the entry is still at the version the save made.
        assertThat(response.getVersion()).isEqualTo(1);
    }

    @Test
    void aFormatThatBridgesToNothingRunsNothingAndSaysNothingRan() {
        put("notes", "notes.txt", FormatFact.newBuilder()
                        .setText(PlainText.newBuilder()
                                .setFilename("notes.txt")).build(),
                "just some prose".getBytes(StandardCharsets.UTF_8));

        BridgeEntryResponse response = archives.bridgeEntry(bridge("notes"));

        assertThat(response.getOutcomesList()).isEmpty();
        assertThat(response.getVersion()).isEqualTo(1);
    }

    @Test
    void anUnclassifiedEntryIsRefusedByItsState() {
        archives.putEntry(PutEntryRequest.newBuilder()
                .setAddress(address("mystery"))
                .addRenditions(RenditionContent.newBuilder()
                        .setRendition(RenditionDescriptor.newBuilder().setName("original"))
                        .setData(ByteString.copyFrom(new byte[] {0x00, 0x01, 0x02, 0x03})))
                .build());

        assertThatThrownBy(() -> archives.bridgeEntry(bridge("mystery")))
                .isInstanceOf(StatusRuntimeException.class)
                .satisfies(e -> assertThat(((StatusRuntimeException) e).getStatus().getCode())
                        .isEqualTo(Status.Code.FAILED_PRECONDITION))
                .hasMessageContaining("UNCLASSIFIED")
                .hasMessageContaining("exactly one format");
    }

    @Test
    void aConflictedEntryIsRefusedBecauseItNamesTwoFormats() {
        // A tar declaration over PDF bytes: the declaration is rule-valid, the
        // bytes rule it out, and the entry lands CONFLICTED.
        put("liar", "liar.tar", tarClaim("liar.tar"),
                "%PDF-1.7 this is a pdf".getBytes(StandardCharsets.UTF_8));

        assertThatThrownBy(() -> archives.bridgeEntry(bridge("liar")))
                .isInstanceOf(StatusRuntimeException.class)
                .hasMessageContaining("CONFLICTED")
                .hasMessageContaining("exactly one format");
    }

    @Test
    void askingForABridgeTheFormatDoesNotApplyIsRefusedByName() {
        put("plain", "plain.tar", tarClaim("plain.tar"), tar("a.txt", "a"));

        assertThatThrownBy(() -> archives.bridgeEntry(BridgeEntryRequest.newBuilder()
                .setAddress(address("plain"))
                .addBridges(BridgeKind.BRIDGE_KIND_OCR_TEXT)
                .build()))
                .isInstanceOf(StatusRuntimeException.class)
                .satisfies(e -> assertThat(((StatusRuntimeException) e).getStatus().getCode())
                        .isEqualTo(Status.Code.INVALID_ARGUMENT))
                .hasMessageContaining("BRIDGE_KIND_OCR_TEXT does not apply");
    }

    @Test
    void aBridgeThatCannotReadItsInputFailsLoudAndLandsNothing() {
        // One valid member, then rubbish where the next header belongs: the
        // bytes still sniff as a tar, so the entry is VERIFIED and the bridge
        // runs, and the bridge is the one that discovers the damage.
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(tarHeader("a.txt", 1));
        out.writeBytes("a".getBytes(StandardCharsets.UTF_8));
        out.writeBytes(new byte[511]);
        byte[] rubbish = new byte[512];
        Arrays.fill(rubbish, (byte) 0x41);
        out.writeBytes(rubbish);
        put("broken", "broken.tar", tarClaim("broken.tar"), out.toByteArray());

        BridgeEntryResponse response = archives.bridgeEntry(bridge("broken"));

        assertThat(response.getOutcomes(0).getStatus())
                .isEqualTo(BridgeStatus.BRIDGE_STATUS_FAILED);
        assertThat(response.getOutcomes(0).getDetail()).isNotBlank();
        assertThat(response.getVersion()).isEqualTo(1);
        assertThat(renditionNames(archives.getEntry(GetEntryRequest.newBuilder()
                .setAddress(address("broken")).build())))
                .doesNotContain("members");
    }

    @Test
    void aDerivedRenditionNeverBecomesTheEntrysPrimary() {
        // The original is named "zip-file", which sorts AFTER "members".
        // Without the derived-name rule the bridge's own output would become
        // the rendition the entry is characterized from.
        byte[] tar = tar("inner.txt", "inner");
        archives.putEntry(PutEntryRequest.newBuilder()
                .setAddress(address("oddly-named"))
                .setFilename("oddly-named.tar")
                .setDeclared(tarClaim("oddly-named.tar"))
                .addRenditions(RenditionContent.newBuilder()
                        .setRendition(RenditionDescriptor.newBuilder().setName("zip-file"))
                        .setData(ByteString.copyFrom(tar)))
                .build());

        archives.bridgeEntry(bridge("oddly-named"));

        // Re-classifying reads the primary's bytes. If "members" had become
        // the primary, the entry would now be characterized from protobuf.
        var classification = archives.classifyEntry(ClassifyEntryRequest.newBuilder()
                        .setAddress(address("oddly-named")).build())
                .getClassification();
        assertThat(classification.getIdentified().getFormatCase())
                .isEqualTo(FormatFact.FormatCase.TAR);
    }

    // ------------------------------------------------------------------

    private static BridgeEntryRequest bridge(String entryId) {
        return BridgeEntryRequest.newBuilder().setAddress(address(entryId)).build();
    }

    private static void put(String entryId, String filename, FormatFact declared, byte[] bytes) {
        archives.putEntry(PutEntryRequest.newBuilder()
                .setAddress(address(entryId))
                .setFilename(filename)
                .setDeclared(declared)
                .setWrittenBy(WriteAttribution.newBuilder().setModule("uploader"))
                .addRenditions(RenditionContent.newBuilder()
                        .setRendition(RenditionDescriptor.newBuilder().setName("original"))
                        .setData(ByteString.copyFrom(bytes)))
                .build());
    }

    private static EntryAddress address(String entryId) {
        return EntryAddress.newBuilder()
                .setAccountId(ACCOUNT)
                .setArchive(ARCHIVE)
                .setEntryId(entryId)
                .build();
    }

    private static ByteString bytesOf(GetEntryResponse entry, String name) {
        return entry.getRenditionsList().stream()
                .filter(item -> item.getRendition().getName().equals(name))
                .findFirst().orElseThrow(() ->
                        new AssertionError("no bytes for rendition '" + name + "'"))
                .getData();
    }

    private static List<String> renditionNames(GetEntryResponse entry) {
        return entry.getRenditionsList().stream()
                .map(item -> item.getRendition().getName()).toList();
    }

    private static RenditionManifestEntry renditionNamed(GetEntryResponse entry, String name) {
        return entry.getManifest().getRenditionsList().stream()
                .filter(item -> item.getRendition().getName().equals(name))
                .findFirst().orElseThrow(() ->
                        new AssertionError("no rendition named '" + name + "'"));
    }

    private static FormatFact tarClaim(String filename) {
        return FormatFact.newBuilder()
                .setTar(TarArchive.newBuilder().setFilename(filename)).build();
    }

    /** A real ustar archive of the given path/content pairs. */
    private static byte[] tar(String... pathsAndContents) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (int i = 0; i < pathsAndContents.length; i += 2) {
            byte[] content = pathsAndContents[i + 1].getBytes(StandardCharsets.UTF_8);
            out.writeBytes(tarHeader(pathsAndContents[i], content.length));
            out.writeBytes(content);
            int padding = content.length % 512;
            if (padding != 0) {
                out.writeBytes(new byte[512 - padding]);
            }
        }
        out.writeBytes(new byte[1024]);
        return out.toByteArray();
    }

    private static byte[] tarHeader(String name, int size) {
        byte[] header = new byte[512];
        put(header, 0, name);
        put(header, 100, "0000644");
        put(header, 108, "0000000");
        put(header, 116, "0000000");
        put(header, 124, String.format("%011o", size));
        put(header, 136, String.format("%011o", 1_700_000_000L));
        Arrays.fill(header, 148, 156, (byte) ' ');
        header[156] = '0';
        put(header, 257, "ustar");
        put(header, 263, "00");
        int sum = 0;
        for (byte b : header) {
            sum += b & 0xFF;
        }
        put(header, 148, String.format("%06o", sum) + "\0");
        return header;
    }

    private static void put(byte[] target, int offset, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        System.arraycopy(bytes, 0, target, offset, bytes.length);
    }
}
