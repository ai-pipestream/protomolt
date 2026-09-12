package ai.protomolt.proto.asset.bridge;

import ai.protomolt.proto.asset.v1.ContainerMember;
import ai.protomolt.proto.asset.v1.ContainerMembers;
import ai.protomolt.proto.asset.v1.FormatFact;
import ai.protomolt.proto.asset.v1.TarArchive;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The container bridge against real tar, tar.gz, and zip bytes. */
class ContainerMembersBridgeTest {

    private static final Bridge.Context TAR = new Bridge.Context(
            FormatFact.newBuilder()
                    .setTar(TarArchive.newBuilder().setFilename("bundle.tar")).build(),
            "bundle.tar");

    private final ContainerMembersBridge bridge = new ContainerMembersBridge();

    @Test
    void aTarListsItsMembersWithSizesAndFixity() throws IOException {
        byte[] archive = Tars.archive()
                .directory("docs")
                .file("docs/readme.md", "# hello")
                .file("data.bin", new byte[1000])
                .bytes();

        ContainerMembers members = list(archive);

        assertThat(members.getMemberCount()).isEqualTo(3);
        assertThat(members.getTruncated()).isFalse();
        assertThat(members.getMembersList()).extracting(ContainerMember::getPath)
                .containsExactly("docs/", "docs/readme.md", "data.bin");
        assertThat(members.getMembers(0).getDirectory()).isTrue();
        assertThat(members.getMembers(0).getSha256()).isEmpty();
        assertThat(members.getMembers(1).getSizeBytes()).isEqualTo(7);
        assertThat(members.getMembers(1).getSha256()).isEqualTo(sha256("# hello"));
        assertThat(members.getMembers(2).getSizeBytes()).isEqualTo(1000);
        // total_bytes sums the members, not the container's own size.
        assertThat(members.getTotalBytes()).isEqualTo(1007);
        assertThat(members.getMembers(1).getModifiedAt().getSeconds()).isEqualTo(1_700_000_000L);
    }

    @Test
    void aUstarNamePrefixRejoinsTheFullPath() throws IOException {
        byte[] archive = Tars.archive()
                .prefixedFile("very/deep/directory/tree", "leaf.txt", "x")
                .bytes();

        assertThat(list(archive).getMembers(0).getPath())
                .isEqualTo("very/deep/directory/tree/leaf.txt");
    }

    @Test
    void aGnuLongNameIsThePathTheArchiveRecords() throws IOException {
        String path = "a/" + "long/".repeat(30) + "name.txt";
        byte[] archive = Tars.archive().gnuLongName(path, "payload").bytes();

        ContainerMembers members = list(archive);
        assertThat(members.getMemberCount()).isEqualTo(1);
        assertThat(members.getMembers(0).getPath()).isEqualTo(path);
        assertThat(members.getMembers(0).getSha256()).isEqualTo(sha256("payload"));
    }

    @Test
    void aPaxPathRecordOverridesTheNameField() throws IOException {
        String path = "unicode/éè/report.txt";
        byte[] archive = Tars.archive().paxPath(path, "payload").bytes();

        ContainerMembers members = list(archive);
        assertThat(members.getMemberCount()).isEqualTo(1);
        assertThat(members.getMembers(0).getPath()).isEqualTo(path);
    }

    @Test
    void aCompressedTarIsUnwrappedFromItsBytesNotItsName() throws IOException {
        byte[] archive = Tars.archive().file("inner.txt", "compressed").bytes();
        ByteArrayOutputStream gz = new ByteArrayOutputStream();
        try (GZIPOutputStream out = new GZIPOutputStream(gz)) {
            out.write(archive);
        }

        // No filename is supplied: the gzip magic alone decides.
        ContainerMembers members = list(gz.toByteArray());
        assertThat(members.getMembersList()).extracting(ContainerMember::getPath)
                .containsExactly("inner.txt");
        assertThat(members.getMembers(0).getSha256()).isEqualTo(sha256("compressed"));
    }

    @Test
    void aZipListsItsMembersWithTheSizesThatCameOutOfTheDecompressor() throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try (ZipOutputStream zip = new ZipOutputStream(buffer)) {
            zip.putNextEntry(new ZipEntry("folder/"));
            zip.closeEntry();
            zip.putNextEntry(new ZipEntry("folder/note.txt"));
            zip.write("zipped".getBytes(StandardCharsets.UTF_8));
            zip.closeEntry();
        }

        ContainerMembers members = list(buffer.toByteArray());

        assertThat(members.getMembersList()).extracting(ContainerMember::getPath)
                .containsExactly("folder/", "folder/note.txt");
        assertThat(members.getMembers(0).getDirectory()).isTrue();
        assertThat(members.getMembers(1).getSizeBytes()).isEqualTo(6);
        assertThat(members.getMembers(1).getSha256()).isEqualTo(sha256("zipped"));
        assertThat(members.getTotalBytes()).isEqualTo(6);
    }

    @Test
    void aBoundedListingSaysSoRatherThanPassingItselfOffAsComplete() throws IOException {
        Tars archive = Tars.archive();
        for (int i = 0; i < 5; i++) {
            archive.file("member-" + i + ".txt", "x");
        }
        ContainerMembersBridge bounded = new ContainerMembersBridge(3, 1 << 20);

        Bridge.Derivation derivation = bounded.derive(
                new ByteArrayInputStream(archive.bytes()), TAR);
        ContainerMembers members = ContainerMembers.parseFrom(derivation.content());

        assertThat(members.getMemberCount()).isEqualTo(3);
        assertThat(members.getTruncated()).isTrue();
        assertThat(members.getTruncationReason()).isEqualTo("member limit 3 reached");
        assertThat(derivation.degraded()).isTrue();
        assertThat(derivation.warnings())
                .containsExactly("container listing truncated: member limit 3 reached");
    }

    @Test
    void hashingStopsAtItsBudgetAndTheListingKeepsGoing() throws IOException {
        byte[] archive = Tars.archive()
                .file("big.bin", new byte[4096])
                .file("small.txt", "after the budget")
                .bytes();
        ContainerMembersBridge stingy = new ContainerMembersBridge(50, 1024);

        ContainerMembers members = ContainerMembers.parseFrom(stingy.derive(
                new ByteArrayInputStream(archive), TAR).content());

        // Both members are listed with their real sizes; neither is hashed.
        // The first blows the budget on its own, and fixity then stops for
        // the whole listing so a blank hash means one thing, not two.
        assertThat(members.getMemberCount()).isEqualTo(2);
        assertThat(members.getMembers(0).getSizeBytes()).isEqualTo(4096);
        assertThat(members.getMembers(0).getSha256()).isEmpty();
        assertThat(members.getMembers(1).getSizeBytes()).isEqualTo(16);
        assertThat(members.getMembers(1).getSha256()).isEmpty();
        assertThat(members.getTruncated()).as("the listing is complete").isFalse();
    }

    @Test
    void aListingThatStoppedHashingReportsTheOmission() throws IOException {
        byte[] archive = Tars.archive().file("big.bin", new byte[4096]).bytes();

        Bridge.Derivation derivation = new ContainerMembersBridge(50, 1024)
                .derive(new ByteArrayInputStream(archive), TAR);

        assertThat(derivation.degraded()).isTrue();
        assertThat(derivation.warnings()).containsExactly(
                "member fixity omitted past the 1024 byte hashing budget");
    }

    @Test
    void bytesThatAreNoContainerFailTheBridgeByName() {
        byte[] notAnArchive = "this is just some text, not an archive at all\n"
                .repeat(40).getBytes(StandardCharsets.UTF_8);

        assertThatThrownBy(() -> bridge.derive(new ByteArrayInputStream(notAnArchive), TAR))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("not a tar header block");
    }

    @Test
    void anEmptyArchiveListsNothingAndClaimsNothing() throws IOException {
        ContainerMembers members = list(Tars.archive().bytes());

        assertThat(members.getMemberCount()).isZero();
        assertThat(members.getTotalBytes()).isZero();
        assertThat(members.getTruncated()).isFalse();
    }

    private ContainerMembers list(byte[] container) throws IOException {
        Bridge.Derivation derivation =
                bridge.derive(new ByteArrayInputStream(container), TAR);
        assertThat(derivation.profile())
                .as("a structural listing describes no content class")
                .isNull();
        return ContainerMembers.parseFrom(derivation.content());
    }

    private static String sha256(String content) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(content.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
