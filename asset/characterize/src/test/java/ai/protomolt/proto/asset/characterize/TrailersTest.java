package ai.protomolt.proto.asset.characterize;

import ai.protomolt.proto.asset.characterize.Trailers.EndOfCentralDirectory;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Reading what a format writes at the end of its content. */
class TrailersTest {

    /** A real archive, built by the JDK writer that produces the ones we read. */
    private static byte[] zip(String comment, String... names) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ZipOutputStream out = new ZipOutputStream(bytes)) {
            if (comment != null) {
                out.setComment(comment);
            }
            for (String name : names) {
                out.putNextEntry(new ZipEntry(name));
                out.write(("content of " + name).getBytes(StandardCharsets.UTF_8));
                out.closeEntry();
            }
        }
        return bytes.toByteArray();
    }

    private static byte[] parquet(boolean sealed) {
        byte[] out = new byte[64];
        System.arraycopy(Trailers.PARQUET_MAGIC, 0, out, 0, 4);
        if (sealed) {
            System.arraycopy(Trailers.PARQUET_MAGIC, 0, out, 60, 4);
        }
        return out;
    }

    @Test
    @DisplayName("footer magic seals a Parquet file")
    void parquetSealed() {
        assertThat(Trailers.parquetSealed(ByteWindows.ofWhole(parquet(true)))).isTrue();
        assertThat(Trailers.parquetSealed(ByteWindows.ofWhole(parquet(false)))).isFalse();
    }

    @Test
    @DisplayName("an unobservable footer is not the same as an absent one")
    void parquetFooterOutsideTheWindow() {
        ByteWindows headOnly = ByteWindows.ofHead(parquet(true));
        assertThat(Trailers.parquetFooterObservable(headOnly)).isFalse();
        assertThat(Trailers.parquetSealed(headOnly)).isFalse();
        assertThat(Trailers.parquetFooterObservable(ByteWindows.ofWhole(parquet(false))))
                .isTrue();
    }

    @Test
    @DisplayName("the central directory record is found and read")
    void zipDirectory() throws IOException {
        byte[] archive = zip(null, "word/document.xml", "[Content_Types].xml");
        EndOfCentralDirectory directory = Trailers.zipDirectory(ByteWindows.ofWhole(archive));
        assertThat(directory).isNotNull();
        assertThat(directory.entryCount()).isEqualTo(2);
        assertThat(directory.zip64()).isFalse();
        assertThat(directory.directoryOffset() + directory.directoryBytes())
                .isEqualTo(directory.recordOffset());
    }

    @Test
    @DisplayName("the directory sits immediately before the record, so it is resident too")
    void directoryIsResident() throws IOException {
        byte[] archive = zip(null, "a.txt", "b.txt", "c.txt");
        ByteWindows windows = ByteWindows.of(
                Arrays.copyOfRange(archive, 0, 32),
                Arrays.copyOfRange(archive, archive.length - 256, archive.length),
                archive.length);
        EndOfCentralDirectory directory = Trailers.zipDirectory(windows);
        assertThat(directory).isNotNull();
        assertThat(Trailers.directoryResident(windows, directory)).isTrue();
    }

    @Test
    @DisplayName("a comment carrying the record's own signature does not fool the search")
    void commentContainingTheSignature() throws IOException {
        // The backwards search has to keep going until the declared comment
        // length actually reaches the end of the content.
        String decoy = new String(new byte[] {'P', 'K', 0x05, 0x06}, StandardCharsets.ISO_8859_1)
                + " looks like a record but is not";
        byte[] archive = zip(decoy, "a.txt");
        EndOfCentralDirectory directory = Trailers.zipDirectory(ByteWindows.ofWhole(archive));
        assertThat(directory).isNotNull();
        assertThat(directory.entryCount()).isEqualTo(1);
        assertThat(directory.recordOffset() + 22
                + decoy.getBytes(StandardCharsets.UTF_8).length).isEqualTo(archive.length);
    }

    @Test
    @DisplayName("a truncated archive declares no directory")
    void truncated() throws IOException {
        byte[] archive = zip(null, "a.txt", "b.txt");
        byte[] cut = Arrays.copyOfRange(archive, 0, archive.length - 8);
        assertThat(Trailers.zipDirectory(ByteWindows.ofWhole(cut))).isNull();
    }

    @Test
    @DisplayName("without a trailing window the directory is not looked for")
    void noTrailingWindow() throws IOException {
        byte[] archive = zip(null, "a.txt");
        assertThat(Trailers.zipDirectory(ByteWindows.ofHead(archive))).isNull();
    }

    @Test
    @DisplayName("content too short to hold a record declares none")
    void tooShort() {
        assertThat(Trailers.zipDirectory(ByteWindows.ofWhole(new byte[4]))).isNull();
        assertThat(Trailers.zipDirectory(ByteWindows.EMPTY)).isNull();
    }

    @Test
    @DisplayName("a directory outside the captured window is located but not resident")
    void directoryOutsideTheWindow() throws IOException {
        String[] names = new String[400];
        for (int i = 0; i < names.length; i++) {
            names[i] = "a-fairly-long-entry-name-so-the-directory-grows/" + i + ".txt";
        }
        byte[] archive = zip(null, names);
        // A trailing window that reaches the record but not the whole
        // directory it points at.
        int tail = 512;
        ByteWindows windows = ByteWindows.of(
                Arrays.copyOfRange(archive, 0, 32),
                Arrays.copyOfRange(archive, archive.length - tail, archive.length),
                archive.length);
        EndOfCentralDirectory directory = Trailers.zipDirectory(windows);
        assertThat(directory).isNotNull();
        assertThat(directory.entryCount()).isEqualTo(400);
        assertThat(directory.directoryBytes()).isGreaterThan(tail);
        assertThat(Trailers.directoryResident(windows, directory)).isFalse();
    }
}
