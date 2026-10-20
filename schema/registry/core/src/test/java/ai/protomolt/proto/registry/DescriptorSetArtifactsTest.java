package ai.protomolt.proto.registry;

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.assertThatNoException;

/**
 * Registry descriptor-set artifacts: a lowercase SHA-256 fingerprint that matches the
 * bytes, a non-empty set of at least one file within 16 MiB, read without following links.
 */
class DescriptorSetArtifactsTest {

    private static final ByteString SET = FileDescriptorSet.newBuilder()
            .addFile(FileDescriptorProto.newBuilder().setName("a.proto").setPackage("a"))
            .build().toByteString();
    private static final String ZEROS = "0".repeat(64);

    @Test
    void aMatchingSetIsAccepted() {
        assertThatNoException().isThrownBy(() -> DescriptorSetArtifacts.validate(sha256(SET), SET));
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"", "abc", "ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789",
            "g000000000000000000000000000000000000000000000000000000000000000",
            "00000000000000000000000000000000000000000000000000000000000000000"})
    void aFingerprintMustBe64LowercaseHexCharacters(String fingerprint) {
        assertThatThrownBy(() -> DescriptorSetArtifacts.requireFingerprint(fingerprint))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("descriptor fingerprint must be a lowercase SHA-256 hex string");
    }

    @Test
    void anUppercaseFingerprintOfTheRightBytesIsStillRefused() {
        assertThatThrownBy(() -> DescriptorSetArtifacts.validate(sha256(SET).toUpperCase(), SET))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("descriptor fingerprint must be a lowercase SHA-256 hex string");
    }

    @Test
    void aNullSetIsRefused() {
        assertThatThrownBy(() -> DescriptorSetArtifacts.validate(ZEROS, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("descriptorSet");
    }

    @Test
    void anEmptySetIsRefused() {
        assertThatThrownBy(() -> DescriptorSetArtifacts.validate(ZEROS, ByteString.EMPTY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("descriptor set must not be empty");
    }

    @Test
    void aSetOver16MiBIsRefusedBeforeItIsHashed() {
        ByteString large = ByteString.copyFrom(new byte[16 * 1024 * 1024 + 1]);

        assertThatThrownBy(() -> DescriptorSetArtifacts.validate(ZEROS, large))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("descriptor set exceeds the 16 MiB limit");
    }

    @Test
    void aFingerprintOfOtherBytesIsRefused() {
        assertThatThrownBy(() -> DescriptorSetArtifacts.validate(ZEROS, SET))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("descriptor fingerprint does not match descriptor-set bytes");
    }

    @Test
    void bytesThatAreNotADescriptorSetAreRefusedWithTheParseCause() {
        // Field 1 as a length-delimited value claiming more bytes than follow.
        ByteString truncated = ByteString.copyFrom(new byte[] {0x0A, 0x05, 0x01});

        assertThatThrownBy(() -> DescriptorSetArtifacts.validate(sha256(truncated), truncated))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("descriptor set is not a FileDescriptorSet")
                .hasCauseInstanceOf(InvalidProtocolBufferException.class);
    }

    @Test
    void aSetWithNoFilesIsRefused() {
        // A lone unknown varint field parses as a FileDescriptorSet with no files.
        ByteString noFiles = ByteString.copyFrom(new byte[] {0x10, 0x01});

        assertThatThrownBy(() -> DescriptorSetArtifacts.validate(sha256(noFiles), noFiles))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("descriptor set must contain at least one file");
    }

    @Test
    void readReturnsTheValidatedBytes(@TempDir Path dir) throws IOException {
        Path file = Files.write(dir.resolve("set.pb"), SET.toByteArray());

        assertThat(DescriptorSetArtifacts.read(file, sha256(SET))).isEqualTo(SET);
    }

    @Test
    void readValidatesTheFingerprint(@TempDir Path dir) throws IOException {
        Path file = Files.write(dir.resolve("set.pb"), SET.toByteArray());

        assertThatThrownBy(() -> DescriptorSetArtifacts.read(file, ZEROS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("descriptor fingerprint does not match descriptor-set bytes");
    }

    @Test
    void readRefusesAFileOver16MiB(@TempDir Path dir) throws IOException {
        Path file = Files.write(dir.resolve("large.pb"), new byte[16 * 1024 * 1024 + 1]);

        assertThatThrownBy(() -> DescriptorSetArtifacts.read(file, ZEROS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("descriptor set exceeds the 16 MiB limit");
    }

    @Test
    void readDoesNotFollowASymbolicLink(@TempDir Path dir) throws IOException {
        Path target = Files.write(dir.resolve("set.pb"), SET.toByteArray());
        Path link = Files.createSymbolicLink(dir.resolve("link.pb"), target);

        assertThatThrownBy(() -> DescriptorSetArtifacts.read(link, sha256(SET)))
                .isInstanceOf(IOException.class);
    }

    private static String sha256(ByteString bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray()));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
