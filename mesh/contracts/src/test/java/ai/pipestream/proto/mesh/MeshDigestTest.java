package ai.pipestream.proto.mesh;

import ai.pipestream.proto.mesh.test.v1.TestDocument;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the canonical hashing rules: fingerprints are stable, order-insensitive, and
 * content-sensitive, and unknown fields are preserved identity content (see {@link MeshDigest}
 * for the policy).
 */
class MeshDigestTest {

    private static FileDescriptorSet closure() {
        return MeshDigest.closure(TestDocument.getDescriptor());
    }

    @Test
    void theFingerprintIsStableAcrossRepeatedComputation() {
        assertThat(MeshDigest.fingerprintOf(TestDocument.getDescriptor()))
                .isEqualTo(MeshDigest.fingerprintOf(TestDocument.getDescriptor()))
                .matches("^[0-9a-f]{64}$");
    }

    @Test
    void theFingerprintIgnoresFileAssemblyOrder() {
        List<FileDescriptorProto> shuffled = new ArrayList<>(closure().getFileList());
        Collections.shuffle(shuffled, new java.util.Random(42));
        FileDescriptorSet reordered = FileDescriptorSet.newBuilder()
                .addAllFile(shuffled)
                .build();
        assertThat(MeshDigest.fingerprint(reordered))
                .isEqualTo(MeshDigest.fingerprint(closure()));
    }

    @Test
    void theFingerprintChangesWithContent() {
        FileDescriptorSet drifted = FileDescriptorSet.newBuilder()
                .addAllFile(closure().getFileList().stream()
                        .map(proto -> proto.getName().endsWith("test_document.proto")
                                ? proto.toBuilder().setSyntax("proto2").build()
                                : proto)
                        .toList())
                .build();
        assertThat(MeshDigest.fingerprint(drifted))
                .isNotEqualTo(MeshDigest.fingerprint(closure()));
    }

    @Test
    void theClosureContainsTheDefiningFileAndItsDependencies() {
        FileDescriptor defining = TestDocument.getDescriptor().getFile();
        List<String> names = closure().getFileList().stream()
                .map(FileDescriptorProto::getName)
                .toList();
        assertThat(names).contains(defining.getName());
        for (FileDescriptor dependency : defining.getDependencies()) {
            assertThat(names).contains(dependency.getName());
        }
    }

    @Test
    void aDescriptorRoundTripPreservesUnknownFieldsAndTheFingerprint() throws Exception {
        FileDescriptorProto original = closure().getFileList().stream()
                .filter(proto -> proto.getName().endsWith("test_document.proto"))
                .findFirst()
                .orElseThrow();
        // Append an unknown field (field number 999, varint 1) to the file proto's wire bytes.
        byte[] withUnknown = appendUnknownVarint(original.toByteArray(), 999, 1);
        FileDescriptorProto reparsed = FileDescriptorProto.parseFrom(withUnknown);
        assertThat(reparsed.getUnknownFields().hasField(999)).isTrue();
        // Parse and re-serialize is byte-stable: the fingerprint survives a wire round trip.
        assertThat(reparsed.toByteArray()).isEqualTo(withUnknown);

        FileDescriptorSet setWithUnknown = FileDescriptorSet.newBuilder()
                .addAllFile(closure().getFileList().stream()
                        .map(proto -> proto == original ? reparsed : proto)
                        .toList())
                .build();
        assertThat(MeshDigest.fingerprint(setWithUnknown))
                .isEqualTo(MeshDigest.fingerprint(FileDescriptorSet.parseFrom(
                        setWithUnknown.toByteArray())));
        // Unknown fields are identity content: the augmented file fingerprints differently.
        assertThat(MeshDigest.fingerprint(setWithUnknown))
                .isNotEqualTo(MeshDigest.fingerprint(closure()));
    }

    private static byte[] appendUnknownVarint(byte[] bytes, int fieldNumber, long value) {
        byte[] out = java.util.Arrays.copyOf(bytes, bytes.length + 16);
        int pos = bytes.length;
        pos = writeVarint(out, pos, (long) fieldNumber << 3);
        pos = writeVarint(out, pos, value);
        return java.util.Arrays.copyOf(out, pos);
    }

    private static int writeVarint(byte[] out, int pos, long value) {
        long remaining = value;
        while ((remaining & ~0x7FL) != 0) {
            out[pos++] = (byte) ((remaining & 0x7F) | 0x80);
            remaining >>>= 7;
        }
        out[pos++] = (byte) remaining;
        return pos;
    }
}
