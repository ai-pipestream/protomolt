package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnknownFieldSet;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.CancellationException;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class DocumentSchemaOccurrenceCodecTest {
    @Test void canonicalVectorPinsFieldOrderLengthsAndSchemaIdentity() throws Exception {
        var path = path();
        var encoded = DocumentSchemaOccurrenceCodec.encode(path, () -> {});
        String expected = "080112dc0122d9010a05752f782e4d1240" + "61".repeat(64)
                + "18012a8b010a470a03782e4d1240" + "62".repeat(64) + "1240" + "63".repeat(64);
        assertThat(HexFormat.of().formatHex(encoded.bytes().toByteArray())).isEqualTo(expected);
        assertThat(read(encoded.bytes())).isEqualTo(path);
        assertThat(encoded.sha256()).isEqualTo(digest(encoded.bytes()));
    }

    @Test void rejectsAlternateEncodingsEvenWithMatchingDigests() throws Exception {
        var bytes = DocumentSchemaOccurrenceCodec.encode(path(), () -> {}).bytes();
        var reordered = bytes.substring(2).concat(bytes.substring(0, 2));
        var duplicate = bytes.concat(ByteString.copyFrom(new byte[]{8, 1}));
        var overlong = ByteString.copyFrom(new byte[]{8, (byte) 0x81, 0}).concat(bytes.substring(2));
        for (var changed : List.of(reordered, duplicate, overlong)) {
            assertThat(RepositorySchemaOccurrencePath.parseFrom(changed)).isEqualTo(path());
            assertThatThrownBy(() -> read(changed)).hasMessageContaining("noncanonical");
        }
        assertThatThrownBy(() -> read(bytes.substring(0, bytes.size() - 1)))
                .isInstanceOf(com.google.protobuf.InvalidProtocolBufferException.class);
    }

    @Test void rejectsUnknownFieldsAtRootAndNestedBoundaries() throws Exception {
        var unknown = UnknownFieldSet.newBuilder().addField(100,
                UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build();
        var root = path().toBuilder().setUnknownFields(unknown).build();
        var nested = path().toBuilder().setSteps(0, path().getSteps(0).toBuilder().setAnyBoundary(
                path().getSteps(0).getAnyBoundary().toBuilder().setUnknownFields(unknown))).build();
        for (var invalid : List.of(root, nested)) {
            assertThatThrownBy(() -> DocumentSchemaOccurrenceCodec.encode(invalid, () -> {})).hasMessageContaining("unknown occurrence fields");
            assertThatThrownBy(() -> read(invalid.toByteString())).hasMessageContaining("unknown occurrence fields");
        }
    }

    @Test void rejectsExplicitEncodingOfImplicitDefaults() throws Exception {
        var resolution = path().getSteps(0).getAnyBoundary().toBuilder().setValueSizeBytes(0).build();
        var canonical = path().toBuilder().setSteps(0, RepositorySchemaOccurrenceStep.newBuilder().setAnyBoundary(resolution)).build();
        var rawResolution = resolution.toByteString().concat(ByteString.copyFrom(new byte[]{24, 0}));
        var bytes = ByteString.copyFrom(new byte[]{8, 1}).concat(frame(2, frame(4, rawResolution)));
        assertThat(RepositorySchemaOccurrencePath.parseFrom(bytes)).isEqualTo(canonical);
        assertThatThrownBy(() -> read(bytes)).hasMessageContaining("noncanonical");
        assertThat(read(DocumentSchemaOccurrenceCodec.encode(canonical, () -> {}).bytes())).isEqualTo(canonical);
    }

    @Test void boundsInputBeforeParsingAndOutputBeforeAllocation() throws Exception {
        var oversized = ByteString.copyFrom(new byte[DocumentSchemaOccurrenceCodec.MAX_BYTES + 1]);
        assertThatThrownBy(() -> read(oversized)).hasMessageContaining("byte bound");
        var many = ByteString.copyFrom(new byte[]{8, 1});
        var emptyStep = ByteString.copyFrom(new byte[]{18, 0});
        for (int i = 0; i < DocumentSchemaOccurrenceCodec.MAX_WIRE_VALUES + 1; i++) many = many.concat(emptyStep);
        var input = many;
        assertThatThrownBy(() -> read(input)).hasMessageContaining("wire value");
        var key = RepositorySchemaOccurrenceStep.newBuilder().setMapKey(RepositoryOccurrenceMapKey.newBuilder()
                .setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_STRING)
                .setStringValue("x".repeat(1048576))).build();
        var large = path().toBuilder();
        for (int i = 0; i < 5; i++) large.addSteps(key);
        large.addSteps(path().getSteps(0));
        assertThatThrownBy(() -> DocumentSchemaOccurrenceCodec.encode(large.build(), () -> {})).hasMessageContaining("byte bound");
    }

    @Test void preservesOneofDefaultsAndRejectsMalformedStrings() throws Exception {
        for (var key : List.of(
                RepositoryOccurrenceMapKey.newBuilder().setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_STRING).setStringValue("").build(),
                RepositoryOccurrenceMapKey.newBuilder().setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_STRING).setStringValue("é水🙂").build(),
                RepositoryOccurrenceMapKey.newBuilder().setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_BOOL).setBoolValue(false).build(),
                RepositoryOccurrenceMapKey.newBuilder().setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_SINT64).setSignedValue(-1).build())) {
            var candidate = path().toBuilder().addSteps(RepositorySchemaOccurrenceStep.newBuilder().setRepeatedIndex(0))
                    .addSteps(RepositorySchemaOccurrenceStep.newBuilder().setMapKey(key)).addSteps(path().getSteps(0)).build();
            assertThat(read(DocumentSchemaOccurrenceCodec.encode(candidate, () -> {}).bytes())).isEqualTo(candidate);
        }
        for (String invalid : List.of("\ud800", "\udc00", "x\ud800y")) {
            var key = RepositoryOccurrenceMapKey.newBuilder().setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_STRING).setStringValue(invalid);
            var candidate = path().toBuilder().addSteps(RepositorySchemaOccurrenceStep.newBuilder().setMapKey(key)).addSteps(path().getSteps(0)).build();
            assertThatThrownBy(() -> DocumentSchemaOccurrenceCodec.encode(candidate, () -> {})).hasMessageContaining("invalid occurrence UTF-16");
        }
    }

    @Test void verifiesVersionsIntegrityAndCancellation() throws Exception {
        var encoded = DocumentSchemaOccurrenceCodec.encode(path(), () -> {});
        assertThatThrownBy(() -> DocumentSchemaOccurrenceCodec.decode("other", 1, encoded.bytes(), encoded.sha256(), () -> {})).hasMessageContaining("unsupported");
        assertThatThrownBy(() -> DocumentSchemaOccurrenceCodec.decode(DocumentSchemaOccurrenceCodec.CODEC, 2, encoded.bytes(), encoded.sha256(), () -> {})).hasMessageContaining("unsupported");
        assertThatThrownBy(() -> DocumentSchemaOccurrenceCodec.decode(DocumentSchemaOccurrenceCodec.CODEC, 1, encoded.bytes(), "0".repeat(64), () -> {})).hasMessageContaining("digest mismatch");
        assertThatThrownBy(() -> DocumentSchemaOccurrenceCodec.encode(path().toBuilder().setEncodingVersion(2).build(), () -> {})).isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> DocumentSchemaOccurrenceCodec.encode(path(), () -> { throw new CancellationException(); })).isInstanceOf(CancellationException.class);
        assertThat(DocumentSchemaOccurrenceCodec.encode(path(), () -> {})).isEqualTo(encoded);
    }

    private static RepositorySchemaOccurrencePath path() {
        return RepositorySchemaOccurrencePath.newBuilder().setEncodingVersion(1)
                .addSteps(RepositorySchemaOccurrenceStep.newBuilder().setAnyBoundary(RepositoryAnyResolution.newBuilder()
                        .setTypeUrl("u/x.M").setValueSha256("a".repeat(64)).setValueSizeBytes(1)
                        .setResolved(RepositoryResolvedSchema.newBuilder().setArtifactSha256("c".repeat(64))
                                .setSchema(PublicationSchemaCondition.newBuilder().setTypeName("x.M").setDescriptorFingerprint("b".repeat(64)))))).build();
    }

    private static RepositorySchemaOccurrencePath read(ByteString bytes) throws Exception {
        return DocumentSchemaOccurrenceCodec.decode(DocumentSchemaOccurrenceCodec.CODEC, 1, bytes, digest(bytes), () -> {});
    }

    private static String digest(ByteString bytes) throws Exception {
        return HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray()));
    }

    private static ByteString frame(int field, ByteString bytes) throws Exception {
        var sink = ByteString.newOutput();
        var output = com.google.protobuf.CodedOutputStream.newInstance(sink);
        output.writeBytes(field, bytes);
        output.flush();
        return sink.toByteString();
    }
}
