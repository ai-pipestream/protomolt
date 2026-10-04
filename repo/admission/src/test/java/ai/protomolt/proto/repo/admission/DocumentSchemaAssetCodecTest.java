package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CancellationException;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class DocumentSchemaAssetCodecTest {
    private static final String SHA = "a".repeat(64);

    @Test void localAndImportedProvenanceRoundTrip() throws Exception {
        for (var asset : List.of(local(), imported())) {
            var encoded = DocumentSchemaAssetCodec.encode(asset, () -> {});
            assertThat(DocumentSchemaAssetCodec.decode(DocumentSchemaAssetCodec.CODEC,
                    DocumentSchemaAssetCodec.VERSION, encoded.bytes(), encoded.sha256(), () -> {})).isEqualTo(asset);
        }
    }

    @Test void compilerOptionsCanonicalizeIndependentOfInsertionOrderAndUseUnsignedUtf8Order() {
        String bmp = "\uE000";
        String supplementary = "\uD800\uDC00";
        assertThat(bmp.compareTo(supplementary)).isGreaterThan(0); // Java UTF-16 order differs from UTF-8 byte order.
        var first = localWithOptions(bmp, "bmp", supplementary, "supplementary");
        var reversed = localWithOptions(supplementary, "supplementary", bmp, "bmp");
        var a = DocumentSchemaAssetCodec.encode(first, () -> {});
        var b = DocumentSchemaAssetCodec.encode(reversed, () -> {});
        assertThat(b).isEqualTo(a);
        int bmpAt = indexOf(a.bytes().toByteArray(), bmp.getBytes(StandardCharsets.UTF_8));
        int supplementaryAt = indexOf(a.bytes().toByteArray(), supplementary.getBytes(StandardCharsets.UTF_8));
        assertThat(bmpAt).isGreaterThanOrEqualTo(0).isLessThan(supplementaryAt);
    }

    @Test void generatedAndDynamicMessagesEncodeEmptyMapValuesIdentically() throws Exception {
        var generated = localWithOptions("empty", "");
        var dynamic = DynamicMessage.parseFrom(RepositorySchemaAsset.getDescriptor(), generated.toByteArray());
        var generatedEncoded = DocumentSchemaEvidenceCodec.encode(generated, () -> {});
        var dynamicEncoded = DocumentSchemaEvidenceCodec.encode(dynamic, () -> {});
        assertThat(dynamicEncoded).isEqualTo(generatedEncoded);
        var assetEncoded = DocumentSchemaAssetCodec.encode(generated, () -> {});
        assertThat(assetEncoded.bytes()).isEqualTo(generatedEncoded.bytes());
        assertThat(assetEncoded.sha256()).isEqualTo(generatedEncoded.sha256());
    }

    @Test void decodeRejectsDuplicateReversedAndOmittedEmptyMapWireEntries() throws Exception {
        var base = local();
        List<WireOption> duplicate = List.of(new WireOption("same", "first"), new WireOption("same", "last"));
        var duplicateWire = rawWithOptions(base, duplicate, false);
        var dynamicDuplicate = DynamicMessage.parseFrom(RepositorySchemaAsset.getDescriptor(), duplicateWire);
        assertThatThrownBy(() -> DocumentSchemaEvidenceCodec.encode(dynamicDuplicate, () -> {}))
                .hasMessageContaining("duplicate schema evidence map key");
        var duplicateExpected = base.toBuilder().setCompilation(base.getCompilation().toBuilder()
                .setKnownCompiler(base.getCompilation().getKnownCompiler().toBuilder().putOptions("same", "last"))).build();
        rejectsNoncanonical(base, duplicate, false, duplicateExpected);

        List<WireOption> reversed = List.of(new WireOption("z", "last"), new WireOption("a", "first"));
        var reversedExpected = base.toBuilder().setCompilation(base.getCompilation().toBuilder()
                .setKnownCompiler(base.getCompilation().getKnownCompiler().toBuilder().putOptions("z", "last").putOptions("a", "first"))).build();
        rejectsNoncanonical(base, reversed, false, reversedExpected);

        List<WireOption> omittedEmptyValue = List.of(new WireOption("empty", ""));
        var emptyExpected = base.toBuilder().setCompilation(base.getCompilation().toBuilder()
                .setKnownCompiler(base.getCompilation().getKnownCompiler().toBuilder().putOptions("empty", ""))).build();
        rejectsNoncanonical(base, omittedEmptyValue, true, emptyExpected);
    }

    @Test void rejectsUnknownFieldsInvalidProvenanceAndWrongEncodingIdentity() throws Exception {
        var encoded = DocumentSchemaAssetCodec.encode(local(), () -> {});
        var withUnknown = encoded.bytes().concat(ByteString.copyFrom(new byte[]{(byte)0x98, 0x06, 1})); // field 99 = 1
        reject(withUnknown, "unknown schema evidence fields");

        var invalid = local().toBuilder().setCompilation(local().getCompilation().toBuilder().clearSourceArtifactSha256()).build();
        assertThatThrownBy(() -> DocumentSchemaAssetCodec.encode(invalid, () -> {}))
                .isInstanceOf(ai.protomolt.proto.validate.ValidationResult.ValidationException.class);

        assertThatThrownBy(() -> DocumentSchemaAssetCodec.decode("other-codec", 1, encoded.bytes(), encoded.sha256(), () -> {}))
                .hasMessageContaining("unsupported");
        assertThatThrownBy(() -> DocumentSchemaAssetCodec.decode(DocumentSchemaAssetCodec.CODEC, 2,
                encoded.bytes(), encoded.sha256(), () -> {})).hasMessageContaining("unsupported");
        assertThatThrownBy(() -> DocumentSchemaAssetCodec.decode(DocumentSchemaAssetCodec.CODEC, 1,
                encoded.bytes(), "0".repeat(64), () -> {})).hasMessageContaining("digest mismatch");
    }

    @Test void rejectsOversizeBeforeParsingAndPreservesCancellation() {
        var oversized = ByteString.copyFrom(new byte[DocumentSchemaAssetCodec.MAX_BYTES + 1]);
        assertThatThrownBy(() -> DocumentSchemaAssetCodec.decode(DocumentSchemaAssetCodec.CODEC, 1,
                oversized, sha256(oversized), () -> {}))
                .hasMessageContaining("schema asset metadata byte bound exceeded");
        var cancellation = new CancellationException("cancel schema asset");
        assertThatThrownBy(() -> DocumentSchemaAssetCodec.encode(local(), () -> { throw cancellation; })).isSameAs(cancellation);
        var encoded = DocumentSchemaAssetCodec.encode(local(), () -> {});
        assertThatThrownBy(() -> DocumentSchemaAssetCodec.decode(DocumentSchemaAssetCodec.CODEC, 1,
                encoded.bytes(), encoded.sha256(), () -> { throw cancellation; })).isSameAs(cancellation);
    }

    private static void rejectsNoncanonical(RepositorySchemaAsset base, List<WireOption> options,
            boolean omitEmptyValue, RepositorySchemaAsset expected) throws Exception {
        var raw = rawWithOptions(base, options, omitEmptyValue);
        var parsed = RepositorySchemaAsset.parseFrom(raw);
        assertThat(parsed).isEqualTo(expected); // The parser accepts the intended semantic map value.
        assertThatThrownBy(() -> decode(raw)).hasMessageContaining("noncanonical");
    }

    private static void reject(ByteString bytes, String message) {
        assertThatThrownBy(() -> decode(bytes)).hasMessageContaining(message);
    }

    private static RepositorySchemaAsset decode(ByteString bytes) throws InvalidProtocolBufferException {
        return DocumentSchemaAssetCodec.decode(DocumentSchemaAssetCodec.CODEC, 1, bytes,
                sha256(bytes), () -> {});
    }

    private static ByteString rawWithOptions(RepositorySchemaAsset base, List<WireOption> options,
            boolean omitEmptyValue) throws IOException {
        byte[] tool = base.getCompilation().getKnownCompiler().getTool().toByteArray();
        byte[] details = wire(out -> {
            out.writeByteArray(1, tool);
            for (var option : options) {
                byte[] entry = wire(map -> {
                    map.writeString(1, option.key());
                    if (!omitEmptyValue || !option.value().isEmpty()) map.writeString(2, option.value());
                });
                out.writeByteArray(2, entry);
            }
        });
        byte[] provenance = wire(out -> {
            var p = base.getCompilation();
            out.writeEnum(1, p.getOriginValue());
            out.writeEnum(2, p.getEvidenceValue());
            out.writeByteArray(3, details);
            out.writeByteArray(5, p.getAdmissionRuntime().toByteArray());
            if (p.hasSourceArtifactSha256()) out.writeString(6, p.getSourceArtifactSha256());
        });
        return ByteString.copyFrom(wire(out -> {
            out.writeByteArray(1, base.getSchema().toByteArray());
            out.writeString(2, base.getArtifactSha256());
            out.writeString(3, base.getTypeUrl());
            out.writeByteArray(4, provenance);
        }));
    }

    private static byte[] wire(IoConsumer<CodedOutputStream> writer) throws IOException {
        var bytes = new ByteArrayOutputStream();
        var output = CodedOutputStream.newInstance(bytes);
        writer.accept(output);
        output.flush();
        return bytes.toByteArray();
    }

    private static String sha256(ByteString bytes) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray())); }
        catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }

    private static int indexOf(byte[] bytes, byte[] needle) {
        outer: for (int i = 0; i <= bytes.length - needle.length; i++) {
            for (int j = 0; j < needle.length; j++) if (bytes[i + j] != needle[j]) continue outer;
            return i;
        }
        return -1;
    }

    private static RepositorySchemaAsset localWithOptions(String... keyValues) {
        var compiler = SchemaCompilerDetails.newBuilder().setTool(tool());
        for (int i = 0; i < keyValues.length; i += 2) compiler.putOptions(keyValues[i], keyValues[i + 1]);
        return local().toBuilder().setCompilation(local().getCompilation().toBuilder().setKnownCompiler(compiler)).build();
    }

    private static RepositorySchemaAsset local() {
        var provenance = SchemaCompilationProvenance.newBuilder()
                .setOrigin(SchemaCompilationOrigin.SCHEMA_COMPILATION_ORIGIN_LOCAL_SOURCE)
                .setEvidence(SchemaCompilerEvidence.SCHEMA_COMPILER_EVIDENCE_LOCAL_OBSERVED)
                .setKnownCompiler(SchemaCompilerDetails.newBuilder().setTool(tool()))
                .setAdmissionRuntime(runtime()).setSourceArtifactSha256("b".repeat(64)).build();
        return asset(provenance);
    }

    private static RepositorySchemaAsset imported() {
        var provenance = SchemaCompilationProvenance.newBuilder()
                .setOrigin(SchemaCompilationOrigin.SCHEMA_COMPILATION_ORIGIN_IMPORTED_DESCRIPTOR)
                .setEvidence(SchemaCompilerEvidence.SCHEMA_COMPILER_EVIDENCE_UNKNOWN)
                .setUnknownCompilerReason("Imported descriptor did not identify its compiler")
                .setAdmissionRuntime(runtime()).build();
        return asset(provenance);
    }

    private static RepositorySchemaAsset asset(SchemaCompilationProvenance provenance) {
        return RepositorySchemaAsset.newBuilder()
                .setSchema(PublicationSchemaCondition.newBuilder().setTypeName("fixture.Record").setDescriptorFingerprint(SHA))
                .setArtifactSha256("c".repeat(64)).setTypeUrl("type.test/fixture.Record").setCompilation(provenance).build();
    }

    private static SchemaToolIdentity tool() {
        return SchemaToolIdentity.newBuilder().setName("protoc").setVersion("27.1").setArtifactSha256("d".repeat(64)).build();
    }

    private static SchemaToolIdentity runtime() {
        return SchemaToolIdentity.newBuilder().setName("protobuf-java").setVersion("4.29.0").build();
    }

    private record WireOption(String key, String value) {}
    @FunctionalInterface private interface IoConsumer<T> { void accept(T value) throws IOException; }
}
