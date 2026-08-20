package ai.pipestream.receipt.verify;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.pipestream.proto.receipt.ConformanceCorpus;
import ai.pipestream.proto.receipt.RecordVerifier;
import ai.pipestream.proto.receipt.TrustSnapshot;
import ai.pipestream.proto.receipt.Verification;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The conformance bridge: the external verifier claims conformance by producing the
 * runtime verifier's verdict on every corpus fixture — same acceptance, same refusing
 * check, same manifest digest — and by agreeing with the runtime on randomly mutated
 * records, where agreement on acceptance is the security property.
 */
class CrossCheckTest {

    private static final byte[] TRUST = ConformanceCorpus.trust().toByteArray();

    static java.util.List<ConformanceCorpus.Fixture> fixtures() {
        return ConformanceCorpus.fixtures();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("fixtures")
    void everyCorpusFixtureGetsTheRuntimeVerdict(ConformanceCorpus.Fixture fixture) {
        Verification runtime =
                RecordVerifier.verify(fixture.record(), ConformanceCorpus.trust());
        ExternalVerifier.Result external =
                ExternalVerifier.verify(fixture.record(), TRUST);

        assertThat(external.verified())
                .as("%s acceptance", fixture.name())
                .isEqualTo(runtime.verified());
        if (!runtime.verified()) {
            assertThat(external.refusal().id())
                    .as("%s refusing check", fixture.name())
                    .isEqualTo(runtime.refusal().id());
        } else {
            assertThat(external.manifestDigest())
                    .as("%s digest", fixture.name())
                    .isEqualTo(runtime.manifestDigest());
            assertThat(external.nonClaims())
                    .containsExactlyElementsOf(runtime.nonClaims());
        }
    }

    @Test
    void rehashAgreesWithTheRuntimeInBothDirections() {
        byte[] record = ConformanceCorpus.fixtures().getFirst().record();
        Map<String, byte[]> artifacts = new HashMap<>();
        for (byte[] content : new byte[][] {"request".getBytes(), "response".getBytes(),
                "output".getBytes()}) {
            artifacts.put(ExternalVerifier.sha256Hex(content), content);
        }
        ExternalVerifier.Result supplied = ExternalVerifier.verify(record, TRUST, artifacts);
        Verification runtime = RecordVerifier.verify(record, ConformanceCorpus.trust(),
                artifacts);
        assertThat(supplied.verified()).isEqualTo(runtime.verified());
        assertThat(supplied.checks().getLast().status())
                .isEqualTo(ExternalVerifier.Check.Status.PASSED);

        artifacts.remove(ExternalVerifier.sha256Hex("output".getBytes()));
        ExternalVerifier.Result missing = ExternalVerifier.verify(record, TRUST, artifacts);
        assertThat(missing.refusal().id())
                .isEqualTo(ExternalVerifier.CHECK_ARTIFACT_REHASH);
    }

    /**
     * Byte-level differential agreement: for hundreds of seeded single-byte mutations of a
     * valid record, the two verifiers accept and refuse the same inputs. Attribution may
     * legitimately differ on arbitrary corruption; acceptance must not.
     */
    @Test
    void mutatedRecordsKeepAcceptanceAgreement() {
        byte[] valid = ConformanceCorpus.fixtures().getFirst().record();
        TrustSnapshot trust = ConformanceCorpus.trust();
        Random random = new Random(626433832795028841L);
        int disagreements = 0;
        StringBuilder detail = new StringBuilder();
        for (int i = 0; i < 400; i++) {
            byte[] mutated = valid.clone();
            int index = random.nextInt(mutated.length);
            int bit = 1 << random.nextInt(8);
            mutated[index] = (byte) (mutated[index] ^ bit);

            boolean runtimeAccepts = RecordVerifier.verify(mutated, trust).verified();
            boolean externalAccepts = ExternalVerifier.verify(mutated, TRUST).verified();
            if (runtimeAccepts != externalAccepts) {
                disagreements++;
                detail.append("byte ").append(index).append(" bit ").append(bit)
                        .append(": runtime=").append(runtimeAccepts)
                        .append(" external=").append(externalAccepts).append('\n');
            }
        }
        assertThat(disagreements).as("acceptance disagreements:\n%s", detail).isZero();
    }

    @Test
    void aNonMinimalVarintRefusesAtCanonicalityInBothVerifiers() {
        byte[] manifest = ConformanceCorpus.manifest().build().toByteArray();
        // The canonical manifest opens with field 1 (manifest_version) as 0x08 0x01;
        // re-encode the value 1 as a two-byte varint, which decodes identically.
        assertThat(manifest[0]).isEqualTo((byte) 0x08);
        assertThat(manifest[1]).isEqualTo((byte) 0x01);
        byte[] padded = new byte[manifest.length + 1];
        padded[0] = 0x08;
        padded[1] = (byte) 0x81;
        padded[2] = 0x00;
        System.arraycopy(manifest, 2, padded, 3, manifest.length - 2);
        assertBothRefuseAtCanonicality(padded);
    }

    @Test
    void reorderedFieldsRefuseAtCanonicalityInBothVerifiers() throws Exception {
        byte[] manifest = ConformanceCorpus.manifest().build().toByteArray();
        // Move the first top-level field's bytes to the end: same content, same field
        // set, no duplicates — only the order distinguishes it from canonical form.
        com.google.protobuf.CodedInputStream in =
                com.google.protobuf.CodedInputStream.newInstance(manifest);
        in.readTag();
        in.skipField(0x08);
        int firstFieldEnd = in.getTotalBytesRead();
        byte[] reordered = new byte[manifest.length];
        System.arraycopy(manifest, firstFieldEnd, reordered, 0,
                manifest.length - firstFieldEnd);
        System.arraycopy(manifest, 0, reordered, manifest.length - firstFieldEnd,
                firstFieldEnd);
        assertBothRefuseAtCanonicality(reordered);
    }

    private static void assertBothRefuseAtCanonicality(byte[] manifestBytes) {
        byte[] record = ConformanceCorpus.signBytes(manifestBytes,
                ConformanceCorpus.KEY_ID,
                ai.pipestream.proto.receipt.RecordKeys.privateKey(ConformanceCorpus.SEED));
        Verification runtime = RecordVerifier.verify(record, ConformanceCorpus.trust());
        ExternalVerifier.Result external = ExternalVerifier.verify(record, TRUST);
        assertThat(runtime.refusal().id())
                .isEqualTo(RecordVerifier.CHECK_RESERIALIZATION_EQUALITY);
        assertThat(external.refusal().id())
                .isEqualTo(ExternalVerifier.CHECK_RESERIALIZATION_EQUALITY);
    }

    @Test
    void anInvalidTrustSnapshotIsTheCallersError() {
        byte[] record = ConformanceCorpus.fixtures().getFirst().record();
        assertThatThrownBy(() -> ExternalVerifier.verify(record, new byte[0]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("invalid");
        assertThatThrownBy(() -> ExternalVerifier.verify(record, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ExternalVerifier.verify(null, TRUST))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void theMainSourceSetImportsOnlyTheJdk() throws Exception {
        // The module's whole value is sharing nothing with the platform runtime; this
        // pins it at the source level, the way the doc guards pin documentation.
        java.nio.file.Path root = java.nio.file.Path.of("").toAbsolutePath();
        while (root != null && !java.nio.file.Files.exists(root.resolve("settings.gradle"))) {
            root = root.getParent();
        }
        assertThat(root).isNotNull();
        java.nio.file.Path main = root.resolve(
                "apps/record-verifier/src/main/java/ai/pipestream/receipt/verify");
        try (var sources = java.nio.file.Files.list(main)) {
            for (java.nio.file.Path source : sources.toList()) {
                for (String line : java.nio.file.Files.readAllLines(source)) {
                    if (line.startsWith("import ")) {
                        assertThat(line)
                                .as("%s must import only the JDK or this package",
                                        source.getFileName())
                                .matches("import (static )?(java\\.|ai\\.pipestream\\.receipt\\.verify\\.).*");
                    }
                }
            }
        }
    }
}
