package ai.pipestream.proto.receipt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.security.PrivateKey;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class RecordVerifierTest {

    private static final PrivateKey KEY = RecordKeys.privateKey(ConformanceCorpus.SEED);

    private static byte[] record() {
        return ConformanceCorpus.sign(ConformanceCorpus.manifest().build(),
                ConformanceCorpus.KEY_ID, KEY);
    }

    private static Map<String, byte[]> allArtifacts() {
        Map<String, byte[]> bytes = new HashMap<>();
        for (byte[] content : new byte[][] {"request".getBytes(),
                "response".getBytes(), "output".getBytes()}) {
            bytes.put(WorkRecords.sha256Hex(content), content);
        }
        return bytes;
    }

    @Test
    void withoutArtifactBytesRehashIsSkippedAndNamedANonClaim() {
        Verification verification =
                RecordVerifier.verify(record(), ConformanceCorpus.trust());
        assertThat(verification.verified()).isTrue();
        assertThat(verification.checks()).last()
                .extracting(Verification.Check::id, Verification.Check::status)
                .containsExactly(RecordVerifier.CHECK_ARTIFACT_REHASH,
                        Verification.Check.Status.SKIPPED);
        assertThat(verification.nonClaims()).contains(
                Verification.NON_CLAIM_ISSUER_HONESTY,
                Verification.NON_CLAIM_TRUSTED_TIME,
                Verification.NON_CLAIM_WORLD_COMPLETENESS,
                Verification.NON_CLAIM_EXECUTION_CORRECTNESS,
                Verification.NON_CLAIM_ARTIFACT_CUSTODY);
    }

    @Test
    void suppliedArtifactBytesRehashAllOrNothing() {
        Verification verification = RecordVerifier.verify(record(),
                ConformanceCorpus.trust(), allArtifacts());
        assertThat(verification.verified()).isTrue();
        assertThat(verification.checks()).last()
                .extracting(Verification.Check::id, Verification.Check::status)
                .containsExactly(RecordVerifier.CHECK_ARTIFACT_REHASH,
                        Verification.Check.Status.PASSED);
        assertThat(verification.nonClaims())
                .doesNotContain(Verification.NON_CLAIM_ARTIFACT_CUSTODY);
    }

    @Test
    void aMissingArtifactFailsTheRehash() {
        Map<String, byte[]> bytes = allArtifacts();
        bytes.remove(WorkRecords.sha256Hex("output".getBytes()));
        Verification verification =
                RecordVerifier.verify(record(), ConformanceCorpus.trust(), bytes);
        assertThat(verification.refusal().id())
                .isEqualTo(RecordVerifier.CHECK_ARTIFACT_REHASH);
        assertThat(verification.refusal().detail()).contains("was not supplied");
    }

    @Test
    void aSizeMismatchFailsTheRehash() {
        Map<String, byte[]> bytes = allArtifacts();
        bytes.put(WorkRecords.sha256Hex("output".getBytes()), "out".getBytes());
        Verification verification =
                RecordVerifier.verify(record(), ConformanceCorpus.trust(), bytes);
        assertThat(verification.refusal().id())
                .isEqualTo(RecordVerifier.CHECK_ARTIFACT_REHASH);
        assertThat(verification.refusal().detail()).contains("3 bytes");
    }

    @Test
    void aDigestMismatchFailsTheRehash() {
        Map<String, byte[]> bytes = allArtifacts();
        bytes.put(WorkRecords.sha256Hex("output".getBytes()), "outpud".getBytes());
        Verification verification =
                RecordVerifier.verify(record(), ConformanceCorpus.trust(), bytes);
        assertThat(verification.refusal().id())
                .isEqualTo(RecordVerifier.CHECK_ARTIFACT_REHASH);
        assertThat(verification.refusal().detail()).contains("does not match its digest");
    }

    @Test
    void aRecordWithoutArtifactsRehashesTrivially() {
        WorkRecord bare = ConformanceCorpus.manifest()
                .clearSteps()
                .clearArtifacts()
                .build();
        byte[] record = ConformanceCorpus.sign(bare, ConformanceCorpus.KEY_ID, KEY);
        Verification verification =
                RecordVerifier.verify(record, ConformanceCorpus.trust(), Map.of());
        assertThat(verification.verified()).isTrue();
        assertThat(verification.checks()).last()
                .extracting(Verification.Check::status)
                .isEqualTo(Verification.Check.Status.PASSED);
    }

    @Test
    void anOversizeRecordRefusesAtTheBound() {
        Verification verification = RecordVerifier.verify(
                new byte[WorkRecords.MAX_RECORD_BYTES + 1], ConformanceCorpus.trust());
        assertThat(verification.refusal().id())
                .isEqualTo(RecordVerifier.CHECK_CONTAINER_BOUNDS);
        assertThat(verification.refusal().detail()).contains("bound");
    }

    @Test
    void theManifestDigestIsReportedOnceTheContainerParses() {
        Verification verification =
                RecordVerifier.verify(record(), ConformanceCorpus.trust());
        byte[] canonical =
                WorkRecords.canonicalBytes(ConformanceCorpus.manifest().build());
        assertThat(verification.manifestDigest())
                .isEqualTo(WorkRecords.sha256Hex(canonical));
        assertThat(verification.manifest().getRecordId()).isEqualTo("record-1");
    }

    @Test
    void anInvalidTrustSnapshotIsTheCallersErrorNotTheRecords() {
        assertThatThrownBy(() -> RecordVerifier.verify(record(), null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> RecordVerifier.verify(record(),
                TrustSnapshot.getDefaultInstance()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("trust snapshot is invalid");

        TrustSnapshot duplicateIssuer = TrustSnapshot.newBuilder()
                .addIssuers(ConformanceCorpus.trust().getIssuers(0))
                .addIssuers(ConformanceCorpus.trust().getIssuers(0))
                .build();
        assertThatThrownBy(() -> RecordVerifier.verify(record(), duplicateIssuer))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duplicates issuer");

        TrustedIssuer issuer = ConformanceCorpus.trust().getIssuers(0);
        TrustSnapshot duplicateKey = TrustSnapshot.newBuilder()
                .addIssuers(issuer.toBuilder().addKeys(issuer.getKeys(0)))
                .build();
        assertThatThrownBy(() -> RecordVerifier.verify(record(), duplicateKey))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duplicates key");

        assertThatThrownBy(() -> RecordVerifier.verify(null, ConformanceCorpus.trust()))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
