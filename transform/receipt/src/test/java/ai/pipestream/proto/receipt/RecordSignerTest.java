package ai.pipestream.proto.receipt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class RecordSignerTest {

    private final RecordSigner signer = new RecordSigner(ConformanceCorpus.KEY_ID,
            RecordKeys.privateKey(ConformanceCorpus.SEED));

    @Test
    void signedRecordsVerify() {
        WorkRecord manifest = ConformanceCorpus.manifest().build();
        SignedWorkRecord record = signer.sign(manifest);

        assertThat(record.getManifest().toByteArray())
                .isEqualTo(WorkRecords.canonicalBytes(manifest));
        assertThat(record.getSignaturesCount()).isEqualTo(1);
        assertThat(record.getSignatures(0).getKeyId())
                .isEqualTo(ConformanceCorpus.KEY_ID);
        assertThat(record.getSignatures(0).getAlgorithm())
                .isEqualTo(SignatureAlgorithm.SIGNATURE_ALGORITHM_ED25519);

        Verification verification = RecordVerifier.verify(record.toByteArray(),
                ConformanceCorpus.trust());
        assertThat(verification.verified())
                .as("refusal: %s", verification.refusal())
                .isTrue();
    }

    @Test
    void aManifestNamingAnotherKeyRefuses() {
        WorkRecord manifest = ConformanceCorpus.manifest()
                .setKeyId(ConformanceCorpus.SECOND_KEY_ID)
                .build();
        assertThatThrownBy(() -> signer.sign(manifest))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(ConformanceCorpus.SECOND_KEY_ID)
                .hasMessageContaining(ConformanceCorpus.KEY_ID);
    }

    @Test
    void blankConstructionRefuses() {
        assertThatThrownBy(() -> new RecordSigner(" ",
                RecordKeys.privateKey(ConformanceCorpus.SEED)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RecordSigner("key", null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> signer.sign(null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
