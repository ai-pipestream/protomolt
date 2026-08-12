package ai.pipestream.proto.delegation;

import ai.pipestream.proto.delegation.storage.v1.EncryptedRepositoryState;
import ai.pipestream.proto.delegation.storage.v1.RepositoryStateEncryptionAlgorithm;
import ai.pipestream.proto.meta.MetadataProto;
import ai.pipestream.proto.validate.ValidationResult;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

class TranscriptStorageContractTest {

    @Test
    void validEnvelopePassesContractValidation() {
        ValidationResult result = ValidationResult.validate(validEnvelope().build());

        assertThat(result.valid()).isTrue();
    }

    @Test
    void contractRejectsInvalidAlgorithmKeyReferenceNonceAndCiphertext() {
        EncryptedRepositoryState invalid = validEnvelope()
                .setAlgorithm(RepositoryStateEncryptionAlgorithm
                        .REPOSITORY_STATE_ENCRYPTION_ALGORITHM_UNSPECIFIED)
                .setKeyRef("not-a-reference")
                .setNonce(ByteString.copyFrom(new byte[11]))
                .clearCiphertext()
                .build();

        ValidationResult result = ValidationResult.validate(invalid);

        assertThat(result.valid()).isFalse();
        assertThat(result.violations())
                .extracting(ValidationResult.Violation::path)
                .contains("algorithm", "key_ref", "nonce", "ciphertext");
    }

    @Test
    void everyStoredFieldDeclaresSensitivityAndContractImportsNoIndexOptions() {
        var descriptor = EncryptedRepositoryState.getDescriptor();
        TreeSet<String> missing = new TreeSet<>();
        descriptor.getFields().forEach(field -> {
            if (field.getOptions().getExtension(MetadataProto.field)
                    .getSensitivity().isEmpty()) {
                missing.add(field.getName());
            }
        });

        assertThat(missing).isEmpty();
        assertThat(descriptor.getFile().getDependencies())
                .extracting(dependency -> dependency.getName())
                .noneMatch(name -> name.contains("index"));
    }

    private static EncryptedRepositoryState.Builder validEnvelope() {
        return EncryptedRepositoryState.newBuilder()
                .setFormatVersion(1)
                .setAlgorithm(RepositoryStateEncryptionAlgorithm
                        .REPOSITORY_STATE_ENCRYPTION_ALGORITHM_AES_256_GCM)
                .setKeyRef("env:PROTOMOLT_TRANSCRIPT_KEY")
                .setNonce(ByteString.copyFrom(new byte[12]))
                .setCiphertext(ByteString.copyFrom(new byte[16]))
                .setPlaintextSha256("0".repeat(64))
                .setRecordCount(1)
                .setSavedAt(Timestamp.newBuilder().setSeconds(1_700_000_000L))
                .setContentType(RepositoryServiceTranscriptRepository.CONTENT_TYPE);
    }
}
