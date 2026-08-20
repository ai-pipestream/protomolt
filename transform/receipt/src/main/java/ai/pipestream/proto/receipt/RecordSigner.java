package ai.pipestream.proto.receipt;

import com.google.protobuf.ByteString;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.Signature;
import java.util.Objects;

/**
 * Signs canonical manifests. The private key is operator-supplied and
 * never appears in config documents, the registry, logs, or errors; the
 * signer holds it for its lifetime and identifies it only by key id.
 */
public final class RecordSigner {

    private final String keyId;
    private final PrivateKey privateKey;

    /**
     * @param keyId the key id the trust snapshot knows this key by
     * @param privateKey the Ed25519 private key
     */
    public RecordSigner(String keyId, PrivateKey privateKey) {
        if (keyId == null || keyId.isBlank()) {
            throw new IllegalArgumentException("keyId must not be blank");
        }
        this.keyId = keyId;
        this.privateKey = Objects.requireNonNull(privateKey, "privateKey");
    }

    /** The key id this signer signs as. */
    public String keyId() {
        return keyId;
    }

    /**
     * Serializes the manifest canonically and returns the signed record.
     * The manifest must name this signer's key: a mismatch between what
     * the record claims and what actually signed it is refused here, not
     * discovered at verification.
     */
    public SignedWorkRecord sign(WorkRecord manifest) {
        if (manifest == null) {
            throw new IllegalArgumentException("manifest must not be null");
        }
        if (!keyId.equals(manifest.getKeyId())) {
            throw new IllegalArgumentException("manifest names key '" + manifest.getKeyId()
                    + "' but the signer holds '" + keyId + "'");
        }
        byte[] canonical = WorkRecords.canonicalBytes(manifest);
        return SignedWorkRecord.newBuilder()
                .setManifest(ByteString.copyFrom(canonical))
                .addSignatures(RecordSignature.newBuilder()
                        .setKeyId(keyId)
                        .setAlgorithm(SignatureAlgorithm.SIGNATURE_ALGORITHM_ED25519)
                        .setSignature(ByteString.copyFrom(sign(canonical))))
                .build();
    }

    private byte[] sign(byte[] bytes) {
        try {
            Signature signature = Signature.getInstance("Ed25519");
            signature.initSign(privateKey);
            signature.update(bytes);
            return signature.sign();
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Ed25519 signing failed", e);
        }
    }
}
