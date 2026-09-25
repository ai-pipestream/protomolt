package ai.protomolt.proto.correction;

import ai.protomolt.proto.receipt.*;
import ai.protomolt.proto.workflow.RecordSigning;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.KeyPair;
import java.security.Signature;
import java.security.interfaces.EdECPrivateKey;
import java.util.Arrays;

/** One persistent candidate issuer, generated once under the mounted workspace. */
final class SigningIdentity {
    private static final String ISSUER = "protomolt-correction-candidate";
    private static final String KEY_ID = "correction-v1";

    private final RecordSigning signing;
    private final TrustSnapshot trust;

    private SigningIdentity(RecordSigning signing, TrustSnapshot trust) {
        this.signing = signing;
        this.trust = trust;
    }

    RecordSigning signing() { return signing; }
    TrustSnapshot trust() { return trust; }

    static SigningIdentity open(Path workspace) throws Exception {
        Files.createDirectories(workspace);
        try (FileChannel channel = FileChannel.open(workspace.resolve(".identity.lock"),
                StandardOpenOption.CREATE, StandardOpenOption.WRITE);
             var ignored = channel.lock()) {
            return openLocked(workspace);
        }
    }

    private static SigningIdentity openLocked(Path workspace) throws Exception {
        Path identity = workspace.resolve("identity.bin");
        byte[] stored;
        if (Files.exists(identity)) {
            stored = Files.readAllBytes(identity);
        } else {
            KeyPair pair = RecordKeys.generate();
            byte[] seed = ((EdECPrivateKey) pair.getPrivate()).getBytes().orElseThrow();
            byte[] publicKey = RecordKeys.rawPublicKey(pair.getPublic());
            stored = ByteBuffer.allocate(64).put(seed).put(publicKey).array();
            Path temporary = Files.createTempFile(workspace, ".identity-", ".tmp",
                    PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
            try {
                Files.write(temporary, stored);
                Files.move(temporary, identity, StandardCopyOption.ATOMIC_MOVE);
            } finally {
                Files.deleteIfExists(temporary);
            }
        }
        if (stored.length != 64) throw new IOException("invalid persisted correction identity");
        byte[] seed = Arrays.copyOfRange(stored, 0, 32);
        byte[] publicKey = Arrays.copyOfRange(stored, 32, 64);
        var privateKey = RecordKeys.privateKey(seed);
        var publicObject = RecordKeys.publicKey(publicKey);
        Signature proof = Signature.getInstance("Ed25519");
        proof.initSign(privateKey);
        proof.update(new byte[] {1, 2, 3});
        byte[] signature = proof.sign();
        proof.initVerify(publicObject);
        proof.update(new byte[] {1, 2, 3});
        if (!proof.verify(signature)) throw new IOException("persisted correction identity is inconsistent");

        TrustSnapshot expected = TrustSnapshot.newBuilder().addIssuers(TrustedIssuer.newBuilder()
                .setIssuer(ISSUER).addSubjectKinds(WorkRecords.SUBJECT_KIND_WORKFLOW_RUN)
                .addKeys(TrustedKey.newBuilder().setKeyId(KEY_ID).setState(KeyState.KEY_STATE_ACTIVE)
                        .setAlgorithm(SignatureAlgorithm.SIGNATURE_ALGORITHM_ED25519)
                        .setPublicKey(ByteString.copyFrom(publicKey)))).build();
        Path trustFile = workspace.resolve("trust.pb");
        if (Files.exists(trustFile)) {
            if (!expected.equals(TrustSnapshot.parseFrom(Files.readAllBytes(trustFile)))) {
                throw new IOException("persisted correction trust differs from signing identity");
            }
        } else {
            Files.write(trustFile, expected.toByteArray());
        }
        return new SigningIdentity(new RecordSigning(ISSUER, new RecordSigner(KEY_ID, privateKey)), expected);
    }
}
