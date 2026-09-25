package ai.protomolt.proto.serve;

import ai.protomolt.proto.receipt.KeyState;
import ai.protomolt.proto.receipt.RecordKeys;
import ai.protomolt.proto.receipt.SignatureAlgorithm;
import ai.protomolt.proto.receipt.TrustSnapshot;
import ai.protomolt.proto.receipt.TrustSnapshots;
import ai.protomolt.proto.receipt.TrustedIssuer;
import ai.protomolt.proto.receipt.TrustedKey;
import com.google.protobuf.ByteString;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.KeyPair;
import java.security.Signature;
import java.security.interfaces.EdECPrivateKey;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Set;

/** Generates or verifies the persisted signing identity of the agent starter. */
public final class StarterSigningIdentity {
    private static final String ISSUER = "local.protomolt.agent-starter";
    private static final byte[] CHALLENGE = "protomolt-agent-starter-identity".getBytes(
            java.nio.charset.StandardCharsets.UTF_8);
    private static final Set<PosixFilePermission> PRIVATE = Set.of(
            PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE);
    private static final Set<PosixFilePermission> PUBLIC = Set.of(
            PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE,
            PosixFilePermission.GROUP_READ, PosixFilePermission.OTHERS_READ);

    private StarterSigningIdentity() {
    }

    /** Used only by the one-shot Compose identity initializer. */
    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("usage: StarterSigningIdentity <persistent-directory>");
            System.exit(2);
        }
        ensure(Path.of(args[0]));
        System.out.println("starter signing identity ready");
    }

    static void ensure(Path directory) throws Exception {
        Files.createDirectories(directory);
        Path seedFile = directory.resolve("seed.bin");
        Path publicFile = directory.resolve("public.raw");
        Path trustFile = directory.resolve("trust.binpb");
        Path keyIdFile = directory.resolve("key-id");
        Path issuerFile = directory.resolve("issuer");
        int present = 0;
        for (Path path : new Path[]{seedFile, publicFile, trustFile, keyIdFile, issuerFile}) {
            if (Files.exists(path)) present++;
        }
        if (present == 0) {
            KeyPair pair = RecordKeys.generate();
            byte[] seed = ((EdECPrivateKey) pair.getPrivate()).getBytes()
                    .orElseThrow(() -> new IllegalStateException("Ed25519 seed is unavailable"));
            byte[] rawPublic = RecordKeys.rawPublicKey(pair.getPublic());
            String keyId = keyId(rawPublic);
            TrustSnapshot trust = snapshot(rawPublic, keyId);
            Files.createFile(seedFile, PosixFilePermissions.asFileAttribute(PRIVATE));
            Files.write(seedFile, seed);
            Files.write(publicFile, rawPublic, java.nio.file.StandardOpenOption.CREATE_NEW);
            Files.write(trustFile, trust.toByteArray(), java.nio.file.StandardOpenOption.CREATE_NEW);
            Files.writeString(keyIdFile, keyId, java.nio.file.StandardOpenOption.CREATE_NEW);
            Files.writeString(issuerFile, ISSUER, java.nio.file.StandardOpenOption.CREATE_NEW);
            for (Path path : new Path[]{publicFile, trustFile, keyIdFile, issuerFile}) {
                Files.setPosixFilePermissions(path, PUBLIC);
            }
        } else if (present != 5) {
            throw new IllegalStateException("starter signing identity is incomplete; restore the original files");
        }
        verify(seedFile, publicFile, trustFile, keyIdFile, issuerFile);
    }

    private static void verify(Path seedFile, Path publicFile, Path trustFile,
                               Path keyIdFile, Path issuerFile) throws Exception {
        byte[] seed = Files.readAllBytes(seedFile);
        byte[] rawPublic = Files.readAllBytes(publicFile);
        if (seed.length != 32 || rawPublic.length != 32) {
            throw new IllegalStateException("starter signing identity has invalid key length");
        }
        String keyId = keyId(rawPublic);
        if (!keyId.equals(Files.readString(keyIdFile))
                || !ISSUER.equals(Files.readString(issuerFile))
                || !snapshot(rawPublic, keyId).equals(TrustSnapshots.load(trustFile))) {
            throw new IllegalStateException("starter signing identity does not match its trust snapshot");
        }
        Signature signer = Signature.getInstance("Ed25519");
        signer.initSign(RecordKeys.privateKey(seed));
        signer.update(CHALLENGE);
        Signature verifier = Signature.getInstance("Ed25519");
        verifier.initVerify(RecordKeys.publicKey(rawPublic));
        verifier.update(CHALLENGE);
        if (!verifier.verify(signer.sign())) {
            throw new IllegalStateException("starter signing key does not match its public trust key");
        }
        Files.setPosixFilePermissions(seedFile, PRIVATE);
    }

    private static TrustSnapshot snapshot(byte[] rawPublic, String keyId) {
        return TrustSnapshots.requireWellFormed(TrustSnapshot.newBuilder()
                .addIssuers(TrustedIssuer.newBuilder().setIssuer(ISSUER)
                        .addKeys(TrustedKey.newBuilder().setKeyId(keyId)
                                .setAlgorithm(SignatureAlgorithm.SIGNATURE_ALGORITHM_ED25519)
                                .setPublicKey(ByteString.copyFrom(rawPublic))
                                .setState(KeyState.KEY_STATE_ACTIVE))
                        .addSubjectKinds("delegation-task"))
                .build());
    }

    private static String keyId(byte[] rawPublic) throws Exception {
        return "starter-" + HexFormat.of().formatHex(
                MessageDigest.getInstance("SHA-256").digest(rawPublic), 0, 8);
    }
}
