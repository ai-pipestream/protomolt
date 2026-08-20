package ai.pipestream.proto.receipt;

import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.interfaces.EdECPublicKey;
import java.security.spec.EdECPoint;
import java.security.spec.EdECPrivateKeySpec;
import java.security.spec.EdECPublicKeySpec;
import java.security.spec.NamedParameterSpec;

/**
 * Ed25519 key material in the portable raw encodings the receipt layer
 * stores: a 32-byte seed for a private key and the 32-byte RFC 8032 point
 * encoding for a public key ({@code TrustedKey.public_key}). Everything
 * rides the JDK provider; no dependency enters here.
 */
public final class RecordKeys {

    private static final int RAW_LENGTH = 32;

    private RecordKeys() {
    }

    /** Generates a fresh Ed25519 key pair. */
    public static KeyPair generate() {
        try {
            return KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Ed25519 is unavailable in this JDK", e);
        }
    }

    /** Builds a private key from its 32-byte seed. */
    public static PrivateKey privateKey(byte[] seed) {
        if (seed == null || seed.length != RAW_LENGTH) {
            throw new IllegalArgumentException("an Ed25519 private key seed is 32 bytes");
        }
        try {
            return KeyFactory.getInstance("Ed25519").generatePrivate(
                    new EdECPrivateKeySpec(NamedParameterSpec.ED25519, seed.clone()));
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Ed25519 private key rejected", e);
        }
    }

    /** Builds a public key from its 32-byte RFC 8032 encoding. */
    public static PublicKey publicKey(byte[] raw) {
        if (raw == null || raw.length != RAW_LENGTH) {
            throw new IllegalArgumentException("an Ed25519 public key is 32 bytes");
        }
        byte[] littleEndian = raw.clone();
        boolean xOdd = (littleEndian[RAW_LENGTH - 1] & 0x80) != 0;
        littleEndian[RAW_LENGTH - 1] &= 0x7f;
        byte[] bigEndian = new byte[RAW_LENGTH];
        for (int i = 0; i < RAW_LENGTH; i++) {
            bigEndian[i] = littleEndian[RAW_LENGTH - 1 - i];
        }
        try {
            return KeyFactory.getInstance("Ed25519").generatePublic(new EdECPublicKeySpec(
                    NamedParameterSpec.ED25519, new EdECPoint(xOdd, new BigInteger(1, bigEndian))));
        } catch (GeneralSecurityException e) {
            throw new IllegalArgumentException("Ed25519 public key rejected", e);
        }
    }

    /** Encodes a public key into its 32-byte RFC 8032 form. */
    public static byte[] rawPublicKey(PublicKey key) {
        if (!(key instanceof EdECPublicKey ed)
                || !"Ed25519".equals(ed.getParams().getName())) {
            throw new IllegalArgumentException("not an Ed25519 public key");
        }
        EdECPoint point = ed.getPoint();
        byte[] y = point.getY().toByteArray();
        if (point.getY().bitLength() > 255) {
            throw new IllegalArgumentException("Ed25519 public key point out of range");
        }
        byte[] raw = new byte[RAW_LENGTH];
        int copy = Math.min(RAW_LENGTH, y.length);
        for (int i = 0; i < copy; i++) {
            raw[i] = y[y.length - 1 - i];
        }
        if (point.isXOdd()) {
            raw[RAW_LENGTH - 1] |= (byte) 0x80;
        }
        return raw;
    }
}
