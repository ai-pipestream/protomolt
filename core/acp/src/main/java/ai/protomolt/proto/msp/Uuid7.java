package ai.protomolt.proto.msp;

import java.security.SecureRandom;
import java.util.UUID;

/**
 * Time-ordered UUIDs (RFC 9562 version 7), the command and session identity MSP requires
 * clients to mint: a 48-bit millisecond timestamp, the version nibble, 12 random bits, the
 * variant bits, and 62 random bits.
 */
public final class Uuid7 {

    private static final SecureRandom RANDOM = new SecureRandom();

    private Uuid7() {
    }

    /** A fresh version 7 UUID for the current instant. */
    public static UUID next() {
        long millis = System.currentTimeMillis();
        long high = (millis << 16) | 0x7000L | (RANDOM.nextLong() & 0x0FFFL);
        long low = (RANDOM.nextLong() & 0x3FFFFFFFFFFFFFFFL) | 0x8000000000000000L;
        return new UUID(high, low);
    }

    /** {@link #next()} as the bare string form MSP carries on the wire. */
    public static String nextString() {
        return next().toString();
    }
}
