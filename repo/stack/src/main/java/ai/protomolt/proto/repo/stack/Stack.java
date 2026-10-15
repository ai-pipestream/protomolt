package ai.protomolt.proto.repo.stack;

import ai.protomolt.proto.repo.v2.StackRef;
import java.io.IOException;
import java.io.InputStream;

/**
 * One configured storage namespace, written once per key (requirements §6).
 * Every put creates a new key and never overwrites; the stack computes the
 * SHA-256 and length itself, so a reference never trusts a caller's digest.
 * No provider versioning, conditional writes or provider copy is relied on.
 */
public interface Stack {
    /** The catalog ID of this namespace; recorded in every reference it returns. */
    String stackId();

    /**
     * Streams {@code length} bytes into a new object.
     *
     * @param body the bytes; read to exactly {@code length}
     * @param length the declared length
     * @return the new object's reference, with the digest computed while writing
     * @throws IOException when the body is shorter or longer than declared, or the write fails
     */
    StackRef put(InputStream body, long length) throws IOException;

    /**
     * Writes an in-memory object. The provider may retry from the array; the
     * digest is computed over the same array.
     *
     * @param body the bytes
     * @return the new object's reference
     * @throws IOException when the write fails
     */
    StackRef put(byte[] body) throws IOException;

    /**
     * Opens a streamed read of the whole object. The stream verifies length and
     * SHA-256 against the reference when it reaches the end, and throws
     * {@link StackDataLoss} instead of returning the end of a mismatched object.
     * A missing object is data loss too: the catalog references it.
     *
     * @param ref the reference to read
     * @return the object's verified bytes
     * @throws StackDataLoss when the object is missing
     * @throws IOException when the read cannot start
     */
    InputStream open(StackRef ref) throws IOException;
}
