package ai.protomolt.proto.repo.stack;

import ai.protomolt.proto.repo.v2.StackRef;
import java.io.IOException;
import java.io.InputStream;

/**
 * One configured storage namespace, written once per key (requirements §6, §11).
 * Every put creates a new key and never overwrites; the stack computes the
 * SHA-256 and length itself, so a reference never trusts a caller's digest.
 * No provider versioning, conditional writes or provider copy is relied on.
 * <p>
 * This is the only storage contract: transports (#317) and purge (#318) build
 * on it and add no storage semantics of their own.
 */
public interface Stack extends AutoCloseable {
    /** The length a streamed put takes when the caller does not know it. */
    long UNKNOWN_LENGTH = -1;

    /** The catalog ID of this namespace; recorded in every reference it returns. */
    String stackId();

    /**
     * Streams a body into a new object and returns once the provider has stored it.
     * <ul>
     * <li>The caller owns {@code body}. The stack reads it to its end, never closes
     *     or rewinds it, and never asks it to mark.</li>
     * <li>The SHA-256 and length in the returned reference are computed here while
     *     writing, never taken from the caller.</li>
     * <li>{@code expectedLength} is exact ({@code >= 0}) or {@link #UNKNOWN_LENGTH}.
     *     A body that ends shorter or runs longer than an exact length fails the
     *     put, and no reference is returned.</li>
     * <li>Provider retries resend from bounded buffers inside the stack, never by
     *     rewinding the caller's stream.</li>
     * <li>The caller cancels by closing {@code body} or interrupting the thread.
     *     The put then throws, aborts any partial upload as a best effort, and
     *     returns no reference. An object left behind is referenced by nothing and
     *     is not swept (requirements §6, "No automatic sweeping").</li>
     * </ul>
     *
     * @param body the bytes
     * @param expectedLength the exact length, or {@link #UNKNOWN_LENGTH}
     * @return the new object's reference
     * @throws IOException when the body differs from its expected length, the body
     *         fails, or the write fails
     */
    StackRef put(InputStream body, long expectedLength) throws IOException;

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
     * Opens a streamed read of the whole object. The caller owns the stream and
     * must close it; closing it before the end abandons the provider transfer
     * without draining it. At the end the stream checks the byte count and
     * SHA-256 against the reference and throws {@link StackDataLoss} instead of
     * signalling the end of a mismatched object. Reading past the reference's
     * length fails at once. A missing object is data loss too: the catalog
     * references it.
     *
     * @param ref the reference to read
     * @return the object's verified bytes
     * @throws StackDataLoss when the object is missing
     * @throws IOException when the read cannot start
     */
    InputStream open(StackRef ref) throws IOException;

    /**
     * Opens a streamed read of {@code length} bytes from {@code offset}. Bounds are
     * checked without overflow against {@code ref.length} before any I/O:
     * {@code 0 <= offset <= ref.length} and {@code 0 <= length <= ref.length - offset}.
     * A zero length returns no bytes. A range covering the whole object
     * ({@code offset == 0 && length == ref.length}) is a full read, verified like
     * {@link #open}. Any other range is length-checked, not digest-verified: one
     * SHA-256 covers the whole object, so a range cannot be proven intact without
     * reading all of it (requirements §11). A short or long range read throws
     * {@link StackDataLoss}. The public "zero means to the end" convention belongs
     * to the transport, which passes {@code ref.length - offset} here.
     *
     * @param ref the reference to read
     * @param offset the first byte
     * @param length the number of bytes
     * @return the range's bytes, with ownership as for {@link #open}
     * @throws IllegalArgumentException when the range is outside the reference
     * @throws StackDataLoss when the object is missing
     * @throws IOException when the read cannot start
     */
    InputStream openRange(StackRef ref, long offset, long length) throws IOException;

    /**
     * Confirms the object exists with its reference's length, without fetching its
     * bytes or computing a digest.
     *
     * @param ref the reference to check
     * @return the stored length, equal to {@code ref.length}
     * @throws StackDataLoss when the object is missing or its stored length differs
     * @throws IOException when the provider cannot answer
     */
    long head(StackRef ref) throws IOException;

    /**
     * Deletes the object's bytes. Idempotent: an absent object is not an error.
     * Only purge byte deletion calls this, after no unpurged reference remains (#318).
     *
     * @param ref the reference whose bytes to delete
     * @throws IOException when the provider fails
     */
    void delete(StackRef ref) throws IOException;

    /** Releases provider resources this stack owns. The default owns none. */
    @Override default void close() throws IOException {}

    /**
     * Checks a range against its reference without overflow.
     *
     * @throws IllegalArgumentException when the range is outside the reference
     */
    static void requireRange(StackRef ref, long offset, long length) {
        if (offset < 0 || offset > ref.getLength() || length < 0 || length > ref.getLength() - offset)
            throw new IllegalArgumentException("Range " + offset + "+" + length + " is outside an object of "
                    + ref.getLength() + " bytes");
    }
}
