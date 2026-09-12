package ai.protomolt.proto.asset.characterize;

/**
 * The bounded view of an asset that identification is allowed to see: a
 * window at the start of the content and a window at the end, addressed in
 * the asset's own offset space.
 *
 * <p>Identification never reads a whole asset. Every signature worth
 * evaluating is anchored — to the first bytes or to the last — so two
 * resident windows answer the question at a cost that does not grow with
 * the asset. A tarball of forty gigabytes and one of forty bytes cost the
 * same to identify.
 *
 * <p>Offsets are absolute, measured from the start of the content, and a
 * byte outside both windows is <em>not observable</em> rather than absent:
 * {@link #byteAt(long)} answers {@link #NOT_OBSERVED} and callers report
 * that they could not look, instead of reporting that they looked and
 * found nothing. The distinction matters — a signature whose reach exceeds
 * the windows has not failed to match, it has failed to be evaluated.
 *
 * <p>The windows may overlap. For content smaller than their combined
 * size, the same bytes are resident in both, and every offset resolves.
 */
public final class ByteWindows {

    /** How many leading bytes a capture keeps by default. */
    public static final int DEFAULT_HEAD_BYTES = 64 * 1024;

    /**
     * How many trailing bytes a capture keeps by default. Sized by the ZIP
     * end-of-central-directory record, which the format permits to sit up
     * to 65,557 bytes from the end.
     */
    public static final int DEFAULT_TAIL_BYTES = 128 * 1024;

    /** The answer from {@link #byteAt(long)} for an offset in neither window. */
    public static final int NOT_OBSERVED = -1;

    /** The content length of an asset whose size the capture never learned. */
    public static final long SIZE_UNKNOWN = -1L;

    /** Nothing was captured. */
    public static final ByteWindows EMPTY =
            new ByteWindows(new byte[0], new byte[0], SIZE_UNKNOWN);

    private static final byte[] NONE = new byte[0];

    private final byte[] head;
    private final byte[] tail;
    private final long sizeBytes;
    private final long tailStart;

    private ByteWindows(byte[] head, byte[] tail, long sizeBytes) {
        this.head = head;
        this.tail = tail;
        this.sizeBytes = sizeBytes;
        this.tailStart = sizeBytes == SIZE_UNKNOWN ? SIZE_UNKNOWN : sizeBytes - tail.length;
    }

    /**
     * Windows over content read whole — every offset is observable.
     *
     * @param content the entire content; may be empty
     * @return the windows
     */
    public static ByteWindows ofWhole(byte[] content) {
        if (content == null || content.length == 0) {
            return new ByteWindows(NONE, NONE, 0);
        }
        return new ByteWindows(content, content, content.length);
    }

    /**
     * A leading window alone, over content whose length is not known —
     * nothing anchored to the end is observable.
     *
     * @param head the leading bytes; may be null or empty
     * @return the windows
     */
    public static ByteWindows ofHead(byte[] head) {
        if (head == null || head.length == 0) {
            return EMPTY;
        }
        return new ByteWindows(head, NONE, SIZE_UNKNOWN);
    }

    /**
     * Both windows over content of a known length.
     *
     * @param head the leading bytes; may be empty
     * @param tail the trailing bytes; may be empty
     * @param sizeBytes the content's full length
     * @return the windows
     * @throws IllegalArgumentException if either window is longer than the
     *         content it is a window on
     */
    public static ByteWindows of(byte[] head, byte[] tail, long sizeBytes) {
        byte[] leading = head == null ? NONE : head;
        byte[] trailing = tail == null ? NONE : tail;
        if (sizeBytes < 0) {
            throw new IllegalArgumentException("sizeBytes must not be negative");
        }
        if (leading.length > sizeBytes || trailing.length > sizeBytes) {
            throw new IllegalArgumentException("a window cannot be longer than the "
                    + sizeBytes + "-byte content it windows");
        }
        return new ByteWindows(leading, trailing, sizeBytes);
    }

    /**
     * The leading window. Not copied — callers read it and do not write it.
     *
     * @return the leading bytes, possibly empty
     */
    public byte[] head() {
        return head;
    }

    /**
     * The trailing window. Not copied — callers read it and do not write it.
     *
     * @return the trailing bytes, possibly empty
     */
    public byte[] tail() {
        return tail;
    }

    /**
     * The content's full length, or {@link #SIZE_UNKNOWN}.
     *
     * @return the length in bytes
     */
    public long sizeBytes() {
        return sizeBytes;
    }

    /** Whether the capture learned how long the content is. */
    public boolean sizeKnown() {
        return sizeBytes != SIZE_UNKNOWN;
    }

    /** Whether anything at all was captured. */
    public boolean isEmpty() {
        return head.length == 0 && tail.length == 0;
    }

    /**
     * Translates an offset measured back from the end of the content into
     * the absolute offset space.
     *
     * @param fromEnd how many bytes back from the end, where 1 is the last
     *        byte of the content
     * @return the absolute offset, or {@link #NOT_OBSERVED} when the size
     *         is unknown or the offset falls before the content starts
     */
    public long fromEnd(long fromEnd) {
        if (!sizeKnown() || fromEnd < 1 || fromEnd > sizeBytes) {
            return NOT_OBSERVED;
        }
        return sizeBytes - fromEnd;
    }

    /**
     * The byte at an absolute offset.
     *
     * @param offset the absolute offset from the start of the content
     * @return the byte as an unsigned int, or {@link #NOT_OBSERVED} when
     *         the offset lies in neither window
     */
    public int byteAt(long offset) {
        if (offset < 0) {
            return NOT_OBSERVED;
        }
        if (offset < head.length) {
            return head[(int) offset] & 0xFF;
        }
        if (tail.length > 0 && offset >= tailStart && offset < sizeBytes) {
            return tail[(int) (offset - tailStart)] & 0xFF;
        }
        return NOT_OBSERVED;
    }

    /**
     * Whether a whole span is resident in one window, and so can be read.
     *
     * @param offset the absolute offset of the span's first byte
     * @param length how many bytes the span covers
     * @return true when the span can be read
     */
    public boolean observable(long offset, int length) {
        if (offset < 0 || length < 0) {
            return false;
        }
        if (length == 0) {
            return true;
        }
        long end = offset + length;
        if (end <= head.length) {
            return true;
        }
        return tail.length > 0 && offset >= tailStart && end <= sizeBytes;
    }

    /**
     * Copies a span out of whichever window holds it.
     *
     * @param offset the absolute offset of the span's first byte
     * @param length how many bytes to copy
     * @return the bytes, or null when the span is not observable
     */
    public byte[] slice(long offset, int length) {
        if (!observable(offset, length)) {
            return null;
        }
        byte[] out = new byte[length];
        for (int i = 0; i < length; i++) {
            out[i] = (byte) byteAt(offset + i);
        }
        return out;
    }

    /**
     * Whether a literal byte pattern sits at an absolute offset.
     *
     * @param offset the absolute offset to compare at
     * @param pattern the bytes to compare against
     * @return true only when the span is observable and every byte matches
     */
    public boolean matchesAt(long offset, byte[] pattern) {
        if (pattern == null || !observable(offset, pattern.length)) {
            return false;
        }
        for (int i = 0; i < pattern.length; i++) {
            if (byteAt(offset + i) != (pattern[i] & 0xFF)) {
                return false;
            }
        }
        return true;
    }
}
