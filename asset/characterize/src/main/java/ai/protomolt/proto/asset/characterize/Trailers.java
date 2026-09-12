package ai.protomolt.proto.asset.characterize;

/**
 * The structures formats write at the <em>end</em> of their content, read
 * out of the trailing window.
 *
 * <p>A leading magic says what a producer started writing; a trailer says
 * what it finished. For the formats that seal themselves, the difference
 * is the difference between an asset and the wreckage of an interrupted
 * upload — a Parquet file with its header and no footer has no schema and
 * no row groups anything can find, however convincingly its first four
 * bytes read.
 *
 * <p>Everything here is bounded. A ZIP's end-of-central-directory record
 * sits within 65,557 bytes of the end because the format caps its trailing
 * comment at 65,535, and the central directory it points at is written
 * immediately before it — so for any archive whose directory fits the
 * trailing window, the entry list is resident too, and no seek into the
 * middle of the content is ever needed.
 */
public final class Trailers {

    /** The four bytes Parquet writes at both ends of a file. */
    public static final byte[] PARQUET_MAGIC = {'P', 'A', 'R', '1'};

    /** The end-of-central-directory signature, {@code PK}. */
    private static final byte[] EOCD_SIGNATURE = {'P', 'K', 0x05, 0x06};

    /** The Zip64 end-of-central-directory locator, {@code PK}. */
    private static final byte[] ZIP64_LOCATOR_SIGNATURE = {'P', 'K', 0x06, 0x07};

    /** The fixed part of an end-of-central-directory record. */
    private static final int EOCD_FIXED_BYTES = 22;

    /** The furthest the record can sit from the end: its size plus a full comment. */
    private static final int EOCD_MAX_SEARCH = EOCD_FIXED_BYTES + 0xFFFF;

    /** The Zip64 locator's fixed size, written immediately before the record. */
    private static final int ZIP64_LOCATOR_BYTES = 20;

    /** The 16-bit sentinel a count carries when its real value is in the Zip64 record. */
    private static final int COUNT_SENTINEL = 0xFFFF;

    /** The 32-bit sentinel an offset carries when its real value is in the Zip64 record. */
    private static final long OFFSET_SENTINEL = 0xFFFFFFFFL;

    /**
     * A ZIP's end-of-central-directory record: where the entry list lives
     * and how big it is.
     *
     * @param recordOffset the absolute offset of the record itself
     * @param directoryOffset the absolute offset of the central directory
     * @param directoryBytes how many bytes the central directory occupies
     * @param entryCount how many entries the archive declares
     * @param zip64 whether the record defers to a Zip64 record for the
     *        real counts, in which case the fields above are sentinels
     */
    public record EndOfCentralDirectory(long recordOffset, long directoryOffset,
                                        long directoryBytes, int entryCount, boolean zip64) {
    }

    private Trailers() {
    }

    /**
     * Whether Parquet's footer magic seals the content.
     *
     * @param windows the captured windows
     * @return true only when the last four bytes are observable and are
     *         Parquet's magic; false covers both "sealed by something else"
     *         and "could not look", which {@link #parquetFooterObservable}
     *         separates
     */
    public static boolean parquetSealed(ByteWindows windows) {
        long start = windows.fromEnd(PARQUET_MAGIC.length);
        return start != ByteWindows.NOT_OBSERVED && windows.matchesAt(start, PARQUET_MAGIC);
    }

    /**
     * Whether the last bytes could be read at all.
     *
     * @param windows the captured windows
     * @return true when the footer's span is resident
     */
    public static boolean parquetFooterObservable(ByteWindows windows) {
        long start = windows.fromEnd(PARQUET_MAGIC.length);
        return start != ByteWindows.NOT_OBSERVED
                && windows.observable(start, PARQUET_MAGIC.length);
    }

    /**
     * Locates a ZIP's end-of-central-directory record by searching back
     * from the end of the content.
     *
     * @param windows the captured windows
     * @return the record, or null when the content declares none within
     *         reach — an archive that is truncated, is not a ZIP, or whose
     *         trailer lies outside the captured window
     */
    public static EndOfCentralDirectory zipDirectory(ByteWindows windows) {
        if (!windows.sizeKnown() || windows.sizeBytes() < EOCD_FIXED_BYTES) {
            return null;
        }
        long last = windows.sizeBytes() - EOCD_FIXED_BYTES;
        long floor = Math.max(0, windows.sizeBytes() - EOCD_MAX_SEARCH);
        // Backwards: a comment may itself contain the signature, and the
        // real record is the last one that fits its own declared comment.
        for (long at = last; at >= floor; at--) {
            if (!windows.observable(at, EOCD_FIXED_BYTES)
                    || !windows.matchesAt(at, EOCD_SIGNATURE)) {
                continue;
            }
            int commentBytes = readShort(windows, at + 20);
            if (at + EOCD_FIXED_BYTES + commentBytes != windows.sizeBytes()) {
                continue;
            }
            int entryCount = readShort(windows, at + 10);
            long directoryBytes = readInt(windows, at + 12);
            long directoryOffset = readInt(windows, at + 16);
            boolean zip64 = entryCount == COUNT_SENTINEL
                    || directoryBytes == OFFSET_SENTINEL
                    || directoryOffset == OFFSET_SENTINEL
                    || hasZip64Locator(windows, at);
            return new EndOfCentralDirectory(at, directoryOffset, directoryBytes,
                    entryCount, zip64);
        }
        return null;
    }

    /**
     * Whether a located central directory is resident in the windows and
     * so can be walked without reading the content again.
     *
     * @param windows the captured windows
     * @param directory the located record
     * @return true when the whole directory can be read
     */
    public static boolean directoryResident(ByteWindows windows,
                                            EndOfCentralDirectory directory) {
        if (directory == null || directory.zip64()
                || directory.directoryBytes() > Integer.MAX_VALUE) {
            return false;
        }
        return windows.observable(directory.directoryOffset(),
                (int) directory.directoryBytes());
    }

    private static boolean hasZip64Locator(ByteWindows windows, long recordOffset) {
        long locator = recordOffset - ZIP64_LOCATOR_BYTES;
        return locator >= 0 && windows.matchesAt(locator, ZIP64_LOCATOR_SIGNATURE);
    }

    /** A little-endian unsigned 16-bit field. */
    private static int readShort(ByteWindows windows, long offset) {
        return windows.byteAt(offset) | (windows.byteAt(offset + 1) << 8);
    }

    /** A little-endian unsigned 32-bit field. */
    private static long readInt(ByteWindows windows, long offset) {
        return (long) windows.byteAt(offset)
                | ((long) windows.byteAt(offset + 1) << 8)
                | ((long) windows.byteAt(offset + 2) << 16)
                | ((long) windows.byteAt(offset + 3) << 24);
    }
}
