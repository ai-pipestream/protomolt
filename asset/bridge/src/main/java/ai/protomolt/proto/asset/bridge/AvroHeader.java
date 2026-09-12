package ai.protomolt.proto.asset.bridge;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The metadata block at the head of an Avro object-container file, read
 * against the JDK. The container's own specification is small: the four
 * magic bytes, a map of metadata whose {@code avro.schema} entry holds the
 * writer's schema as JSON, then a sync marker and the data blocks.
 *
 * <p>Only the header is read. The schema an Avro file declares is the fact
 * a schema bridge wants, and reading it costs the first few hundred bytes
 * rather than a pass over the data.
 */
final class AvroHeader {

    private static final byte[] MAGIC = {'O', 'b', 'j', 0x01};

    /** The metadata key holding the writer's schema. */
    static final String SCHEMA_KEY = "avro.schema";

    private AvroHeader() {
    }

    /**
     * Reads the container's metadata map.
     *
     * @param in the file's bytes, positioned at the start
     * @return the metadata, in file order
     * @throws IOException when the bytes are not an Avro object container
     */
    static Map<String, String> metadata(InputStream in) throws IOException {
        byte[] magic = in.readNBytes(MAGIC.length);
        if (magic.length != MAGIC.length || !java.util.Arrays.equals(magic, MAGIC)) {
            throw new IOException("not an Avro object container: the magic bytes are wrong");
        }
        Map<String, String> metadata = new LinkedHashMap<>();
        // The map arrives as blocks: a count, that many key/value pairs, then
        // the next count, ending at a zero count. A negative count states the
        // pair count AND is followed by the block's byte size.
        while (true) {
            long count = readLong(in);
            if (count == 0) {
                return metadata;
            }
            if (count < 0) {
                count = -count;
                readLong(in);
            }
            if (count > 1024) {
                throw new IOException("an Avro header block of " + count
                        + " entries is out of bounds");
            }
            for (long i = 0; i < count; i++) {
                String key = new String(readBytes(in), StandardCharsets.UTF_8);
                metadata.put(key, new String(readBytes(in), StandardCharsets.UTF_8));
            }
        }
    }

    /** Length-prefixed bytes, bounded so a corrupt length cannot allocate wildly. */
    private static byte[] readBytes(InputStream in) throws IOException {
        long length = readLong(in);
        if (length < 0 || length > 1 << 20) {
            throw new IOException("an Avro header value of " + length
                    + " bytes is out of bounds");
        }
        byte[] value = in.readNBytes((int) length);
        if (value.length != length) {
            throw new EOFException("the Avro header ended early");
        }
        return value;
    }

    /** Avro's variable-length zig-zag integer. */
    private static long readLong(InputStream in) throws IOException {
        long value = 0;
        int shift = 0;
        while (shift < 64) {
            int b = in.read();
            if (b < 0) {
                throw new EOFException("the Avro header ended early");
            }
            value |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return (value >>> 1) ^ -(value & 1);
            }
            shift += 7;
        }
        throw new IOException("an Avro variable-length integer never terminates");
    }
}
