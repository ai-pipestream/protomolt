package ai.protomolt.proto.asset.bridge;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * A POSIX tar reader, hand-rolled against the JDK. Enough of the format to
 * list a real archive honestly: ustar name prefixes, GNU long names, and pax
 * extended headers all produce the path the archive actually records, and a
 * header the reader cannot make sense of stops the listing rather than
 * inventing entries.
 *
 * <p>Only what a member listing needs is decoded — path, size, modification
 * time, and whether the member is a directory. Ownership, permissions, and
 * link targets are skipped: a catalog of an archive's contents is not a
 * restore tool.
 */
final class TarReader implements AutoCloseable {

    /** Every tar structure is a multiple of this block size. */
    static final int BLOCK = 512;

    private static final int NAME = 0;
    private static final int SIZE = 124;
    private static final int MTIME = 136;
    private static final int TYPEFLAG = 156;
    private static final int MAGIC = 257;
    private static final int PREFIX = 345;

    /** One member header, with the member's data still unread. */
    record Header(String path, long size, long modifiedEpochSecond, boolean directory) {
    }

    private final InputStream in;
    private final byte[] block = new byte[BLOCK];
    private long remaining;
    private long padding;

    TarReader(InputStream in) {
        this.in = in;
    }

    /**
     * Advances to the next member.
     *
     * @return the next member's header, or null at the end of the archive
     * @throws IOException when the stream fails or a header is malformed
     */
    Header next() throws IOException {
        skipCurrentMember();
        String longName = null;
        String paxPath = null;
        while (true) {
            if (!readBlock()) {
                return null;
            }
            if (isZeroBlock()) {
                // One zero block ends the archive proper; the second is the
                // padding tar always writes. Either way, the listing is done.
                return null;
            }
            if (!isUstar() && !isOldStyle()) {
                throw new IOException("not a tar header block at this offset");
            }
            char type = (char) (block[TYPEFLAG] == 0 ? '0' : block[TYPEFLAG]);
            long size = parseNumeric(SIZE, 12);
            if (type == 'L') {
                // GNU long name: the member's data IS the next member's path.
                longName = trimNul(new String(readMemberData(size), StandardCharsets.UTF_8));
                continue;
            }
            if (type == 'x' || type == 'g') {
                // pax extended header records; "path" overrides the name field.
                String path = paxPath(new String(readMemberData(size),
                        StandardCharsets.UTF_8));
                if (path != null) {
                    paxPath = path;
                }
                continue;
            }
            String path = paxPath != null ? paxPath
                    : longName != null ? longName : ustarPath();
            boolean directory = type == '5' || path.endsWith("/");
            remaining = directory ? 0 : size;
            padding = remaining % BLOCK == 0 ? 0 : BLOCK - (remaining % BLOCK);
            return new Header(path, directory ? 0 : size, parseNumeric(MTIME, 12), directory);
        }
    }

    /**
     * Reads up to {@code limit} bytes of the current member's data into the
     * digest-friendly callback, then leaves the stream positioned for
     * {@link #next()}.
     *
     * @param sink receives each chunk read
     * @param limit stop after this many bytes; the rest of the member is
     *        skipped
     * @return how many bytes were passed to the sink
     * @throws IOException when the stream fails
     */
    long readMember(ByteSink sink, long limit) throws IOException {
        byte[] buffer = new byte[8192];
        long read = 0;
        while (remaining > 0 && read < limit) {
            int want = (int) Math.min(buffer.length, Math.min(remaining, limit - read));
            int n = in.read(buffer, 0, want);
            if (n < 0) {
                throw new EOFException("tar member truncated");
            }
            sink.accept(buffer, 0, n);
            remaining -= n;
            read += n;
        }
        return read;
    }

    @Override
    public void close() throws IOException {
        in.close();
    }

    /** Consumes whatever is left of the current member plus its padding. */
    private void skipCurrentMember() throws IOException {
        skipExactly(remaining + padding);
        remaining = 0;
        padding = 0;
    }

    private byte[] readMemberData(long size) throws IOException {
        if (size < 0 || size > 1 << 20) {
            throw new IOException("tar metadata block of " + size + " bytes is out of bounds");
        }
        byte[] data = in.readNBytes((int) size);
        if (data.length != size) {
            throw new EOFException("tar metadata block truncated");
        }
        skipExactly(size % BLOCK == 0 ? 0 : BLOCK - (size % BLOCK));
        return data;
    }

    private void skipExactly(long count) throws IOException {
        long left = count;
        byte[] sink = new byte[8192];
        while (left > 0) {
            int n = in.read(sink, 0, (int) Math.min(sink.length, left));
            if (n < 0) {
                throw new EOFException("tar stream ended mid-member");
            }
            left -= n;
        }
    }

    private boolean readBlock() throws IOException {
        int filled = in.readNBytes(block, 0, BLOCK);
        if (filled == 0) {
            return false;
        }
        if (filled != BLOCK) {
            throw new EOFException("tar stream ended mid-header");
        }
        return true;
    }

    private boolean isZeroBlock() {
        for (byte b : block) {
            if (b != 0) {
                return false;
            }
        }
        return true;
    }

    private boolean isUstar() {
        return block[MAGIC] == 'u' && block[MAGIC + 1] == 's' && block[MAGIC + 2] == 't'
                && block[MAGIC + 3] == 'a' && block[MAGIC + 4] == 'r';
    }

    /** Pre-ustar tars have no magic; the checksum field is the only tell. */
    private boolean isOldStyle() {
        int stored;
        try {
            stored = (int) parseNumeric(148, 8);
        } catch (IOException e) {
            return false;
        }
        int unsigned = 0;
        for (int i = 0; i < BLOCK; i++) {
            unsigned += (i >= 148 && i < 156) ? ' ' : (block[i] & 0xFF);
        }
        return stored == unsigned;
    }

    private String ustarPath() {
        String name = trimNul(new String(block, NAME, 100, StandardCharsets.UTF_8));
        String prefix = isUstar()
                ? trimNul(new String(block, PREFIX, 155, StandardCharsets.UTF_8)) : "";
        return prefix.isEmpty() ? name : prefix + "/" + name;
    }

    /** Octal, or base-256 when the field's high bit is set (sizes past 8 GiB). */
    private long parseNumeric(int offset, int length) throws IOException {
        if ((block[offset] & 0x80) != 0) {
            long value = block[offset] & 0x7F;
            for (int i = offset + 1; i < offset + length; i++) {
                value = (value << 8) | (block[i] & 0xFF);
            }
            return value;
        }
        String text = trimNul(new String(block, offset, length, StandardCharsets.US_ASCII)).trim();
        if (text.isEmpty()) {
            return 0;
        }
        try {
            return Long.parseLong(text, 8);
        } catch (NumberFormatException e) {
            throw new IOException("tar numeric field is not octal: '" + text + "'");
        }
    }

    /** pax records are "&lt;length&gt; &lt;key&gt;=&lt;value&gt;\n"; only path matters here. */
    private static String paxPath(String records) {
        for (String line : records.split("\n")) {
            int space = line.indexOf(' ');
            int equals = line.indexOf('=');
            if (space < 0 || equals < space) {
                continue;
            }
            if (line.substring(space + 1, equals).equals("path")) {
                return line.substring(equals + 1);
            }
        }
        return null;
    }

    private static String trimNul(String text) {
        int end = text.indexOf('\0');
        return end < 0 ? text : text.substring(0, end);
    }

    /** Receives member bytes as they stream past. */
    interface ByteSink {
        void accept(byte[] buffer, int offset, int length);
    }
}
