package ai.protomolt.proto.asset.bridge;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * A minimal tar writer for the tests: enough of the format to produce the
 * header shapes a real archive contains (ustar, ustar with a name prefix,
 * GNU long names, pax extended records) so the reader is exercised against
 * spec-shaped bytes rather than against its own assumptions.
 */
final class Tars {

    private final ByteArrayOutputStream out = new ByteArrayOutputStream();

    private Tars() {
    }

    static Tars archive() {
        return new Tars();
    }

    /** A regular ustar member. */
    Tars file(String path, String content) throws IOException {
        return file(path, content.getBytes(StandardCharsets.UTF_8));
    }

    Tars file(String path, byte[] content) throws IOException {
        writeHeader(path, "", content.length, '0');
        writeData(content);
        return this;
    }

    /** A ustar member whose path is split across the name prefix field. */
    Tars prefixedFile(String prefix, String name, String content) throws IOException {
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        writeHeader(name, prefix, bytes.length, '0');
        writeData(bytes);
        return this;
    }

    /** A directory member. */
    Tars directory(String path) throws IOException {
        writeHeader(path.endsWith("/") ? path : path + "/", "", 0, '5');
        return this;
    }

    /** A member whose path arrives as a GNU long-name block. */
    Tars gnuLongName(String path, String content) throws IOException {
        byte[] name = (path + "\0").getBytes(StandardCharsets.UTF_8);
        writeHeader("././@LongLink", "", name.length, 'L');
        writeData(name);
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        writeHeader(path.substring(0, Math.min(99, path.length())), "", bytes.length, '0');
        writeData(bytes);
        return this;
    }

    /** A member whose path arrives as a pax extended header record. */
    Tars paxPath(String path, String content) throws IOException {
        String record = paxRecord("path", path);
        byte[] records = record.getBytes(StandardCharsets.UTF_8);
        writeHeader("PaxHeaders/entry", "", records.length, 'x');
        writeData(records);
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        writeHeader("placeholder", "", bytes.length, '0');
        writeData(bytes);
        return this;
    }

    /** The archive's bytes, terminated by the two zero blocks tar writes. */
    byte[] bytes() throws IOException {
        out.write(new byte[TarReader.BLOCK * 2]);
        return out.toByteArray();
    }

    /** pax records are self-describing: "&lt;total length&gt; key=value\n". */
    private static String paxRecord(String key, String value) {
        String body = " " + key + "=" + value + "\n";
        int length = body.length() + 1;
        // The length digit count is part of the length, so it may need a round.
        while (String.valueOf(length).length() + body.length() != length) {
            length = String.valueOf(length).length() + body.length();
        }
        return length + body;
    }

    private void writeHeader(String name, String prefix, long size, char type)
            throws IOException {
        byte[] header = new byte[TarReader.BLOCK];
        put(header, 0, name, 100);
        put(header, 100, "0000644", 8);      // mode
        put(header, 108, "0000000", 8);      // uid
        put(header, 116, "0000000", 8);      // gid
        put(header, 124, octal(size, 11), 12);
        put(header, 136, octal(1_700_000_000L, 11), 12);
        for (int i = 148; i < 156; i++) {    // checksum field counts as spaces
            header[i] = ' ';
        }
        header[156] = (byte) type;
        put(header, 257, "ustar", 6);
        put(header, 263, "00", 2);
        put(header, 345, prefix, 155);
        int sum = 0;
        for (byte b : header) {
            sum += b & 0xFF;
        }
        put(header, 148, octal(sum, 6) + "\0", 8);
        out.write(header);
    }

    private void writeData(byte[] content) throws IOException {
        out.write(content);
        int padding = content.length % TarReader.BLOCK;
        if (padding != 0) {
            out.write(new byte[TarReader.BLOCK - padding]);
        }
    }

    private static String octal(long value, int width) {
        return String.format("%0" + width + "o", value);
    }

    private static void put(byte[] target, int offset, String value, int width) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        System.arraycopy(bytes, 0, target, offset, Math.min(bytes.length, width));
    }
}
