package ai.protomolt.proto.repo.accession;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Random;

/**
 * Synthetic file bytes for the fixture corpus: every byte is generated from a
 * seed, so nothing is downloaded or copied, and the same seed yields the same
 * bytes. The text formats are small readable files; the binary formats are
 * structurally valid minimal files of their type, with pseudo-random content.
 * {@link java.util.Random} is used because its algorithm is specified by the
 * JDK, so its sequence is the same on every runtime.
 */
public final class SyntheticBytes {
    private SyntheticBytes() {}

    /** A seed for one named thing under a corpus seed. */
    public static long seedFor(long seed, String name) {
        long hash = seed;
        for (byte b : name.getBytes(StandardCharsets.UTF_8)) {
            hash = hash * 1_000_003L + (b & 0xff);
        }
        return hash;
    }

    /** A short plain-text file with a title line and a few sentences. */
    public static byte[] plainText(String title, String body) {
        return (title + "\n\n" + body + "\n").getBytes(StandardCharsets.UTF_8);
    }

    /** A CSV file with a header and {@code rows} rows of seeded numbers. */
    public static byte[] csv(long seed, int rows) {
        var random = new Random(seed);
        var text = new StringBuilder("quarter,region,revenue,units\n");
        for (int row = 0; row < rows; row++) {
            text.append("Q").append(1 + row % 4).append(',')
                    .append(REGIONS[random.nextInt(REGIONS.length)]).append(',')
                    .append(random.nextInt(900_000) + 100_000).append(',')
                    .append(random.nextInt(9_000) + 1_000).append('\n');
        }
        return text.toString().getBytes(StandardCharsets.UTF_8);
    }

    /** A small HTML page with a heading and a paragraph. */
    public static byte[] html(String title, String body) {
        return ("<!doctype html>\n<html><head><meta charset=\"utf-8\"><title>" + title
                + "</title></head>\n<body><h1>" + title + "</h1>\n<p>" + body + "</p>\n</body></html>\n")
                .getBytes(StandardCharsets.UTF_8);
    }

    /** A Markdown document with a heading and a paragraph. */
    public static byte[] markdown(String title, String body) {
        return ("# " + title + "\n\n" + body + "\n").getBytes(StandardCharsets.UTF_8);
    }

    /**
     * A minimal single-page PDF whose page shows {@code text}. The cross
     * reference table is computed, so the file is a well-formed PDF 1.4 document.
     */
    public static byte[] pdf(String text) {
        var escaped = text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)");
        var stream = "BT /F1 18 Tf 72 720 Td (" + escaped + ") Tj ET";
        var objects = new String[] {
            "<< /Type /Catalog /Pages 2 0 R >>",
            "<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
            "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>",
            "<< /Length " + stream.getBytes(StandardCharsets.ISO_8859_1).length + " >>\nstream\n" + stream + "\nendstream",
            "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
        };
        var out = new StringBuilder("%PDF-1.4\n");
        var offsets = new long[objects.length];
        for (int i = 0; i < objects.length; i++) {
            offsets[i] = out.length();
            out.append(i + 1).append(" 0 obj\n").append(objects[i]).append("\nendobj\n");
        }
        long xref = out.length();
        out.append("xref\n0 ").append(objects.length + 1).append("\n0000000000 65535 f \n");
        for (long offset : offsets) {
            out.append(String.format(java.util.Locale.ROOT, "%010d 00000 n \n", offset));
        }
        out.append("trailer\n<< /Size ").append(objects.length + 1).append(" /Root 1 0 R >>\nstartxref\n")
                .append(xref).append("\n%%EOF\n");
        return out.toString().getBytes(StandardCharsets.ISO_8859_1);
    }

    /** A binary PPM image ({@code image/x-portable-pixmap}) of seeded pixels. */
    public static byte[] ppm(long seed, int width, int height) {
        var header = ("P6\n" + width + " " + height + "\n255\n").getBytes(StandardCharsets.US_ASCII);
        var pixels = new byte[width * height * 3];
        new Random(seed).nextBytes(pixels);
        var out = new byte[header.length + pixels.length];
        System.arraycopy(header, 0, out, 0, header.length);
        System.arraycopy(pixels, 0, out, header.length, pixels.length);
        return out;
    }

    /** {@code length} opaque bytes from the seed. */
    public static byte[] opaque(long seed, int length) {
        var bytes = new byte[length];
        new Random(seed).nextBytes(bytes);
        return bytes;
    }

    /**
     * A stream of {@code length} opaque bytes from the seed, generated as it is
     * read. Used for the large blob, so its size never has to fit in memory or
     * in the repository.
     */
    public static InputStream opaqueStream(long seed, long length) {
        return new InputStream() {
            private final Random random = new Random(seed);
            private long remaining = length;
            private final byte[] buffer = new byte[8192];
            private int position;
            private int limit;

            @Override
            public int read() {
                if (remaining <= 0) return -1;
                if (position == limit) fill();
                remaining--;
                return buffer[position++] & 0xff;
            }

            @Override
            public int read(byte[] target, int offset, int count) {
                if (count == 0) return 0;
                if (remaining <= 0) return -1;
                if (position == limit) fill();
                int n = (int) Math.min(Math.min(count, limit - position), remaining);
                System.arraycopy(buffer, position, target, offset, n);
                position += n;
                remaining -= n;
                return n;
            }

            private void fill() {
                random.nextBytes(buffer);
                position = 0;
                limit = buffer.length;
            }
        };
    }

    /** Lowercase hex SHA-256 of the bytes. */
    public static String sha256(byte[] bytes) {
        return HexFormat.of().formatHex(digest().digest(bytes));
    }

    /** Lowercase hex SHA-256 of everything the stream yields; the stream is consumed and closed. */
    public static String sha256(InputStream stream) throws IOException {
        var digest = digest();
        var buffer = new byte[8192];
        try (stream) {
            int n;
            while ((n = stream.read(buffer)) != -1) {
                digest.update(buffer, 0, n);
            }
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private static MessageDigest digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException missing) {
            throw new IllegalStateException("SHA-256 unavailable", missing);
        }
    }

    private static final String[] REGIONS = {"emea", "apac", "amer"};
}
