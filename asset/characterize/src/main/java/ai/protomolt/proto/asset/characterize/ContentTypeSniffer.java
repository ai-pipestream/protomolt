package ai.protomolt.proto.asset.characterize;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Magic-byte content detection — the platform's one source of "what media
 * type are these bytes". Parse routing and asset characterization both
 * call this table, so they can never disagree about what a signature
 * means.
 *
 * <p>The sniffer looks at the first bytes of a blob and answers with the
 * MIME type to route on. It is deliberately small and table-driven:
 * exhaustive format coverage is a parser's job, not the router's — the
 * goal here is an HONEST answer. When the bytes are inconclusive the
 * sniffer says so ({@code sniffed=false}) instead of guessing, and the
 * caller falls back to the declared type and finally
 * {@code application/octet-stream}.
 *
 * <p>ZIP containers dispatch on the filename extension (docx/xlsx/pptx/epub
 * are all ZIP at byte level); everything else is pure magic.
 */
public final class ContentTypeSniffer {

    /** The last-resort content type when nothing is known about the bytes. */
    public static final String OCTET_STREAM = "application/octet-stream";

    /**
     * One sniff verdict.
     *
     * @param mimeType the content type to route on
     * @param sniffed true when the type came from the bytes; false when the
     *        bytes were inconclusive and the type is a fallback
     */
    public record Sniff(String mimeType, boolean sniffed) {
    }

    /** One magic-prefix table entry. */
    private record Magic(int offset, byte[] prefix, String mimeType) {
        Magic(String prefix, String mimeType) {
            this(0, prefix.getBytes(StandardCharsets.ISO_8859_1), mimeType);
        }
    }

    /** Prefix magics with a fixed answer, checked in table order. */
    private static final List<Magic> MAGICS = List.of(
            new Magic("%PDF", "application/pdf"),
            new Magic(0, new byte[] {(byte) 0xD0, (byte) 0xCF, 0x11, (byte) 0xE0},
                    "application/x-ole-storage"),
            new Magic(0, new byte[] {(byte) 0x89, 'P', 'N', 'G'}, "image/png"),
            new Magic(0, new byte[] {(byte) 0xFF, (byte) 0xD8, (byte) 0xFF}, "image/jpeg"),
            new Magic("GIF87a", "image/gif"),
            new Magic("GIF89a", "image/gif"),
            new Magic(0, new byte[] {'I', 'I', '*', 0}, "image/tiff"),
            new Magic(0, new byte[] {'M', 'M', 0, '*'}, "image/tiff"),
            new Magic("ID3", "audio/mpeg"),
            new Magic(4, "ftyp".getBytes(StandardCharsets.ISO_8859_1), "video/mp4"),
            new Magic(0, new byte[] {0x1F, (byte) 0x8B}, "application/gzip"),
            new Magic("PAR1", "application/vnd.apache.parquet"),
            new Magic(0, new byte[] {'O', 'b', 'j', 0x01}, "application/avro"));

    /** ZIP-container extension dispatch (lowercase extension → MIME type). */
    private static final Map<String, String> ZIP_EXTENSIONS = Map.of(
            "docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "pptx", "application/vnd.openxmlformats-officedocument.presentationml.presentation",
            "epub", "application/epub+zip");

    /** POSIX tar: "ustar" at offset 257 (both the POSIX and GNU stamps). */
    private static final int TAR_MAGIC_OFFSET = 257;
    private static final byte[] TAR_MAGIC = "ustar".getBytes(StandardCharsets.ISO_8859_1);

    private ContentTypeSniffer() {
    }

    /**
     * Sniffs the content type from the first bytes of a blob.
     *
     * @param head the first bytes of the blob (512 are plenty; tar detection
     *        wants at least 262); may be empty
     * @param filename the original filename, used only to disambiguate ZIP
     *        containers; may be blank
     * @return the verdict — {@code sniffed=false} with
     *         {@link #OCTET_STREAM} when the bytes are inconclusive
     */
    public static Sniff sniff(byte[] head, String filename) {
        if (head == null || head.length == 0) {
            return new Sniff(OCTET_STREAM, false);
        }
        if (startsWith(head, 0, new byte[] {'P', 'K', 0x03, 0x04})) {
            return new Sniff(ZIP_EXTENSIONS.getOrDefault(extensionOf(filename), "application/zip"), true);
        }
        if (startsWith(head, TAR_MAGIC_OFFSET, TAR_MAGIC)) {
            return new Sniff("application/x-tar", true);
        }
        for (Magic magic : MAGICS) {
            if (startsWith(head, magic.offset(), magic.prefix())) {
                return new Sniff(magic.mimeType(), true);
            }
        }
        Sniff markup = sniffMarkup(head);
        if (markup != null) {
            return markup;
        }
        if (startsWith(head, 0, new byte[] {'R', 'I', 'F', 'F'}) && head.length >= 12) {
            String form = new String(head, 8, 4, StandardCharsets.ISO_8859_1);
            if ("WAVE".equals(form)) {
                return new Sniff("audio/wav", true);
            }
            if ("WEBP".equals(form)) {
                return new Sniff("image/webp", true);
            }
        }
        // MPEG audio frame sync: 0xFF followed by 0b111xxxxx.
        if (head.length >= 2 && (head[0] & 0xFF) == 0xFF && (head[1] & 0xE0) == 0xE0) {
            return new Sniff("audio/mpeg", true);
        }
        if (isMostlyPrintableUtf8(head)) {
            return new Sniff("text/plain", true);
        }
        return new Sniff(OCTET_STREAM, false);
    }

    /**
     * Sniffs and applies the fallback chain: bytes first, then the declared
     * type, finally {@link #OCTET_STREAM}.
     *
     * @param head the first bytes of the blob; may be empty
     * @param filename the original filename; may be blank
     * @param declaredMimeType the caller-declared type; may be blank
     * @return the sniffed verdict, or the declared type with
     *         {@code sniffed=false} when the bytes were inconclusive
     */
    public static Sniff resolve(byte[] head, String filename, String declaredMimeType) {
        Sniff sniff = sniff(head, filename);
        if (sniff.sniffed()) {
            return sniff;
        }
        if (declaredMimeType != null && !declaredMimeType.isBlank()) {
            return new Sniff(declaredMimeType, false);
        }
        return new Sniff(OCTET_STREAM, false);
    }

    /**
     * The lowercase filename extension, {@code ""} when there is none.
     *
     * @param filename the filename; may be null or blank
     * @return the extension without its dot, lowercased
     */
    public static String extensionOf(String filename) {
        if (filename == null) {
            return "";
        }
        int dot = filename.lastIndexOf('.');
        if (dot < 0 || dot == filename.length() - 1) {
            return "";
        }
        return filename.substring(dot + 1).toLowerCase(Locale.ROOT);
    }

    /** XML/HTML: {@code <?xml} or leading whitespace + {@code <} with {@code <html}. */
    private static Sniff sniffMarkup(byte[] head) {
        String text = new String(head, StandardCharsets.UTF_8);
        String trimmed = text.stripLeading();
        String lower = trimmed.toLowerCase(Locale.ROOT);
        if (lower.startsWith("<?xml")) {
            return new Sniff(lower.contains("<html") ? "text/html" : "application/xml", true);
        }
        if (lower.startsWith("<") && lower.contains("<html")) {
            return new Sniff("text/html", true);
        }
        return null;
    }

    private static boolean startsWith(byte[] head, int offset, byte[] prefix) {
        if (head.length < offset + prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (head[offset + i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }

    /**
     * Text heuristic: no NUL bytes and at least 90% of the bytes printable
     * ASCII, whitespace, or part of a multi-byte UTF-8 sequence.
     */
    private static boolean isMostlyPrintableUtf8(byte[] head) {
        int printable = 0;
        for (byte value : head) {
            int b = value & 0xFF;
            if (b == 0) {
                return false;
            }
            if (b == '\t' || b == '\n' || b == '\r' || (b >= 0x20 && b < 0x7F) || b >= 0x80) {
                printable++;
            }
        }
        return printable * 10 >= head.length * 9;
    }
}
