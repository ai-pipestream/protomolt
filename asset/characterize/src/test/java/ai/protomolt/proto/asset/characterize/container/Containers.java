package ai.protomolt.proto.asset.characterize.container;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.CRC32;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Containers built for tests: a real ZIP from the JDK writer, and a
 * compound file assembled byte by byte.
 *
 * <p>The compound file is written here rather than checked in because a
 * builder states the layout the reader has to understand. A checked-in
 * document proves the reader works on that document; a builder shows what
 * the reader is reading.
 */
final class Containers {

    /** The content type an OOXML word processor document declares. */
    static final String WORD_CONTENT_TYPE =
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml";

    /** The content type an OOXML spreadsheet declares. */
    static final String SPREADSHEET_CONTENT_TYPE =
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml";

    /** The content type an OOXML presentation declares. */
    static final String PRESENTATION_CONTENT_TYPE =
            "application/vnd.openxmlformats-officedocument.presentationml.presentation.main+xml";

    private Containers() {
    }

    /**
     * An archive with the named members, deflated.
     *
     * @param members member path to content
     * @return the archive's bytes
     */
    static byte[] zip(Map<String, String> members) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ZipOutputStream out = new ZipOutputStream(bytes)) {
            for (Map.Entry<String, String> member : members.entrySet()) {
                out.putNextEntry(new ZipEntry(member.getKey()));
                out.write(member.getValue().getBytes(StandardCharsets.UTF_8));
                out.closeEntry();
            }
        }
        return bytes.toByteArray();
    }

    /** An archive with one member stored (not deflated). */
    static byte[] zipStored(String name, String content) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        byte[] data = content.getBytes(StandardCharsets.UTF_8);
        CRC32 crc = new CRC32();
        crc.update(data);
        try (ZipOutputStream out = new ZipOutputStream(bytes)) {
            ZipEntry entry = new ZipEntry(name);
            entry.setMethod(ZipEntry.STORED);
            entry.setSize(data.length);
            entry.setCompressedSize(data.length);
            entry.setCrc(crc.getValue());
            out.putNextEntry(entry);
            out.write(data);
            out.closeEntry();
        }
        return bytes.toByteArray();
    }

    /** An archive with a trailing archive comment after the central directory. */
    static byte[] zipWithComment(Map<String, String> members, String comment) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ZipOutputStream out = new ZipOutputStream(bytes)) {
            for (Map.Entry<String, String> member : members.entrySet()) {
                out.putNextEntry(new ZipEntry(member.getKey()));
                out.write(member.getValue().getBytes(StandardCharsets.UTF_8));
                out.closeEntry();
            }
            out.setComment(comment);
        }
        return bytes.toByteArray();
    }

    /** An archive with no members at all: just an end-of-central-directory record. */
    static byte[] emptyZip() throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ZipOutputStream out = new ZipOutputStream(bytes)) {
            // No entries; closing writes a bare end-of-central-directory record.
        }
        return bytes.toByteArray();
    }

    /** An OOXML document declaring one main content type. */
    static byte[] ooxml(String mainContentType, String bodyPath) throws IOException {
        Map<String, String> members = new LinkedHashMap<>();
        members.put("[Content_Types].xml",
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
                        + "<Types xmlns=\"http://schemas.openxmlformats.org/package/2006/"
                        + "content-types\">"
                        + "<Default Extension=\"rels\" ContentType=\"application/vnd."
                        + "openxmlformats-package.relationships+xml\"/>"
                        + "<Override PartName=\"/" + bodyPath + "\" ContentType=\""
                        + mainContentType + "\"/>"
                        + "</Types>");
        members.put("_rels/.rels", "<Relationships/>");
        members.put(bodyPath, "<document><body/></document>");
        return zip(members);
    }

    // ------------------------------------------------------------------
    // Compound files
    // ------------------------------------------------------------------

    private static final int SECTOR_BYTES = 512;
    private static final int ENTRY_BYTES = 128;
    private static final long END_OF_CHAIN = 0xFFFFFFFEL;
    private static final long FREE = 0xFFFFFFFFL;
    private static final long TABLE = 0xFFFFFFFDL;

    /** Streams shorter than this live in the nested filing system. */
    private static final int MINI_CUTOFF = 4096;

    /** The nested filing system's unit size. */
    private static final int MINI_SECTOR_BYTES = 64;

    /**
     * A compound file holding one stream per named member.
     *
     * <p>A stream shorter than the cutoff goes into the nested filing
     * system, exactly as a real writer would put it there, so both storage
     * paths are exercised by whichever streams a test asks for.
     *
     * @param streams member name to content
     * @return the compound file's bytes
     */
    static byte[] compound(Map<String, byte[]> streams) {
        Map<String, byte[]> regular = new LinkedHashMap<>();
        Map<String, byte[]> small = new LinkedHashMap<>();
        for (Map.Entry<String, byte[]> stream : streams.entrySet()) {
            (stream.getValue().length < MINI_CUTOFF ? small : regular)
                    .put(stream.getKey(), stream.getValue());
        }

        // The nested container is one long stream; each small member takes
        // a whole number of its units inside it.
        int miniUnits = 0;
        for (byte[] content : small.values()) {
            miniUnits += unitsFor(content.length, MINI_SECTOR_BYTES);
        }
        byte[] miniContainer = new byte[miniUnits * MINI_SECTOR_BYTES];
        int miniFatSectors = miniUnits == 0
                ? 0 : unitsFor(miniUnits * 4, SECTOR_BYTES);
        int miniContainerSectors = miniUnits == 0
                ? 0 : unitsFor(miniContainer.length, SECTOR_BYTES);

        int directorySectors = Math.max(1, (streams.size() + 1 + 3) / 4);
        int regularSectors = 0;
        for (byte[] content : regular.values()) {
            regularSectors += unitsFor(content.length, SECTOR_BYTES);
        }
        int totalSectors = 1 + directorySectors + regularSectors
                + miniFatSectors + miniContainerSectors;
        byte[] out = new byte[SECTOR_BYTES * (1 + totalSectors)];

        int firstRegular = 1 + directorySectors;
        int firstMiniFat = firstRegular + regularSectors;
        int firstMiniContainer = firstMiniFat + miniFatSectors;

        System.arraycopy(Ole2Members.MAGIC, 0, out, 0, Ole2Members.MAGIC.length);
        writeShort(out, 24, 0x003E);
        writeShort(out, 26, 3);
        writeShort(out, 28, 0xFFFE);
        writeShort(out, 30, 9);
        writeShort(out, 32, 6);
        writeInt(out, 44, 1);
        writeInt(out, 48, 1);
        writeInt(out, 56, MINI_CUTOFF);
        writeInt(out, 60, miniFatSectors == 0 ? END_OF_CHAIN : firstMiniFat);
        writeInt(out, 64, miniFatSectors);
        writeInt(out, 68, END_OF_CHAIN);
        writeInt(out, 72, 0);
        for (int i = 0; i < 109; i++) {
            writeInt(out, 76 + i * 4, i == 0 ? 0 : FREE);
        }

        int tableAt = SECTOR_BYTES;
        for (int i = 0; i < SECTOR_BYTES / 4; i++) {
            writeInt(out, tableAt + i * 4, FREE);
        }
        writeInt(out, tableAt, TABLE);
        chain(out, tableAt, 1, directorySectors);
        chain(out, tableAt, firstMiniFat, miniFatSectors);
        chain(out, tableAt, firstMiniContainer, miniContainerSectors);

        int directoryAt = SECTOR_BYTES * 2;
        for (int i = 0; i < directorySectors * 4; i++) {
            writeShort(out, directoryAt + i * ENTRY_BYTES + 64, 0);
            out[directoryAt + i * ENTRY_BYTES + 66] = 0;
            writeInt(out, directoryAt + i * ENTRY_BYTES + 68, FREE);
            writeInt(out, directoryAt + i * ENTRY_BYTES + 72, FREE);
            writeInt(out, directoryAt + i * ENTRY_BYTES + 76, FREE);
        }
        writeEntry(out, directoryAt, "Root Entry", 5, FREE, FREE,
                streams.isEmpty() ? FREE : 1,
                miniUnits == 0 ? END_OF_CHAIN : firstMiniContainer, miniContainer.length);

        int index = 1;
        int sector = firstRegular;
        for (Map.Entry<String, byte[]> stream : regular.entrySet()) {
            byte[] content = stream.getValue();
            int span = unitsFor(content.length, SECTOR_BYTES);
            writeEntry(out, directoryAt + index * ENTRY_BYTES, stream.getKey(), 2,
                    FREE, index + 1 <= streams.size() ? index + 1 : FREE, FREE,
                    sector, content.length);
            chain(out, tableAt, sector, span);
            for (int i = 0; i < span; i++) {
                int from = i * SECTOR_BYTES;
                System.arraycopy(content, from, out, SECTOR_BYTES * (1 + sector + i),
                        Math.min(SECTOR_BYTES, content.length - from));
            }
            index++;
            sector += span;
        }

        int miniFatAt = SECTOR_BYTES * (1 + firstMiniFat);
        int unit = 0;
        for (Map.Entry<String, byte[]> stream : small.entrySet()) {
            byte[] content = stream.getValue();
            int span = unitsFor(content.length, MINI_SECTOR_BYTES);
            writeEntry(out, directoryAt + index * ENTRY_BYTES, stream.getKey(), 2,
                    FREE, index + 1 <= streams.size() ? index + 1 : FREE, FREE,
                    unit, content.length);
            for (int i = 0; i < span; i++) {
                writeInt(out, miniFatAt + (unit + i) * 4,
                        i + 1 == span ? END_OF_CHAIN : unit + i + 1);
            }
            System.arraycopy(content, 0, miniContainer, unit * MINI_SECTOR_BYTES,
                    content.length);
            index++;
            unit += span;
        }
        System.arraycopy(miniContainer, 0, out, SECTOR_BYTES * (1 + firstMiniContainer),
                miniContainer.length);
        return out;
    }

    /**
     * A compound file whose sectors are 4096 bytes, declaring major version
     * 4. Every stream lands in a full sector regardless of size, so the
     * nested mini filing system is not exercised here — the point is the
     * larger sector size, not the mini stream, which {@link #compound} at
     * 512 bytes already covers.
     *
     * @param streams member name to content
     * @return the compound file's bytes
     */
    static byte[] compoundWideSectors(Map<String, byte[]> streams) {
        final int sectorBytes = 4096;
        final int entriesPerSector = sectorBytes / ENTRY_BYTES;

        int directorySectors = Math.max(1,
                (streams.size() + 1 + entriesPerSector - 1) / entriesPerSector);
        int regularSectors = 0;
        for (byte[] content : streams.values()) {
            regularSectors += unitsFor(content.length, sectorBytes);
        }
        int totalSectors = 1 + directorySectors + regularSectors;
        byte[] out = new byte[sectorBytes * (1 + totalSectors)];
        int firstRegular = 1 + directorySectors;

        System.arraycopy(Ole2Members.MAGIC, 0, out, 0, Ole2Members.MAGIC.length);
        writeShort(out, 24, 0x003E);
        writeShort(out, 26, 4);
        writeShort(out, 28, 0xFFFE);
        writeShort(out, 30, 12);
        writeShort(out, 32, 6);
        writeInt(out, 44, 1);
        writeInt(out, 48, 1);
        writeInt(out, 56, MINI_CUTOFF);
        writeInt(out, 60, END_OF_CHAIN);
        writeInt(out, 64, 0);
        writeInt(out, 68, END_OF_CHAIN);
        writeInt(out, 72, 0);
        for (int i = 0; i < 109; i++) {
            writeInt(out, 76 + i * 4, i == 0 ? 0 : FREE);
        }

        int tableAt = sectorBytes;
        for (int i = 0; i < sectorBytes / 4; i++) {
            writeInt(out, tableAt + i * 4, FREE);
        }
        writeInt(out, tableAt, TABLE);
        chain(out, tableAt, 1, directorySectors);

        int directoryAt = sectorBytes * 2;
        for (int i = 0; i < directorySectors * entriesPerSector; i++) {
            writeShort(out, directoryAt + i * ENTRY_BYTES + 64, 0);
            out[directoryAt + i * ENTRY_BYTES + 66] = 0;
            writeInt(out, directoryAt + i * ENTRY_BYTES + 68, FREE);
            writeInt(out, directoryAt + i * ENTRY_BYTES + 72, FREE);
            writeInt(out, directoryAt + i * ENTRY_BYTES + 76, FREE);
        }
        writeEntry(out, directoryAt, "Root Entry", 5, FREE, FREE,
                streams.isEmpty() ? FREE : 1, END_OF_CHAIN, 0);

        int index = 1;
        int sector = firstRegular;
        for (Map.Entry<String, byte[]> stream : streams.entrySet()) {
            byte[] content = stream.getValue();
            int span = unitsFor(content.length, sectorBytes);
            writeEntry(out, directoryAt + index * ENTRY_BYTES, stream.getKey(), 2,
                    FREE, index + 1 <= streams.size() ? index + 1 : FREE, FREE,
                    sector, content.length);
            chain(out, tableAt, sector, span);
            for (int i = 0; i < span; i++) {
                int from = i * sectorBytes;
                System.arraycopy(content, from, out, sectorBytes * (1 + sector + i),
                        Math.min(sectorBytes, content.length - from));
            }
            index++;
            sector += span;
        }
        return out;
    }

    /** Links a run of sectors into one chain in the sector table. */
    private static void chain(byte[] out, int tableAt, int first, int span) {
        for (int i = 0; i < span; i++) {
            writeInt(out, tableAt + (first + i) * 4,
                    i + 1 == span ? END_OF_CHAIN : first + i + 1);
        }
    }

    /** How many whole units a run of this length occupies. */
    private static int unitsFor(int length, int unitBytes) {
        return Math.max(1, (length + unitBytes - 1) / unitBytes);
    }

    private static void writeEntry(byte[] out, int at, String name, int type,
                                   long left, long right, long child,
                                   long startSector, long size) {
        byte[] utf16 = name.getBytes(StandardCharsets.UTF_16LE);
        System.arraycopy(utf16, 0, out, at, utf16.length);
        writeShort(out, at + 64, utf16.length + 2);
        out[at + 66] = (byte) type;
        out[at + 67] = 1;
        writeInt(out, at + 68, left);
        writeInt(out, at + 72, right);
        writeInt(out, at + 76, child);
        writeInt(out, at + 116, startSector);
        writeInt(out, at + 120, size);
        writeInt(out, at + 124, 0);
    }

    private static void writeShort(byte[] out, int at, int value) {
        out[at] = (byte) value;
        out[at + 1] = (byte) (value >>> 8);
    }

    private static void writeInt(byte[] out, int at, long value) {
        out[at] = (byte) value;
        out[at + 1] = (byte) (value >>> 8);
        out[at + 2] = (byte) (value >>> 16);
        out[at + 3] = (byte) (value >>> 24);
    }
}
