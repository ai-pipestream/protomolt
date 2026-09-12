package ai.protomolt.proto.asset.characterize.container;

import ai.protomolt.proto.asset.characterize.ByteWindows;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The members of a compound file, read out of the byte windows.
 *
 * <p>A compound file is a filing system in a byte array: fixed-size
 * sectors, a table saying which sector follows which, a directory of named
 * streams laid out as a tree, and a second smaller filing system nested
 * inside one stream for members too short to deserve a whole sector. Every
 * legacy Office document is one, which is why detecting the eight bytes
 * they all begin with says so little.
 *
 * <p>Reading follows the sector table wherever it leads, and stops
 * wherever it leads outside the windows. A member whose sectors are not
 * resident reads as null, and the difference between that and an absent
 * member is what keeps a windowed reader honest.
 */
public final class Ole2Members {

    /** The eight bytes every compound file begins with. */
    public static final byte[] MAGIC =
            {(byte) 0xD0, (byte) 0xCF, 0x11, (byte) 0xE0,
             (byte) 0xA1, (byte) 0xB1, 0x1A, (byte) 0xE1};

    /** How much of one member identification is willing to read. */
    public static final int MAX_MEMBER_BYTES = 1 << 20;

    /** How many directory entries a reader will walk before giving up. */
    private static final int MAX_ENTRIES = 10_000;

    /** How deep the directory tree may nest before a reader gives up. */
    private static final int MAX_DEPTH = 256;

    /** The header's fixed size, and the smallest a compound file can be. */
    private static final int HEADER_BYTES = 512;

    /** How many sector-table pointers the header itself holds. */
    private static final int HEADER_TABLE_POINTERS = 109;

    /** One directory entry's size. */
    private static final int ENTRY_BYTES = 128;

    private static final long END_OF_CHAIN = 0xFFFFFFFEL;
    private static final long NO_STREAM = 0xFFFFFFFFL;
    private static final long MAX_REGULAR_SECTOR = 0xFFFFFFFAL;

    private static final int TYPE_STORAGE = 1;
    private static final int TYPE_STREAM = 2;
    private static final int TYPE_ROOT = 5;

    private final ByteWindows bytes;
    private final int sectorBytes;
    private final int miniSectorBytes;
    private final long miniCutoff;
    private final long firstDirectorySector;
    private final long firstMiniTableSector;
    private final long firstExtraTablePointerSector;
    private final Map<String, Entry> entries = new LinkedHashMap<>();
    private boolean complete = true;
    private long miniContainerStart = END_OF_CHAIN;
    private long miniContainerBytes;

    /**
     * One member of a compound file.
     *
     * @param path the member's path, storages separated by {@code /}
     * @param startSector where the member's bytes begin
     * @param sizeBytes how long the member is
     * @param mini whether it lives in the nested filing system
     */
    public record Entry(String path, long startSector, long sizeBytes, boolean mini) {
    }

    private Ole2Members(ByteWindows bytes) {
        this.bytes = bytes;
        this.sectorBytes = 1 << readShort(30);
        this.miniSectorBytes = 1 << readShort(32);
        this.miniCutoff = readInt(56);
        this.firstDirectorySector = readInt(48);
        this.firstMiniTableSector = readInt(60);
        this.firstExtraTablePointerSector = readInt(68);
    }

    /**
     * Reads a compound file's directory.
     *
     * @param bytes the content's windows
     * @return the reader, or null when the content is not a compound file
     *         or its header is not resident
     */
    public static Ole2Members read(ByteWindows bytes) {
        if (!bytes.observable(0, HEADER_BYTES) || !bytes.matchesAt(0, MAGIC)) {
            return null;
        }
        Ole2Members members = new Ole2Members(bytes);
        if (members.sectorBytes < 128 || members.sectorBytes > 1 << 20
                || members.miniSectorBytes < 16 || members.miniSectorBytes > members.sectorBytes) {
            return null;
        }
        members.walkDirectory();
        return members;
    }

    /** The members found, in the order the directory lists them. */
    public List<Entry> entries() {
        return List.copyOf(entries.values());
    }

    /** The member at a path, or null when the directory does not list one. */
    public Entry entry(String path) {
        return entries.get(path);
    }

    /**
     * Whether the whole directory was walked. A false answer means some of
     * it lay outside the windows, so an absent member may only be unseen.
     */
    public boolean complete() {
        return complete;
    }

    /**
     * Reads a member's content.
     *
     * @param entry the member to read
     * @return the bytes, or null when they are outside the windows or
     *         longer than {@link #MAX_MEMBER_BYTES}
     */
    public byte[] content(Entry entry) {
        if (entry == null || entry.sizeBytes() < 0 || entry.sizeBytes() > MAX_MEMBER_BYTES) {
            return null;
        }
        if (!entry.mini()) {
            return readRegular(entry.startSector(), entry.sizeBytes());
        }
        if (miniContainerStart > MAX_REGULAR_SECTOR) {
            return null;
        }
        byte[] container = readRegular(miniContainerStart,
                Math.min(miniContainerBytes, MAX_MEMBER_BYTES));
        return container == null
                ? null : readMini(entry.startSector(), entry.sizeBytes(), container);
    }

    // ------------------------------------------------------------------
    // The directory
    // ------------------------------------------------------------------

    private void walkDirectory() {
        byte[] directory = readRegular(firstDirectorySector,
                Math.min((long) MAX_ENTRIES * ENTRY_BYTES, MAX_MEMBER_BYTES));
        if (directory == null || directory.length < ENTRY_BYTES) {
            complete = false;
            return;
        }
        ByteWindows table = ByteWindows.ofWhole(directory);
        int count = directory.length / ENTRY_BYTES;
        // The root entry owns the nested filing system's container.
        if (typeOf(table, 0) == TYPE_ROOT) {
            miniContainerStart = readInt(table, 116L);
            miniContainerBytes = readLong(table, 120L);
        }
        visit(table, count, readInt(table, 76L), "", new HashSet<>(), 0);
    }

    /** Walks the directory's tree, which is ordered but not by path. */
    private void visit(ByteWindows table, int count, long id, String prefix, Set<Long> seen,
                       int depth) {
        if (id == NO_STREAM || id >= count || depth > MAX_DEPTH || !seen.add(id)
                || entries.size() >= MAX_ENTRIES) {
            return;
        }
        long base = id * (long) ENTRY_BYTES;
        visit(table, count, readInt(table, base + 68), prefix, seen, depth + 1);
        String name = nameOf(table, base);
        int type = typeOf(table, (int) id);
        if (!name.isEmpty() && (type == TYPE_STREAM || type == TYPE_STORAGE)) {
            String path = prefix + name;
            long size = readLong(table, base + 120);
            entries.putIfAbsent(path, new Entry(path, readInt(table, base + 116), size,
                    type == TYPE_STREAM && size < miniCutoff));
            if (type == TYPE_STORAGE) {
                visit(table, count, readInt(table, base + 76), path + "/", seen,
                        depth + 1);
            }
        }
        visit(table, count, readInt(table, base + 72), prefix, seen, depth + 1);
    }

    /** A directory name is UTF-16, and the registries write it trimmed. */
    private static String nameOf(ByteWindows table, long base) {
        int nameBytes = (int) readUnsignedShort(table, base + 64);
        if (nameBytes < 2 || nameBytes > 64) {
            return "";
        }
        byte[] raw = table.slice(base, nameBytes - 2);
        if (raw == null) {
            return "";
        }
        return new String(raw, StandardCharsets.UTF_16LE).trim();
    }

    private static int typeOf(ByteWindows table, int index) {
        int value = table.byteAt((long) index * ENTRY_BYTES + 66);
        return value == ByteWindows.NOT_OBSERVED ? 0 : value;
    }

    // ------------------------------------------------------------------
    // Sectors
    // ------------------------------------------------------------------

    /** Follows a chain of full-size sectors through the content. */
    private byte[] readRegular(long start, long wanted) {
        if (wanted <= 0 || wanted > MAX_MEMBER_BYTES) {
            return wanted == 0 ? new byte[0] : null;
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream((int) Math.min(wanted, 1 << 16));
        Set<Long> seen = new HashSet<>();
        long sector = start;
        while (out.size() < wanted) {
            if (sector > MAX_REGULAR_SECTOR || !seen.add(sector)) {
                break;
            }
            long at = sectorOffset(sector);
            if (at < 0) {
                return null;
            }
            byte[] chunk = bytes.slice(at, (int) Math.min(sectorBytes, wanted - out.size()));
            if (chunk == null) {
                return null;
            }
            out.write(chunk, 0, chunk.length);
            sector = nextSector(sector);
        }
        return out.toByteArray();
    }

    /** Follows a chain through the nested filing system's container. */
    private byte[] readMini(long start, long wanted, byte[] container) {
        if (wanted <= 0 || wanted > MAX_MEMBER_BYTES) {
            return wanted == 0 ? new byte[0] : null;
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream((int) Math.min(wanted, 1 << 16));
        Set<Long> seen = new HashSet<>();
        long sector = start;
        while (out.size() < wanted) {
            if (sector > MAX_REGULAR_SECTOR || !seen.add(sector)) {
                break;
            }
            long at = sector * miniSectorBytes;
            int take = (int) Math.min(miniSectorBytes, wanted - out.size());
            if (at < 0 || at + take > container.length) {
                return null;
            }
            out.write(container, (int) at, take);
            sector = nextMiniSector(sector);
        }
        return out.toByteArray();
    }

    /** Where a sector's bytes begin; the header occupies the space before. */
    private long sectorOffset(long sector) {
        long at = (sector + 1) * (long) sectorBytes;
        if (!bytes.sizeKnown()) {
            return bytes.observable(at, sectorBytes) ? at : -1;
        }
        return at >= 0 && at + sectorBytes <= bytes.sizeBytes() ? at : -1;
    }

    /** The sector table entry saying what follows a sector. */
    private long nextSector(long sector) {
        int perSector = sectorBytes / 4;
        long tableIndex = sector / perSector;
        long tableSector = tablePointer(tableIndex);
        if (tableSector > MAX_REGULAR_SECTOR) {
            return END_OF_CHAIN;
        }
        long at = sectorOffset(tableSector);
        if (at < 0) {
            complete = false;
            return END_OF_CHAIN;
        }
        return readInt(at + (sector % perSector) * 4);
    }

    /** The nested filing system's table entry for a mini sector. */
    private long nextMiniSector(long sector) {
        int perSector = sectorBytes / 4;
        long index = sector / perSector;
        long tableSector = firstMiniTableSector;
        for (long step = 0; step < index; step++) {
            if (tableSector > MAX_REGULAR_SECTOR) {
                return END_OF_CHAIN;
            }
            tableSector = nextSector(tableSector);
        }
        if (tableSector > MAX_REGULAR_SECTOR) {
            return END_OF_CHAIN;
        }
        long at = sectorOffset(tableSector);
        if (at < 0) {
            complete = false;
            return END_OF_CHAIN;
        }
        return readInt(at + (sector % perSector) * 4);
    }

    /** Which sector holds a stretch of the sector table. */
    private long tablePointer(long index) {
        if (index < HEADER_TABLE_POINTERS) {
            return readInt(76 + index * 4);
        }
        // Past what the header holds, the pointers continue in their own
        // chain of sectors, each ending with the next one's number.
        int perSector = sectorBytes / 4 - 1;
        long remaining = index - HEADER_TABLE_POINTERS;
        long pointerSector = firstExtraTablePointerSector;
        while (remaining >= perSector) {
            long at = sectorOffset(pointerSector);
            if (at < 0) {
                complete = false;
                return END_OF_CHAIN;
            }
            pointerSector = readInt(at + (long) perSector * 4);
            remaining -= perSector;
        }
        long at = sectorOffset(pointerSector);
        if (at < 0) {
            complete = false;
            return END_OF_CHAIN;
        }
        return readInt(at + remaining * 4);
    }

    // ------------------------------------------------------------------
    // Little-endian reads
    // ------------------------------------------------------------------

    private int readShort(long offset) {
        return (int) readUnsignedShort(bytes, offset);
    }

    private long readInt(long offset) {
        return readInt(bytes, offset);
    }

    private static long readUnsignedShort(ByteWindows source, long offset) {
        int low = source.byteAt(offset);
        int high = source.byteAt(offset + 1);
        if (low == ByteWindows.NOT_OBSERVED || high == ByteWindows.NOT_OBSERVED) {
            return 0;
        }
        return low | ((long) high << 8);
    }

    private static long readInt(ByteWindows source, long offset) {
        long value = 0;
        for (int i = 3; i >= 0; i--) {
            int part = source.byteAt(offset + i);
            if (part == ByteWindows.NOT_OBSERVED) {
                return NO_STREAM;
            }
            value = (value << 8) | part;
        }
        return value;
    }

    /** A 64-bit field; a size beyond what a reader will hold clamps. */
    private static long readLong(ByteWindows source, long offset) {
        if (!source.observable(offset, 8)) {
            return 0;
        }
        long low = readInt(source, offset);
        long high = readInt(source, offset + 4);
        if (high != 0) {
            return Long.MAX_VALUE;
        }
        return low & 0xFFFFFFFFL;
    }

    /** The paths of every member found, for reporting. */
    public List<String> paths() {
        return new ArrayList<>(entries.keySet());
    }
}
