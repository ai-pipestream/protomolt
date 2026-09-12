package ai.protomolt.proto.asset.bridge;

import ai.protomolt.proto.asset.v1.BridgeKind;
import ai.protomolt.proto.asset.v1.ContainerMember;
import ai.protomolt.proto.asset.v1.ContainerMembers;
import com.google.protobuf.util.Timestamps;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * The container bridge: a tar or zip archive in, its member listing out.
 * Purely structural and pure JDK — no member is extracted, nothing is
 * written back into the container, and the original's bytes are read exactly
 * once.
 *
 * <p>Each member is hashed as it streams past, so the listing carries fixity
 * for the container's contents the way the archive's manifests carry it for
 * renditions. Hashing is bounded: an archive with more members than
 * {@link #maxMembers} or more decompressed bytes than {@link #maxBytes}
 * stops at the bound and says so in the listing, because a bounded listing
 * that claims to be complete is worse than no listing at all.
 *
 * <p>Which container it is comes from the bytes, not the name: gzip magic
 * wraps the stream in a decompressor, a zip signature reads as a zip, and
 * anything else reads as a tar. A stream that is neither fails the bridge by
 * name rather than producing an empty listing.
 */
public final class ContainerMembersBridge implements Bridge {

    /** Members listed before the listing declares itself truncated. */
    public static final int DEFAULT_MAX_MEMBERS = 50_000;

    /** Decompressed bytes read before hashing stops. */
    public static final long DEFAULT_MAX_BYTES = 2L * 1024 * 1024 * 1024;

    private static final byte[] GZIP_MAGIC = {(byte) 0x1F, (byte) 0x8B};
    private static final byte[] ZIP_MAGIC = {'P', 'K', 0x03, 0x04};

    private final int maxMembers;
    private final long maxBytes;

    /** A bridge with the default bounds. */
    public ContainerMembersBridge() {
        this(DEFAULT_MAX_MEMBERS, DEFAULT_MAX_BYTES);
    }

    /**
     * A bridge with explicit bounds.
     *
     * @param maxMembers members listed before truncating
     * @param maxBytes decompressed bytes hashed before hashing stops
     */
    public ContainerMembersBridge(int maxMembers, long maxBytes) {
        this.maxMembers = maxMembers;
        this.maxBytes = maxBytes;
    }

    @Override
    public BridgeKind kind() {
        return BridgeKind.BRIDGE_KIND_CONTAINER_MEMBERS;
    }

    @Override
    public Derivation derive(InputStream original, String filename) throws IOException {
        PushbackInputStream head = new PushbackInputStream(
                new BufferedInputStream(original), ZIP_MAGIC.length);
        if (startsWith(head, GZIP_MAGIC)) {
            // A compressed container: gunzip, then read what is inside. In
            // the v1 routing that is always a tar.
            return tar(new GZIPInputStream(head));
        }
        return startsWith(head, ZIP_MAGIC) ? zip(head) : tar(head);
    }

    /** Peeks the stream's first bytes without consuming them. */
    private static boolean startsWith(PushbackInputStream in, byte[] magic) throws IOException {
        byte[] head = new byte[magic.length];
        int read = in.readNBytes(head, 0, head.length);
        if (read > 0) {
            in.unread(head, 0, read);
        }
        return read == magic.length && Arrays.equals(head, magic);
    }

    private Derivation tar(InputStream body) throws IOException {
        Listing listing = new Listing();
        TarReader reader = new TarReader(body);
        TarReader.Header header;
        while ((header = reader.next()) != null) {
            if (listing.full()) {
                return listing.truncated("member limit " + maxMembers + " reached");
            }
            ContainerMember.Builder member = ContainerMember.newBuilder()
                    .setPath(header.path())
                    .setSizeBytes(header.size())
                    .setDirectory(header.directory());
            if (header.modifiedEpochSecond() > 0) {
                member.setModifiedAt(Timestamps.fromSeconds(header.modifiedEpochSecond()));
            }
            if (!header.directory() && listing.mayHash(header.size())) {
                MessageDigest digest = sha256();
                long hashed = reader.readMember(digest::update, header.size());
                if (hashed == header.size()) {
                    member.setSha256(hex(digest));
                }
                listing.hashed(hashed);
            }
            listing.add(member.build());
        }
        return listing.complete();
    }

    private Derivation zip(InputStream body) throws IOException {
        Listing listing = new Listing();
        ZipInputStream reader = new ZipInputStream(body);
        ZipEntry entry;
        while ((entry = reader.getNextEntry()) != null) {
            if (listing.full()) {
                return listing.truncated("member limit " + maxMembers + " reached");
            }
            ContainerMember.Builder member = ContainerMember.newBuilder()
                    .setPath(entry.getName())
                    .setDirectory(entry.isDirectory());
            if (entry.getLastModifiedTime() != null) {
                member.setModifiedAt(Timestamps.fromMillis(
                        entry.getLastModifiedTime().toMillis()));
            }
            if (entry.isDirectory()) {
                listing.add(member.build());
                continue;
            }
            // A streamed zip does not state its sizes up front, so the
            // member's size is what actually came out of the decompressor.
            MessageDigest digest = sha256();
            long size = 0;
            byte[] buffer = new byte[8192];
            boolean hashing = listing.mayHash(0);
            for (int n; (n = reader.read(buffer)) > 0; ) {
                size += n;
                if (hashing) {
                    digest.update(buffer, 0, n);
                    hashing = listing.hashed(n);
                }
            }
            member.setSizeBytes(size);
            if (hashing) {
                member.setSha256(hex(digest));
            }
            listing.add(member.build());
        }
        return listing.complete();
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required of every JVM", e);
        }
    }

    private static String hex(MessageDigest digest) {
        return HexFormat.of().formatHex(digest.digest());
    }

    /** The listing under construction, with its bounds. */
    private final class Listing {
        private final ContainerMembers.Builder members = ContainerMembers.newBuilder();
        private long totalBytes;
        private long hashedBytes;
        private boolean fixityStopped;

        boolean full() {
            return members.getMembersCount() >= maxMembers;
        }

        /**
         * Whether the budget still allows hashing a member of this size.
         * Once a member does not fit, fixity stops for the WHOLE listing:
         * hashing whichever later members happen to be small would make the
         * blank hashes mean two different things.
         */
        boolean mayHash(long size) {
            if (fixityStopped) {
                return false;
            }
            if (hashedBytes + size > maxBytes) {
                fixityStopped = true;
                return false;
            }
            return true;
        }

        /** Records hashed bytes; returns whether the budget still holds. */
        boolean hashed(long count) {
            hashedBytes += count;
            if (hashedBytes > maxBytes) {
                fixityStopped = true;
                return false;
            }
            return true;
        }

        void add(ContainerMember member) {
            members.addMembers(member);
            totalBytes += member.getSizeBytes();
        }

        Derivation complete() {
            return new Derivation(build(), null, warnings(List.of()));
        }

        Derivation truncated(String reason) {
            members.setTruncated(true).setTruncationReason(reason);
            return new Derivation(build(), null, warnings(List.of(
                    "container listing truncated: " + reason)));
        }

        /** A listing that stopped hashing says so; blank fixity is never silent. */
        private List<String> warnings(List<String> base) {
            if (!fixityStopped) {
                return base;
            }
            List<String> all = new ArrayList<>(base);
            all.add("member fixity omitted past the " + maxBytes + " byte hashing budget");
            return List.copyOf(all);
        }

        private ContainerMembers build() {
            return members
                    .setMemberCount(members.getMembersCount())
                    .setTotalBytes(totalBytes)
                    .build();
        }
    }
}
