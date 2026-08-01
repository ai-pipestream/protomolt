package ai.pipestream.proto.repo.container.codec;

import ai.pipestream.proto.repo.v1.DocumentManifest;
import ai.pipestream.proto.repo.v1.DocumentPart;
import ai.pipestream.proto.repo.v1.PartManifestEntry;
import ai.pipestream.proto.repo.v1.PartState;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;

/**
 * Pure split/assemble codec for the four-part addressable document model —
 * descriptor-driven, so ANY protobuf message type can be decomposed into
 * independently addressable parts on clean proto boundaries (see
 * {@link PartLayout}).
 *
 * <p>With the default {@link PartLayouts#document()} layout a stored
 * {@code Document} is split into:
 * <ul>
 *   <li><b>CORE</b> — everything except {@code blob_bag}, {@code parser_results}
 *       and {@code search_metadata.semantic_results};</li>
 *   <li><b>BLOBS</b> — {@code doc_id} + {@code blob_bag};</li>
 *   <li><b>CHUNKS</b> — one fragment per <i>chunk set</i>: a consecutive run of
 *       {@code semantic_results} entries sharing a {@code result_id}.
 *       Consecutive-run grouping (not global grouping) preserves the exact
 *       original ordering on reassembly — a byte-fidelity requirement;</li>
 *   <li><b>PARSED</b> — {@code doc_id} + {@code parser_results}.</li>
 * </ul>
 *
 * <p>Each fragment is itself a valid serialized message of the SAME type
 * carrying only that part's fields (plus the layout's identity field for
 * self-description). Assembly is a field-level {@code mergeFrom} of the
 * fragments in manifest order, which round-trips the original document
 * byte-for-byte (verified by the round-trip tests' byte comparison).
 *
 * <p>Everything here is a pure function of its inputs — no IO, no frameworks.
 */
public final class DocumentPartCodec {

    private DocumentPartCodec() {
    }

    /** Filename of the CORE part object under the document's prefix. */
    public static final String CORE_FILE = "core.pb";
    /** Filename of the BLOBS part object under the document's prefix. */
    public static final String BLOBS_FILE = "blobs.pb";
    /** Filename of the PARSED part object under the document's prefix. */
    public static final String PARSED_FILE = "parsed.pb";
    /** Directory of CHUNKS sub-objects under the document's prefix. */
    public static final String CHUNKS_DIR = "chunks";

    /**
     * Splits a document into its part fragments, in canonical manifest order:
     * CORE first, then the remaining parts in {@link DocumentPart} number
     * order (BLOBS, CHUNKS sub-objects in original repeated-field order,
     * PARSED). Parts with no content are simply absent from the returned
     * list — the caller records them as {@code PART_STATE_EMPTY}.
     *
     * @param doc the document to split
     * @param layout the part mapping over the document's message type
     * @return the non-empty part fragments in assembly order
     */
    public static List<PartObject> split(Message doc, PartLayout layout) {
        if (!doc.getDescriptorForType().equals(layout.messageType())) {
            throw new IllegalArgumentException("Layout is over " + layout.messageType().getFullName()
                    + " but document is " + doc.getDescriptorForType().getFullName());
        }
        List<PartObject> parts = new ArrayList<>();

        // CORE: strip the other parts' fields from a full copy. Guard the
        // nested-message descent — touching a chunked parent that is absent
        // would materialize an empty message and change bytes.
        Message.Builder core = doc.toBuilder();
        for (PartLayout.PartField pf : layout.partFields()) {
            core.clearField(pf.field());
        }
        for (PartLayout.ChunkedField cf : layout.chunkedFields()) {
            if (doc.hasField(cf.parent())) {
                // Keep the parent set (even if clearing leaves a default
                // instance) — the parent was set in the original, so CORE
                // keeps it set.
                Message parentStripped = ((Message) doc.getField(cf.parent()))
                        .toBuilder().clearField(cf.repeatedField()).build();
                core.setField(cf.parent(), parentStripped);
            }
        }
        parts.add(toPartObject(DocumentPart.DOCUMENT_PART_CORE, "", core.build()));

        // Remaining parts in DocumentPart number order (BLOBS < CHUNKS < PARSED).
        List<PartLayout.PartField> partFields = new ArrayList<>(layout.partFields());
        partFields.sort(Comparator.comparingInt(pf -> pf.part().getNumber()));
        List<PartLayout.ChunkedField> chunkedFields = new ArrayList<>(layout.chunkedFields());
        chunkedFields.sort(Comparator.comparingInt(cf -> cf.part().getNumber()));

        for (DocumentPart part : orderedNonCoreParts(partFields, chunkedFields)) {
            for (PartLayout.PartField pf : partFields) {
                if (pf.part() == part && isPresent(doc, pf.field())) {
                    parts.add(toPartObject(part, "", singleFieldFragment(doc, layout, pf.field())));
                }
            }
            for (PartLayout.ChunkedField cf : chunkedFields) {
                if (cf.part() == part && doc.hasField(cf.parent())
                        && !elements(doc, cf).isEmpty()) {
                    parts.addAll(splitChunkSets(doc, layout, cf));
                }
            }
        }

        return parts;
    }

    private static List<DocumentPart> orderedNonCoreParts(List<PartLayout.PartField> partFields,
                                                          List<PartLayout.ChunkedField> chunkedFields) {
        List<DocumentPart> order = new ArrayList<>();
        for (PartLayout.PartField pf : partFields) {
            if (!order.contains(pf.part())) {
                order.add(pf.part());
            }
        }
        for (PartLayout.ChunkedField cf : chunkedFields) {
            if (!order.contains(cf.part())) {
                order.add(cf.part());
            }
        }
        order.sort(Comparator.comparingInt(DocumentPart::getNumber));
        return order;
    }

    /** Presence: non-empty for repeated/map fields, {@code hasField} otherwise. */
    private static boolean isPresent(Message doc, Descriptors.FieldDescriptor fd) {
        if (fd.isRepeated()) {
            return !((List<?>) doc.getField(fd)).isEmpty();
        }
        return doc.hasField(fd);
    }

    /** A fragment carrying only the identity field plus one part field. */
    private static Message singleFieldFragment(Message doc, PartLayout layout,
                                               Descriptors.FieldDescriptor partFd) {
        Message.Builder frag = doc.newBuilderForType();
        if (layout.identityField() != null) {
            frag.setField(layout.identityField(), doc.getField(layout.identityField()));
        }
        frag.setField(partFd, doc.getField(partFd));
        return frag.build();
    }

    @SuppressWarnings("unchecked")
    private static List<Message> elements(Message doc, PartLayout.ChunkedField cf) {
        return (List<Message>) ((Message) doc.getField(cf.parent())).getField(cf.repeatedField());
    }

    /**
     * One CHUNKS fragment per consecutive run of elements sharing the key
     * field's value. Blank keys group as singleton runs named {@code set-<index>}.
     * A repeated run of an already-seen key gets a {@code #n} disambiguator so
     * object keys stay unique while the manifest order still reproduces the
     * original element order exactly.
     */
    private static List<PartObject> splitChunkSets(Message doc, PartLayout layout,
                                                   PartLayout.ChunkedField cf) {
        List<PartObject> out = new ArrayList<>();
        List<Message> elements = elements(doc, cf);

        List<String> seen = new ArrayList<>();
        int i = 0;
        while (i < elements.size()) {
            String runKey = rawKeyOf(elements.get(i), i, cf.keyField());
            int j = i;
            while (j < elements.size() && rawKeyOf(elements.get(j), j, cf.keyField()).equals(runKey)) {
                j++;
            }
            String subKey = runKey;
            int dup = 2;
            while (seen.contains(subKey)) {
                subKey = runKey + "#" + dup++;
            }
            seen.add(subKey);

            Message.Builder frag = doc.newBuilderForType();
            if (layout.identityField() != null) {
                frag.setField(layout.identityField(), doc.getField(layout.identityField()));
            }
            Message.Builder parentFrag = frag.newBuilderForField(cf.parent());
            parentFrag.setField(cf.repeatedField(), new ArrayList<>(elements.subList(i, j)));
            frag.setField(cf.parent(), parentFrag.build());
            out.add(toPartObject(cf.part(), subKey, frag.build()));
            i = j;
        }
        return out;
    }

    private static String rawKeyOf(Message element, int index, Descriptors.FieldDescriptor keyFd) {
        String raw = element.getField(keyFd).toString();
        return raw.isBlank() ? "set-" + index : raw;
    }

    private static PartObject toPartObject(DocumentPart part, String subKey, Message fragment) {
        byte[] bytes = fragment.toByteArray();
        return new PartObject(part, subKey, bytes, sha256Hex(bytes));
    }

    /**
     * Reassembles a document from part fragment bytes, merged in the order
     * given (which must be manifest order — CHUNKS sub-object ordering carries
     * the original repeated-field sequence).
     *
     * @param fragmentBytes the fragments' serialized bytes, in manifest order
     * @param prototype a prototype of the document's message type (only its
     *                  type is used; the returned message is built fresh)
     * @return the assembled document
     * @throws InvalidProtocolBufferException when a fragment fails to parse
     */
    @SuppressWarnings("unchecked")
    public static <T extends Message> T assemble(List<byte[]> fragmentBytes, T prototype)
            throws InvalidProtocolBufferException {
        Message.Builder builder = prototype.newBuilderForType();
        for (byte[] bytes : fragmentBytes) {
            builder.mergeFrom(bytes);
        }
        return (T) builder.build();
    }

    /**
     * Builds the storage object key of one part object.
     *
     * @param basePrefix the address prefix ({@code {keyPrefix}/{accountId}/…/{nodeId}})
     * @param part the part
     * @param subKey the chunk-set sub key (CHUNKS only; ignored otherwise)
     * @return the full object key
     */
    public static String objectKey(String basePrefix, DocumentPart part, String subKey) {
        return switch (part) {
            case DOCUMENT_PART_CORE -> basePrefix + "/" + CORE_FILE;
            case DOCUMENT_PART_BLOBS -> basePrefix + "/" + BLOBS_FILE;
            case DOCUMENT_PART_PARSED -> basePrefix + "/" + PARSED_FILE;
            case DOCUMENT_PART_CHUNKS -> basePrefix + "/" + CHUNKS_DIR + "/" + chunkFileName(subKey) + ".pb";
            default -> throw new IllegalArgumentException("No object key for part " + part);
        };
    }

    /**
     * Deterministic, S3-safe, collision-free filename for a chunk-set sub
     * object: the sanitized sub key plus a short hash of the raw sub key (two
     * distinct raw keys that sanitize identically stay distinct).
     */
    static String chunkFileName(String subKey) {
        String safe = subKey.trim().replaceAll("[^A-Za-z0-9._-]", "_");
        if (safe.length() > 96) {
            safe = safe.substring(0, 96);
        }
        return (safe.isEmpty() ? "set" : safe) + "-" + sha256Hex(subKey.getBytes(StandardCharsets.UTF_8)).substring(0, 8);
    }

    /**
     * The whole-document root checksum: SHA-256 over the ordered part
     * identities and hashes (a Merkle-style root). Identical documents split
     * into identical parts and therefore carry identical roots, so the intake
     * dedupe comparison keeps working.
     *
     * @param parts the split parts in manifest order
     * @return lowercase hex SHA-256 root
     */
    public static String rootChecksum(List<PartObject> parts) {
        StringBuilder sb = new StringBuilder();
        for (PartObject p : parts) {
            sb.append(p.part().getNumber()).append('|').append(p.subKey()).append('|').append(p.sha256()).append('\n');
        }
        return sha256Hex(sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    /**
     * The root checksum computed from a manifest's PRESENT entries in manifest
     * order — identical to {@link #rootChecksum(List)} over the same parts, so
     * a partial save can derive the new root from copied parts' stored hashes
     * without ever reading their bytes.
     *
     * @param manifest the manifest whose PRESENT entries to hash
     * @return lowercase hex SHA-256 root
     */
    public static String rootChecksumFromManifest(DocumentManifest manifest) {
        StringBuilder sb = new StringBuilder();
        for (PartManifestEntry e : manifest.getPartsList()) {
            if (e.getState() == PartState.PART_STATE_PRESENT) {
                sb.append(e.getPart().getNumber()).append('|').append(e.getSubKey()).append('|').append(e.getSha256()).append('\n');
            }
        }
        return sha256Hex(sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Serializes a manifest to JSON for storage alongside the document row.
     *
     * @param manifest the manifest to serialize
     * @return the JSON document
     */
    public static String manifestToJson(DocumentManifest manifest) {
        try {
            return JsonFormat.printer().omittingInsignificantWhitespace().print(manifest);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException("Manifest JSON serialization failed", e);
        }
    }

    /**
     * Parses a manifest from its stored JSON value.
     *
     * @param json the stored JSON document
     * @return the parsed manifest
     */
    public static DocumentManifest manifestFromJson(String json) {
        try {
            DocumentManifest.Builder b = DocumentManifest.newBuilder();
            JsonFormat.parser().ignoringUnknownFields().merge(json, b);
            return b.build();
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException("Manifest JSON parse failed", e);
        }
    }

    /**
     * Lowercase hex SHA-256.
     *
     * @param data the bytes to hash
     * @return the digest as lowercase hex
     */
    public static String sha256Hex(byte[] data) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(data));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
