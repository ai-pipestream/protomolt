package ai.protomolt.proto.repo.codec;

import ai.protomolt.proto.repo.v1.DocumentManifest;
import ai.protomolt.proto.repo.v1.DocumentPart;
import ai.protomolt.proto.repo.v1.PartManifestEntry;
import ai.protomolt.proto.repo.v1.PartState;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
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
 *   <li><b>CORE</b> — everything except {@code blob_bag}, {@code parser_results},
 *       {@code search_metadata.semantic_results}, {@code search_metadata.segmentations}
 *       and {@code search_metadata.embedding_sets};</li>
 *   <li><b>BLOBS</b> — {@code doc_id} + {@code blob_bag};</li>
 *   <li><b>CHUNKS</b> — one fragment per <i>chunk set</i>: a consecutive run of
 *       {@code semantic_results} entries sharing a {@code result_id}.
 *       Consecutive-run grouping (not global grouping) preserves the exact
 *       original ordering on reassembly — a byte-fidelity requirement; and one
 *       fragment per {@code segmentations} entry, keyed by its
 *       {@code segmentation_id};</li>
 *   <li><b>PARSED</b> — {@code doc_id} + {@code parser_results};</li>
 *   <li><b>EMBEDDINGS</b> — one fragment per {@code embedding_sets} entry, keyed
 *       by its {@code segmentation_id} and {@code embedding_set_id}.</li>
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
    /** Directory of EMBEDDINGS sub-objects under the document's prefix. */
    public static final String EMBEDDINGS_DIR = "embeddings";

    /**
     * Splits a document into its part fragments, in canonical manifest order:
     * CORE first, then the remaining parts in {@link DocumentPart} number
     * order (BLOBS, CHUNKS sub-objects in original repeated-field order,
     * PARSED, EMBEDDINGS in original repeated-field order). Parts with no
     * content are simply absent from the returned list — the caller records
     * them as {@code PART_STATE_EMPTY}.
     *
     * @param doc the document to split
     * @param layout the part mapping over the document's message type
     * @return the non-empty part fragments in assembly order
     * @throws IllegalArgumentException when two elements of a keyed part share a key
     */
    public static List<PartObject> split(Message doc, PartLayout layout) {
        return split(doc, layout, false);
    }

    /**
     * Splits a document into its part fragments in canonical manifest order and
     * encodes each fragment deterministically in a single pass (map entries sorted
     * by key, serialized with {@code CodedOutputStream.useDeterministicSerialization()}).
     *
     * @param doc the document to split
     * @param layout the part mapping over the document's message type
     * @return the deterministically encoded non-empty part fragments in assembly order
     * @throws IllegalArgumentException when two elements of a keyed part share a key
     */
    public static List<PartObject> splitDeterministic(Message doc, PartLayout layout) {
        return split(doc, layout, true);
    }

    private static List<PartObject> split(Message doc, PartLayout layout, boolean deterministic) {
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
            stripNested(doc, core, cf.parent(), cf.repeatedField());
        }
        for (PartLayout.KeyedField kf : layout.keyedFields()) {
            stripNested(doc, core, kf.parent(), kf.repeatedField());
        }
        parts.add(toPartObject(DocumentPart.DOCUMENT_PART_CORE, "", core.build(), deterministic));

        // Remaining parts in DocumentPart number order (BLOBS < CHUNKS < PARSED < EMBEDDINGS),
        // precomputed on the immutable PartLayout.
        List<PartLayout.PartField> partFields = layout.sortedPartFields();
        List<PartLayout.ChunkedField> chunkedFields = layout.sortedChunkedFields();
        List<PartLayout.KeyedField> keyedFields = layout.sortedKeyedFields();

        for (DocumentPart part : layout.orderedNonCoreParts()) {
            for (PartLayout.PartField pf : partFields) {
                if (pf.part() == part && isPresent(doc, pf.field())) {
                    parts.add(toPartObject(part, "", singleFieldFragment(doc, layout, pf.field()), deterministic));
                }
            }
            for (PartLayout.ChunkedField cf : chunkedFields) {
                if (cf.part() == part && doc.hasField(cf.parent())
                        && !elements(doc, cf.parent(), cf.repeatedField()).isEmpty()) {
                    parts.addAll(splitChunkSets(doc, layout, cf, deterministic));
                }
            }
            for (PartLayout.KeyedField kf : keyedFields) {
                if (kf.part() == part && doc.hasField(kf.parent())
                        && !elements(doc, kf.parent(), kf.repeatedField()).isEmpty()) {
                    parts.addAll(splitKeyed(doc, layout, kf, deterministic));
                }
            }
        }

        return parts;
    }

    /**
     * Removes a nested repeated field from CORE while keeping its parent set:
     * touching an absent parent would materialize an empty message and change
     * bytes, and a present parent was set in the original, so CORE keeps it set.
     */
    private static void stripNested(Message doc, Message.Builder core, Descriptors.FieldDescriptor parent,
                                    Descriptors.FieldDescriptor repeatedField) {
        if (!doc.hasField(parent)) return;
        Message current = (Message) core.getField(parent);
        core.setField(parent, current.toBuilder().clearField(repeatedField).build());
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
    private static List<Message> elements(Message doc, Descriptors.FieldDescriptor parent,
                                          Descriptors.FieldDescriptor repeatedField) {
        return (List<Message>) ((Message) doc.getField(parent)).getField(repeatedField);
    }

    /**
     * One fragment per element of a keyed part, in original element order,
     * carrying the identity field and the parent with that single element.
     * Two elements with the same key are refused: a key names one object.
     */
    private static List<PartObject> splitKeyed(Message doc, PartLayout layout, PartLayout.KeyedField kf,
                                               boolean deterministic) {
        List<PartObject> out = new ArrayList<>();
        var seen = new java.util.HashSet<List<String>>();
        for (Message element : elements(doc, kf.parent(), kf.repeatedField())) {
            List<String> key = kf.keyOf(element);
            if (!seen.add(key)) {
                throw new IllegalArgumentException("Duplicate " + kf.repeatedField().getFullName()
                        + " key " + key);
            }
            Message.Builder frag = doc.newBuilderForType();
            if (layout.identityField() != null) {
                frag.setField(layout.identityField(), doc.getField(layout.identityField()));
            }
            Message.Builder parentFrag = frag.newBuilderForField(kf.parent());
            parentFrag.setField(kf.repeatedField(), List.of(element));
            frag.setField(kf.parent(), parentFrag.build());
            Message built = frag.build();
            byte[] bytes = deterministic ? encodeDeterministic(built) : built.toByteArray();
            out.add(PartObject.keyed(kf.part(), key, bytes, sha256Hex(bytes)));
        }
        return out;
    }

    /**
     * One CHUNKS fragment per consecutive run of elements sharing the key
     * field's effective value. Blank keys become {@code set-<global-index>}; an
     * adjacent explicit key with that same value belongs to the same run.
     * A repeated run of an already-seen key gets a {@code #n} disambiguator so
     * object keys stay unique while the manifest order still reproduces the
     * original element order exactly.
     */
    private static List<PartObject> splitChunkSets(Message doc, PartLayout layout,
                                                   PartLayout.ChunkedField cf, boolean deterministic) {
        List<PartObject> out = new ArrayList<>();
        List<Message> elements = elements(doc, cf.parent(), cf.repeatedField());

        var names = new ChunkRunNames();
        int i = 0;
        while (i < elements.size()) {
            String runKey = rawKeyOf(elements.get(i), i, cf.keyField());
            int j = i;
            while (j < elements.size() && rawKeyOf(elements.get(j), j, cf.keyField()).equals(runKey)) {
                j++;
            }
            String subKey = names.claim(runKey);

            Message.Builder frag = doc.newBuilderForType();
            if (layout.identityField() != null) {
                frag.setField(layout.identityField(), doc.getField(layout.identityField()));
            }
            Message.Builder parentFrag = frag.newBuilderForField(cf.parent());
            parentFrag.setField(cf.repeatedField(), new ArrayList<>(elements.subList(i, j)));
            frag.setField(cf.parent(), parentFrag.build());
            out.add(toPartObject(cf.part(), subKey, frag.build(), deterministic));
            i = j;
        }
        return out;
    }

    private static String rawKeyOf(Message element, int index, Descriptors.FieldDescriptor keyFd) {
        String raw = element.getField(keyFd).toString();
        return ChunkRunNames.key(raw, index);
    }

    private static PartObject toPartObject(DocumentPart part, String subKey, Message fragment, boolean deterministic) {
        byte[] bytes = deterministic ? encodeDeterministic(fragment) : fragment.toByteArray();
        return new PartObject(part, subKey, bytes, sha256Hex(bytes));
    }

    /**
     * Re-encodes a split fragment with deterministic serialization: map entries
     * sorted by key, so equal content gives equal bytes and one SHA-256 whatever
     * order a client sent its map entries in. Field presence, unknown fields and
     * exact scalar values are unchanged.
     *
     * @param part the fragment to re-encode
     * @param prototype a prototype of the fragment's message type
     * @return the fragment with deterministic bytes and their SHA-256
     * @throws InvalidProtocolBufferException when the fragment fails to parse
     */
    public static PartObject deterministic(PartObject part, Message prototype) throws InvalidProtocolBufferException {
        byte[] bytes = encodeDeterministic(prototype.newBuilderForType().mergeFrom(part.bytes()).build());
        return new PartObject(part.part(), part.subKey(), part.keyValues(), bytes, sha256Hex(bytes));
    }

    private static byte[] encodeDeterministic(Message message) {
        // Generated maps sort under deterministic serialization; a DynamicMessage keeps its
        // entries in wire order, so maps are put in the same key order first.
        Message parsed = sortedMaps(message, 0);
        byte[] bytes = new byte[parsed.getSerializedSize()];
        var coded = com.google.protobuf.CodedOutputStream.newInstance(bytes);
        coded.useDeterministicSerialization();
        try {
            parsed.writeTo(coded);
            coded.checkNoSpaceLeft();
        } catch (java.io.IOException impossible) {
            throw new IllegalStateException("in-memory encoding failed", impossible);
        }
        return bytes;
    }

    /**
     * The message with every map's entries in ascending key order, nested maps
     * included, keeping the last entry for a repeated key as a parse does. The key
     * order is the natural order of protobuf-java's boxed key type, the order its
     * generated messages serialize in deterministically.
     */
    static Message sortedMaps(Message message, int depth) {
        if (depth > 100) throw new IllegalArgumentException("message nesting exceeds 100 levels");
        Message.Builder changed = null;
        for (var entry : message.getAllFields().entrySet()) {
            var field = entry.getKey();
            if (field.getJavaType() != Descriptors.FieldDescriptor.JavaType.MESSAGE) continue;
            Object value = entry.getValue();
            Object canonical;
            if (field.isMapField()) {
                var byKey = new java.util.LinkedHashMap<Object, Message>();
                var keyField = field.getMessageType().findFieldByNumber(1);
                for (Object element : (List<?>) value) {
                    var mapEntry = sortedMaps((Message) element, depth + 1);
                    byKey.remove(mapEntry.getField(keyField));
                    byKey.put(mapEntry.getField(keyField), mapEntry);
                }
                var sorted = new ArrayList<>(byKey.entrySet());
                sorted.sort((left, right) -> compareKeys(left.getKey(), right.getKey()));
                canonical = sorted.stream().map(java.util.Map.Entry::getValue).toList();
            } else if (field.isRepeated()) {
                var elements = new ArrayList<Message>();
                for (Object element : (List<?>) value) elements.add(sortedMaps((Message) element, depth + 1));
                canonical = elements;
            } else {
                canonical = sortedMaps((Message) value, depth + 1);
            }
            // Identity, not equals: message equality ignores map entry order.
            if (!same(canonical, value)) {
                if (changed == null) changed = message.toBuilder();
                changed.setField(field, canonical);
            }
        }
        return changed == null ? message : changed.build();
    }

    private static boolean same(Object canonical, Object value) {
        if (canonical == value) return true;
        if (!(canonical instanceof List<?> left) || !(value instanceof List<?> right) || left.size() != right.size()) return false;
        for (int i = 0; i < left.size(); i++) if (left.get(i) != right.get(i)) return false;
        return true;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static int compareKeys(Object left, Object right) {
        return ((Comparable) left).compareTo(right);
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
     * @param subKey the chunk-set or keyed sub key (CHUNKS and EMBEDDINGS only; ignored otherwise)
     * @return the full object key
     */
    public static String objectKey(String basePrefix, DocumentPart part, String subKey) {
        return switch (part) {
            case DOCUMENT_PART_CORE -> basePrefix + "/" + CORE_FILE;
            case DOCUMENT_PART_BLOBS -> basePrefix + "/" + BLOBS_FILE;
            case DOCUMENT_PART_PARSED -> basePrefix + "/" + PARSED_FILE;
            case DOCUMENT_PART_CHUNKS -> basePrefix + "/" + CHUNKS_DIR + "/" + chunkFileName(subKey) + ".pb";
            case DOCUMENT_PART_EMBEDDINGS -> basePrefix + "/" + EMBEDDINGS_DIR + "/" + chunkFileName(subKey) + ".pb";
            default -> throw new IllegalArgumentException("No object key for part " + part);
        };
    }

    /**
     * Deterministic, S3-safe, collision-free filename for a chunk-set sub
     * object: the sanitized sub key plus a short hash of the raw sub key (two
     * distinct raw keys that sanitize identically stay distinct).
     */
    static String chunkFileName(String subKey) {
        String trimmed = subKey.trim();
        int len = Math.min(trimmed.length(), 96);
        StringBuilder safe = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            char c = trimmed.charAt(i);
            safe.append(isSafeChunkChar(c) ? c : '_');
        }
        return (safe.isEmpty() ? "set" : safe.toString()) + "-"
                + sha256Hex(subKey.getBytes(StandardCharsets.UTF_8)).substring(0, 8);
    }

    private static boolean isSafeChunkChar(char c) {
        return (c >= 'A' && c <= 'Z')
                || (c >= 'a' && c <= 'z')
                || (c >= '0' && c <= '9')
                || c == '.'
                || c == '_'
                || c == '-';
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

    /**
     * Lowercase hex SHA-256 over a {@link com.google.protobuf.ByteString} without copying its backing buffers.
     *
     * @param data the bytes to hash
     * @return the digest as lowercase hex
     */
    public static String sha256Hex(com.google.protobuf.ByteString data) {
        try {
            var digest = MessageDigest.getInstance("SHA-256");
            for (var buffer : data.asReadOnlyByteBufferList()) {
                digest.update(buffer);
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Tests whether {@code value} is a 64-character lowercase hexadecimal SHA-256 digest.
     *
     * @param value the string to test; may be {@code null}
     * @return {@code true} if {@code value} consists of exactly 64 characters in {@code [0-9a-f]}
     */
    public static boolean isSha256Hex(String value) {
        if (value == null || value.length() != 64) {
            return false;
        }
        for (int i = 0; i < 64; i++) {
            char c = value.charAt(i);
            if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
                return false;
            }
        }
        return true;
    }
}
