package ai.protomolt.proto.repo.catalog;

import ai.protomolt.proto.projection.MessageProjection;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v2.CatalogEntry;
import ai.protomolt.proto.repo.v2.DocumentLookup;
import ai.protomolt.proto.repo.v2.StoredManifest;
import com.google.protobuf.Any;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Builds the catalog entry of a document's current revision (requirements §2):
 * identity and ownership from the stored manifest, the lookup fields from the
 * CORE part through the {@link DocumentLookup} projection. Only committed
 * metadata is projected; no content is fetched here and no JSON is involved.
 */
public final class CatalogEntries {
    private static final MessageProjection ENTRY_PROJECTION = projection(CatalogEntry.getDescriptor());
    private static final MessageProjection LOOKUP_PROJECTION = projection(DocumentLookup.getDescriptor());

    private CatalogEntries() {}

    private static MessageProjection projection(Descriptor target) {
        return MessageProjection.forTarget(target, name -> name.equals(Document.getDescriptor().getFullName())
                ? Optional.of(Document.getDescriptor()) : Optional.empty()).orElseThrow();
    }

    /**
     * The lookup fields of a CORE fragment. The fragment may be the generated
     * {@link Document} or a dynamic message of the same type from a retained schema.
     * The projection carries no map fields, so {@code SearchMetadata.metadata} is
     * copied here.
     *
     * @param core the decoded CORE fragment
     * @return the lookup fields; unset where the fragment has none
     */
    public static DocumentLookup lookup(Message core) {
        Objects.requireNonNull(core, "core");
        if (!LOOKUP_PROJECTION.supports(core.getDescriptorForType()))
            throw new IllegalArgumentException("A CORE fragment of " + core.getDescriptorForType().getFullName()
                    + " is not a " + Document.getDescriptor().getFullName());
        var lookup = DocumentLookup.newBuilder().mergeFrom(LOOKUP_PROJECTION.project(core));
        var searchField = core.getDescriptorForType().findFieldByName("search_metadata");
        if (searchField != null && searchField.getJavaType() == FieldDescriptor.JavaType.MESSAGE && core.hasField(searchField)) {
            var search = (Message) core.getField(searchField);
            var metadataField = search.getDescriptorForType().findFieldByName("metadata");
            if (metadataField != null && metadataField.isMapField()) {
                var keyField = metadataField.getMessageType().findFieldByName("key");
                var valueField = metadataField.getMessageType().findFieldByName("value");
                if (keyField.getJavaType() == FieldDescriptor.JavaType.STRING && valueField.getJavaType() == FieldDescriptor.JavaType.STRING)
                    for (int i = 0; i < search.getRepeatedFieldCount(metadataField); i++) {
                        var entry = (Message) search.getRepeatedField(metadataField, i);
                        lookup.putMetadata((String) entry.getField(keyField), (String) entry.getField(valueField));
                    }
            }
        }
        structuredDataType(core).ifPresent(lookup::setStructuredDataType);
        return lookup.build();
    }

    /**
     * The protobuf full name in a CORE fragment's structured_data type URL: the
     * text after its last slash, as the Any contract defines it.
     *
     * @param core the decoded CORE fragment
     * @return the full name; empty when structured_data is unset or names no type
     */
    public static Optional<String> structuredDataType(Message core) {
        var field = core.getDescriptorForType().findFieldByName("structured_data");
        if (field == null || field.getJavaType() != FieldDescriptor.JavaType.MESSAGE || field.isRepeated()
                || !field.getMessageType().getFullName().equals(Any.getDescriptor().getFullName()) || !core.hasField(field))
            return Optional.empty();
        var any = (Message) core.getField(field);
        var typeUrl = (String) any.getField(any.getDescriptorForType().findFieldByName("type_url"));
        var name = typeUrl.substring(typeUrl.lastIndexOf('/') + 1);
        return name.isEmpty() ? Optional.empty() : Optional.of(name);
    }

    /** The longest title the catalog stores, in UTF-8 bytes: it sits in a btree index row. */
    public static final int MAX_TITLE_BYTES = 2000;
    /** The longest metadata key the catalog stores, in UTF-8 bytes: it is half of a primary key. */
    public static final int MAX_METADATA_KEY_BYTES = 200;

    /** One lookup value the catalog cannot store, named by its path in the document. */
    public record Unstorable(String fieldPath, String message) {}

    /**
     * Why the catalog would refuse this lookup: an over-long title or metadata key,
     * a blank metadata key, or a NUL character in any string, which PostgreSQL text
     * columns do not hold. Checked at admission so a save is refused by name
     * instead of failing inside the commit.
     *
     * @param lookup the lookup of a CORE part
     * @return the problems, empty when the catalog stores it
     */
    public static List<Unstorable> unstorable(DocumentLookup lookup) {
        var found = new ArrayList<Unstorable>();
        if (lookup.hasTitle() && utf8Length(lookup.getTitle()) > MAX_TITLE_BYTES)
            found.add(new Unstorable("search_metadata.title", "exceeds " + MAX_TITLE_BYTES + " bytes"));
        for (var field : lookup.getAllFields().entrySet())
            if (field.getKey().getJavaType() == FieldDescriptor.JavaType.STRING && !field.getKey().isRepeated()
                    && ((String) field.getValue()).indexOf('\0') >= 0)
                found.add(new Unstorable(path(field.getKey()), "contains a NUL character"));
        for (var pair : lookup.getMetadataMap().entrySet()) {
            var key = "search_metadata.metadata[" + pair.getKey() + "]";
            if (pair.getKey().isBlank()) found.add(new Unstorable("search_metadata.metadata", "has a blank key"));
            else if (utf8Length(pair.getKey()) > MAX_METADATA_KEY_BYTES)
                found.add(new Unstorable(key, "key exceeds " + MAX_METADATA_KEY_BYTES + " bytes"));
            if (pair.getKey().indexOf('\0') >= 0 || pair.getValue().indexOf('\0') >= 0)
                found.add(new Unstorable(key, "contains a NUL character"));
        }
        return List.copyOf(found);
    }

    private static String path(FieldDescriptor field) {
        return switch (field.getName()) {
            case "datasource_id", "connector_id" -> "ownership." + field.getName();
            case "structured_data_type" -> "structured_data.type_url";
            default -> "search_metadata." + field.getName();
        };
    }

    private static int utf8Length(String value) { return value.getBytes(StandardCharsets.UTF_8).length; }

    /**
     * The entry of a revision: a tombstone carries no lookup and no owner. The
     * lookup's datasource and connector are the revision's ownership, which a
     * sourced CORE part does not decide (decision 47).
     *
     * @param manifest the stored manifest of the revision
     * @param lookup the lookup of the revision's CORE part; empty for a tombstone
     * @return the entry
     */
    public static CatalogEntry entry(StoredManifest manifest, Optional<DocumentLookup> lookup) {
        var projected = ENTRY_PROJECTION.project(Document.newBuilder()
                .setDocId(manifest.getAddress().getDocId()).setOwnership(manifest.getOwnership()).build());
        var entry = CatalogEntry.newBuilder().mergeFrom(projected).setCommittedAt(manifest.getCommittedAt());
        if (manifest.getTombstone()) {
            if (lookup.isPresent()) throw new IllegalArgumentException("A tombstone carries no lookup");
            return entry.build();
        }
        var owned = lookup.orElseThrow(() -> new IllegalArgumentException("A save needs the lookup of its CORE part"))
                .toBuilder().setDatasourceId(manifest.getOwnership().getDatasourceId()).clearConnectorId();
        if (manifest.getOwnership().hasConnectorId()) owned.setConnectorId(manifest.getOwnership().getConnectorId());
        entry.setLookup(owned);
        if (manifest.hasOwner()) entry.setOwner(manifest.getOwner());
        return entry.build();
    }

    /** The earliest instant the catalog orders by, as epoch nanoseconds in a signed 64-bit column. */
    public static final Timestamp MIN_COMMITTED = timestamp(Long.MIN_VALUE);
    /** The latest instant the catalog orders by. */
    public static final Timestamp MAX_COMMITTED = timestamp(Long.MAX_VALUE);

    /**
     * Epoch nanoseconds of a timestamp, the catalog's exact sort key.
     *
     * @param value a timestamp between {@link #MIN_COMMITTED} and {@link #MAX_COMMITTED}
     * @return epoch nanoseconds
     * @throws IllegalArgumentException outside that range
     */
    public static long nanos(Timestamp value) {
        try {
            return Math.addExact(Math.multiplyExact(value.getSeconds(), 1_000_000_000L), value.getNanos());
        } catch (ArithmeticException outside) {
            throw new IllegalArgumentException("Timestamp is outside the catalog's range", outside);
        }
    }

    /** The timestamp of epoch nanoseconds. */
    public static Timestamp timestamp(long nanos) {
        return Timestamp.newBuilder().setSeconds(Math.floorDiv(nanos, 1_000_000_000L))
                .setNanos((int) Math.floorMod(nanos, 1_000_000_000L)).build();
    }
}
