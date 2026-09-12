package ai.protomolt.proto.asset.bridge;

import ai.protomolt.proto.asset.v1.BridgeKind;
import ai.protomolt.proto.asset.v1.Classification;
import ai.protomolt.proto.asset.v1.ClassificationState;
import ai.protomolt.proto.asset.v1.ContainerMembers;
import ai.protomolt.proto.asset.v1.DatasetSchema;
import ai.protomolt.proto.asset.v1.FormatFact;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The routing rule: characterized format applies these bridges. This is the
 * whole of bridging's decision-making — everything else is the
 * transformations themselves and the archive machinery that lands their
 * output.
 *
 * <p>The rule is gated on the classification state machine. An asset nobody
 * has classified has no format to route on, and an asset whose declaration
 * the bytes contradict has two formats and no way to choose between them;
 * neither bridges. {@code DECLARED}, {@code IDENTIFIED} and {@code VERIFIED}
 * each name exactly one format of record, and that format selects the
 * bridges.
 *
 * <p>Most format pairs deliberately do not bridge. A format absent from
 * {@link #applicableTo} is not an oversight — it means nothing in the tree
 * produces a derived rendition from it yet.
 */
public final class Bridges {

    /** Every well-known derived rendition name, for {@link #derivedName}. */
    private static final Set<String> DERIVED_NAMES = Arrays.stream(BridgeKind.values())
            .filter(kind -> kind != BridgeKind.BRIDGE_KIND_UNSPECIFIED
                    && kind != BridgeKind.UNRECOGNIZED)
            .map(Bridges::renditionName)
            .collect(Collectors.toUnmodifiableSet());

    private Bridges() {
    }

    /**
     * Whether an asset in this classification state may be bridged.
     *
     * @param state the stored classification state
     * @return true for the three states that name exactly one format
     */
    public static boolean bridgeable(ClassificationState state) {
        return state == ClassificationState.CLASSIFICATION_STATE_DECLARED
                || state == ClassificationState.CLASSIFICATION_STATE_IDENTIFIED
                || state == ClassificationState.CLASSIFICATION_STATE_VERIFIED;
    }

    /**
     * The format bridging routes on: the declaration when one stands, the
     * identification otherwise. Under {@code VERIFIED} the two name the same
     * format, so either answers.
     *
     * @param classification the entry's stored classification
     * @return the format of record, or null when the state names none
     */
    public static FormatFact formatOfRecord(Classification classification) {
        if (!bridgeable(classification.getState())) {
            return null;
        }
        return classification.hasDeclared()
                ? classification.getDeclared() : classification.getIdentified();
    }

    /**
     * The bridges a format applies, in the order a caller should run them.
     *
     * @param format the format of record
     * @return the applicable kinds; empty when the format bridges to nothing
     */
    public static List<BridgeKind> applicableTo(FormatFact format) {
        return switch (format.getFormatCase()) {
            // Containers list their members: purely structural, and the
            // cheapest useful thing to know about an archive.
            case TAR, ZIP -> List.of(BridgeKind.BRIDGE_KIND_CONTAINER_MEMBERS);
            // Self-describing datasets carry their schema; reading it is a
            // read of the footer, not of the data.
            case PARQUET, AVRO -> List.of(BridgeKind.BRIDGE_KIND_DATASET_SCHEMA);
            // Text tables have no declared schema, so the schema is inferred
            // from values and the normalized form is worth materializing.
            case DELIMITED, NDJSON -> List.of(BridgeKind.BRIDGE_KIND_DATASET_SCHEMA,
                    BridgeKind.BRIDGE_KIND_TABULAR_DATASET);
            case SPREADSHEET -> List.of(BridgeKind.BRIDGE_KIND_TABULAR_DATASET);
            // Documents yield prose. A scanned PDF yields none, and escalates
            // to OCR only once the text bridge reports it found nothing —
            // "scanned" is a finding, never an assumption from the format.
            case PDF, WORD, HTML -> List.of(BridgeKind.BRIDGE_KIND_DOCUMENT_TEXT);
            case IMAGE -> List.of(BridgeKind.BRIDGE_KIND_OCR_TEXT);
            // Everything else: gzip (one opaque compressed file), JSON, XML,
            // YAML, Markdown, plain text, presentations. Nothing in the tree
            // derives a rendition from them, so nothing is claimed.
            default -> List.of();
        };
    }

    /**
     * The well-known rendition name a bridge's output lands under. These are
     * conventions in the archive's open rendition vocabulary, not a closed
     * naming — they are simply the names the platform's own bridges use.
     *
     * @param kind the bridge
     * @return the derived rendition's name
     */
    public static String renditionName(BridgeKind kind) {
        return switch (kind) {
            case BRIDGE_KIND_CONTAINER_MEMBERS -> "members";
            case BRIDGE_KIND_DATASET_SCHEMA -> "schema";
            case BRIDGE_KIND_TABULAR_DATASET -> "dataset";
            case BRIDGE_KIND_DOCUMENT_TEXT -> "text";
            case BRIDGE_KIND_OCR_TEXT -> "ocr-text";
            case BRIDGE_KIND_CONVERSATION -> "conversation";
            case BRIDGE_KIND_UNSPECIFIED, UNRECOGNIZED ->
                    throw new IllegalArgumentException("no rendition name for " + kind);
        };
    }

    /**
     * Whether a rendition name is one the platform's bridges produce.
     *
     * <p>Consumers that pick a "primary" rendition need this: a derived
     * rendition must never become the one an asset is characterized from, or
     * bridging an entry would silently re-point its classification at its own
     * output.
     *
     * @param renditionName the rendition's name
     * @return true when a bridge produces renditions under this name
     */
    public static boolean derivedName(String renditionName) {
        return DERIVED_NAMES.contains(renditionName);
    }

    /**
     * The media type of a bridge's output bytes.
     *
     * @param kind the bridge
     * @return the IANA media type the derived rendition is stored as
     */
    public static String mediaType(BridgeKind kind) {
        return switch (kind) {
            case BRIDGE_KIND_DOCUMENT_TEXT, BRIDGE_KIND_OCR_TEXT -> "text/plain";
            default -> "application/x-protobuf";
        };
    }

    /**
     * The schema subject pinning a bridge output's shape, so the derived
     * rendition is schema-validated data rather than loose bytes. Protobuf
     * outputs pin their message's fully-qualified name, taken from the
     * generated descriptor so the pin can never drift from the contract.
     *
     * @param kind the bridge
     * @return the subject, or "" for outputs that are not protobuf
     */
    public static String schemaSubject(BridgeKind kind) {
        return switch (kind) {
            case BRIDGE_KIND_CONTAINER_MEMBERS ->
                    ContainerMembers.getDescriptor().getFullName();
            case BRIDGE_KIND_DATASET_SCHEMA -> DatasetSchema.getDescriptor().getFullName();
            // The remaining outputs pin their shape when their bridge lands.
            default -> "";
        };
    }
}
