package ai.protomolt.proto.asset.bridge;

import ai.protomolt.proto.asset.v1.BridgeKind;
import ai.protomolt.proto.asset.v1.ContentProfile;
import ai.protomolt.proto.asset.v1.FormatFact;
import com.google.protobuf.Message;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * One characterization-gated transformation. A bridge reads an asset's
 * original bytes and derives a typed rendition from them; it does not mutate
 * what it read.
 *
 * <p>Bridges are single-pass over a stream on purpose: the original may be
 * far larger than memory, and the archive hands the bridge the object's
 * stream instead of its bytes.
 */
public interface Bridge {

    /** The kind this bridge implements. */
    BridgeKind kind();

    /**
     * Whether this bridge can read a given format. One kind can cover
     * several formats and still not cover all of them: the schema bridge
     * reads delimited tables, NDJSON, and Avro, while a Parquet footer needs
     * a reader the asset family does not carry. A format this returns false
     * for is reported as deferred, not attempted.
     *
     * @param format the format of record
     * @return true when this bridge reads that format
     */
    default boolean handles(FormatFact format) {
        return true;
    }

    /**
     * Derives the rendition.
     *
     * @param original the original rendition's bytes
     * @param context what the classification established about the asset
     * @return the derivation
     * @throws IOException when the original cannot be read
     */
    Derivation derive(InputStream original, Context context) throws IOException;

    /**
     * What the classification established about the asset being bridged.
     * A bridge often needs more than the bytes: a delimited table's
     * delimiter and header presence are part of the producer's claim, not
     * something the bytes state, so the format of record travels with the
     * stream.
     *
     * @param format the format of record the routing rule selected
     * @param filename the asset's filename, or null when it has none
     */
    record Context(FormatFact format, String filename) {
    }

    /**
     * One bridge's product: the derived rendition's bytes, the content
     * profile when the bridge measured one, and any degradation it wants
     * reported. A bridge that degraded still returns its product and
     * reports how — the same honesty the parser contract requires.
     *
     * <p>Bytes rather than a message, because not every derived rendition
     * is protobuf: a structural listing is, and extracted prose is plain
     * text. What a rendition decodes as is pinned on its descriptor by
     * {@link Bridges#schemaSubject}, which is where that question belongs.
     *
     * @param content the derived rendition's bytes
     * @param profile what the content is, or null when the bridge measures
     *        no profile (a structural listing describes no content class)
     * @param warnings degradation notes, verbatim; empty when the bridge ran
     *        clean
     */
    record Derivation(byte[] content, ContentProfile profile, List<String> warnings) {

        /** A clean derivation of a protobuf shape, with no content profile. */
        public static Derivation of(Message content) {
            return new Derivation(content.toByteArray(), null, List.of());
        }

        /** A derivation of a protobuf shape carrying a content profile. */
        public static Derivation of(Message content, ContentProfile profile) {
            return new Derivation(content.toByteArray(), profile, List.of());
        }

        /** A derivation of UTF-8 text carrying a content profile. */
        public static Derivation ofText(String text, ContentProfile profile,
                                        List<String> warnings) {
            return new Derivation(text.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                    profile, List.copyOf(warnings));
        }

        /** Whether the bridge reported degradation. */
        public boolean degraded() {
            return !warnings.isEmpty();
        }
    }
}
