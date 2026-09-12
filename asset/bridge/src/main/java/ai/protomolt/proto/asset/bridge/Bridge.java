package ai.protomolt.proto.asset.bridge;

import ai.protomolt.proto.asset.v1.BridgeKind;
import ai.protomolt.proto.asset.v1.ContentProfile;
import com.google.protobuf.Message;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * One characterization-gated transformation. A bridge reads an asset's
 * original bytes and derives a typed rendition from them; it never mutates
 * what it read.
 *
 * <p>Bridges are single-pass over a stream on purpose: the original may be
 * far larger than memory, and the archive hands the bridge the object's
 * stream rather than its bytes.
 */
public interface Bridge {

    /** The kind this bridge implements. */
    BridgeKind kind();

    /**
     * Derives the rendition.
     *
     * @param original the original rendition's bytes
     * @param filename the asset's filename, or null when it has none
     * @return the derivation
     * @throws IOException when the original cannot be read
     */
    Derivation derive(InputStream original, String filename) throws IOException;

    /**
     * One bridge's product: the typed content, the content profile when the
     * bridge measured one, and any degradation it wants reported. A bridge
     * that degraded still returns its product and says how — the same
     * honesty the parser contract requires.
     *
     * @param content the derived rendition's message
     * @param profile what the content is, or null when the bridge measures
     *        no profile (a structural listing describes no content class)
     * @param warnings degradation notes, verbatim; empty when the bridge ran
     *        clean
     */
    record Derivation(Message content, ContentProfile profile, List<String> warnings) {

        /** A clean derivation with no content profile. */
        public static Derivation of(Message content) {
            return new Derivation(content, null, List.of());
        }

        /** A clean derivation carrying a content profile. */
        public static Derivation of(Message content, ContentProfile profile) {
            return new Derivation(content, profile, List.of());
        }

        /** Whether the bridge reported degradation. */
        public boolean degraded() {
            return !warnings.isEmpty();
        }
    }
}
