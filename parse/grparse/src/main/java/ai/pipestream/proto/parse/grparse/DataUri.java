package ai.pipestream.proto.parse.grparse;

import com.google.protobuf.ByteString;
import java.util.Base64;

/**
 * A parsed base64 data URI ({@code data:<mime>;base64,<payload>}), the form
 * the docling model embeds page and picture images in. {@link #parse}
 * returns {@code null} for anything else — a plain URI references storage
 * the caller must decide about, and a malformed payload is not an image.
 *
 * @param mimeType the declared MIME type
 * @param data the decoded bytes
 */
record DataUri(String mimeType, ByteString data) {

    private static final String SCHEME = "data:";
    private static final String ENCODING = ";base64,";

    /**
     * Parses a base64 data URI.
     *
     * @param uri the URI to parse
     * @return the parsed image, or {@code null} when the URI is not a
     *         well-formed base64 data URI
     */
    static DataUri parse(String uri) {
        if (uri == null || !uri.startsWith(SCHEME)) {
            return null;
        }
        int encoding = uri.indexOf(ENCODING, SCHEME.length());
        if (encoding < 0) {
            return null;
        }
        String mime = uri.substring(SCHEME.length(), encoding);
        if (mime.isBlank()) {
            return null;
        }
        try {
            ByteString data = ByteString.copyFrom(
                    Base64.getDecoder().decode(uri.substring(encoding + ENCODING.length())));
            // An empty payload is not an image.
            return data.isEmpty() ? null : new DataUri(mime, data);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
