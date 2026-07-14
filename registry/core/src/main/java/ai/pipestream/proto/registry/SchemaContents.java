package ai.pipestream.proto.registry;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;

/**
 * Canonical content identity of a schema: SHA-256 over the schema text plus each reference's
 * {@code name}/{@code subject}/{@code version}, NUL-separated so field boundaries cannot
 * collide.
 */
public final class SchemaContents {

    private SchemaContents() {
    }

    /** SHA-256 hex of the canonical schema text + references. */
    public static String contentHash(String schemaText, List<SchemaReference> references) {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
        digest.update(schemaText.getBytes(StandardCharsets.UTF_8));
        for (SchemaReference reference : references) {
            digest.update((byte) 0);
            digest.update(reference.name().getBytes(StandardCharsets.UTF_8));
            digest.update((byte) 0);
            digest.update(reference.subject().getBytes(StandardCharsets.UTF_8));
            digest.update((byte) 0);
            digest.update(Integer.toString(reference.version()).getBytes(StandardCharsets.UTF_8));
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    /** Whether the stored schema's content (text + references) equals the candidate's. */
    public static boolean sameContent(StoredSchema stored, String schemaText,
                                      List<SchemaReference> references) {
        return stored.schemaText().equals(schemaText) && stored.references().equals(references);
    }
}
