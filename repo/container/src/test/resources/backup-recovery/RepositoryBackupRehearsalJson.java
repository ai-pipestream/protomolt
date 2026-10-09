package ai.protomolt.proto.repo.container.ledger;

import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

/**
 * Small private JSON records (identities, manifests) over the Gson component already on
 * the container runtime. No wire contract: the layout is private to this rehearsal.
 */
final class RepositoryBackupRehearsalJson {
    private static final com.google.gson.Gson GSON = new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().serializeNulls().create();
    private RepositoryBackupRehearsalJson() {}

    static String write(Map<String, Object> document) { return GSON.toJson(document); }

    /** The JSON round-trip shape of a record, so in-memory values compare equal to re-read ones (longs become doubles). */
    static String canonical(Map<String, Object> document) {
        Map<String, Object> reread = GSON.fromJson(GSON.toJson(document), new TypeToken<Map<String, Object>>() {}.getType());
        return GSON.toJson(reread);
    }

    static void write(Path file, Map<String, Object> document) {
        try { Files.writeString(file, write(document) + "\n", StandardCharsets.UTF_8); }
        catch (IOException failure) { throw new RepositoryBackupRehearsalFailure("Cannot write " + file, failure); }
    }

    static Map<String, Object> read(Path file) {
        try (var reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            return GSON.fromJson(reader, new TypeToken<Map<String, Object>>() {}.getType());
        } catch (IOException failure) { throw new RepositoryBackupRehearsalFailure("Cannot read " + file, failure); }
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> object(Map<String, Object> parent, String key) {
        var value = parent.get(key);
        if (!(value instanceof Map<?, ?>)) throw new RepositoryBackupRehearsalFailure("Missing object '" + key + "'");
        return (Map<String, Object>) value;
    }

    @SuppressWarnings("unchecked")
    static List<Map<String, Object>> list(Map<String, Object> parent, String key) {
        var value = parent.get(key);
        if (!(value instanceof List<?>)) throw new RepositoryBackupRehearsalFailure("Missing list '" + key + "'");
        return (List<Map<String, Object>>) value;
    }

    static String string(Map<String, Object> parent, String key) {
        var value = parent.get(key);
        if (!(value instanceof String s) || s.isEmpty()) throw new RepositoryBackupRehearsalFailure("Missing string '" + key + "'");
        return s;
    }

    static long number(Map<String, Object> parent, String key) {
        var value = parent.get(key);
        if (value instanceof Number n) return n.longValue();
        if (value instanceof String s) return Long.parseLong(s);
        throw new RepositoryBackupRehearsalFailure("Missing number '" + key + "'");
    }

    static boolean bool(Map<String, Object> parent, String key) {
        var value = parent.get(key);
        if (value instanceof Boolean b) return b;
        throw new RepositoryBackupRehearsalFailure("Missing boolean '" + key + "'");
    }

    static String sha256(byte[] bytes) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)); }
        catch (NoSuchAlgorithmException failure) { throw new IllegalStateException(failure); }
    }
}
