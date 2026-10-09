package ai.protomolt.proto.repo.container.ledger;

import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The private backup set layout of this rehearsal, not a wire contract: manifest.json (nonsecret
 * identity, component digests), SEALED (manifest digest, written last), sql/ledger.dump,
 * provider/rustfs-data.tar, schema/catalog.tsv, catalog/fingerprints.json and identities/.
 * Adapted from GitHub PR #413 (4862f35f9).
 */
final class RepositoryBackupRehearsalBackupSet {
    static final String FORMAT = "protomolt-repository-backup-rehearsal/v2";
    static final List<String> REQUIRED = List.of("sql/ledger.dump", "provider/rustfs-data.tar", "schema/catalog.tsv",
            "catalog/fingerprints.json", "identities/identities.json");
    private static final com.google.gson.Gson GSON = new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().serializeNulls().create();

    private RepositoryBackupRehearsalBackupSet() {}

    static Path manifest(Path backup) { return backup.resolve("manifest.json"); }
    static Path seal(Path backup) { return backup.resolve("SEALED"); }

    /** Digest every component, write the manifest with the identity fields, then seal. */
    static Map<String, Object> seal(Path backup, Map<String, Object> identity) {
        var components = new LinkedHashMap<String, Object>();
        for (var entry : tree(backup).entrySet()) {
            if (entry.getKey().equals("manifest.json") || entry.getKey().equals("SEALED")) continue;
            try {
                components.put(entry.getKey(), Map.of("sha256", entry.getValue(), "size", Files.size(backup.resolve(entry.getKey()))));
            } catch (IOException failure) { throw new IllegalStateException("Cannot size " + entry.getKey(), failure); }
        }
        for (String required : REQUIRED) if (!components.containsKey(required)) throw new IllegalStateException("Backup is missing required component " + required);
        var manifest = new LinkedHashMap<String, Object>();
        manifest.put("format", FORMAT);
        manifest.put("createdAt", java.time.Instant.now().toString());
        manifest.put("required", REQUIRED);
        manifest.put("components", components);
        manifest.putAll(identity);
        writeJson(manifest(backup), manifest);
        try {
            Files.writeString(seal(backup), FORMAT + " " + sha256(manifest(backup)) + "\n", StandardCharsets.UTF_8);
        } catch (IOException failure) { throw new IllegalStateException("Cannot seal backup", failure); }
        return manifest;
    }

    /** Preflight: seal, manifest digest, format, every required component present with its recorded digest and size. */
    static Map<String, Object> verify(Path backup) {
        if (!Files.isRegularFile(seal(backup))) throw new IllegalStateException("Backup set is not sealed: " + seal(backup) + " is missing");
        if (!Files.isRegularFile(manifest(backup))) throw new IllegalStateException("Backup set has no manifest");
        String sealed;
        try { sealed = Files.readString(seal(backup), StandardCharsets.UTF_8).strip(); }
        catch (IOException failure) { throw new IllegalStateException("Cannot read seal", failure); }
        String expected = FORMAT + " " + sha256(manifest(backup));
        if (!sealed.equals(expected)) throw new IllegalStateException("Backup seal does not match the manifest digest");
        var manifest = readJson(manifest(backup));
        if (!FORMAT.equals(manifest.get("format"))) throw new IllegalStateException("Unknown backup format " + manifest.get("format"));
        var components = object(manifest, "components");
        for (String required : REQUIRED) {
            if (!components.containsKey(required)) throw new IllegalStateException("Manifest lacks required component " + required);
            if (!Files.isRegularFile(backup.resolve(required))) throw new IllegalStateException("Required component is missing from the backup set: " + required);
        }
        for (var entry : components.entrySet()) {
            var file = backup.resolve(entry.getKey());
            var recorded = object(components, entry.getKey());
            if (!Files.isRegularFile(file)) throw new IllegalStateException("Component listed in the manifest is missing: " + entry.getKey());
            try {
                if (Files.size(file) != number(recorded, "size")) throw new IllegalStateException("Component size differs from the manifest: " + entry.getKey());
            } catch (IOException failure) { throw new IllegalStateException("Cannot size " + entry.getKey(), failure); }
            String digest = sha256(file);
            if (!digest.equals(string(recorded, "sha256"))) throw new IllegalStateException("Component checksum differs from the manifest: " + entry.getKey() + " actual=" + digest);
        }
        return manifest;
    }

    /** Recursive copy for disposable negative-case copies; the original set is never modified. */
    static void copy(Path source, Path target) {
        try (var paths = Files.walk(source)) {
            for (var path : paths.toList()) {
                var destination = target.resolve(source.relativize(path).toString());
                if (Files.isDirectory(path)) Files.createDirectories(destination); else Files.copy(path, destination);
            }
        } catch (IOException failure) { throw new IllegalStateException("Cannot copy backup set", failure); }
    }

    static TreeMap<String, String> fingerprint(Path backup) { return tree(backup); }

    /** Relative path to digest for every regular file below the root, sorted; stable across runs. */
    static TreeMap<String, String> tree(Path root) {
        var result = new TreeMap<String, String>();
        try (var paths = Files.walk(root)) {
            for (var path : paths.filter(Files::isRegularFile).sorted().toList())
                result.put(root.relativize(path).toString().replace('\\', '/'), sha256(path));
        } catch (IOException failure) { throw new IllegalStateException("Cannot walk " + root, failure); }
        return result;
    }

    static String sha256(Path file) {
        var digest = digest();
        var buffer = new byte[65536];
        try (InputStream input = Files.newInputStream(file)) {
            int n;
            while ((n = input.read(buffer)) != -1) digest.update(buffer, 0, n);
        } catch (IOException failure) { throw new IllegalStateException("Cannot digest " + file, failure); }
        return HexFormat.of().formatHex(digest.digest());
    }

    static String sha256(byte[] bytes) { return HexFormat.of().formatHex(digest().digest(bytes)); }

    private static MessageDigest digest() {
        try { return MessageDigest.getInstance("SHA-256"); }
        catch (NoSuchAlgorithmException failure) { throw new IllegalStateException(failure); }
    }

    static void writeJson(Path file, Map<String, Object> document) {
        try { Files.writeString(file, GSON.toJson(document) + "\n", StandardCharsets.UTF_8); }
        catch (IOException failure) { throw new IllegalStateException("Cannot write " + file, failure); }
    }

    static String json(Object value) { return GSON.toJson(value); }

    static Map<String, Object> readJson(Path file) {
        try (var reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            return GSON.fromJson(reader, new TypeToken<Map<String, Object>>() {}.getType());
        } catch (IOException failure) { throw new IllegalStateException("Cannot read " + file, failure); }
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> object(Map<String, Object> parent, String key) {
        var value = parent.get(key);
        if (!(value instanceof Map<?, ?>)) throw new IllegalStateException("Missing object '" + key + "'");
        return (Map<String, Object>) value;
    }

    @SuppressWarnings("unchecked")
    static List<Map<String, Object>> list(Map<String, Object> parent, String key) {
        var value = parent.get(key);
        if (!(value instanceof List<?>)) throw new IllegalStateException("Missing list '" + key + "'");
        return (List<Map<String, Object>>) value;
    }

    static String string(Map<String, Object> parent, String key) {
        var value = parent.get(key);
        if (!(value instanceof String s) || s.isEmpty()) throw new IllegalStateException("Missing string '" + key + "'");
        return s;
    }

    static long number(Map<String, Object> parent, String key) {
        var value = parent.get(key);
        if (value instanceof Number n) return n.longValue();
        if (value instanceof String s) return Long.parseLong(s);
        throw new IllegalStateException("Missing number '" + key + "'");
    }
}
