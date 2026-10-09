package ai.protomolt.proto.repo.recovery;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The private backup set layout of this qualified procedure. Not a public contract:
 * manifest.json (nonsecret identity and component digests), SEALED (manifest digest, written
 * last), sql/ledger.dump, provider/rustfs-data.tar, schema/catalog.tsv.
 */
final class BackupSet {
    static final String FORMAT = "protomolt-repository-recovery-backup/v1";
    static final List<String> REQUIRED = List.of("sql/ledger.dump", "provider/rustfs-data.tar", "schema/catalog.tsv", "identities/identities.json");

    private BackupSet() {}

    static Path manifest(Path backup) { return backup.resolve("manifest.json"); }
    static Path seal(Path backup) { return backup.resolve("SEALED"); }

    /** Digest every component, write the manifest with the identity fields, then seal. */
    static Map<String, Object> seal(Path backup, Map<String, Object> identity) {
        var components = new LinkedHashMap<String, Object>();
        for (var entry : Digests.tree(backup).entrySet()) {
            if (entry.getKey().equals("manifest.json") || entry.getKey().equals("SEALED")) continue;
            try {
                components.put(entry.getKey(), Map.of("sha256", entry.getValue(), "size", Files.size(backup.resolve(entry.getKey()))));
            } catch (IOException failure) { throw new RehearsalFailure("Cannot size " + entry.getKey(), failure); }
        }
        for (String required : REQUIRED) if (!components.containsKey(required)) throw new RehearsalFailure("Backup is missing required component " + required);
        var manifest = new LinkedHashMap<String, Object>();
        manifest.put("format", FORMAT);
        manifest.put("createdAt", java.time.Instant.now().toString());
        manifest.put("required", REQUIRED);
        manifest.put("components", components);
        manifest.putAll(identity);
        Json.write(manifest(backup), manifest);
        try {
            Files.writeString(seal(backup), FORMAT + " " + Digests.sha256(manifest(backup)) + "\n", StandardCharsets.UTF_8);
        } catch (IOException failure) { throw new RehearsalFailure("Cannot seal backup", failure); }
        return manifest;
    }

    /** Preflight: seal, manifest digest, format, every required component present with its recorded digest and size. */
    static Map<String, Object> verify(Path backup) {
        if (!Files.isRegularFile(seal(backup))) throw new RehearsalFailure("Backup set is not sealed: " + seal(backup) + " is missing");
        if (!Files.isRegularFile(manifest(backup))) throw new RehearsalFailure("Backup set has no manifest");
        String sealed;
        try { sealed = Files.readString(seal(backup), StandardCharsets.UTF_8).strip(); }
        catch (IOException failure) { throw new RehearsalFailure("Cannot read seal", failure); }
        String expected = FORMAT + " " + Digests.sha256(manifest(backup));
        if (!sealed.equals(expected)) throw new RehearsalFailure("Backup seal does not match the manifest digest");
        var manifest = Json.read(manifest(backup));
        if (!FORMAT.equals(manifest.get("format"))) throw new RehearsalFailure("Unknown backup format " + manifest.get("format"));
        var components = Json.object(manifest, "components");
        for (String required : REQUIRED) {
            if (!components.containsKey(required)) throw new RehearsalFailure("Manifest lacks required component " + required);
            var file = backup.resolve(required);
            if (!Files.isRegularFile(file)) throw new RehearsalFailure("Required component is missing from the backup set: " + required);
        }
        for (var entry : components.entrySet()) {
            var file = backup.resolve(entry.getKey());
            var recorded = Json.object(components, entry.getKey());
            if (!Files.isRegularFile(file)) throw new RehearsalFailure("Component listed in the manifest is missing: " + entry.getKey());
            try {
                if (Files.size(file) != Json.number(recorded, "size")) throw new RehearsalFailure("Component size differs from the manifest: " + entry.getKey());
            } catch (IOException failure) { throw new RehearsalFailure("Cannot size " + entry.getKey(), failure); }
            String digest = Digests.sha256(file);
            if (!digest.equals(Json.string(recorded, "sha256"))) throw new RehearsalFailure("Component checksum differs from the manifest: " + entry.getKey() + " actual=" + digest);
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
        } catch (IOException failure) { throw new RehearsalFailure("Cannot copy backup set", failure); }
    }

    static TreeMap<String, String> fingerprint(Path backup) { return Digests.tree(backup); }
}
