package ai.protomolt.proto.repo.container.ledger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.TreeMap;

/** SHA-256 identities of backup components and evidence files. Ported from the draft PR #413 harness. */
final class RepositoryBackupRehearsalDigests {
    private RepositoryBackupRehearsalDigests() {}

    static String sha256(byte[] bytes) {
        return HexFormat.of().formatHex(digest().digest(bytes));
    }

    static String sha256(Path file) {
        var digest = digest();
        var buffer = new byte[65536];
        try (InputStream input = Files.newInputStream(file)) {
            int n;
            while ((n = input.read(buffer)) != -1) digest.update(buffer, 0, n);
        } catch (IOException failure) { throw new RepositoryBackupRehearsalFailure("Cannot digest " + file, failure); }
        return HexFormat.of().formatHex(digest.digest());
    }

    /** Relative path to digest for every regular file below the root, sorted; stable across runs. */
    static TreeMap<String, String> tree(Path root) {
        var result = new TreeMap<String, String>();
        try (var paths = Files.walk(root)) {
            for (var path : paths.filter(Files::isRegularFile).sorted().toList())
                result.put(root.relativize(path).toString().replace('\\', '/'), sha256(path));
        } catch (IOException failure) { throw new RepositoryBackupRehearsalFailure("Cannot walk " + root, failure); }
        return result;
    }

    private static MessageDigest digest() {
        try { return MessageDigest.getInstance("SHA-256"); }
        catch (NoSuchAlgorithmException failure) { throw new IllegalStateException(failure); }
    }
}
