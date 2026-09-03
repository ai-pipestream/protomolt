package ai.protomolt.receipt.verify;

import ai.protomolt.receipt.verify.ExternalVerifier.Check;
import ai.protomolt.receipt.verify.ExternalVerifier.Result;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * The command line: verify one signed record against a pinned trust snapshot, offline.
 * Exit 0 when the record verifies, 1 when it is refused, 2 on usage or input errors.
 *
 * <pre>
 * java -jar protomolt-record-verifier.jar record.binpb trust.binpb [artifact-dir]
 * </pre>
 *
 * The optional artifact directory supplies referenced artifact bytes for the rehash
 * check, one file per artifact named by its SHA-256 hex digest.
 */
public final class Main {

    private Main() {
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 2 || args.length > 3) {
            System.err.println(
                    "usage: record-verifier <record-file> <trust-file> [artifact-dir]");
            System.exit(2);
            return;
        }
        byte[] record = Files.readAllBytes(Path.of(args[0]));
        byte[] trust = Files.readAllBytes(Path.of(args[1]));
        Map<String, byte[]> artifacts = null;
        if (args.length == 3) {
            artifacts = new HashMap<>();
            try (DirectoryStream<Path> entries =
                         Files.newDirectoryStream(Path.of(args[2]))) {
                for (Path entry : entries) {
                    if (Files.isRegularFile(entry)) {
                        artifacts.put(entry.getFileName().toString(),
                                Files.readAllBytes(entry));
                    }
                }
            }
        }
        Result result;
        try {
            result = ExternalVerifier.verify(record, trust, artifacts);
        } catch (IllegalArgumentException e) {
            System.err.println("input error: " + e.getMessage());
            System.exit(2);
            return;
        }
        for (Check check : result.checks()) {
            System.out.println(check.status() + "  " + check.id() + ": " + check.detail());
        }
        System.out.println("non-claims: " + String.join(", ", result.nonClaims()));
        if (!result.manifestDigest().isEmpty()) {
            System.out.println("manifest digest: " + result.manifestDigest());
        }
        System.out.println(result.verified() ? "VERIFIED" : "REFUSED");
        System.exit(result.verified() ? 0 : 1);
    }
}
