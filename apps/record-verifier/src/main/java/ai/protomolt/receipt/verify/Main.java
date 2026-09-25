package ai.protomolt.receipt.verify;

import ai.protomolt.receipt.verify.ExternalVerifier.Check;
import ai.protomolt.receipt.verify.ExternalVerifier.Result;
import java.io.IOException;
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
        if (args.length == 3 && "--list-artifacts".equals(args[0])) {
            byte[] record = Files.readAllBytes(Path.of(args[1]));
            byte[] trust = Files.readAllBytes(Path.of(args[2]));
            try {
                for (String digest : ExternalVerifier.verifiedArtifactDigests(record, trust)) {
                    System.out.println(digest);
                }
            } catch (IllegalArgumentException refused) {
                System.err.println("refused: " + refused.getMessage());
                System.exit(1);
            }
            return;
        }
        if (args.length < 2 || args.length > 3
                || (args.length > 0 && "--list-artifacts".equals(args[0]))) {
            System.err.println(
                    "usage: record-verifier <record-file> <trust-file> [artifact-dir]"
                            + " | --list-artifacts <record-file> <trust-file>");
            System.exit(2);
            return;
        }
        byte[] record = Files.readAllBytes(Path.of(args[0]));
        byte[] trust = Files.readAllBytes(Path.of(args[1]));
        Map<String, byte[]> artifacts = null;
        if (args.length == 3) {
            artifacts = loadReferencedArtifacts(record, trust, Path.of(args[2]));
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

    static Map<String, byte[]> loadReferencedArtifacts(byte[] record, byte[] trust,
                                                        Path directory) throws IOException {
        Map<String, byte[]> artifacts = new HashMap<>();
        if (!ExternalVerifier.verify(record, trust).verified()) return artifacts;
        for (String digest : ExternalVerifier.verifiedArtifactDigests(record, trust)) {
            Path entry = directory.resolve(digest);
            if (Files.isRegularFile(entry)) {
                artifacts.put(digest, Files.readAllBytes(entry));
            }
        }
        return artifacts;
    }
}
