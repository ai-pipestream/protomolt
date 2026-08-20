package ai.pipestream.proto.receipt;

import java.util.List;

/**
 * A verification outcome: every check that ran with its result, the
 * non-claims, and — once the manifest parsed — the manifest digest and
 * the parsed manifest. The pipeline stops at the first failure, so a
 * failed verification's last check names the refusal.
 *
 * @param checks the checks that ran, in pipeline order
 * @param nonClaims what this verification does not establish, as stable
 *         identifiers
 * @param manifestDigest SHA-256 of the manifest bytes, empty until the
 *         container parsed
 * @param manifest the parsed manifest, or null until it parsed
 */
public record Verification(List<Check> checks, List<String> nonClaims,
                           String manifestDigest, WorkRecord manifest) {

    /** Verification does not establish that the issuer told the truth. */
    public static final String NON_CLAIM_ISSUER_HONESTY = "issuer-honesty";
    /** Issuance time is claimed by the issuer, not proven. */
    public static final String NON_CLAIM_TRUSTED_TIME = "trusted-time";
    /** The record describes what the issuer recorded, not all that happened. */
    public static final String NON_CLAIM_WORLD_COMPLETENESS = "world-completeness";
    /** A verified record does not prove the work was correct. */
    public static final String NON_CLAIM_EXECUTION_CORRECTNESS = "execution-correctness";
    /** Referenced artifact bytes were not checked (rehash skipped). */
    public static final String NON_CLAIM_ARTIFACT_CUSTODY = "artifact-custody";

    public Verification {
        checks = List.copyOf(checks);
        nonClaims = List.copyOf(nonClaims);
        if (manifestDigest == null) {
            throw new IllegalArgumentException("manifestDigest must not be null");
        }
    }

    /** True when every check that ran passed or was skipped by design. */
    public boolean verified() {
        return checks.stream().noneMatch(check -> check.status() == Check.Status.FAILED);
    }

    /** The failed check, or null when verification passed. */
    public Check refusal() {
        return checks.stream()
                .filter(check -> check.status() == Check.Status.FAILED)
                .findFirst().orElse(null);
    }

    /**
     * One named check's outcome.
     *
     * @param id the stable check identifier
     * @param status how the check ended
     * @param detail what passed, failed, or why the check was skipped
     */
    public record Check(String id, Status status, String detail) {

        /** Check statuses. */
        public enum Status {
            /** The check ran and held. */
            PASSED,
            /** The check ran and refused; verification stopped here. */
            FAILED,
            /** The check could not run by design (no artifact bytes supplied). */
            SKIPPED
        }

        public Check {
            if (id == null || id.isBlank()) {
                throw new IllegalArgumentException("check id must not be blank");
            }
            if (status == null) {
                throw new IllegalArgumentException("check status must not be null");
            }
            if (detail == null) {
                throw new IllegalArgumentException("check detail must not be null");
            }
        }
    }
}
