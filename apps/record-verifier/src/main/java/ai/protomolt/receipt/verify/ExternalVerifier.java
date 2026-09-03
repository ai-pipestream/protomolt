package ai.protomolt.receipt.verify;

import ai.protomolt.receipt.verify.RecordWire.Artifact;
import ai.protomolt.receipt.verify.RecordWire.Completeness;
import ai.protomolt.receipt.verify.RecordWire.Issuer;
import ai.protomolt.receipt.verify.RecordWire.Key;
import ai.protomolt.receipt.verify.RecordWire.Manifest;
import ai.protomolt.receipt.verify.RecordWire.Sig;
import ai.protomolt.receipt.verify.RecordWire.Signed;
import ai.protomolt.receipt.verify.RecordWire.Step;
import ai.protomolt.receipt.verify.RecordWire.Subject;
import ai.protomolt.receipt.verify.RecordWire.Trust;
import ai.protomolt.receipt.verify.Wire.MalformedException;
import ai.protomolt.receipt.verify.Wire.Notes;
import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.Signature;
import java.security.spec.EdECPoint;
import java.security.spec.EdECPublicKeySpec;
import java.security.spec.NamedParameterSpec;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The external verifier: the same fixed pipeline of named checks as the platform's
 * verifier, sharing nothing with it but the wire contract and the JDK. The conformance
 * corpus is the bridge — this verifier claims conformance by producing the runtime
 * verifier's verdict on every corpus fixture. Like the runtime, it stops at the first
 * failure, refuses by name, and reports what verification does not establish.
 */
public final class ExternalVerifier {

    public static final int MANIFEST_VERSION = 1;
    public static final int MAX_RECORD_BYTES = 4 * 1024 * 1024;

    public static final String CHECK_CONTAINER_BOUNDS = "container-bounds";
    public static final String CHECK_MANIFEST_PARSE = "manifest-parse";
    public static final String CHECK_RESERIALIZATION_EQUALITY = "reserialization-equality";
    public static final String CHECK_KEY_TRUSTED = "key-trusted";
    public static final String CHECK_SIGNATURE_VALID = "signature-valid";
    public static final String CHECK_ISSUER_AUTHORIZED = "issuer-authorized";
    public static final String CHECK_COMPLETENESS_CONSISTENT = "completeness-consistent";
    public static final String CHECK_ARTIFACT_REHASH = "artifact-rehash";

    private static final List<String> BASE_NON_CLAIMS = List.of(
            "issuer-honesty", "trusted-time", "world-completeness", "execution-correctness");
    private static final String NON_CLAIM_ARTIFACT_CUSTODY = "artifact-custody";

    /** One named check outcome. */
    public record Check(String id, Status status, String detail) {
        public enum Status { PASSED, FAILED, SKIPPED }
    }

    /** The verification result: ordered checks, non-claims, and the manifest digest. */
    public record Result(List<Check> checks, List<String> nonClaims, String manifestDigest) {
        public boolean verified() {
            return checks.stream().noneMatch(check -> check.status() == Check.Status.FAILED);
        }

        /** The refusing check, when there is one. */
        public Check refusal() {
            return checks.stream().filter(check -> check.status() == Check.Status.FAILED)
                    .findFirst().orElse(null);
        }
    }

    private ExternalVerifier() {
    }

    /** Verifies without artifact bytes; the rehash check is skipped and named so. */
    public static Result verify(byte[] recordBytes, byte[] trustBytes) {
        return verify(recordBytes, trustBytes, null);
    }

    /**
     * Verifies a signed record against a serialized trust snapshot.
     *
     * @param recordBytes the serialized signed work record
     * @param trustBytes the serialized trust snapshot; an invalid snapshot is the
     *         caller's error and throws rather than failing the record
     * @param artifactBytes referenced artifact bytes by SHA-256 hex, or null to skip
     *         the rehash check
     */
    public static Result verify(byte[] recordBytes, byte[] trustBytes,
                                Map<String, byte[]> artifactBytes) {
        if (recordBytes == null) {
            throw new IllegalArgumentException("recordBytes must not be null");
        }
        Map<String, Issuer> issuers = indexTrust(trustBytes);
        List<String> nonClaims = new ArrayList<>(BASE_NON_CLAIMS);
        if (artifactBytes == null) {
            nonClaims.add(NON_CLAIM_ARTIFACT_CUSTODY);
        }
        List<Check> checks = new ArrayList<>();

        // container-bounds
        if (recordBytes.length == 0 || recordBytes.length > MAX_RECORD_BYTES) {
            return refuse(checks, nonClaims, "", CHECK_CONTAINER_BOUNDS, "record is "
                    + recordBytes.length + " bytes; the bound is 1.." + MAX_RECORD_BYTES);
        }
        Notes containerNotes = new Notes();
        Signed container;
        try {
            container = RecordWire.signed(recordBytes, containerNotes);
        } catch (MalformedException e) {
            return refuse(checks, nonClaims, "", CHECK_CONTAINER_BOUNDS,
                    "record does not parse as a signed work record: " + e.getMessage());
        }
        if (!containerNotes.unknownFields.isEmpty()) {
            return refuse(checks, nonClaims, "", CHECK_CONTAINER_BOUNDS,
                    "container carries unknown fields at " + containerNotes.unknownFields);
        }
        String containerViolation = containerRules(container);
        if (containerViolation != null) {
            return refuse(checks, nonClaims, "", CHECK_CONTAINER_BOUNDS, containerViolation);
        }
        Set<String> signatureKeyIds = new HashSet<>();
        for (Sig signature : container.signatures()) {
            if (signature.algorithm() != 1) {
                return refuse(checks, nonClaims, "", CHECK_CONTAINER_BOUNDS, "signature by key '"
                        + signature.keyId() + "' names an algorithm outside the v1 profile");
            }
            if (!signatureKeyIds.add(signature.keyId())) {
                return refuse(checks, nonClaims, "", CHECK_CONTAINER_BOUNDS,
                        "duplicate signature by key '" + signature.keyId() + "'");
            }
        }
        checks.add(new Check(CHECK_CONTAINER_BOUNDS, Check.Status.PASSED,
                recordBytes.length + " bytes, " + container.signatures().size()
                        + " signature(s)"));

        // manifest-parse
        byte[] manifestBytes = container.manifest();
        String digest = sha256Hex(manifestBytes);
        Notes manifestNotes = new Notes();
        Manifest manifest;
        try {
            manifest = RecordWire.manifest(manifestBytes, manifestNotes);
        } catch (MalformedException e) {
            return refuse(checks, nonClaims, digest, CHECK_MANIFEST_PARSE,
                    "manifest does not parse as a work record: " + e.getMessage());
        }
        if (!manifestNotes.unknownFields.isEmpty()) {
            return refuse(checks, nonClaims, digest, CHECK_MANIFEST_PARSE,
                    "manifest carries unknown fields at " + manifestNotes.unknownFields);
        }
        if (manifest.version() != MANIFEST_VERSION) {
            return refuse(checks, nonClaims, digest, CHECK_MANIFEST_PARSE, "manifest version "
                    + manifest.version() + " is not the known version " + MANIFEST_VERSION);
        }
        String manifestViolation = manifestRules(manifest);
        if (manifestViolation != null) {
            return refuse(checks, nonClaims, digest, CHECK_MANIFEST_PARSE, manifestViolation);
        }
        checks.add(new Check(CHECK_MANIFEST_PARSE, Check.Status.PASSED,
                "manifest version " + manifest.version() + ", digest " + digest));

        // reserialization-equality: a strict reader that tolerated nothing would have
        // produced these bytes itself, so canonical form is exactly "no deviations noted".
        if (!manifestNotes.nonCanonical.isEmpty()) {
            return refuse(checks, nonClaims, digest, CHECK_RESERIALIZATION_EQUALITY,
                    "manifest bytes are not in canonical form: " + manifestNotes.nonCanonical);
        }
        checks.add(new Check(CHECK_RESERIALIZATION_EQUALITY, Check.Status.PASSED, "canonical"));

        // key-trusted
        Issuer issuer = issuers.get(manifest.issuer());
        if (issuer == null) {
            return refuse(checks, nonClaims, digest, CHECK_KEY_TRUSTED, "issuer '"
                    + manifest.issuer() + "' is not in the trust snapshot");
        }
        if (!signatureKeyIds.contains(manifest.keyId())) {
            return refuse(checks, nonClaims, digest, CHECK_KEY_TRUSTED, "the manifest's key '"
                    + manifest.keyId() + "' carries no signature in the container");
        }
        Map<String, Key> keys = new HashMap<>();
        for (Key key : issuer.keys()) {
            keys.put(key.keyId(), key);
        }
        Map<String, PublicKey> resolved = new HashMap<>();
        for (String keyId : signatureKeyIds) {
            Key key = keys.get(keyId);
            if (key == null) {
                return refuse(checks, nonClaims, digest, CHECK_KEY_TRUSTED, "key '" + keyId
                        + "' is not in the trust snapshot under issuer '"
                        + manifest.issuer() + "'");
            }
            if (key.state() == 3) {
                return refuse(checks, nonClaims, digest, CHECK_KEY_TRUSTED,
                        "key '" + keyId + "' is revoked");
            }
            if (key.algorithm() != 1) {
                return refuse(checks, nonClaims, digest, CHECK_KEY_TRUSTED, "key '" + keyId
                        + "' names an algorithm outside the v1 profile");
            }
            if (key.hasNotBefore() && manifest.issuedAt().compareTo(key.notBefore()) < 0) {
                return refuse(checks, nonClaims, digest, CHECK_KEY_TRUSTED, "key '" + keyId
                        + "' was not yet valid at the claimed issuance time");
            }
            if (key.hasNotAfter() && manifest.issuedAt().compareTo(key.notAfter()) > 0) {
                return refuse(checks, nonClaims, digest, CHECK_KEY_TRUSTED, "key '" + keyId
                        + "' was no longer valid at the claimed issuance time");
            }
            PublicKey decoded = decodePublicKey(key.publicKey());
            if (decoded == null) {
                return refuse(checks, nonClaims, digest, CHECK_KEY_TRUSTED, "key '" + keyId
                        + "' does not decode as an Ed25519 public key");
            }
            resolved.put(keyId, decoded);
        }
        checks.add(new Check(CHECK_KEY_TRUSTED, Check.Status.PASSED, signatureKeyIds.size()
                + " key(s) resolved under issuer '" + manifest.issuer() + "'"));

        // signature-valid
        for (Sig signature : container.signatures()) {
            if (!signatureVerifies(resolved.get(signature.keyId()), manifestBytes,
                    signature.signature())) {
                return refuse(checks, nonClaims, digest, CHECK_SIGNATURE_VALID,
                        "signature by key '" + signature.keyId()
                                + "' does not verify over the manifest bytes");
            }
        }
        checks.add(new Check(CHECK_SIGNATURE_VALID, Check.Status.PASSED,
                container.signatures().size() + " signature(s) verified"));

        // issuer-authorized
        String kind = manifest.subject().kind();
        if (!issuer.subjectKinds().contains(kind)) {
            return refuse(checks, nonClaims, digest, CHECK_ISSUER_AUTHORIZED, "issuer '"
                    + manifest.issuer() + "' is not authorized for subject kind '"
                    + kind + "'");
        }
        checks.add(new Check(CHECK_ISSUER_AUTHORIZED, Check.Status.PASSED,
                "authorized for '" + kind + "'"));

        // completeness-consistent
        Completeness completeness = manifest.completeness();
        boolean complete = completeness.status() == 1;
        if (complete && !completeness.missingReasons().isEmpty()) {
            return refuse(checks, nonClaims, digest, CHECK_COMPLETENESS_CONSISTENT,
                    "a complete record must not carry missing-evidence reasons");
        }
        if (!complete && completeness.missingReasons().isEmpty()) {
            return refuse(checks, nonClaims, digest, CHECK_COMPLETENESS_CONSISTENT,
                    "a record that is not complete must say what is missing and why");
        }
        checks.add(new Check(CHECK_COMPLETENESS_CONSISTENT, Check.Status.PASSED,
                "status " + completeness.status() + " against policy '"
                        + completeness.policyId() + "'"));

        // artifact-rehash
        if (artifactBytes == null) {
            checks.add(new Check(CHECK_ARTIFACT_REHASH, Check.Status.SKIPPED,
                    "no artifact bytes supplied"));
            return new Result(List.copyOf(checks), List.copyOf(nonClaims), digest);
        }
        List<Artifact> references = new ArrayList<>(manifest.artifacts());
        for (Step step : manifest.steps()) {
            if (step.requestArtifact() != null) {
                references.add(step.requestArtifact());
            }
            if (step.responseArtifact() != null) {
                references.add(step.responseArtifact());
            }
        }
        for (Artifact reference : references) {
            byte[] bytes = artifactBytes.get(reference.sha256());
            if (bytes == null) {
                return refuse(checks, nonClaims, digest, CHECK_ARTIFACT_REHASH,
                        "artifact " + reference.sha256() + " was not supplied");
            }
            if (bytes.length != reference.sizeBytes()) {
                return refuse(checks, nonClaims, digest, CHECK_ARTIFACT_REHASH, "artifact "
                        + reference.sha256() + " is " + bytes.length
                        + " bytes; the record says " + reference.sizeBytes());
            }
            if (!sha256Hex(bytes).equals(reference.sha256())) {
                return refuse(checks, nonClaims, digest, CHECK_ARTIFACT_REHASH, "artifact "
                        + reference.sha256() + " does not match its digest");
            }
        }
        checks.add(new Check(CHECK_ARTIFACT_REHASH, Check.Status.PASSED,
                references.size() + " artifact(s) matched digest and size"));
        return new Result(List.copyOf(checks), List.copyOf(nonClaims), digest);
    }

    private static Result refuse(List<Check> checks, List<String> nonClaims, String digest,
                                 String id, String detail) {
        checks.add(new Check(id, Check.Status.FAILED, detail));
        return new Result(List.copyOf(checks), List.copyOf(nonClaims), digest);
    }

    // ---------------------------------------------------------------- trust snapshot

    private static Map<String, Issuer> indexTrust(byte[] trustBytes) {
        if (trustBytes == null) {
            throw new IllegalArgumentException("trust snapshot must not be null");
        }
        Notes notes = new Notes();
        Trust trust;
        try {
            trust = RecordWire.trust(trustBytes, notes);
        } catch (MalformedException e) {
            throw new IllegalArgumentException(
                    "trust snapshot does not parse: " + e.getMessage());
        }
        String violation = trustRules(trust, notes);
        if (violation != null) {
            throw new IllegalArgumentException("trust snapshot is invalid: " + violation);
        }
        Map<String, Issuer> issuers = new HashMap<>();
        for (Issuer issuer : trust.issuers()) {
            if (issuers.put(issuer.issuer(), issuer) != null) {
                throw new IllegalArgumentException(
                        "trust snapshot duplicates issuer '" + issuer.issuer() + "'");
            }
            Set<String> keyIds = new HashSet<>();
            for (Key key : issuer.keys()) {
                if (!keyIds.add(key.keyId())) {
                    throw new IllegalArgumentException("trust snapshot duplicates key '"
                            + key.keyId() + "' under issuer '" + issuer.issuer() + "'");
                }
            }
        }
        return issuers;
    }

    private static String trustRules(Trust trust, Notes notes) {
        if (!notes.unknownFields.isEmpty()) {
            return "unknown fields at " + notes.unknownFields;
        }
        if (trust.issuers().isEmpty() || trust.issuers().size() > 256) {
            return "issuers must number 1..256";
        }
        for (Issuer issuer : trust.issuers()) {
            String name = Rules.requiredSlug(issuer.issuer(), 256, "issuer");
            if (name != null) {
                return name;
            }
            if (issuer.keys().isEmpty() || issuer.keys().size() > 256) {
                return "keys must number 1..256";
            }
            for (Key key : issuer.keys()) {
                String keyViolation = keyRules(key);
                if (keyViolation != null) {
                    return keyViolation;
                }
            }
            if (issuer.subjectKinds().isEmpty() || issuer.subjectKinds().size() > 64) {
                return "subject_kinds must number 1..64";
            }
            if (new HashSet<>(issuer.subjectKinds()).size() != issuer.subjectKinds().size()) {
                return "subject_kinds must be unique";
            }
            for (String subjectKind : issuer.subjectKinds()) {
                String kindViolation = Rules.requiredSlug(subjectKind, 64, "subject_kinds");
                if (kindViolation != null) {
                    return kindViolation;
                }
            }
        }
        return null;
    }

    private static String keyRules(Key key) {
        String keyId = Rules.requiredSlug(key.keyId(), 128, "key_id");
        if (keyId != null) {
            return keyId;
        }
        if (key.algorithm() != 1) {
            return "algorithm must be a defined non-zero value";
        }
        if (key.publicKey().length != 32) {
            return "public_key must be 32 bytes";
        }
        if (key.state() < 1 || key.state() > 3) {
            return "state must be a defined non-zero value";
        }
        return null;
    }

    // ---------------------------------------------------------------- declared rules

    private static String containerRules(Signed container) {
        if (container.manifest().length == 0) {
            return "manifest: must not be empty";
        }
        if (container.signatures().isEmpty()) {
            return "signatures: at least one is required";
        }
        for (Sig signature : container.signatures()) {
            String keyId = Rules.requiredSlug(signature.keyId(), 128, "signatures.key_id");
            if (keyId != null) {
                return keyId;
            }
            if (signature.algorithm() < 0 || signature.algorithm() > 1
                    || signature.algorithm() == 0) {
                return "signatures.algorithm: must be a defined non-zero value";
            }
            if (signature.signature().length != 64) {
                return "signatures.signature: must be 64 bytes";
            }
        }
        return null;
    }

    private static String manifestRules(Manifest manifest) {
        if (manifest.version() < 1) {
            return "manifest_version: must be at least 1";
        }
        String violation = Rules.requiredSlug(manifest.recordId(), 128, "record_id");
        if (violation == null) {
            violation = Rules.requiredSlug(manifest.issuer(), 256, "issuer");
        }
        if (violation == null) {
            violation = Rules.requiredSlug(manifest.keyId(), 128, "key_id");
        }
        if (violation != null) {
            return violation;
        }
        if (!manifest.hasIssuedAt()) {
            return "issued_at: is required";
        }
        if (manifest.subject() == null) {
            return "subject: is required";
        }
        violation = subjectRules(manifest.subject());
        if (violation != null) {
            return violation;
        }
        if (manifest.steps().size() > 1024) {
            return "steps: at most 1024";
        }
        for (Step step : manifest.steps()) {
            violation = stepRules(step);
            if (violation != null) {
                return violation;
            }
        }
        if (manifest.artifacts().size() > 1024) {
            return "artifacts: at most 1024";
        }
        for (Artifact artifact : manifest.artifacts()) {
            violation = artifactRules(artifact, "artifacts");
            if (violation != null) {
                return violation;
            }
        }
        if (manifest.completeness() == null) {
            return "completeness: is required";
        }
        violation = completenessRules(manifest.completeness());
        if (violation != null) {
            return violation;
        }
        if (!manifest.prior().isEmpty() && !Rules.isSha256Hex(manifest.prior())) {
            return "prior_manifest_sha256: must be a lowercase hex sha-256";
        }
        if (manifest.disclosure() != null) {
            if (!Rules.isSha256Hex(manifest.disclosure().sourceManifestSha256())) {
                return "disclosure.source_manifest_sha256: must be a lowercase hex sha-256";
            }
            String policy = manifest.disclosure().policy();
            if (policy.isEmpty() || Rules.codePoints(policy) > 512) {
                return "disclosure.policy: must be 1..512 characters";
            }
        }
        return null;
    }

    private static String subjectRules(Subject subject) {
        String violation = Rules.requiredSlug(subject.kind(), 64, "subject.kind");
        if (violation != null) {
            return violation;
        }
        // Set fields validate their format whatever the kind; the kind then
        // demands its own identity fields, mirroring the manifest rules.
        if (!subject.workflowName().isEmpty()) {
            violation = Rules.requiredSlug(subject.workflowName(), 128,
                    "subject.workflow_name");
            if (violation != null) {
                return violation;
            }
        }
        if (!subject.workflowVersion().isEmpty()) {
            violation = Rules.requiredSlug(subject.workflowVersion(), 128,
                    "subject.workflow_version");
            if (violation != null) {
                return violation;
            }
        }
        if (!subject.workflowFingerprint().isEmpty()
                && !Rules.isSha256Hex(subject.workflowFingerprint())) {
            return "subject.workflow_fingerprint: must be a lowercase hex sha-256";
        }
        if (!subject.runId().isEmpty()) {
            violation = Rules.requiredSlug(subject.runId(), 128, "subject.run_id");
            if (violation != null) {
                return violation;
            }
        }
        if (!subject.taskId().isEmpty() && !Rules.isUuid(subject.taskId())) {
            return "subject.task_id: must be a UUID";
        }
        if (!subject.workerId().isEmpty()) {
            violation = Rules.requiredSlug(subject.workerId(), 128, "subject.worker_id");
            if (violation != null) {
                return violation;
            }
        }
        if (!subject.specSha256().isEmpty() && !Rules.isSha256Hex(subject.specSha256())) {
            return "subject.spec_sha256: must be a lowercase hex sha-256";
        }
        return switch (subject.kind()) {
            case "workflow-run" -> subject.workflowName().isEmpty()
                    || subject.workflowFingerprint().isEmpty() || subject.runId().isEmpty()
                    ? "subject: a workflow-run subject names the workflow, its fingerprint,"
                            + " and the run"
                    : null;
            case "delegation-task" -> subject.taskId().isEmpty()
                    || subject.workerId().isEmpty() || subject.specSha256().isEmpty()
                    ? "subject: a delegation-task subject names the task, its worker, and"
                            + " the spec fingerprint"
                    : null;
            default -> "subject.kind: '" + subject.kind()
                    + "' is not a kind this manifest version defines";
        };
    }

    private static String stepRules(Step step) {
        String violation = Rules.requiredSlug(step.name(), 128, "steps.name");
        if (violation != null) {
            return violation;
        }
        if (Rules.codePoints(step.method()) > 512) {
            return "steps.method: at most 512 characters";
        }
        if (step.outcome() < 1 || step.outcome() > 4) {
            return "steps.outcome: must be a defined non-zero value";
        }
        if (step.requestArtifact() != null) {
            violation = artifactRules(step.requestArtifact(), "steps.request_artifact");
            if (violation != null) {
                return violation;
            }
        }
        if (step.responseArtifact() != null) {
            violation = artifactRules(step.responseArtifact(), "steps.response_artifact");
            if (violation != null) {
                return violation;
            }
        }
        if (step.promptTokens() < 0 || step.completionTokens() < 0) {
            return "steps token counts must not be negative";
        }
        if (Rules.codePoints(step.model()) > 256) {
            return "steps.model: at most 256 characters";
        }
        if (Rules.codePoints(step.modelVersion()) > 512) {
            return "steps.model_version: at most 512 characters";
        }
        if (Rules.codePoints(step.summary()) > 4096) {
            return "steps.summary: at most 4096 characters";
        }
        return null;
    }

    private static String artifactRules(Artifact artifact, String path) {
        if (!Rules.isSha256Hex(artifact.sha256())) {
            return path + ".sha256: must be a lowercase hex sha-256";
        }
        if (!Rules.isMediaType(artifact.mediaType())) {
            return path + ".media_type: must be a media type";
        }
        return null;
    }

    private static String completenessRules(Completeness completeness) {
        if (completeness.status() < 1 || completeness.status() > 3) {
            return "completeness.status: must be a defined non-zero value";
        }
        if (completeness.missingReasons().size() > 64) {
            return "completeness.missing_reasons: at most 64";
        }
        for (String reason : completeness.missingReasons()) {
            int length = Rules.codePoints(reason);
            if (length < 1 || length > 512) {
                return "completeness.missing_reasons: entries must be 1..512 characters";
            }
        }
        String violation = Rules.requiredSlug(completeness.policyId(), 128,
                "completeness.policy_id");
        if (violation != null) {
            return violation;
        }
        String version = completeness.policyVersion();
        if (version.isEmpty() || Rules.codePoints(version) > 128) {
            return "completeness.policy_version: must be 1..128 characters";
        }
        if (!Rules.isSha256Hex(completeness.policySha256())) {
            return "completeness.policy_sha256: must be a lowercase hex sha-256";
        }
        return null;
    }

    // ---------------------------------------------------------------- crypto

    private static PublicKey decodePublicKey(byte[] raw) {
        if (raw == null || raw.length != 32) {
            return null;
        }
        byte[] littleEndian = raw.clone();
        boolean xOdd = (littleEndian[31] & 0x80) != 0;
        littleEndian[31] &= 0x7f;
        byte[] bigEndian = new byte[32];
        for (int i = 0; i < 32; i++) {
            bigEndian[i] = littleEndian[31 - i];
        }
        try {
            return KeyFactory.getInstance("Ed25519").generatePublic(new EdECPublicKeySpec(
                    NamedParameterSpec.ED25519,
                    new EdECPoint(xOdd, new BigInteger(1, bigEndian))));
        } catch (GeneralSecurityException e) {
            return null;
        }
    }

    private static boolean signatureVerifies(PublicKey key, byte[] bytes, byte[] signature) {
        try {
            Signature verifier = Signature.getInstance("Ed25519");
            verifier.initVerify(key);
            verifier.update(bytes);
            return verifier.verify(signature);
        } catch (GeneralSecurityException e) {
            return false;
        }
    }

    static String sha256Hex(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is a required JDK algorithm", e);
        }
    }
}
