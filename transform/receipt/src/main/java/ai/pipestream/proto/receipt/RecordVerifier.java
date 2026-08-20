package ai.pipestream.proto.receipt;

import ai.pipestream.proto.receipt.Verification.Check;
import ai.pipestream.proto.receipt.Verification.Check.Status;
import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.ValidationResult;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import java.security.GeneralSecurityException;
import java.security.PublicKey;
import java.security.Signature;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The strict offline verifier: a fixed ordered pipeline of named checks
 * over a signed record and a caller-supplied trust snapshot, zero network
 * calls, refusal by name. The pipeline stops at the first failure — a
 * record is refused whole, never partially accepted — and every result
 * carries the non-claims: what verification does not establish.
 */
public final class RecordVerifier {

    /** Size, strict parse, and profile bounds on the container. */
    public static final String CHECK_CONTAINER_BOUNDS = "container-bounds";
    /** Strict manifest parse, known version, declared rules hold. */
    public static final String CHECK_MANIFEST_PARSE = "manifest-parse";
    /** Reserializing the parsed manifest reproduces the signed bytes. */
    public static final String CHECK_RESERIALIZATION_EQUALITY = "reserialization-equality";
    /** Every signing key resolves in the snapshot, in state and window. */
    public static final String CHECK_KEY_TRUSTED = "key-trusted";
    /** Every signature verifies over the manifest bytes. */
    public static final String CHECK_SIGNATURE_VALID = "signature-valid";
    /** The issuer is authorized for the record's subject kind. */
    public static final String CHECK_ISSUER_AUTHORIZED = "issuer-authorized";
    /** The completeness claim is internally consistent. */
    public static final String CHECK_COMPLETENESS_CONSISTENT = "completeness-consistent";
    /** Supplied artifact bytes match every reference, all-or-nothing. */
    public static final String CHECK_ARTIFACT_REHASH = "artifact-rehash";

    private static final List<String> BASE_NON_CLAIMS = List.of(
            Verification.NON_CLAIM_ISSUER_HONESTY,
            Verification.NON_CLAIM_TRUSTED_TIME,
            Verification.NON_CLAIM_WORLD_COMPLETENESS,
            Verification.NON_CLAIM_EXECUTION_CORRECTNESS);

    private RecordVerifier() {
    }

    /** Verifies without artifact bytes; the rehash check is skipped and named so. */
    public static Verification verify(byte[] recordBytes, TrustSnapshot trust) {
        return verify(recordBytes, trust, null);
    }

    /**
     * Verifies a signed record.
     *
     * @param recordBytes the serialized {@link SignedWorkRecord}
     * @param trust the trust snapshot; an invalid snapshot is the caller's
     *         error and throws rather than failing the record
     * @param artifactBytes referenced artifact bytes by SHA-256, or null to
     *         skip the rehash check
     */
    public static Verification verify(byte[] recordBytes, TrustSnapshot trust,
                                      Map<String, byte[]> artifactBytes) {
        if (recordBytes == null) {
            throw new IllegalArgumentException("recordBytes must not be null");
        }
        Map<String, TrustedIssuer> issuers = indexSnapshot(trust);
        List<String> nonClaims = new ArrayList<>(BASE_NON_CLAIMS);
        if (artifactBytes == null) {
            nonClaims.add(Verification.NON_CLAIM_ARTIFACT_CUSTODY);
        }
        Pipeline pipeline = new Pipeline(nonClaims);

        // container-bounds
        SignedWorkRecord container;
        if (recordBytes.length == 0 || recordBytes.length > WorkRecords.MAX_RECORD_BYTES) {
            return pipeline.refuse(CHECK_CONTAINER_BOUNDS, "record is " + recordBytes.length
                    + " bytes; the bound is 1.." + WorkRecords.MAX_RECORD_BYTES);
        }
        try {
            container = SignedWorkRecord.parseFrom(recordBytes);
        } catch (InvalidProtocolBufferException e) {
            return pipeline.refuse(CHECK_CONTAINER_BOUNDS,
                    "record does not parse as a signed work record: " + e.getMessage());
        }
        List<String> unknownContainer = WorkRecords.unknownFieldPaths(container);
        if (!unknownContainer.isEmpty()) {
            return pipeline.refuse(CHECK_CONTAINER_BOUNDS,
                    "container carries unknown fields at " + unknownContainer);
        }
        ValidationResult containerRules = ProtoValidator.create().validate(container);
        if (!containerRules.valid()) {
            return pipeline.refuse(CHECK_CONTAINER_BOUNDS, violations(containerRules));
        }
        Set<String> signatureKeyIds = new HashSet<>();
        for (RecordSignature signature : container.getSignaturesList()) {
            if (signature.getAlgorithm() != SignatureAlgorithm.SIGNATURE_ALGORITHM_ED25519) {
                return pipeline.refuse(CHECK_CONTAINER_BOUNDS, "signature by key '"
                        + signature.getKeyId() + "' names an algorithm outside the v1 profile");
            }
            if (!signatureKeyIds.add(signature.getKeyId())) {
                return pipeline.refuse(CHECK_CONTAINER_BOUNDS,
                        "duplicate signature by key '" + signature.getKeyId() + "'");
            }
        }
        pipeline.pass(CHECK_CONTAINER_BOUNDS, recordBytes.length + " bytes, "
                + container.getSignaturesCount() + " signature(s)");

        // manifest-parse
        byte[] manifestBytes = container.getManifest().toByteArray();
        String digest = WorkRecords.sha256Hex(manifestBytes);
        pipeline.digest(digest);
        WorkRecord manifest;
        try {
            manifest = WorkRecord.parseFrom(manifestBytes);
        } catch (InvalidProtocolBufferException e) {
            return pipeline.refuse(CHECK_MANIFEST_PARSE,
                    "manifest does not parse as a work record: " + e.getMessage());
        }
        List<String> unknownManifest = WorkRecords.unknownFieldPaths(manifest);
        if (!unknownManifest.isEmpty()) {
            return pipeline.refuse(CHECK_MANIFEST_PARSE,
                    "manifest carries unknown fields at " + unknownManifest);
        }
        if (manifest.getManifestVersion() != WorkRecords.MANIFEST_VERSION) {
            return pipeline.refuse(CHECK_MANIFEST_PARSE, "manifest version "
                    + manifest.getManifestVersion() + " is not the known version "
                    + WorkRecords.MANIFEST_VERSION);
        }
        ValidationResult manifestRules = ProtoValidator.create().validate(manifest);
        if (!manifestRules.valid()) {
            return pipeline.refuse(CHECK_MANIFEST_PARSE, violations(manifestRules));
        }
        pipeline.manifest(manifest);
        pipeline.pass(CHECK_MANIFEST_PARSE, "manifest version "
                + manifest.getManifestVersion() + ", digest " + digest);

        // reserialization-equality
        byte[] reserialized = WorkRecords.canonicalBytes(manifest);
        if (!Arrays.equals(manifestBytes, reserialized)) {
            return pipeline.refuse(CHECK_RESERIALIZATION_EQUALITY,
                    "manifest bytes are not in canonical form");
        }
        pipeline.pass(CHECK_RESERIALIZATION_EQUALITY, "canonical");

        // key-trusted
        TrustedIssuer issuer = issuers.get(manifest.getIssuer());
        if (issuer == null) {
            return pipeline.refuse(CHECK_KEY_TRUSTED, "issuer '" + manifest.getIssuer()
                    + "' is not in the trust snapshot");
        }
        if (!signatureKeyIds.contains(manifest.getKeyId())) {
            return pipeline.refuse(CHECK_KEY_TRUSTED, "the manifest's key '"
                    + manifest.getKeyId() + "' carries no signature in the container");
        }
        Map<String, TrustedKey> keys = issuer.getKeysList().stream()
                .collect(Collectors.toMap(TrustedKey::getKeyId, key -> key));
        Map<String, PublicKey> resolved = new HashMap<>();
        for (String keyId : signatureKeyIds) {
            TrustedKey key = keys.get(keyId);
            if (key == null) {
                return pipeline.refuse(CHECK_KEY_TRUSTED, "key '" + keyId
                        + "' is not in the trust snapshot under issuer '"
                        + manifest.getIssuer() + "'");
            }
            if (key.getState() == KeyState.KEY_STATE_REVOKED) {
                return pipeline.refuse(CHECK_KEY_TRUSTED, "key '" + keyId + "' is revoked");
            }
            if (key.getAlgorithm() != SignatureAlgorithm.SIGNATURE_ALGORITHM_ED25519) {
                return pipeline.refuse(CHECK_KEY_TRUSTED, "key '" + keyId
                        + "' names an algorithm outside the v1 profile");
            }
            if (key.hasNotBefore()
                    && compare(manifest.getIssuedAt(), key.getNotBefore()) < 0) {
                return pipeline.refuse(CHECK_KEY_TRUSTED, "key '" + keyId
                        + "' was not yet valid at the claimed issuance time");
            }
            if (key.hasNotAfter()
                    && compare(manifest.getIssuedAt(), key.getNotAfter()) > 0) {
                return pipeline.refuse(CHECK_KEY_TRUSTED, "key '" + keyId
                        + "' was no longer valid at the claimed issuance time");
            }
            try {
                resolved.put(keyId, RecordKeys.publicKey(key.getPublicKey().toByteArray()));
            } catch (IllegalArgumentException e) {
                return pipeline.refuse(CHECK_KEY_TRUSTED, "key '" + keyId
                        + "' does not decode as an Ed25519 public key");
            }
        }
        pipeline.pass(CHECK_KEY_TRUSTED, signatureKeyIds.size()
                + " key(s) resolved under issuer '" + manifest.getIssuer() + "'");

        // signature-valid
        for (RecordSignature signature : container.getSignaturesList()) {
            if (!signatureVerifies(resolved.get(signature.getKeyId()), manifestBytes,
                    signature.getSignature().toByteArray())) {
                return pipeline.refuse(CHECK_SIGNATURE_VALID, "signature by key '"
                        + signature.getKeyId() + "' does not verify over the manifest bytes");
            }
        }
        pipeline.pass(CHECK_SIGNATURE_VALID,
                container.getSignaturesCount() + " signature(s) verified");

        // issuer-authorized
        String kind = manifest.getSubject().getKind();
        if (!issuer.getSubjectKindsList().contains(kind)) {
            return pipeline.refuse(CHECK_ISSUER_AUTHORIZED, "issuer '" + manifest.getIssuer()
                    + "' is not authorized for subject kind '" + kind + "'");
        }
        pipeline.pass(CHECK_ISSUER_AUTHORIZED, "authorized for '" + kind + "'");

        // completeness-consistent
        Completeness completeness = manifest.getCompleteness();
        boolean complete =
                completeness.getStatus() == CompletenessStatus.COMPLETENESS_STATUS_COMPLETE;
        if (complete && completeness.getMissingReasonsCount() > 0) {
            return pipeline.refuse(CHECK_COMPLETENESS_CONSISTENT,
                    "a complete record must not carry missing-evidence reasons");
        }
        if (!complete && completeness.getMissingReasonsCount() == 0) {
            return pipeline.refuse(CHECK_COMPLETENESS_CONSISTENT,
                    "a record that is not complete must say what is missing and why");
        }
        pipeline.pass(CHECK_COMPLETENESS_CONSISTENT, completeness.getStatus().name()
                + " against policy '" + completeness.getPolicyId() + "'");

        // artifact-rehash
        if (artifactBytes == null) {
            pipeline.skip(CHECK_ARTIFACT_REHASH, "no artifact bytes supplied");
            return pipeline.result();
        }
        List<RecordArtifact> references = new ArrayList<>(manifest.getArtifactsList());
        for (RecordStep step : manifest.getStepsList()) {
            if (step.hasRequestArtifact()) {
                references.add(step.getRequestArtifact());
            }
            if (step.hasResponseArtifact()) {
                references.add(step.getResponseArtifact());
            }
        }
        for (RecordArtifact reference : references) {
            byte[] bytes = artifactBytes.get(reference.getSha256());
            if (bytes == null) {
                return pipeline.refuse(CHECK_ARTIFACT_REHASH,
                        "artifact " + reference.getSha256() + " was not supplied");
            }
            if (bytes.length != reference.getSizeBytes()) {
                return pipeline.refuse(CHECK_ARTIFACT_REHASH, "artifact "
                        + reference.getSha256() + " is " + bytes.length
                        + " bytes; the record says " + reference.getSizeBytes());
            }
            if (!WorkRecords.sha256Hex(bytes).equals(reference.getSha256())) {
                return pipeline.refuse(CHECK_ARTIFACT_REHASH, "artifact "
                        + reference.getSha256() + " does not match its digest");
            }
        }
        pipeline.pass(CHECK_ARTIFACT_REHASH,
                references.size() + " artifact(s) matched digest and size");
        return pipeline.result();
    }

    private static Map<String, TrustedIssuer> indexSnapshot(TrustSnapshot trust) {
        if (trust == null) {
            throw new IllegalArgumentException("trust snapshot must not be null");
        }
        ValidationResult rules = ProtoValidator.create().validate(trust);
        if (!rules.valid()) {
            throw new IllegalArgumentException(
                    "trust snapshot is invalid: " + violations(rules));
        }
        Map<String, TrustedIssuer> issuers = new LinkedHashMap<>();
        for (TrustedIssuer issuer : trust.getIssuersList()) {
            if (issuers.put(issuer.getIssuer(), issuer) != null) {
                throw new IllegalArgumentException(
                        "trust snapshot duplicates issuer '" + issuer.getIssuer() + "'");
            }
            Set<String> keyIds = new HashSet<>();
            for (TrustedKey key : issuer.getKeysList()) {
                if (!keyIds.add(key.getKeyId())) {
                    throw new IllegalArgumentException("trust snapshot duplicates key '"
                            + key.getKeyId() + "' under issuer '" + issuer.getIssuer() + "'");
                }
            }
        }
        return issuers;
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

    private static int compare(Timestamp left, Timestamp right) {
        int seconds = Long.compare(left.getSeconds(), right.getSeconds());
        return seconds != 0 ? seconds : Integer.compare(left.getNanos(), right.getNanos());
    }

    private static String violations(ValidationResult result) {
        return result.violations().stream()
                .map(violation -> violation.path() + ": " + violation.message())
                .collect(Collectors.joining("; "));
    }

    /** Accumulates checks in order and freezes into a {@link Verification}. */
    private static final class Pipeline {
        private final List<Check> checks = new ArrayList<>();
        private final List<String> nonClaims;
        private String digest = "";
        private WorkRecord manifest;

        private Pipeline(List<String> nonClaims) {
            this.nonClaims = nonClaims;
        }

        private void digest(String value) {
            digest = value;
        }

        private void manifest(WorkRecord value) {
            manifest = value;
        }

        private void pass(String id, String detail) {
            checks.add(new Check(id, Status.PASSED, detail));
        }

        private void skip(String id, String detail) {
            checks.add(new Check(id, Status.SKIPPED, detail));
        }

        private Verification refuse(String id, String detail) {
            checks.add(new Check(id, Status.FAILED, detail));
            return result();
        }

        private Verification result() {
            return new Verification(checks, nonClaims, digest, manifest);
        }
    }
}
