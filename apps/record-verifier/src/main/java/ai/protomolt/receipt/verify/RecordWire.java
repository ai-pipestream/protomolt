package ai.protomolt.receipt.verify;

import ai.protomolt.receipt.verify.Wire.MalformedException;
import ai.protomolt.receipt.verify.Wire.Notes;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The receipt wire contract, restated by hand: typed models and strict parsers for the
 * signed container, the manifest, and the trust snapshot, matching
 * {@code ai/protomolt/proto/receipt/v1/receipt.proto} field for field. A schema change
 * there is a change here — the conformance corpus cross-check is what keeps the two honest.
 */
final class RecordWire {

    private RecordWire() {
    }

    record Ts(long seconds, int nanos) {
        static final Ts ZERO = new Ts(0, 0);

        int compareTo(Ts other) {
            int bySeconds = Long.compare(seconds, other.seconds);
            return bySeconds != 0 ? bySeconds : Integer.compare(nanos, other.nanos);
        }
    }

    record Signed(byte[] manifest, List<Sig> signatures) {
    }

    record Sig(String keyId, long algorithm, byte[] signature) {
    }

    record Manifest(long version, String recordId, String issuer, String keyId, Ts issuedAt,
                    boolean hasIssuedAt, Subject subject, List<Step> steps,
                    List<Artifact> artifacts, Completeness completeness, String prior,
                    Disclosure disclosure) {
    }

    record Subject(String kind, String workflowName, String workflowVersion,
                   String workflowFingerprint, String runId, String taskId,
                   String workerId, String specSha256) {
    }

    record Step(String name, String method, long outcome, Artifact requestArtifact,
                Artifact responseArtifact, long promptTokens, long completionTokens,
                String model, String modelVersion, String summary) {
    }

    record Artifact(String sha256, String mediaType, long sizeBytes, boolean redacted) {
    }

    record Completeness(long status, List<String> missingReasons, String policyId,
                        String policyVersion, String policySha256) {
    }

    record Disclosure(String sourceManifestSha256, String policy) {
    }

    record Trust(List<Issuer> issuers) {
    }

    record Issuer(String issuer, List<Key> keys, List<String> subjectKinds) {
    }

    record Key(String keyId, long algorithm, byte[] publicKey, long state,
               Ts notBefore, boolean hasNotBefore, Ts notAfter, boolean hasNotAfter) {
    }

    static Signed signed(byte[] bytes, Notes notes) throws MalformedException {
        Wire wire = new Wire(bytes, notes, "");
        byte[] manifest = new byte[0];
        List<Sig> signatures = new ArrayList<>();
        Set<Integer> seen = new HashSet<>();
        while (wire.hasMore()) {
            int tag = wire.readTag();
            int number = tag >>> 3;
            int wireType = tag & 7;
            wire.ordered(number);
            switch (number) {
                case 1 -> {
                    if (expectLen(wire, number, wireType, seen)) {
                        manifest = wire.readLengthDelimited("manifest").bytes();
                        if (manifest.length == 0) {
                            wire.noteNonCanonical("manifest encodes its default value");
                        }
                    } else {
                        wire.skip(wireType, number, "manifest");
                    }
                }
                case 2 -> {
                    if (wireType == 2) {
                        signatures.add(signature(wire.readLengthDelimited("signatures")));
                    } else {
                        wire.noteUnknown(number);
                        wire.skip(wireType, number, "signatures");
                    }
                }
                default -> {
                    wire.noteUnknown(number);
                    wire.skip(wireType, number, "field " + number);
                }
            }
        }
        return new Signed(manifest, signatures);
    }

    private static Sig signature(Wire wire) throws MalformedException {
        String keyId = "";
        long algorithm = 0;
        byte[] signature = new byte[0];
        Set<Integer> seen = new HashSet<>();
        while (wire.hasMore()) {
            int tag = wire.readTag();
            int number = tag >>> 3;
            int wireType = tag & 7;
            wire.ordered(number);
            switch (number) {
                case 1 -> keyId = string(wire, number, wireType, seen, "key_id");
                case 2 -> algorithm = varint(wire, number, wireType, seen, "algorithm");
                case 3 -> {
                    if (expectLen(wire, number, wireType, seen)) {
                        signature = wire.readLengthDelimited("signature").bytes();
                        if (signature.length == 0) {
                            wire.noteNonCanonical("signature encodes its default value");
                        }
                    } else {
                        wire.skip(wireType, number, "signature");
                    }
                }
                default -> {
                    wire.noteUnknown(number);
                    wire.skip(wireType, number, "field " + number);
                }
            }
        }
        return new Sig(keyId, algorithm, signature);
    }

    static Manifest manifest(byte[] bytes, Notes notes) throws MalformedException {
        Wire wire = new Wire(bytes, notes, "");
        long version = 0;
        String recordId = "";
        String issuer = "";
        String keyId = "";
        Ts issuedAt = Ts.ZERO;
        boolean hasIssuedAt = false;
        Subject subject = null;
        List<Step> steps = new ArrayList<>();
        List<Artifact> artifacts = new ArrayList<>();
        Completeness completeness = null;
        String prior = "";
        Disclosure disclosure = null;
        Set<Integer> seen = new HashSet<>();
        while (wire.hasMore()) {
            int tag = wire.readTag();
            int number = tag >>> 3;
            int wireType = tag & 7;
            wire.ordered(number);
            switch (number) {
                case 1 -> version = varint(wire, number, wireType, seen, "manifest_version");
                case 2 -> recordId = string(wire, number, wireType, seen, "record_id");
                case 3 -> issuer = string(wire, number, wireType, seen, "issuer");
                case 4 -> keyId = string(wire, number, wireType, seen, "key_id");
                case 5 -> {
                    if (expectLen(wire, number, wireType, seen)) {
                        issuedAt = timestamp(wire.readLengthDelimited("issued_at"));
                        hasIssuedAt = true;
                    } else {
                        wire.skip(wireType, number, "issued_at");
                    }
                }
                case 6 -> {
                    if (expectLen(wire, number, wireType, seen)) {
                        subject = subject(wire.readLengthDelimited("subject"));
                    } else {
                        wire.skip(wireType, number, "subject");
                    }
                }
                case 7 -> {
                    if (wireType == 2) {
                        steps.add(step(wire.readLengthDelimited("steps")));
                    } else {
                        wire.noteUnknown(number);
                        wire.skip(wireType, number, "steps");
                    }
                }
                case 8 -> {
                    if (wireType == 2) {
                        artifacts.add(artifact(wire.readLengthDelimited("artifacts")));
                    } else {
                        wire.noteUnknown(number);
                        wire.skip(wireType, number, "artifacts");
                    }
                }
                case 9 -> {
                    if (expectLen(wire, number, wireType, seen)) {
                        completeness = completeness(wire.readLengthDelimited("completeness"));
                    } else {
                        wire.skip(wireType, number, "completeness");
                    }
                }
                case 10 -> prior = string(wire, number, wireType, seen,
                        "prior_manifest_sha256");
                case 11 -> {
                    if (expectLen(wire, number, wireType, seen)) {
                        disclosure = disclosure(wire.readLengthDelimited("disclosure"));
                    } else {
                        wire.skip(wireType, number, "disclosure");
                    }
                }
                default -> {
                    wire.noteUnknown(number);
                    wire.skip(wireType, number, "field " + number);
                }
            }
        }
        return new Manifest(version, recordId, issuer, keyId, issuedAt, hasIssuedAt, subject,
                steps, artifacts, completeness, prior, disclosure);
    }

    private static Subject subject(Wire wire) throws MalformedException {
        String kind = "";
        String name = "";
        String version = "";
        String fingerprint = "";
        String runId = "";
        String taskId = "";
        String workerId = "";
        String specSha256 = "";
        Set<Integer> seen = new HashSet<>();
        while (wire.hasMore()) {
            int tag = wire.readTag();
            int number = tag >>> 3;
            int wireType = tag & 7;
            wire.ordered(number);
            switch (number) {
                case 1 -> kind = string(wire, number, wireType, seen, "kind");
                case 2 -> name = string(wire, number, wireType, seen, "workflow_name");
                case 3 -> version = string(wire, number, wireType, seen, "workflow_version");
                case 4 -> fingerprint = string(wire, number, wireType, seen,
                        "workflow_fingerprint");
                case 5 -> runId = string(wire, number, wireType, seen, "run_id");
                case 6 -> taskId = string(wire, number, wireType, seen, "task_id");
                case 7 -> workerId = string(wire, number, wireType, seen, "worker_id");
                case 8 -> specSha256 = string(wire, number, wireType, seen, "spec_sha256");
                default -> {
                    wire.noteUnknown(number);
                    wire.skip(wireType, number, "field " + number);
                }
            }
        }
        return new Subject(kind, name, version, fingerprint, runId, taskId, workerId,
                specSha256);
    }

    private static Step step(Wire wire) throws MalformedException {
        String name = "";
        String method = "";
        long outcome = 0;
        Ts started = null;
        Ts completed = null;
        Artifact request = null;
        Artifact response = null;
        long promptTokens = 0;
        long completionTokens = 0;
        String model = "";
        String modelVersion = "";
        String summary = "";
        Set<Integer> seen = new HashSet<>();
        while (wire.hasMore()) {
            int tag = wire.readTag();
            int number = tag >>> 3;
            int wireType = tag & 7;
            wire.ordered(number);
            switch (number) {
                case 1 -> name = string(wire, number, wireType, seen, "name");
                case 2 -> method = string(wire, number, wireType, seen, "method");
                case 3 -> outcome = varint(wire, number, wireType, seen, "outcome");
                case 4 -> {
                    if (expectLen(wire, number, wireType, seen)) {
                        started = timestamp(wire.readLengthDelimited("started_at"));
                    } else {
                        wire.skip(wireType, number, "started_at");
                    }
                }
                case 5 -> {
                    if (expectLen(wire, number, wireType, seen)) {
                        completed = timestamp(wire.readLengthDelimited("completed_at"));
                    } else {
                        wire.skip(wireType, number, "completed_at");
                    }
                }
                case 6 -> {
                    if (expectLen(wire, number, wireType, seen)) {
                        request = artifact(wire.readLengthDelimited("request_artifact"));
                    } else {
                        wire.skip(wireType, number, "request_artifact");
                    }
                }
                case 7 -> {
                    if (expectLen(wire, number, wireType, seen)) {
                        response = artifact(wire.readLengthDelimited("response_artifact"));
                    } else {
                        wire.skip(wireType, number, "response_artifact");
                    }
                }
                case 8 -> promptTokens = varint(wire, number, wireType, seen, "prompt_tokens");
                case 9 -> completionTokens = varint(wire, number, wireType, seen,
                        "completion_tokens");
                case 10 -> model = string(wire, number, wireType, seen, "model");
                case 11 -> modelVersion = string(wire, number, wireType, seen, "model_version");
                case 12 -> summary = string(wire, number, wireType, seen, "summary");
                default -> {
                    wire.noteUnknown(number);
                    wire.skip(wireType, number, "field " + number);
                }
            }
        }
        return new Step(name, method, outcome, request, response, promptTokens,
                completionTokens, model, modelVersion, summary);
    }

    private static Artifact artifact(Wire wire) throws MalformedException {
        String sha256 = "";
        String mediaType = "";
        long sizeBytes = 0;
        boolean redacted = false;
        Set<Integer> seen = new HashSet<>();
        while (wire.hasMore()) {
            int tag = wire.readTag();
            int number = tag >>> 3;
            int wireType = tag & 7;
            wire.ordered(number);
            switch (number) {
                case 1 -> sha256 = string(wire, number, wireType, seen, "sha256");
                case 2 -> mediaType = string(wire, number, wireType, seen, "media_type");
                case 3 -> sizeBytes = varint(wire, number, wireType, seen, "size_bytes");
                case 4 -> redacted = varint(wire, number, wireType, seen, "redacted") != 0;
                default -> {
                    wire.noteUnknown(number);
                    wire.skip(wireType, number, "field " + number);
                }
            }
        }
        return new Artifact(sha256, mediaType, sizeBytes, redacted);
    }

    private static Completeness completeness(Wire wire) throws MalformedException {
        long status = 0;
        List<String> reasons = new ArrayList<>();
        String policyId = "";
        String policyVersion = "";
        String policySha256 = "";
        Set<Integer> seen = new HashSet<>();
        while (wire.hasMore()) {
            int tag = wire.readTag();
            int number = tag >>> 3;
            int wireType = tag & 7;
            wire.ordered(number);
            switch (number) {
                case 1 -> status = varint(wire, number, wireType, seen, "status");
                case 2 -> {
                    if (wireType == 2) {
                        reasons.add(wire.readLengthDelimited("missing_reasons")
                                .utf8("missing_reasons"));
                    } else {
                        wire.noteUnknown(number);
                        wire.skip(wireType, number, "missing_reasons");
                    }
                }
                case 3 -> policyId = string(wire, number, wireType, seen, "policy_id");
                case 4 -> policyVersion = string(wire, number, wireType, seen,
                        "policy_version");
                case 5 -> policySha256 = string(wire, number, wireType, seen, "policy_sha256");
                default -> {
                    wire.noteUnknown(number);
                    wire.skip(wireType, number, "field " + number);
                }
            }
        }
        return new Completeness(status, reasons, policyId, policyVersion, policySha256);
    }

    private static Disclosure disclosure(Wire wire) throws MalformedException {
        String source = "";
        String policy = "";
        Set<Integer> seen = new HashSet<>();
        while (wire.hasMore()) {
            int tag = wire.readTag();
            int number = tag >>> 3;
            int wireType = tag & 7;
            wire.ordered(number);
            switch (number) {
                case 1 -> source = string(wire, number, wireType, seen,
                        "source_manifest_sha256");
                case 2 -> policy = string(wire, number, wireType, seen, "policy");
                default -> {
                    wire.noteUnknown(number);
                    wire.skip(wireType, number, "field " + number);
                }
            }
        }
        return new Disclosure(source, policy);
    }

    static Trust trust(byte[] bytes, Notes notes) throws MalformedException {
        Wire wire = new Wire(bytes, notes, "");
        List<Issuer> issuers = new ArrayList<>();
        while (wire.hasMore()) {
            int tag = wire.readTag();
            int number = tag >>> 3;
            int wireType = tag & 7;
            wire.ordered(number);
            if (number == 1 && wireType == 2) {
                issuers.add(issuer(wire.readLengthDelimited("issuers")));
            } else {
                wire.noteUnknown(number);
                wire.skip(wireType, number, "field " + number);
            }
        }
        return new Trust(issuers);
    }

    private static Issuer issuer(Wire wire) throws MalformedException {
        String name = "";
        List<Key> keys = new ArrayList<>();
        List<String> kinds = new ArrayList<>();
        Set<Integer> seen = new HashSet<>();
        while (wire.hasMore()) {
            int tag = wire.readTag();
            int number = tag >>> 3;
            int wireType = tag & 7;
            wire.ordered(number);
            switch (number) {
                case 1 -> name = string(wire, number, wireType, seen, "issuer");
                case 2 -> {
                    if (wireType == 2) {
                        keys.add(key(wire.readLengthDelimited("keys")));
                    } else {
                        wire.noteUnknown(number);
                        wire.skip(wireType, number, "keys");
                    }
                }
                case 3 -> {
                    if (wireType == 2) {
                        kinds.add(wire.readLengthDelimited("subject_kinds")
                                .utf8("subject_kinds"));
                    } else {
                        wire.noteUnknown(number);
                        wire.skip(wireType, number, "subject_kinds");
                    }
                }
                default -> {
                    wire.noteUnknown(number);
                    wire.skip(wireType, number, "field " + number);
                }
            }
        }
        return new Issuer(name, keys, kinds);
    }

    private static Key key(Wire wire) throws MalformedException {
        String keyId = "";
        long algorithm = 0;
        byte[] publicKey = new byte[0];
        long state = 0;
        Ts notBefore = Ts.ZERO;
        boolean hasNotBefore = false;
        Ts notAfter = Ts.ZERO;
        boolean hasNotAfter = false;
        Set<Integer> seen = new HashSet<>();
        while (wire.hasMore()) {
            int tag = wire.readTag();
            int number = tag >>> 3;
            int wireType = tag & 7;
            wire.ordered(number);
            switch (number) {
                case 1 -> keyId = string(wire, number, wireType, seen, "key_id");
                case 2 -> algorithm = varint(wire, number, wireType, seen, "algorithm");
                case 3 -> {
                    if (expectLen(wire, number, wireType, seen)) {
                        publicKey = wire.readLengthDelimited("public_key").bytes();
                        if (publicKey.length == 0) {
                            wire.noteNonCanonical("public_key encodes its default value");
                        }
                    } else {
                        wire.skip(wireType, number, "public_key");
                    }
                }
                case 4 -> state = varint(wire, number, wireType, seen, "state");
                case 5 -> {
                    if (expectLen(wire, number, wireType, seen)) {
                        notBefore = timestamp(wire.readLengthDelimited("not_before"));
                        hasNotBefore = true;
                    } else {
                        wire.skip(wireType, number, "not_before");
                    }
                }
                case 6 -> {
                    if (expectLen(wire, number, wireType, seen)) {
                        notAfter = timestamp(wire.readLengthDelimited("not_after"));
                        hasNotAfter = true;
                    } else {
                        wire.skip(wireType, number, "not_after");
                    }
                }
                default -> {
                    wire.noteUnknown(number);
                    wire.skip(wireType, number, "field " + number);
                }
            }
        }
        return new Key(keyId, algorithm, publicKey, state, notBefore, hasNotBefore,
                notAfter, hasNotAfter);
    }

    private static Ts timestamp(Wire wire) throws MalformedException {
        long seconds = 0;
        long nanos = 0;
        Set<Integer> seen = new HashSet<>();
        while (wire.hasMore()) {
            int tag = wire.readTag();
            int number = tag >>> 3;
            int wireType = tag & 7;
            wire.ordered(number);
            switch (number) {
                case 1 -> seconds = varint(wire, number, wireType, seen, "seconds");
                case 2 -> nanos = varint(wire, number, wireType, seen, "nanos");
                default -> {
                    wire.noteUnknown(number);
                    wire.skip(wireType, number, "field " + number);
                }
            }
        }
        return new Ts(seconds, (int) nanos);
    }

    /** A singular string field: claims the slot, notes explicit defaults, decodes UTF-8. */
    private static String string(Wire wire, int number, int wireType, Set<Integer> seen,
                                 String field) throws MalformedException {
        if (!expectLen(wire, number, wireType, seen)) {
            wire.skip(wireType, number, field);
            return "";
        }
        String value = wire.readLengthDelimited(field).utf8(field);
        if (value.isEmpty()) {
            wire.noteNonCanonical(field + " encodes its default value");
        }
        return value;
    }

    /** A singular varint field: claims the slot and notes explicit zero. */
    private static long varint(Wire wire, int number, int wireType, Set<Integer> seen,
                               String field) throws MalformedException {
        if (wireType != 0) {
            wire.noteUnknown(number);
            wire.skip(wireType, number, field);
            return 0;
        }
        if (!seen.add(number)) {
            wire.noteNonCanonical(field + " appears more than once");
        }
        long value = wire.readVarint(field);
        if (value == 0) {
            wire.noteNonCanonical(field + " encodes its default value");
        }
        return value;
    }

    /**
     * Claims a singular length-delimited slot. Returns false (and records the unknown)
     * when the wire type is wrong. Emptiness is the caller's to judge: a present-but-empty
     * message is canonical, while an empty string or bytes field is its default value, so
     * only the scalar callers note it.
     */
    private static boolean expectLen(Wire wire, int number, int wireType, Set<Integer> seen) {
        if (wireType != 2) {
            wire.noteUnknown(number);
            return false;
        }
        if (!seen.add(number)) {
            wire.noteNonCanonical("field " + number + " appears more than once");
        }
        return true;
    }
}
