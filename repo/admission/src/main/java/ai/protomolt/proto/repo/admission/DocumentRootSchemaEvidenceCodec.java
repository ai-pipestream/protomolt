package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.repo.v1.DocumentRootSchemaEvidence;
import ai.protomolt.proto.repo.v1.RepositorySchemaOccurrencePath;
import ai.protomolt.proto.repo.v1.RepositorySchemaOccurrenceStep;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CancellationException;

/** Canonical one-root evidence set. The host still proves candidate and revision binding. */
final class DocumentRootSchemaEvidenceCodec {
    static final String CODEC = "document-root-schema-evidence";
    static final int VERSION = 1;
    private DocumentRootSchemaEvidenceCodec() {}

    record Encoded(ByteString bytes, String sha256) {}
    private record Path(RepositorySchemaOccurrencePath value, ByteString bytes) {}

    static Encoded encode(DocumentRootSchemaEvidence evidence, Runnable control) {
        return encode(evidence, DocumentSchemaEvidenceCodec.MAX_BYTES, control);
    }

    /** Apply the remaining member allowance before allocating canonical path buffers. */
    static Encoded encode(DocumentRootSchemaEvidence evidence, long remainingBytes, Runnable control) {
        var canonical = canonical(evidence, remainingBytes, null, control);
        var encoded = DocumentSchemaEvidenceCodec.encode(canonical, control);
        return new Encoded(encoded.bytes(), encoded.sha256());
    }

    /** Sorting buffers close before final output allocation; only the output lease escapes. */
    static DocumentSchemaEvidenceCodec.OwnedEncoded encodeOwned(DocumentRootSchemaEvidence evidence, long remainingBytes,
            DocumentAdmissionReservations reservations, Runnable control) {
        java.util.Objects.requireNonNull(reservations);
        var canonical = canonical(evidence, remainingBytes, reservations, control);
        return DocumentSchemaEvidenceCodec.encodeOwned(canonical, reservations, control);
    }

    private static DocumentRootSchemaEvidence canonical(DocumentRootSchemaEvidence evidence, long remainingBytes,
            DocumentAdmissionReservations reservations, Runnable control) {
        // Reject oversized/unknown/invalid input before allocating per-path canonical buffers.
        int size = DocumentSchemaEvidenceCodec.measureAndValidate(evidence, control);
        if (size > remainingBytes) throw new IllegalArgumentException("member evidence byte limit exceeded");
        var root = evidence.getOccurrencesList().stream().filter(p -> p.getStepsCount() == 1)
                .findFirst().orElseThrow().getSteps(0).getAnyBoundary();
        var paths = new ArrayList<Path>();
        var scratch = new ArrayList<DocumentSchemaEvidenceCodec.OwnedEncoded>();
        try {
            var selectors = new HashSet<List<RepositorySchemaOccurrenceStep>>();
            long steps = 0;
            long textBytes = 0;
            for (var path : evidence.getOccurrencesList()) {
                active(control);
                if (!path.getSteps(0).getAnyBoundary().equals(root))
                    throw new IllegalArgumentException("evidence paths disagree on root boundary");
                if (!selectors.add(List.copyOf(path.getStepsList().subList(0, path.getStepsCount() - 1))))
                    throw new IllegalArgumentException("duplicate or conflicting evidence occurrence selector");
                steps += path.getStepsCount();
                if (steps > DocumentSchemaOccurrences.Limits.DEFAULT.maxPathSteps())
                    throw new IllegalArgumentException("aggregate evidence path steps exceed limit");
                for (var step : path.getStepsList()) {
                    active(control);
                    if (step.hasAnyBoundary()) textBytes += DocumentSchemaEvidenceCodec.utf8Length(step.getAnyBoundary().getTypeUrl(), control);
                    if (step.hasMapKey() && step.getMapKey().hasStringValue())
                        textBytes += DocumentSchemaEvidenceCodec.utf8Length(step.getMapKey().getStringValue(), control);
                    if (textBytes > DocumentSchemaOccurrences.Limits.DEFAULT.maxTextBytes())
                        throw new IllegalArgumentException("aggregate evidence path text exceeds limit");
                }
                if (reservations == null) {
                    paths.add(new Path(path, DocumentSchemaOccurrenceCodec.encode(path, control).bytes()));
                } else {
                    var owned = DocumentSchemaEvidenceCodec.encodeOwned(path, reservations, control);
                    scratch.add(owned);
                    paths.add(new Path(path, owned.value().bytes()));
                }
            }
            var comparator = ByteString.unsignedLexicographicalComparator();
            paths.sort((a, b) -> { active(control); return comparator.compare(a.bytes(), b.bytes()); });
            var canonical = evidence.toBuilder().clearOccurrences();
            ByteString previous = null;
            for (var path : paths) {
                active(control);
                if (path.bytes().equals(previous)) throw new IllegalArgumentException("duplicate evidence occurrence path");
                canonical.addOccurrences(path.value());
                previous = path.bytes();
            }
            return canonical.build();
        } finally {
            for (var owned : scratch) owned.close();
        }
    }

    static DocumentRootSchemaEvidence decode(String codec, int version, ByteString bytes, String sha256,
            Runnable control) throws InvalidProtocolBufferException {
        var evidence = DocumentSchemaEvidenceCodec.decode(CODEC, codec, version, bytes, sha256,
                DocumentRootSchemaEvidence.getDescriptor(), DocumentRootSchemaEvidence.parser(), control);
        var canonical = encode(evidence, control);
        if (!canonical.bytes().equals(bytes)) throw new IllegalArgumentException("noncanonical evidence occurrence ordering");
        return evidence;
    }

    static DocumentRootSchemaEvidence decode(String codec, int version, ByteString bytes, String sha256,
            DocumentAdmissionReservations reservations, Runnable control) throws InvalidProtocolBufferException {
        java.util.Objects.requireNonNull(reservations);
        var evidence = DocumentSchemaEvidenceCodec.decode(CODEC, codec, version, bytes, sha256,
                DocumentRootSchemaEvidence.getDescriptor(), DocumentRootSchemaEvidence.parser(), reservations, control);
        try (var canonical = encodeOwned(evidence, DocumentSchemaEvidenceCodec.MAX_BYTES, reservations, control)) {
            if (!canonical.value().bytes().equals(bytes)) throw new IllegalArgumentException("noncanonical evidence occurrence ordering");
        }
        return evidence;
    }

    private static void active(Runnable control) {
        if (Thread.currentThread().isInterrupted()) throw new CancellationException("schema evidence encoding interrupted");
        control.run();
    }
}
