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
        // Reject oversized/unknown/invalid input before allocating per-path canonical buffers.
        DocumentSchemaEvidenceCodec.measureAndValidate(evidence, control);
        var root = evidence.getOccurrencesList().stream().filter(p -> p.getStepsCount() == 1)
                .findFirst().orElseThrow().getSteps(0).getAnyBoundary();
        var paths = new ArrayList<Path>();
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
                if (step.hasAnyBoundary()) textBytes += step.getAnyBoundary().getTypeUrlBytes().size();
                if (step.hasMapKey() && step.getMapKey().hasStringValue()) textBytes += step.getMapKey().getStringValueBytes().size();
                if (textBytes > DocumentSchemaOccurrences.Limits.DEFAULT.maxTextBytes())
                    throw new IllegalArgumentException("aggregate evidence path text exceeds limit");
            }
            paths.add(new Path(path, DocumentSchemaOccurrenceCodec.encode(path, control).bytes()));
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
        var encoded = DocumentSchemaEvidenceCodec.encode(canonical.build(), control);
        return new Encoded(encoded.bytes(), encoded.sha256());
    }

    static DocumentRootSchemaEvidence decode(String codec, int version, ByteString bytes, String sha256,
            Runnable control) throws InvalidProtocolBufferException {
        var evidence = DocumentSchemaEvidenceCodec.decode(CODEC, codec, version, bytes, sha256,
                DocumentRootSchemaEvidence.getDescriptor(), DocumentRootSchemaEvidence.parser(), control);
        var canonical = encode(evidence, control);
        if (!canonical.bytes().equals(bytes)) throw new IllegalArgumentException("noncanonical evidence occurrence ordering");
        return evidence;
    }

    private static void active(Runnable control) {
        if (Thread.currentThread().isInterrupted()) throw new CancellationException("schema evidence encoding interrupted");
        control.run();
    }
}
