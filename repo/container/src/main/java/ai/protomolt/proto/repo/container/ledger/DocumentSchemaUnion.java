package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** Operation-wide semantic limits shared by successful proofs and invalid-value assessments. */
final class DocumentSchemaUnion {
    private final Map<String, ByteString> artifacts = new TreeMap<>();
    private long artifactBytes;
    private long evidenceBytes;
    private int roots;

    /** The caller abandons this accumulator after any failure; bytes remain owned by its members. */
    void add(List<DocumentSchemaAdmission.RootEvidence> memberRoots, Map<String, ByteString> memberArtifacts, Runnable control) {
        if (memberRoots.size() > 4096 - roots) throw new IllegalArgumentException("Operation schema root count exceeds limit");
        roots += memberRoots.size();
        for (var root : memberRoots) {
            control.run();
            if (root.encoded().bytes().size() > 64L * 1024 * 1024 - evidenceBytes)
                throw new IllegalArgumentException("Operation schema evidence bytes exceed limit");
            evidenceBytes += root.encoded().bytes().size();
        }
        for (var asset : memberArtifacts.entrySet()) {
            control.run();
            var prior = artifacts.get(asset.getKey());
            if (prior != null) {
                if (!prior.equals(asset.getValue())) throw new IllegalArgumentException("Schema artifact digest collision");
            } else {
                if (artifacts.size() >= RepositorySchemaArtifacts.MAX_ARTIFACTS
                        || asset.getValue().size() > RepositorySchemaArtifacts.MAX_BATCH_BYTES - artifactBytes)
                    throw new IllegalArgumentException("Operation schema artifact union exceeds limit");
                artifacts.put(asset.getKey(), asset.getValue()); artifactBytes += asset.getValue().size();
            }
        }
    }
    Map<String, ByteString> artifacts() { return Map.copyOf(artifacts); }
}
