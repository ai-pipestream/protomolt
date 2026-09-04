package ai.protomolt.proto.mesh;

import ai.protomolt.proto.mesh.runtime.v1.ProcessorContract;
import ai.protomolt.proto.mesh.v1.SchemaReference;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/** Canonical identity operations for the shared processor contract. */
public final class ProcessorContracts {

    private static final Comparator<SchemaReference> SCHEMA_ORDER =
            Comparator.comparing(SchemaReference::getTypeName)
                    .thenComparing(SchemaReference::getDescriptorFingerprint);

    private ProcessorContracts() {
    }

    /** Returns a canonical contract with sorted output schemas and its fingerprint bound. */
    public static ProcessorContract canonical(ProcessorContract contract) {
        Objects.requireNonNull(contract, "contract");
        List<SchemaReference> outputs = new ArrayList<>(contract.getOutputSchemasList());
        outputs.sort(SCHEMA_ORDER);
        ProcessorContract unsigned = contract.toBuilder()
                .clearOutputSchemas()
                .addAllOutputSchemas(outputs)
                .clearContractFingerprint()
                .build();
        String fingerprint = MeshDigest.sha256(unsigned.toByteArray());
        if (!contract.getContractFingerprint().isBlank()
                && !contract.getContractFingerprint().equals(fingerprint)) {
            throw new IllegalArgumentException("processor contract fingerprint does not match "
                    + "the canonical contract for '" + contract.getProcessorId() + "'");
        }
        return unsigned.toBuilder().setContractFingerprint(fingerprint).build();
    }

    /** Returns the canonical lowercase SHA-256 fingerprint of a processor contract. */
    public static String fingerprint(ProcessorContract contract) {
        return canonical(contract).getContractFingerprint();
    }

    /** Requires both contracts to have the same canonical bytes and fingerprint. */
    public static boolean exactMatch(ProcessorContract first, ProcessorContract second) {
        return canonical(first).equals(canonical(second));
    }
}
