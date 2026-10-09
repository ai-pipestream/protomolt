package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.descriptors.MessageWireBudget;
import ai.protomolt.proto.repo.v1.RepositoryResolvedSchema;
import com.google.protobuf.Any;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CancellationException;

/**
 * Pure, borrowed view of one Any boundary. The host authenticates the occurrence
 * path, owns stable input/binding memory and bounds concurrent decoded heap.
 * This verifies the supplied URL/value identity, not its location in a document.
 * There is no admission verdict, recursive Any expansion, registry or retention claim.
 */
final class DocumentAnyMaterialization {
    private DocumentAnyMaterialization() {}

    enum Mode { PRESERVE, MATERIALIZE_IF_AVAILABLE }
    /** The supplied envelope does not match its selected value identity. */
    static final class IdentityMismatch extends IllegalArgumentException {
        IdentityMismatch(String message) { super(message); }
    }
    enum Reason {
        DEFINITION_MISSING, AMBIGUOUS, ACCESS_DENIED, LOOKUP_UNAVAILABLE,
        CORRUPT_DEFINITION, MALFORMED_PAYLOAD, RESOURCE_LIMIT
    }
    record Limits(int maxValueBytes, long maxWireValues, int maxDepth) {
        Limits {
            if (maxValueBytes < 0 || maxWireValues < 1 || maxDepth < 0 || maxDepth > 100)
                throw new IllegalArgumentException("invalid Any materialization limits");
        }
    }
    /** Causes are internal diagnostics; never serialize their potentially sensitive messages. */
    record Failure(Reason reason, Optional<Exception> cause) {
        Failure { Objects.requireNonNull(reason); Objects.requireNonNull(cause); }
        static Failure of(Reason reason) { return new Failure(reason, Optional.empty()); }
    }
    sealed interface Lookup permits Resolved, Unavailable {}
    /** The authorized resolver must bind exact immutable assets before constructing this value. */
    record Resolved(DocumentSchemaAssetBinding binding) implements Lookup {
        Resolved { Objects.requireNonNull(binding); }
    }
    record Unavailable(Failure failure) implements Lookup {
        Unavailable {
            Objects.requireNonNull(failure);
            if (failure.reason() == Reason.MALFORMED_PAYLOAD)
                throw new IllegalArgumentException("schema lookup cannot classify payload bytes");
        }
    }
    @FunctionalInterface interface Resolver {
        /** Null and unclassified exceptions are programming/operational failures, never absence. */
        Lookup resolve(DocumentSchemaAdmission.Selection occurrence);
    }

    sealed interface View permits Preserved, Decoded, Failed {
        Any original();
        DocumentSchemaAdmission.Selection occurrence();
    }
    record Preserved(Any original, DocumentSchemaAdmission.Selection occurrence) implements View {}
    /** Nested Any payloads remain bytes. A decoded proto2 value may still be uninitialized. */
    record Decoded(Any original, DocumentSchemaAdmission.Selection occurrence,
                   RepositoryResolvedSchema schema, DynamicMessage value,
                   ai.protomolt.proto.repo.v1.RepositorySchemaAsset metadata,
                   com.google.protobuf.ByteString descriptorArtifact) implements View {}
    record Failed(Any original, DocumentSchemaAdmission.Selection occurrence, Failure failure) implements View {}

    /**
     * Control may enforce cancellation/deadline/current access. Its exceptions propagate unchanged.
     * Preserve does not resolve or parse inner bytes. Limits bound serialized inputs and wire
     * occurrences, not JVM heap; callers own result lifetime. Oversized input fails before hashing.
     * Identity mismatch is an invalid selection, not malformed payload. Outer Any unknown fields
     * remain in original; the optional decoded view interprets only its value using the binding.
     */
    static View read(DocumentSchemaAdmission.Selection occurrence, Any original, Mode mode,
                     Limits limits, Resolver resolver, Runnable control) {
        Objects.requireNonNull(occurrence); Objects.requireNonNull(original); Objects.requireNonNull(mode);
        Objects.requireNonNull(limits); Objects.requireNonNull(resolver); Objects.requireNonNull(control);
        Runnable active = () -> {
            if (Thread.currentThread().isInterrupted()) throw new CancellationException("Any materialization interrupted");
            control.run();
        };
        active.run();
        if (!original.getTypeUrl().equals(occurrence.typeUrl())
                || original.getValue().size() != occurrence.valueSizeBytes())
            throw new IdentityMismatch("Any differs from selected URL or value size");
        if (original.getValue().size() > limits.maxValueBytes())
            return deliver(new Failed(original, occurrence, Failure.of(Reason.RESOURCE_LIMIT)), active);
        if (!digest(original, active).equals(occurrence.valueSha256()))
            throw new IdentityMismatch("Any differs from selected value digest");
        if (mode == Mode.PRESERVE) return deliver(new Preserved(original, occurrence), active);

        var lookup = Objects.requireNonNull(resolver.resolve(occurrence), "resolver returned null");
        active.run();
        if (lookup instanceof Unavailable unavailable)
            return deliver(new Failed(original, occurrence, unavailable.failure()), active);
        var binding = ((Resolved) lookup).binding();
        if (!binding.metadata().getTypeUrl().equals(original.getTypeUrl()))
            return deliver(new Failed(original, occurrence, Failure.of(Reason.CORRUPT_DEFINITION)), active);
        var schema = RepositoryResolvedSchema.newBuilder().setSchema(binding.schema().condition())
                .setArtifactSha256(binding.schema().artifactSha256()).build();
        final DynamicMessage decoded;
        // Guard only host control: an I/O- or limit-shaped host exception must not be relabeled.
        Runnable guarded = () -> {
            try { active.run(); }
            catch (RuntimeException failure) { throw new ControlFailure(failure); }
        };
        try {
            new MessageWireBudget(limits.maxWireValues(), limits.maxDepth(), guarded)
                    .check(original.getValue(), binding.schema().type());
            guarded.run();
            var input = original.getValue().newCodedInput();
            input.setRecursionLimit(limits.maxDepth());
            input.setSizeLimit(limits.maxValueBytes());
            // buildPartial preserves decoding/validation separation for proto2 required fields.
            decoded = DynamicMessage.newBuilder(binding.schema().type()).mergeFrom(input).buildPartial();
            input.checkLastTagWas(0);
            if (input.getTotalBytesRead() != original.getValue().size())
                throw new InvalidProtocolBufferException("incomplete Any payload decode");
        } catch (ControlFailure failure) {
            throw failure.original;
        } catch (MessageWireBudget.LimitExceededException failure) {
            return deliver(new Failed(original, occurrence,
                    new Failure(Reason.RESOURCE_LIMIT, Optional.of(failure))), active);
        } catch (IOException failure) {
            return deliver(new Failed(original, occurrence,
                    new Failure(Reason.MALFORMED_PAYLOAD, Optional.of(failure))), active);
        }
        return deliver(new Decoded(original, occurrence, schema, decoded, binding.metadata(), binding.schema().artifact()), active);
    }

    private static View deliver(View view, Runnable active) { active.run(); return view; }
    private static String digest(Any original, Runnable active) {
        try {
            var hash = MessageDigest.getInstance("SHA-256");
            for (var buffer : original.getValue().asReadOnlyByteBufferList()) {
                active.run(); hash.update(buffer);
            }
            active.run();
            return HexFormat.of().formatHex(hash.digest());
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 unavailable", failure);
        }
    }
    private static final class ControlFailure extends RuntimeException {
        private final RuntimeException original;
        private ControlFailure(RuntimeException original) { this.original = original; }
    }
}
