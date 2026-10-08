package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.codec.PartObject;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.Map;
import java.util.Objects;

/** Copies verified ordinary and historical inputs before their borrowed views close. */
final class DocumentHistoricalPublicationPreparation {
    private DocumentHistoricalPublicationPreparation() {}

    record Prepared(DocumentUploadCoordinator.Staged staged, DocumentPublicationAssessment.Historical assessment)
            implements AutoCloseable {
        Prepared { Objects.requireNonNull(staged); Objects.requireNonNull(assessment); }
        @Override public void close() { assessment.close(); }
    }

    @FunctionalInterface interface Assessment {
        DocumentPublicationAssessment.Historical prepare(Map<String, Map<Integer, ByteString>> ordinary,
                RepositoryReadControl control) throws InvalidProtocolBufferException;
    }

    static Prepared prepare(RepositoryCaller caller, RepositoryOperationLedger.Owner owner,
            DocumentOperationUploadAdmission.Prepared plan, DocumentOperationUploadAdmission admission,
            DocumentReadLedger reads, DocumentRetainedReader reader, DocumentUploadCoordinator uploads,
            DocumentUploadAuthority authority, Map<DocumentUploadPayloads.Key, PartObject> bodies,
            Map<String, String> attributes, Assessment assessment, RepositoryReadControl control)
            throws InvalidProtocolBufferException {
        Objects.requireNonNull(assessment); Objects.requireNonNull(control).check();
        reads.releaseDrainedAtCapacity(1);
        control.check();
        var pinned = reads.capture(admission, caller, owner, plan);
        try {
            return uploads.stageAndPrepareAuthorizedOwned(plan, bodies, attributes, control::check, authority,
                    (staged, view, active) -> {
                        var current = new RepositoryReadControl() {
                            public long remainingNanos() { return control.remainingNanos(); }
                            public boolean isCancelled() { return control.isCancelled(); }
                            public void check() { control.check(); active.run(); }
                        };
                        DocumentPublicationAssessment.Historical pending = null;
                        try (var inputs = DocumentPublicationInputs.capture(plan.plan().command(), owner, view,
                                pinned, reader, current)) {
                            pending = assessment.prepare(inputs.fragments(), current);
                            return new Prepared(staged, pending);
                        } catch (RuntimeException | Error failure) {
                            if (pending != null) {
                                try { pending.close(); }
                                catch (RuntimeException | Error cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
                            }
                            throw failure;
                        } catch (InvalidProtocolBufferException malformed) { throw new ParseFailure(malformed); }
                    });
        } catch (ParseFailure failure) {
            var malformed = (InvalidProtocolBufferException) failure.getCause();
            for (var cleanup : failure.getSuppressed()) malformed.addSuppressed(cleanup);
            throw malformed;
        } finally {
            // The ledger retains SQL pins until actual provider uses drain, including cancellation.
            pinned.close();
        }
    }

    private static final class ParseFailure extends RuntimeException {
        ParseFailure(InvalidProtocolBufferException cause) { super(cause); }
    }
}
