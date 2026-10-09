package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentSchemaMaterialization;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import ai.protomolt.proto.repo.v1.NodeAddress;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CancellationException;

/** Selected decoding from a sealed SQL snapshot; the caller owns the revision read pin. */
final class DocumentHistoricalMaterializer {
    private final Tx tx;
    DocumentHistoricalMaterializer(Tx tx) { this.tx = tx; }
    private static final class ControlFailure extends RuntimeException {
        final RuntimeException original;
        ControlFailure(RuntimeException original) { this.original = original; }
    }

    DocumentSchemaMaterialization.Result read(RepositoryCaller caller, NodeAddress address, UUID revision,
            int ordinal, ByteString fragment, DocumentSchemaMaterialization.Selection selection,
            DocumentSchemaMaterialization.Limits limits, PayloadBudget budget, RepositoryReadControl control) {
        var snapshotLeases = new ArrayList<PayloadBudget.Lease>();
        DocumentSchemaMaterialization.Result decoded = null;
        boolean transferred = false;
        Runnable active = () -> {
            try { control.check(); } catch (RuntimeException failure) { throw new ControlFailure(failure); }
        };
        try {
            active.run();
            var snapshot = tx.inTransaction(em -> {
                DocumentAdmissionAuthorization.authorizeHistory(em, caller, address);
                return DocumentHistoricalSchemaRows.capture(em, address, revision, active,
                        bytes -> snapshotLeases.add(reserve(budget, bytes)));
            });
            final DocumentHistoricalSchemaBinding binding;
            try { binding = DocumentHistoricalSchemaBinding.read(address, snapshot, active); }
            catch (InvalidProtocolBufferException | IllegalArgumentException failure) {
                throw new RepositoryException(RepositoryException.Code.DATA_LOSS, "Historical schema binding is invalid", failure);
            }
            var roots = snapshot.roots().stream().filter(root -> root.ordinal() == ordinal
                    && root.locatorSha().equals(selection.rootSha256())).toList();
            if (roots.isEmpty()) throw new RepositoryException(RepositoryException.Code.NOT_FOUND, "Historical occurrence is unavailable");
            if (roots.size() != 1) throw new RepositoryException(RepositoryException.Code.DATA_LOSS, "Historical root association is ambiguous");
            var row = roots.getFirst();
            var input = new DocumentSchemaMaterialization.Input(binding.member(), ordinal, fragment,
                    new DocumentSchemaMaterialization.Root(row.locatorSha(), row.fragmentSha(), row.fragmentSize(), row.evidence()),
                    binding.container(), binding.references());
            try {
                decoded = DocumentSchemaMaterialization.read(input, selection,
                        hash -> Optional.ofNullable(snapshot.artifacts().get(hash)), limits,
                        bytes -> { var lease = reserve(budget, bytes); return lease::close; }, active);
            } catch (DocumentSchemaMaterialization.OccurrenceNotFound failure) {
                throw new RepositoryException(RepositoryException.Code.NOT_FOUND, "Historical occurrence is unavailable");
            } catch (DocumentSchemaMaterialization.LimitExceeded failure) {
                throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED, "Historical materialization limits exceeded", failure);
            } catch (DocumentSchemaMaterialization.DataLoss failure) {
                throw new RepositoryException(RepositoryException.Code.DATA_LOSS, "Historical occurrence or retained schema is invalid", failure);
            }
            authorize(caller, address, control);
            transferred = true;
            return decoded;
        } catch (RuntimeException failure) {
            var original = failure instanceof ControlFailure guarded ? guarded.original : failure;
            if (original instanceof CancellationException) throw original;
            if (original instanceof RepositoryException repository && (repository.code() == RepositoryException.Code.CANCELLED
                    || repository.code() == RepositoryException.Code.DEADLINE_EXCEEDED)) throw original;
            // Current denial must suppress detailed decoding/storage failures as well as successful content.
            authorize(caller, address, control);
            throw original;
        } finally {
            if (!transferred && decoded != null) decoded.close();
            // The result owns private copies; the complete SQL snapshot cannot escape this method.
            for (int i = snapshotLeases.size() - 1; i >= 0; i--) snapshotLeases.get(i).close();
        }
    }

    private void authorize(RepositoryCaller caller, NodeAddress address, RepositoryReadControl control) {
        control.check();
        tx.inTransaction(em -> { DocumentAdmissionAuthorization.authorizeHistory(em, caller, address); control.check(); });
        control.check();
    }
    private static PayloadBudget.Lease reserve(PayloadBudget budget, long bytes) {
        try { return budget.reserve(bytes); }
        catch (PayloadBudget.CapacityExceededException failure) {
            throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED, "Historical materialization capacity exhausted", failure);
        }
    }
}
