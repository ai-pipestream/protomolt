package ai.protomolt.proto.repo.engine;

import ai.protomolt.proto.repo.archive.v1.ArchiveMutationReceipt;
import ai.protomolt.proto.repo.archive.v1.ArchiveMutationRequest;
import ai.protomolt.proto.repo.archive.v1.GetArchiveMutationRequest;
import ai.protomolt.proto.repo.container.archive.ArchiveIds;
import ai.protomolt.proto.repo.container.archive.ArchiveLedger;
import ai.protomolt.proto.repo.container.archive.ArchiveMutationCommand;
import ai.protomolt.proto.repo.container.archive.ArchiveMutationLedger;
import ai.protomolt.proto.repo.container.archive.ArchiveMutationObservations;
import ai.protomolt.proto.repo.spi.ArchiveMutationRepository;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.validate.ProtoValidator;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;
import static ai.protomolt.proto.repo.spi.RepositoryException.Code.*;

/** Shared library/transport implementation. Only trusted process callers until archive ACL integration. */
public final class ArchiveMutationOperations implements ArchiveMutationRepository {
    private static final ProtoValidator VALIDATOR = ProtoValidator.create();
    private final ArchiveLedger archive;
    private final ArchiveMutationLedger mutations;
    private final ArchiveMutationObservations observations;

    public ArchiveMutationOperations(ArchiveLedger archive, ArchiveMutationLedger mutations, ArchiveMutationObservations observations) {
        this.archive = Objects.requireNonNull(archive);
        this.mutations = Objects.requireNonNull(mutations);
        this.observations = Objects.requireNonNull(observations);
    }

    @Override public ArchiveMutationReceipt mutateArchive(RepositoryCaller caller, ArchiveMutationRequest request) {
        RepositoryErrors.requireProcessAuthority(caller); // Includes replays, before any receipt lookup.
        if (Thread.currentThread().isInterrupted()) throw new RepositoryException(CANCELLED, "Archive mutation cancelled before admission");
        return call(() -> {
            var command = new ArchiveMutationCommand(request);
            // Replay precedes mutable archive/entry existence checks. The logical
            // outcome survives deletion of the original address or archive.
            boolean replay = mutations.find(caller.principalName(), command.address().getAccountId(), command.operationId()).isPresent();
            long revision = 0;
            if (!replay) {
                if (archive.findArchive(command.address().getAccountId(), command.address().getArchive()).isEmpty())
                    throw new RepositoryException(NOT_FOUND, "Archive does not exist");
                revision = archive.findEntry(ArchiveIds.entryUuid(command.address())).map(e -> e.mutationRevision).orElse(0L);
            }
            mutations.execute(caller.principalName(), command, revision);
            return observations.observe(caller.principalName(), command.address().getAccountId(), command.operationId())
                    .orElseThrow(() -> new IllegalStateException("Admitted archive mutation is missing"));
        });
    }

    @Override public ArchiveMutationReceipt getArchiveMutation(RepositoryCaller caller, GetArchiveMutationRequest request) {
        RepositoryErrors.requireProcessAuthority(caller);
        return call(() -> {
            if (!request.getUnknownFields().asMap().isEmpty() || !VALIDATOR.validate(request).valid())
                throw new IllegalArgumentException("Invalid archive mutation lookup");
            return observations.observe(caller.principalName(), request.getAccountId(), UUID.fromString(request.getOperationId()))
                    .orElseThrow(() -> new RepositoryException(NOT_FOUND, "Archive mutation does not exist"));
        });
    }

    private static ArchiveMutationReceipt call(Supplier<ArchiveMutationReceipt> action) {
        return RepositoryErrors.call(() -> {
            try { return action.get(); }
            catch (ArchiveMutationLedger.OperationConflictException failure) { throw new RepositoryException(CONFLICT, failure.getMessage(), failure); }
            catch (ArchiveMutationLedger.EntryMissingException failure) { throw new RepositoryException(NOT_FOUND, failure.getMessage(), failure); }
            catch (ArchiveMutationLedger.MigrationRequiredException failure) { throw new RepositoryException(FAILED_PRECONDITION, failure.getMessage(), failure); }
            catch (java.util.concurrent.CancellationException failure) { throw new RepositoryException(CANCELLED, failure.getMessage(), failure); }
            catch (IllegalStateException failure) { throw new RepositoryException(DATA_LOSS, failure.getMessage(), failure); }
        });
    }
}
