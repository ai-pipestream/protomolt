package ai.protomolt.proto.repo.spi;

import ai.protomolt.proto.repo.archive.v1.ArchiveMutationReceipt;
import ai.protomolt.proto.repo.archive.v1.ArchiveMutationRequest;
import ai.protomolt.proto.repo.archive.v1.GetArchiveMutationRequest;

/** Durable archive mutations; physical reclamation proceeds independently. */
public interface ArchiveMutationRepository {
    ArchiveMutationReceipt mutateArchive(RepositoryCaller caller, ArchiveMutationRequest request);
    ArchiveMutationReceipt getArchiveMutation(RepositoryCaller caller, GetArchiveMutationRequest request);
}
