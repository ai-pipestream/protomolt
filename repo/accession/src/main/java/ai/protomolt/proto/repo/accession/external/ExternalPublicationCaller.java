package ai.protomolt.proto.repo.accession.external;

import ai.protomolt.proto.repo.spi.RepositoryCaller;

/**
 * The caller an {@link ExternalPublicationCoordinator} call acts for: the same identity the
 * embedding host forwards to {@code DocumentService} on that call. The repository authorizes
 * the coordinator's reads; this identity is what the coordinator checks itself, such as the
 * WRITE access that publishing a source document requires.
 */
@FunctionalInterface
public interface ExternalPublicationCaller {

    /**
     * Returns the caller of the call in progress.
     *
     * @return the resolved repository caller, never {@code null}
     */
    RepositoryCaller current();
}
