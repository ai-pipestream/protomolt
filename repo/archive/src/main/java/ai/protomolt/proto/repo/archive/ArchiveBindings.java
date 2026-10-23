package ai.protomolt.proto.repo.archive;

import java.util.Optional;

/**
 * Looks up the {@link ArchiveBinding} of an account's archive. Where archive
 * definitions are recorded once the ledger retires is a follow-up under #535;
 * this seam keeps that choice out of the translator.
 */
@FunctionalInterface
public interface ArchiveBindings {

    /**
     * @return the archive's binding, or empty when the account has no such archive
     */
    Optional<ArchiveBinding> find(String accountId, String archive);
}
