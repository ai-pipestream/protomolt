package ai.protomolt.proto.repo.spi;

/**
 * Trusted host policy for administrator purge. Implementations check both
 * administrator status and the target account's write ACL. A remote authority
 * outage throws {@link RepositoryException} with code UNAVAILABLE, never denial.
 * Writer grants and process authority cannot authorize purge.
 */
@FunctionalInterface
public interface PurgeAuthorization {
    Decision evaluate(RepositoryCaller caller, String accountId);

    /** Both conditions must hold. Administrator status does not bypass the account ACL. */
    record Decision(boolean administrator, boolean accountWriteAllowed) {}
}
