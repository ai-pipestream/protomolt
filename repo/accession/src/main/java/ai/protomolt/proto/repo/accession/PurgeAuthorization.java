package ai.protomolt.proto.repo.accession;

import ai.protomolt.proto.repo.spi.RepositoryCaller;

/**
 * Required trusted-host policy for purge. Implementations consult the host's
 * administrator directory and the target account's write ACL, not document ACLs
 * or writer grants. A policy lookup failure must propagate, never become approval.
 */
@FunctionalInterface
public interface PurgeAuthorization {
    Decision evaluate(RepositoryCaller caller, String accountId);

    /** Both independent conditions must hold; administrator status does not bypass the ACL. */
    record Decision(boolean administrator, boolean accountWriteAllowed) {}
}
