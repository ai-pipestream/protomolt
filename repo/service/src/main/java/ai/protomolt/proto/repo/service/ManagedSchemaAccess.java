package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import ai.protomolt.proto.repo.v1.DocumentPublicationMember;
import java.time.Duration;

/**
 * Optional host-bound schema resolution and its lifecycle. Implementations select
 * their registry/security context from trusted host configuration and authorize
 * every occurrence under the supplied caller, including cache hits. Request fields
 * are not access grants. Scopes are thread-confined; different calls may run concurrently.
 *
 * A successful RepoServices build takes exclusive lifecycle ownership of this
 * component. If construction fails, the caller retains cleanup responsibility.
 * After transfer, open scopes only through the owning composition.
 * Underlying registry stores remain borrowed: close them only after RepoServices
 * has successfully drained and closed. No publication transport is enabled here.
 */
public interface ManagedSchemaAccess extends AutoCloseable {
    DocumentSchemaAdmission.Resolution open(RepositoryCaller caller, DocumentPublicationMember member,
            RepositoryReadControl control);

    /** Idempotently reject new work and wake waiting scopes; do not block on provider I/O. */
    @Override void close();

    /**
     * Called after publication scopes drain. Include abandoned provider loads;
     * false leaves shared host resources retained for a later shutdown attempt.
     */
    boolean awaitIdle(Duration timeout) throws InterruptedException;
}
