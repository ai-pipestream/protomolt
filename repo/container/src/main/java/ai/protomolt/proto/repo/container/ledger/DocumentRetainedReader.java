package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.codec.PartObject;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import java.util.List;

/** Provider-read port for a publication coordinator; no engine implementation dependency. */
@FunctionalInterface
public interface DocumentRetainedReader {
    /**
     * Read the selected member's exact retained objects from their original backend
     * bindings. The ledger-issued plan supplies protected read identity, not permission
     * to publish. Verify size, digest and recorded provider version before delivery.
     * Results follow the plan's filtered member-entry order, including gaps in full
     * revision ordinals. Never substitute the current drive or latest object version.
     *
     * Transfer a plan use and byte reservations to the returned batch. On failure,
     * release them only after actual provider work exits; cancelled futures do not
     * establish that exit. The coordinator separately closes/drains/releases the plan
     * and rechecks source authorization and revisions at publication. The provider
     * and reader lifecycle belong to the host, not this method or its result.
     */
    Batch readRetained(DocumentReadLedger.PinnedPlan plan, String memberId, RepositoryReadControl control);

    /** Verified borrowed payloads; callers must finish copying/consuming before close. */
    interface Batch extends AutoCloseable {
        /** Ordered immutable list; contained byte arrays must not be mutated or retained after close. */
        List<PartObject> parts();

        /** Nonthrowing local release. Outstanding provider workers retain their protection until they exit. */
        @Override void close();
    }
}
