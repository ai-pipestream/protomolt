package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** Bounded same-key exclusion across local retry routing and publication; never a distributed lease. */
final class RepositoryPublicationCalls {
    private final int capacity;
    private final Map<RepositoryOperationLedger.Key,Call> active=new HashMap<>();

    RepositoryPublicationCalls(int capacity) {
        if (capacity<1) throw new IllegalArgumentException("Publication call capacity must be positive");
        this.capacity=capacity;
    }

    synchronized Call enter(RepositoryCaller caller, DocumentPublicationCommand command) {
        Objects.requireNonNull(command);
        if (caller==null) throw new RepositoryException(RepositoryException.Code.UNAUTHENTICATED,
                "Authenticated repository caller is required");
        var key=new RepositoryOperationLedger.Key(command.intent().getAccountId(),caller.principalName(),command.operationId());
        DocumentAdmissionAuthorization.requireCaller(caller,key,key.account());
        if (active.containsKey(key)) throw new RepositoryException(RepositoryException.Code.CONFLICT,
                "Publication operation already has an active local call");
        if (active.size()>=capacity) throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED,
                "Publication call capacity exhausted");
        var call=new Call(key);
        active.put(key,call);
        return call;
    }

    final class Call implements AutoCloseable {
        private final RepositoryOperationLedger.Key key;
        private Call(RepositoryOperationLedger.Key key) { this.key=key; }
        @Override public void close() {
            synchronized (RepositoryPublicationCalls.this) { active.remove(key,this); }
        }
    }
}
