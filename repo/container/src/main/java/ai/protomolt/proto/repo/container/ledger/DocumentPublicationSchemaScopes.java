package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import ai.protomolt.proto.repo.v1.DocumentPublicationMember;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/** Call-owned scopes; never retained in an idempotency session or opened during replay. */
final class DocumentPublicationSchemaScopes implements DocumentPublicationRuntime.Schemas, AutoCloseable {
    private final RepositoryCaller caller;
    private final Map<String, DocumentPublicationMember> members;
    private final DocumentPublicationRuntime.SchemaScopes factory;
    private final RepositoryReadControl control;
    private final LinkedHashMap<String, DocumentSchemaAdmission.Resolution> scopes = new LinkedHashMap<>();
    private final Thread thread = Thread.currentThread();
    private boolean closed;

    DocumentPublicationSchemaScopes(RepositoryCaller caller, DocumentPublicationCommand command,
            DocumentPublicationRuntime.SchemaScopes factory, RepositoryReadControl control) {
        if (caller == null) throw new ai.protomolt.proto.repo.spi.RepositoryException(
                ai.protomolt.proto.repo.spi.RepositoryException.Code.UNAUTHENTICATED, "Authenticated repository caller is required");
        this.caller = caller;
        this.factory = Objects.requireNonNull(factory); this.control = Objects.requireNonNull(control);
        members = command.intent().getMembersList().stream().collect(Collectors.toUnmodifiableMap(
                DocumentPublicationMember::getMemberId, member -> member));
    }

    @Override public DocumentSchemaAdmission.Definition resolve(RepositoryCaller observed,
            DocumentPublicationMember member, DocumentSchemaAdmission.Selection occurrence) {
        requireThread();
        if (closed) throw new IllegalStateException("Publication schema scopes closed");
        if (observed != caller || !member.equals(members.get(member.getMemberId())))
            throw new IllegalArgumentException("Schema scope differs from publication caller or member");
        control.check();
        var scope = scopes.get(member.getMemberId());
        if (scope == null) {
            scope = Objects.requireNonNull(factory.open(caller, member, control), "Schema scope");
            try { scopes.put(member.getMemberId(), scope); }
            catch (RuntimeException | Error failure) {
                try { scope.close(); }
                catch (RuntimeException | Error cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
                throw failure;
            }
        }
        control.check();
        var definition = scope.select(occurrence);
        control.check();
        return definition;
    }

    private void requireThread() {
        if (Thread.currentThread() != thread) throw new IllegalStateException("Publication schema scope thread changed");
    }

    @Override public void close() {
        requireThread();
        if (closed) return;
        closed = true;
        Throwable first = null;
        for (var scope : scopes.sequencedValues().reversed()) {
            try { scope.close(); }
            catch (RuntimeException | Error failure) {
                if (first == null) first = failure;
                else if (failure != first) first.addSuppressed(failure);
            }
        }
        scopes.clear();
        if (first instanceof RuntimeException failure) throw failure;
        if (first instanceof Error failure) throw failure;
    }
}
