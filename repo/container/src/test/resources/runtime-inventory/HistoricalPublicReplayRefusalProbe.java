package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import java.util.*;

/** Valid envelopes with conflicting intent or unauthorized identity must not replay a receipt. */
final class HistoricalPublicReplayRefusalProbe {
    static void intent(DocumentPublicationRepository repository, RepositoryCaller caller, PublishDocumentRequest request,
            boolean pending) {
        var member = request.getIntent().getMembers(0);
        var changedCondition = request.toBuilder().setIntent(request.getIntent().toBuilder().setMembers(0,
                member.toBuilder().setDestination(member.getDestination().toBuilder()
                        .setExpectedMutationRevision(member.getDestination().getExpectedMutationRevision() + 1)))).build();
        refuse(repository, caller, changedCondition, RepositoryException.Code.CONFLICT);
        var changedMode = request.toBuilder().setModes(0, request.getModes(0).toBuilder()
                .setMode(DocumentPublicationMode.DOCUMENT_PUBLICATION_MODE_OPAQUE)).build();
        refuse(repository, caller, changedMode, pending ? RepositoryException.Code.CONFLICT : RepositoryException.Code.FAILED_PRECONDITION);
        for (int ordinal = 0; ordinal < member.getPartsCount(); ordinal++) {
            var part = member.getParts(ordinal);
            if (!part.hasHistoricalReuse()) continue;
            var changedIdentity = request.toBuilder().setIntent(request.getIntent().toBuilder().setMembers(0,
                    member.toBuilder().setParts(ordinal, part.toBuilder().setHistoricalReuse(part.getHistoricalReuse().toBuilder()
                            .setObject(part.getHistoricalReuse().getObject().toBuilder().setObjectKey(
                                    part.getHistoricalReuse().getObject().getObjectKey() + "-changed")))))).build();
            refuse(repository, caller, changedIdentity, RepositoryException.Code.CONFLICT);
            return;
        }
        throw new AssertionError("Historical refusal fixture lacks a historical part");
    }

    record Unauthorized(RepositoryCaller caller, RepositoryException.Code code) {}
    static List<Unauthorized> unauthorized(RepositoryCaller caller) {
        var binding = caller.credentialBinding().orElseThrow();
        return List.of(new Unauthorized(new RepositoryCaller(caller.principalName(), false, Set.of(), caller.identities(), caller.credentialBinding()),
                        RepositoryException.Code.PERMISSION_DENIED),
                new Unauthorized(new RepositoryCaller(caller.principalName(), false, caller.accountIds(), caller.identities(), Optional.of(
                        new RepositoryCredentialBinding(binding.issuer(), binding.credentialId(), binding.generation() + 1))),
                        RepositoryException.Code.UNAUTHENTICATED));
    }

    static RepositoryException refuse(DocumentPublicationRepository repository, RepositoryCaller caller, PublishDocumentRequest request,
            RepositoryException.Code expected) {
        return refuse(repository, caller, request, expected, null);
    }

    static RepositoryException refuse(DocumentPublicationRepository repository, RepositoryCaller caller, PublishDocumentRequest request,
            RepositoryException.Code expected, String message) {
        try {
            repository.publishDocument(caller, request, RepositoryReadControl.NONE);
            throw new AssertionError("Invalid public replay returned a receipt; expected " + expected);
        } catch (RepositoryException failure) {
            if (failure.code() != expected) throw new AssertionError("Expected " + expected + " but got " + failure.code(), failure);
            if (message != null && !message.equals(failure.getMessage()))
                throw new AssertionError("Expected exact refusal message: " + message, failure);
            return failure;
        }
    }
}
