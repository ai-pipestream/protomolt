package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Private pre-admission random identities. Not a durable store, execution claim or permission grant.
 * A future session store must bind these to the exact command/key and fence replica execution;
 * possession alone must never enable two hosts to resume the same owner concurrently.
 */
final class DocumentPublicationSeeds {
    private final RepositoryOperationLedger.Key key;
    private final String commandSha256;
    private final UUID ownerNonce;
    private final Map<String, UUID> attempts;
    private final Map<String, UUID> uploadTokens;

    private DocumentPublicationSeeds(RepositoryOperationLedger.Key key, DocumentPublicationCommand command,
            UUID ownerNonce, Map<String, UUID> attempts, Map<String, UUID> uploadTokens) {
        this.key = Objects.requireNonNull(key); Objects.requireNonNull(command);
        if (!key.account().equals(command.intent().getAccountId()) || !key.operationId().equals(command.operationId()))
            throw new IllegalArgumentException("Publication identities differ from command scope");
        this.commandSha256 = command.sha256(); this.ownerNonce = Objects.requireNonNull(ownerNonce);
        var members = command.intent().getMembersList().stream()
                .filter(member -> member.getPartsList().stream().anyMatch(part -> part.hasUpload()))
                .map(member -> member.getMemberId()).collect(java.util.stream.Collectors.toSet());
        if (!attempts.keySet().equals(members) || !uploadTokens.keySet().equals(members))
            throw new IllegalArgumentException("Publication identities differ from uploading members");
        this.attempts = Map.copyOf(attempts); this.uploadTokens = Map.copyOf(uploadTokens);
        var unique = new HashSet<UUID>(); unique.add(ownerNonce);
        for (var values : java.util.List.of(this.attempts.values(), this.uploadTokens.values()))
            for (var value : values) if (!unique.add(value))
                throw new IllegalArgumentException("Publication identities must be distinct");
    }

    static DocumentPublicationSeeds mint(RepositoryOperationLedger.Key key, DocumentPublicationCommand command) {
        var attempts = new HashMap<String, UUID>(); var tokens = new HashMap<String, UUID>();
        for (var member : command.intent().getMembersList()) {
            if (member.getPartsList().stream().anyMatch(part -> part.hasUpload())) {
                attempts.put(member.getMemberId(), UUID.randomUUID()); tokens.put(member.getMemberId(), UUID.randomUUID());
            }
        }
        return restore(key, command, UUID.randomUUID(), attempts, tokens);
    }

    /** Decode-side structural check only. The private store must authenticate the complete persisted state. */
    static DocumentPublicationSeeds restore(RepositoryOperationLedger.Key key, DocumentPublicationCommand command,
            UUID ownerNonce, Map<String, UUID> attempts, Map<String, UUID> uploadTokens) {
        return new DocumentPublicationSeeds(key, command, ownerNonce, attempts, uploadTokens);
    }

    void requireCommand(RepositoryOperationLedger.Key expected, DocumentPublicationCommand command) {
        if (!key.equals(expected) || !commandSha256.equals(command.sha256()) || !key.operationId().equals(command.operationId()))
            throw new IllegalArgumentException("Publication identities belong to a different command or caller scope");
    }
    UUID ownerNonce() { return ownerNonce; }
    Map<String, UUID> attempts() { return attempts; }
    Map<String, UUID> uploadTokens() { return uploadTokens; }
    @Override public String toString() { return "DocumentPublicationSeeds[private]"; }
}
