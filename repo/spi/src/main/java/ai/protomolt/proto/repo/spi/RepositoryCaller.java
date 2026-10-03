package ai.protomolt.proto.repo.spi;

/** Identity resolved by a trusted embedding host or transport, never from a request payload. */
public record RepositoryCaller(String principalName, boolean processAuthority,
        java.util.Set<String> accountIds,
        java.util.Set<ai.protomolt.proto.repo.v1.Principal> identities) {
    /** Existing callers receive no implicit account membership or ACL identities. */
    public RepositoryCaller(String principalName, boolean processAuthority) {
        this(principalName, processAuthority, java.util.Set.of(), java.util.Set.of());
    }

    public RepositoryCaller {
        if (principalName == null || principalName.isBlank())
            throw new IllegalArgumentException("Repository principal name is required");
        accountIds = java.util.Set.copyOf(accountIds);
        identities = java.util.Set.copyOf(identities);
        for (String account : accountIds) {
            if (account.isBlank()) throw new IllegalArgumentException("Account identity must not be blank");
        }
        for (var identity : identities) {
            if (identity.getIdentity().isBlank() || identity.getIdentityType().isBlank()
                    || !identity.getUnknownFields().asMap().isEmpty())
                throw new IllegalArgumentException("ACL identity and identity type are required");
        }
        if (processAuthority && (!accountIds.isEmpty() || !identities.isEmpty()))
            throw new IllegalArgumentException("Process authority does not enumerate account grants or ACL identities");
    }
}
