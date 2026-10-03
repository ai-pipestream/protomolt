package ai.protomolt.proto.repo.spi;

/** Identity resolved by a trusted embedding host or transport, never from a request payload. */
public record RepositoryCaller(String principalName, boolean processAuthority) {
    public RepositoryCaller {
        if (principalName == null || principalName.isBlank())
            throw new IllegalArgumentException("Repository principal name is required");
    }
}
