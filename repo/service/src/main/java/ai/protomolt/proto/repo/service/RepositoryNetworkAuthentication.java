package ai.protomolt.proto.repo.service;

/** Network listeners require an explicit operator credential before allocating host resources. */
final class RepositoryNetworkAuthentication {
    private RepositoryNetworkAuthentication() {}

    static String requireOperatorToken(String token) {
        if (token == null || token.isBlank())
            throw new IllegalArgumentException("Repository network transport requires a nonblank API token (PROTOMOLT_API_TOKEN)");
        return token;
    }
}
