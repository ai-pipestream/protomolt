package ai.protomolt.proto.repo.service;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.URI;
import java.io.IOException;

/** Rejects direct routing back to this process; it cannot discover proxy or multi-node cycles. */
final class RemoteRouting {
    private RemoteRouting() {}

    static void rejectInProcess(RepoServiceConfig config, String listenerName) {
        if (RepoServiceConfig.BLOB_STORE_REPO_INPROCESS.equals(config.blobStore())
                && config.repoTarget().equals(listenerName)) {
            throw new IllegalArgumentException("Remote repository target is this in-process listener");
        }
    }

    static void rejectTcp(RepoServiceConfig config, int listenerPort) {
        if (!RepoServiceConfig.BLOB_STORE_REPO.equals(config.blobStore())) return;
        URI endpoint = endpoint(config.repoTarget());
        if (endpoint.getPort() != listenerPort) return;
        try {
            for (InetAddress address : InetAddress.getAllByName(endpoint.getHost())) {
                if (address.isAnyLocalAddress() || address.isLoopbackAddress()
                        || NetworkInterface.getByInetAddress(address) != null) {
                    throw new IllegalArgumentException("Remote repository target is this TCP listener");
                }
            }
        } catch (IOException failure) {
            throw new IllegalArgumentException("Cannot verify remote repository routing", failure);
        }
    }

    static URI endpoint(String target) {
        String authority = target.startsWith("dns:///") ? target.substring(7) : target;
        URI endpoint;
        try { endpoint = URI.create("//" + authority); }
        catch (IllegalArgumentException invalid) {
            throw new IllegalArgumentException("Remote repository target must be host:port or dns:///host:port");
        }
        if (endpoint.getHost() == null || endpoint.getPort() < 1 || endpoint.getPort() > 65535
                || endpoint.getUserInfo() != null || !endpoint.getRawPath().isEmpty()
                || endpoint.getQuery() != null || endpoint.getFragment() != null) {
            throw new IllegalArgumentException("Remote repository target must be host:port or dns:///host:port");
        }
        return endpoint;
    }
}
