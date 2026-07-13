package ai.pipestream.proto.server;

import ai.pipestream.proto.rest.ProtoRestGateway;

/**
 * Common host contract for every {@code servers/*} adapter.
 * Logic lives in {@link ProtoRestGateway}; hosts only bind HTTP.
 *
 * <p>Shared behavior across hosts:
 * <ul>
 *   <li>Repeated headers and query parameters keep the FIRST value; later duplicates are ignored.</li>
 *   <li>Request bodies above {@link ProtoToolsServerConfig#maxRequestBytes()} get 413.</li>
 *   <li>Trailing slashes on invoke routes ({@code prefix/Service/Method/}) are 404.</li>
 *   <li>405 responses carry an {@code Allow} header listing the accepted verbs.</li>
 *   <li>5xx bodies are generic; exception details are logged server-side only.</li>
 * </ul>
 */
public interface ProtoRestServerHost extends AutoCloseable {

    /** Starts listening; returns the bound port (useful when {@code port=0}). */
    int start();

    int actualPort();

    ProtoToolsServerConfig config();

    ProtoRestGateway gateway();

    String engineId();

    @Override
    void close();
}
