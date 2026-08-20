package ai.pipestream.proto.workflow;

import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.receipt.TrustSnapshot;
import ai.pipestream.proto.receipt.TrustSnapshots;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;

/**
 * The operator-pinned trust snapshot the verifying verbs default to: the same document a
 * relying party pins as a file, in the server's custody. A request that carries its own
 * snapshot always wins; the pin is the default, never an override. A configured file that
 * does not load or verify refuses at startup naming the path.
 *
 * @param snapshot the pinned snapshot
 */
public record TrustPin(TrustSnapshot snapshot) {

    /** Path of the pinned snapshot file ({@code .json}, {@code .binpb}, or {@code .pb}). */
    public static final String ENV_TRUST_SNAPSHOT = "PROTOMOLT_TRUST_SNAPSHOT";

    public TrustPin {
        Objects.requireNonNull(snapshot, "snapshot");
    }

    /** Resolves from the process environment; null when unset. */
    public static TrustPin fromEnvironment() {
        return fromEnvironment(System.getenv());
    }

    static TrustPin fromEnvironment(Map<String, String> env) {
        String file = env.get(ENV_TRUST_SNAPSHOT);
        if (file == null || file.isBlank()) {
            return null;
        }
        try {
            return new TrustPin(TrustSnapshots.load(Path.of(file.trim())));
        } catch (IOException e) {
            throw new IllegalStateException("failed to read the trust snapshot at "
                    + file.trim() + ": " + e.getMessage(), e);
        }
    }

    /** The snapshot a verifying verb runs against: the request's, else the pin. */
    static TrustSnapshot resolve(ObjectNode input, TrustSnapshot defaultTrust)
            throws ActionException {
        if (input.has("trust") && !input.get("trust").isNull()) {
            return (TrustSnapshot) WorkflowActionJson.parse(
                    WorkflowActionJson.object(input, "trust"), TrustSnapshot.newBuilder(),
                    "/trust");
        }
        if (defaultTrust != null) {
            return defaultTrust;
        }
        throw WorkflowActionJson.invalid("the request carries no trust snapshot and the "
                + "server pins none; supply 'trust' or set " + ENV_TRUST_SNAPSHOT, "/trust");
    }
}
