package ai.pipestream.proto.graph;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.PrintStream;

/**
 * One command that answers "what can this tenant actually do?" before any integration work:
 * signs in (device code by default, client credentials with a secret) and probes the
 * surfaces ProtoMolt integrates — the signed-in user, OneDrive, SharePoint sites, and the
 * Copilot connectors API — reporting each as available, permission-denied, or absent.
 *
 * <pre>
 * java -cp ... ai.pipestream.proto.graph.GraphProbe --tenant &lt;tenant-id&gt; --client &lt;app-id&gt;
 * java -cp ... ai.pipestream.proto.graph.GraphProbe --tenant ... --client ... --secret ...
 * </pre>
 *
 * <p>{@code --verbose} prints Graph's full error body under each failing probe; the
 * {@code innerError} block in there distinguishes a permissions problem from a tenant that
 * has not finished provisioning (unassigned license, OneDrive never visited).</p>
 *
 * <p>Device code needs the app registration marked as a public client with delegated
 * {@code User.Read Files.Read.All Sites.Read.All} permissions; the connectors probe under
 * client credentials needs {@code ExternalConnection.ReadWrite.OwnedBy} with admin
 * consent. Nothing is written to the tenant; every probe is a read.</p>
 */
public final class GraphProbe {

    static final String USAGE = "usage: GraphProbe --tenant <tenant-id> --client <app-id> "
            + "[--secret <client-secret>] [--verbose]";

    private GraphProbe() {
    }

    /**
     * The parsed command line. {@code secret} present means the app-only client-credentials
     * lane (no delegated user surfaces); absent means the delegated device-code lane.
     */
    record Options(String tenant, String client, String secret, boolean verbose) {
        boolean appOnly() {
            return secret != null;
        }
    }

    /** Returns the parsed options, or {@code null} when the required tenant/client are missing. */
    static Options parse(String[] args) {
        String tenant = null;
        String client = null;
        String secret = null;
        boolean verbose = false;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--tenant" -> tenant = i + 1 < args.length ? args[++i] : null;
                case "--client" -> client = i + 1 < args.length ? args[++i] : null;
                case "--secret" -> secret = i + 1 < args.length ? args[++i] : null;
                case "--verbose" -> verbose = true;
                default -> {
                }
            }
        }
        if (tenant == null || client == null) {
            return null;
        }
        return new Options(tenant, client, secret, verbose);
    }

    public static void main(String[] args) throws Exception {
        Options options = parse(args);
        if (options == null) {
            System.err.println(USAGE);
            System.exit(2);
            return;
        }

        GraphAuth.Token token;
        if (options.appOnly()) {
            token = new GraphAuth(GraphAuth.Config.application(
                    options.tenant(), options.client(), options.secret())).clientCredentials();
            System.out.println("Signed in with client credentials (application permissions).");
        } else {
            token = new GraphAuth(GraphAuth.Config.delegated(options.tenant(), options.client()))
                    .deviceCode("User.Read Files.Read.All Sites.Read.All offline_access",
                            prompt -> System.out.println("\n>>> " + prompt.message() + "\n"));
            System.out.println("Signed in with a delegated device-code session.");
        }
        GraphClient graph = new GraphClient(token::accessToken);
        run(graph, !options.appOnly(), options.verbose(), System.out);
        System.out.println("\nProbe complete.");
    }

    /**
     * Runs the read-only probes against an already-authorized client, writing one line per
     * surface to {@code out}. {@code includeUserLane} adds the delegated /me and OneDrive
     * probes that a client-credentials token cannot reach. Package-visible so a fake Graph
     * server can drive it without a real tenant.
     */
    static void run(GraphClient graph, boolean includeUserLane, boolean verbose, PrintStream out) {
        GraphFiles files = new GraphFiles(graph);
        if (includeUserLane) {
            probe(out, verbose, "Signed-in user (/me)", () -> {
                JsonNode me = files.me();
                return me.path("displayName").asText() + " <"
                        + me.path("userPrincipalName").asText() + ">";
            });
            probe(out, verbose, "OneDrive (/me/drive)", () -> {
                JsonNode drive = files.meDrive();
                return drive.path("driveType").asText() + " drive, "
                        + drive.path("quota").path("used").asLong() / (1024 * 1024)
                        + " MB used, id " + drive.path("id").asText();
            });
            probe(out, verbose, "OneDrive root listing", () -> {
                JsonNode drive = files.meDrive();
                JsonNode children = files.children(drive.path("id").asText(), "/");
                return children.path("value").size() + " item(s) in the root";
            });
        }
        probe(out, verbose, "SharePoint Online (/sites?search=*)", () -> {
            JsonNode sites = files.searchSites("*");
            int count = sites.path("value").size();
            return count == 0
                    ? "reachable, but no sites visible (OneDrive-only tenant or no access)"
                    : count + " site(s), first: "
                            + sites.path("value").get(0).path("webUrl").asText();
        });
        probe(out, verbose, "Copilot connectors (/external/connections)", () -> {
            JsonNode connections = new GraphConnections(graph).list();
            return connections.path("value").size() + " connection(s) visible";
        });
    }

    private interface Check {
        String run() throws Exception;
    }

    private static void probe(PrintStream out, boolean verbose, String what, Check check) {
        out.printf("%-45s ", what + ":");
        try {
            out.println("OK - " + check.run());
        } catch (GraphClient.GraphApiException e) {
            out.println(e.status() == 403 || e.status() == 401
                    ? "PERMISSION DENIED - grant/consent the permission and retry ("
                            + e.code() + ")"
                    : "UNAVAILABLE - " + e.getMessage());
            if (verbose && !e.body().isBlank()) {
                out.println("    body: " + e.body());
            }
        } catch (Exception e) {
            out.println("FAILED - " + e.getMessage());
        }
    }
}
