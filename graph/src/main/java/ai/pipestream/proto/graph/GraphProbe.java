package ai.pipestream.proto.graph;

import com.fasterxml.jackson.databind.JsonNode;

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
 * <p>Device code needs the app registration marked as a public client with delegated
 * {@code User.Read Files.Read.All Sites.Read.All} permissions; the connectors probe under
 * client credentials needs {@code ExternalConnection.ReadWrite.OwnedBy} with admin
 * consent. Nothing is written to the tenant; every probe is a read.</p>
 */
public final class GraphProbe {

    private GraphProbe() {
    }

    public static void main(String[] args) throws Exception {
        String tenant = null;
        String client = null;
        String secret = null;
        for (int i = 0; i < args.length - 1; i++) {
            switch (args[i]) {
                case "--tenant" -> tenant = args[++i];
                case "--client" -> client = args[++i];
                case "--secret" -> secret = args[++i];
                default -> {
                }
            }
        }
        if (tenant == null || client == null) {
            System.err.println("usage: GraphProbe --tenant <tenant-id> --client <app-id> "
                    + "[--secret <client-secret>]");
            System.exit(2);
        }

        GraphAuth.Token token;
        if (secret != null) {
            token = new GraphAuth(GraphAuth.Config.application(tenant, client, secret))
                    .clientCredentials();
            System.out.println("Signed in with client credentials (application permissions).");
        } else {
            token = new GraphAuth(GraphAuth.Config.delegated(tenant, client)).deviceCode(
                    "User.Read Files.Read.All Sites.Read.All offline_access",
                    prompt -> System.out.println("\n>>> " + prompt.message() + "\n"));
            System.out.println("Signed in with a delegated device-code session.");
        }
        GraphClient graph = new GraphClient(() -> token.accessToken());
        GraphFiles files = new GraphFiles(graph);

        if (secret == null) {
            probe("Signed-in user (/me)", () -> {
                JsonNode me = files.me();
                return me.path("displayName").asText() + " <"
                        + me.path("userPrincipalName").asText() + ">";
            });
            probe("OneDrive (/me/drive)", () -> {
                JsonNode drive = files.meDrive();
                return drive.path("driveType").asText() + " drive, "
                        + drive.path("quota").path("used").asLong() / (1024 * 1024)
                        + " MB used, id " + drive.path("id").asText();
            });
            probe("OneDrive root listing", () -> {
                JsonNode drive = files.meDrive();
                JsonNode children = files.children(drive.path("id").asText(), "/");
                return children.path("value").size() + " item(s) in the root";
            });
        }
        probe("SharePoint Online (/sites?search=*)", () -> {
            JsonNode sites = files.searchSites("*");
            int count = sites.path("value").size();
            return count == 0
                    ? "reachable, but no sites visible (OneDrive-only tenant or no access)"
                    : count + " site(s), first: "
                            + sites.path("value").get(0).path("webUrl").asText();
        });
        probe("Copilot connectors (/external/connections)", () -> {
            JsonNode connections = new GraphConnections(graph).list();
            return connections.path("value").size() + " connection(s) visible";
        });
        System.out.println("\nProbe complete.");
    }

    private interface Check {
        String run() throws Exception;
    }

    private static void probe(String what, Check check) {
        System.out.printf("%-45s ", what + ":");
        try {
            System.out.println("OK - " + check.run());
        } catch (GraphClient.GraphApiException e) {
            System.out.println(e.status() == 403 || e.status() == 401
                    ? "PERMISSION DENIED - grant/consent the permission and retry ("
                            + e.code() + ")"
                    : "UNAVAILABLE - " + e.getMessage());
        } catch (Exception e) {
            System.out.println("FAILED - " + e.getMessage());
        }
    }
}
