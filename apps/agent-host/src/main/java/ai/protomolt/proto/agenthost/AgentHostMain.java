package ai.protomolt.proto.agenthost;

import ai.protomolt.proto.acp.AcpClient;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/** Command line entry point for one persistent Codex or Kimi delegation participant. */
public final class AgentHostMain {

    public static void main(String[] args) {
        if (java.util.Arrays.asList(args).contains("--help")
                || java.util.Arrays.asList(args).contains("-h")) {
            System.out.println(Options.usage());
            return;
        }
        try {
            Options options = Options.parse(args);
            run(options);
        } catch (IllegalArgumentException | AgentHostException e) {
            System.err.println("agent-host: " + e.getMessage());
            System.err.println(Options.usage());
            System.exit(2);
        }
    }

    static void run(Options options) {
        AgentHostStateStore store = new AgentHostStateStore(options.state());
        AgentHostState state = store.loadOrCreate(options.identity(), options.role(),
                options.provider(), options.workspace());
        AgentProvider provider = provider(options, state);
        McpHttpClient mcp = McpHttpClient.usingTokenEnvironment(
                options.endpoint(), options.tokenEnvironment());
        AgentHost.Config config = new AgentHost.Config(options.role(), options.identity(),
                options.model(), options.workspace(), options.pollTimeout(),
                options.maxEvents(), options.resetOnTranscriptLoss());
        try (AgentHost host = new AgentHost(config, mcp, provider, store, state)) {
            host.connect();
            if (options.bootstrap() != null) {
                final String objective;
                try {
                    objective = Files.readString(options.bootstrap());
                } catch (java.io.IOException e) {
                    throw new AgentHostException("could not read bootstrap objective", e);
                }
                host.bootstrap(objective);
            }
            if (options.once()) {
                host.pollOnce();
                return;
            }
            Thread runner = Thread.ofVirtual().name("protomolt-agent-host-")
                    .start(host::run);
            Runtime.getRuntime().addShutdownHook(
                    Thread.ofPlatform().unstarted(host::close));
            try {
                runner.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static AgentProvider provider(Options options, AgentHostState state) {
        return switch (options.provider()) {
            case "kimi" -> new AcpAgentProvider(options.workspace(), state.providerSessionId(),
                    List.of("kimi", "acp"), options.turnTimeout(), options.permissionPolicy());
            case "codex" -> new CodexAgentProvider(options.workspace(), options.state(),
                    state.providerSessionId(), options.role(), List.of("codex"),
                    options.model(), options.turnTimeout());
            case "openai" -> new OpenAiAgentProvider(options.providerEndpoint(),
                    options.model(), options.role(), state.providerSessionId(),
                    options.turnTimeout());
            default -> throw new IllegalArgumentException(
                    "provider must be 'kimi', 'codex', or 'openai'");
        };
    }

    record Options(URI endpoint, AgentRole role, String identity, String provider,
                   String model, Path workspace, Path state, String tokenEnvironment,
                   Path bootstrap, Duration pollTimeout, Duration turnTimeout,
                   int maxEvents, AcpClient.PermissionPolicy permissionPolicy,
                   URI providerEndpoint, boolean once, boolean resetOnTranscriptLoss) {

        static Options parse(String[] args) {
            String endpoint = environment("PROTOMOLT_AGENT_MCP_ENDPOINT");
            String role = environment("PROTOMOLT_AGENT_ROLE");
            String identity = environment("PROTOMOLT_AGENT_ID");
            String provider = environment("PROTOMOLT_AGENT_PROVIDER");
            String model = environment("PROTOMOLT_AGENT_MODEL");
            String workspace = environment("PROTOMOLT_AGENT_WORKSPACE");
            String state = environment("PROTOMOLT_AGENT_STATE");
            String tokenEnvironment = environment("PROTOMOLT_AGENT_TOKEN_ENV");
            String bootstrap = environment("PROTOMOLT_AGENT_BOOTSTRAP");
            String providerEndpoint = environment("PROTOMOLT_AGENT_PROVIDER_ENDPOINT");
            long pollSeconds = 30;
            long turnMinutes = 30;
            int maxEvents = 64;
            AcpClient.PermissionPolicy permissionPolicy =
                    AcpClient.PermissionPolicy.ALLOW_SINGLE;
            boolean once = false;
            boolean resetOnTranscriptLoss = false;
            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "--endpoint" -> endpoint = value(args, ++i);
                    case "--role" -> role = value(args, ++i);
                    case "--identity" -> identity = value(args, ++i);
                    case "--provider" -> provider = value(args, ++i);
                    case "--model" -> model = value(args, ++i);
                    case "--workspace" -> workspace = value(args, ++i);
                    case "--state" -> state = value(args, ++i);
                    case "--token-env" -> tokenEnvironment = value(args, ++i);
                    case "--bootstrap" -> bootstrap = value(args, ++i);
                    case "--provider-endpoint" -> providerEndpoint = value(args, ++i);
                    case "--poll-seconds" -> pollSeconds = positiveLong(
                            value(args, ++i), "--poll-seconds");
                    case "--turn-minutes" -> turnMinutes = positiveLong(
                            value(args, ++i), "--turn-minutes");
                    case "--max-events" -> maxEvents = positiveInt(
                            value(args, ++i), "--max-events");
                    case "--acp-permissions" -> permissionPolicy = permissionPolicy(
                            value(args, ++i));
                    case "--once" -> once = true;
                    case "--reset-on-transcript-loss" -> resetOnTranscriptLoss = true;
                    default -> throw new IllegalArgumentException(
                            "unknown option: " + args[i]);
                }
            }
            List<String> missing = new ArrayList<>();
            required(endpoint, "--endpoint", missing);
            required(role, "--role", missing);
            required(identity, "--identity", missing);
            required(provider, "--provider", missing);
            required(workspace, "--workspace", missing);
            required(state, "--state", missing);
            if (!missing.isEmpty()) {
                throw new IllegalArgumentException("missing required options: "
                        + String.join(", ", missing));
            }
            AgentRole parsedRole = AgentRole.parse(role);
            String normalizedProvider = provider.toLowerCase(Locale.ROOT);
            Path workspacePath = Path.of(workspace).toAbsolutePath().normalize();
            if (!Files.isDirectory(workspacePath)) {
                throw new IllegalArgumentException("workspace must be an existing directory");
            }
            if (pollSeconds > 30) {
                throw new IllegalArgumentException("poll seconds must be at most 30");
            }
            if (turnMinutes > 60) {
                throw new IllegalArgumentException("turn minutes must be at most 60");
            }
            if (maxEvents > 256) {
                throw new IllegalArgumentException("max events must be at most 256");
            }
            URI providerEndpointUri = null;
            if ("openai".equals(normalizedProvider)) {
                if (providerEndpoint == null || providerEndpoint.isBlank()) {
                    throw new IllegalArgumentException(
                            "--provider-endpoint is required with --provider openai");
                }
                if (model == null || model.isBlank()) {
                    throw new IllegalArgumentException(
                            "--model is required with --provider openai");
                }
                providerEndpointUri = URI.create(providerEndpoint);
            } else if (providerEndpoint != null && !providerEndpoint.isBlank()) {
                throw new IllegalArgumentException(
                        "--provider-endpoint is valid only with --provider openai");
            }
            Path bootstrapPath = bootstrap == null ? null
                    : Path.of(bootstrap).toAbsolutePath().normalize();
            if (bootstrapPath != null && parsedRole != AgentRole.COORDINATOR) {
                throw new IllegalArgumentException(
                        "--bootstrap is valid only for a coordinator");
            }
            return new Options(URI.create(endpoint), parsedRole, identity,
                    normalizedProvider, model, workspacePath,
                    Path.of(state).toAbsolutePath().normalize(), tokenEnvironment,
                    bootstrapPath, Duration.ofSeconds(pollSeconds),
                    Duration.ofMinutes(turnMinutes), maxEvents, permissionPolicy,
                    providerEndpointUri, once, resetOnTranscriptLoss);
        }

        static String usage() {
            return "Usage: protomolt-agent-host --endpoint <https://host/mcp> "
                    + "--role <worker|coordinator> --identity <id> "
                    + "--provider <kimi|codex|openai> --workspace <dir> --state <file> "
                    + "[--model <name>] [--provider-endpoint <http://host:port/v1>] "
                    + "[--token-env <ENV_NAME>] "
                    + "[--bootstrap <objective-file>] [--acp-permissions "
                    + "<allow-single|reject>] [--reset-on-transcript-loss]";
        }

        private static String value(String[] args, int index) {
            if (index >= args.length || args[index].isBlank()) {
                throw new IllegalArgumentException("option requires a value");
            }
            return args[index];
        }

        private static long positiveLong(String value, String option) {
            try {
                long parsed = Long.parseLong(value);
                if (parsed < 1) {
                    throw new NumberFormatException();
                }
                return parsed;
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(option + " requires a positive integer");
            }
        }

        private static int positiveInt(String value, String option) {
            long parsed = positiveLong(value, option);
            if (parsed > Integer.MAX_VALUE) {
                throw new IllegalArgumentException(option + " is too large");
            }
            return (int) parsed;
        }

        private static AcpClient.PermissionPolicy permissionPolicy(String value) {
            return switch (value) {
                case "allow-single" -> AcpClient.PermissionPolicy.ALLOW_SINGLE;
                case "reject" -> AcpClient.PermissionPolicy.REJECT;
                default -> throw new IllegalArgumentException(
                        "ACP permissions must be 'allow-single' or 'reject'");
            };
        }

        private static void required(String value, String option, List<String> missing) {
            if (value == null || value.isBlank()) {
                missing.add(option);
            }
        }

        private static String environment(String name) {
            String value = System.getenv(name);
            return value == null || value.isBlank() ? null : value;
        }
    }

    private AgentHostMain() {
    }
}
