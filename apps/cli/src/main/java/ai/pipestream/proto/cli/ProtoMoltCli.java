package ai.pipestream.proto.cli;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.grpc.service.ProtoMoltCatalog;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * The ProtoMolt command line. Every verb the servers expose over gRPC, REST, and MCP is here too,
 * JSON in and JSON out: {@code protomolt <verb> <json>} runs one, {@code protomolt list} names them
 * all, and {@code protomolt console} opens an interactive REPL over the same catalog. The dispatch
 * takes its streams and catalog as arguments so it is driven directly by tests; {@code main} only
 * wires the process streams and exit code.
 */
public final class ProtoMoltCli {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws IOException {
        ActionCatalog catalog = ProtoMoltCatalog.full(ActionContext.create());
        int code = run(args, System.in, System.out, System.err, catalog);
        if (code != 0) {
            System.exit(code);
        }
    }

    /**
     * Runs one invocation and returns a process exit code: 0 on success, 1 when a verb fails, 2 for
     * a usage error (unknown verb or unreadable input).
     */
    public static int run(String[] args, InputStream stdin, PrintStream out, PrintStream err,
                          ActionCatalog catalog) throws IOException {
        if (args.length == 0 || isHelp(args[0])) {
            usage(out, catalog);
            return 0;
        }
        String verb = args[0];
        switch (verb) {
            case "list" -> {
                list(out, catalog);
                return 0;
            }
            case "console" -> {
                return new Console(catalog, MAPPER).run(new BufferedReader(
                        new InputStreamReader(stdin, StandardCharsets.UTF_8)), out);
            }
            default -> {
                // fall through to verb dispatch
            }
        }
        if (!catalog.names().contains(verb)) {
            err.println("Unknown verb '" + verb + "'. Run 'protomolt list' to see them.");
            return 2;
        }
        ObjectNode input;
        try {
            input = readInput(args, stdin);
        } catch (Exception e) {
            err.println("Could not read input: " + e.getMessage());
            return 2;
        }
        try {
            ObjectNode result = catalog.execute(verb, input);
            out.println(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(result));
            return 0;
        } catch (ActionException e) {
            err.println(e.code() + ": " + e.getMessage());
            return 1;
        } catch (Exception e) {
            err.println("internal-error: " + verb + " failed: " + e.getMessage());
            return 1;
        }
    }

    /** Reads the verb input: an inline {@code --input}/positional JSON, {@code --input-file}, or stdin. */
    private static ObjectNode readInput(String[] args, InputStream stdin) throws IOException {
        String json = null;
        for (int i = 1; i < args.length; i++) {
            switch (args[i]) {
                case "-i", "--input" -> {
                    if (i + 1 >= args.length) {
                        throw new IllegalArgumentException(args[i] + " needs a JSON value");
                    }
                    json = args[++i];
                }
                case "--input-file" -> {
                    if (i + 1 >= args.length) {
                        throw new IllegalArgumentException(args[i] + " needs a path");
                    }
                    json = Files.readString(Path.of(args[++i]));
                }
                default -> {
                    if (json == null && !args[i].startsWith("-")) {
                        json = args[i];
                    }
                }
            }
        }
        if (json == null) {
            json = new String(stdin.readAllBytes(), StandardCharsets.UTF_8);
        }
        return json.isBlank()
                ? MAPPER.createObjectNode()
                : CliJson.readObject(MAPPER, json);
    }

    private static void usage(PrintStream out, ActionCatalog catalog) {
        out.println("protomolt - the protobuf toolkit on the command line");
        out.println();
        out.println("Usage:");
        out.println("  protomolt <verb> [<json>|--input <json>|--input-file <path>]"
                + "   run a verb (JSON in and out; reads stdin if no input is given)");
        out.println("  protomolt list"
                + "                                              list every verb");
        out.println("  protomolt console"
                + "                                           open an interactive console");
        out.println();
        out.println("There are " + catalog.names().size()
                + " verbs; run 'protomolt list' to see them.");
    }

    private static void list(PrintStream out, ActionCatalog catalog) {
        catalog.list().forEach(entry -> out.printf("%-22s %s%n",
                entry.path("name").asText(), entry.path("description").asText()));
    }

    private static boolean isHelp(String arg) {
        return arg.equals("--help") || arg.equals("-h") || arg.equals("help");
    }

    private ProtoMoltCli() {
    }
}
