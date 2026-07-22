package ai.pipestream.proto.cli;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintStream;

/**
 * The interactive console: a line is {@code <verb> <json>}, run against the same catalog the CLI
 * dispatches, with the result printed as JSON. {@code list}/{@code help} name the verbs and
 * {@code exit}/{@code quit} leave. A verb failure prints its error and keeps the session going,
 * so a typo does not end the console.
 */
final class Console {

    private static final String PROMPT = "protomolt> ";

    private final ActionCatalog catalog;
    private final ObjectMapper mapper;

    Console(ActionCatalog catalog, ObjectMapper mapper) {
        this.catalog = catalog;
        this.mapper = mapper;
    }

    int run(BufferedReader in, PrintStream out) throws IOException {
        out.println("ProtoMolt console. Enter '<verb> <json>', 'list', 'help', or 'exit'.");
        out.print(PROMPT);
        out.flush();
        String line;
        while ((line = in.readLine()) != null) {
            line = line.trim();
            if (!line.isEmpty()) {
                if (line.equals("exit") || line.equals("quit")) {
                    break;
                }
                if (line.equals("list") || line.equals("help")) {
                    catalog.list().forEach(entry -> out.printf("%-22s %s%n",
                            entry.path("name").asText(), entry.path("description").asText()));
                } else {
                    runLine(line, out);
                }
            }
            out.print(PROMPT);
            out.flush();
        }
        out.println();
        return 0;
    }

    private void runLine(String line, PrintStream out) {
        int space = line.indexOf(' ');
        String verb = space < 0 ? line : line.substring(0, space);
        String json = space < 0 ? "" : line.substring(space + 1).trim();
        if (!catalog.names().contains(verb)) {
            out.println("Unknown verb '" + verb + "'. Type 'list' to see them.");
            return;
        }
        try {
            ObjectNode input = json.isBlank()
                    ? mapper.createObjectNode()
                    : CliJson.readObject(mapper, json);
            ObjectNode result = catalog.execute(verb, input);
            out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(result));
        } catch (ActionException e) {
            out.println(e.code() + ": " + e.getMessage());
        } catch (Exception e) {
            out.println("error: " + e.getMessage());
        }
    }
}
