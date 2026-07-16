package ai.pipestream.proto.samples;

import ai.pipestream.proto.graph.GraphAuth;
import ai.pipestream.proto.graph.GraphClient;
import ai.pipestream.proto.graph.GraphFiles;
import ai.pipestream.proto.shapes.SchemaInferrer;
import ai.pipestream.proto.shapes.ShapeSynthesizer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;

import java.util.ArrayList;
import java.util.List;

/**
 * Live read -&gt; infer-schema against a real tenant: signs in by device code, reads the SharePoint
 * list-item columns of the documents in a folder, and infers one typed proto message from them —
 * the live mirror of {@code GraphInferSchemaDemoTest}. Nothing is written to the tenant.
 *
 * <pre>
 * ./gradlew -q :samples:runGraphInferSchema \
 *     -Ptenant=&lt;tenant-id&gt; -Pclient=&lt;app-id&gt; [-Pfolder=/Shared Documents] [-Ptype=sharepoint.v1.Documents]
 * </pre>
 */
public final class GraphInferSchemaSample {

    private GraphInferSchemaSample() {
    }

    public static void main(String[] args) throws Exception {
        String tenant = arg(args, "--tenant", null);
        String client = arg(args, "--client", null);
        String folder = arg(args, "--folder", "/");
        String typeName = arg(args, "--type", "sharepoint.v1.Documents");
        if (tenant == null || client == null) {
            System.err.println("usage: GraphInferSchemaSample --tenant <id> --client <id> "
                    + "[--folder <path>] [--type <full.Name>] [--limit <n>]");
            System.exit(2);
            return;
        }
        int limit;
        try {
            limit = Integer.parseInt(arg(args, "--limit", "25"));
        } catch (NumberFormatException e) {
            System.err.println("--limit must be a whole number");
            System.exit(2);
            return;
        }

        GraphAuth.Token token = new GraphAuth(GraphAuth.Config.delegated(tenant, client))
                .deviceCode("Files.Read.All Sites.Read.All offline_access",
                        prompt -> System.out.println("\n>>> " + prompt.message() + "\n"));
        GraphFiles files = new GraphFiles(new GraphClient(token::accessToken));

        JsonNode drive = files.meDrive();
        String driveId = drive.path("id").asText();

        List<Struct> samples = new ArrayList<>();
        for (JsonNode child : files.children(driveId, folder).path("value")) {
            if (samples.size() >= limit) {
                break;   // enough samples gathered
            }
            if (child.path("folder").isObject()) {
                continue;   // sample the files in the folder, not sub-folders
            }
            ObjectNode fields = files.listItemFieldsOnly(driveId, child.path("id").asText());
            if (fields.isEmpty()) {
                continue;   // no list item behind this file (a plain personal-OneDrive file)
            }
            Struct.Builder sample = Struct.newBuilder();
            JsonFormat.parser().merge(fields.toString(), sample);
            samples.add(sample.build());
        }

        if (samples.isEmpty()) {
            System.out.println("No list-item columns found under " + folder
                    + " - point --folder at a SharePoint document library folder.");
            return;
        }

        ShapeSynthesizer.SynthesizedShape shape = new SchemaInferrer().infer(typeName, samples);
        System.out.println("Inferred " + shape.type().getFullName() + " from " + samples.size()
                + " document(s) under " + folder + ":\n");
        System.out.println(shape.protoSource());
    }

    private static String arg(String[] args, String name, String fallback) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(name)) {
                return args[i + 1];
            }
        }
        return fallback;
    }
}
