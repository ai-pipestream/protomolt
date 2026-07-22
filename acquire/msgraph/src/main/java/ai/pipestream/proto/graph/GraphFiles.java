package ai.pipestream.proto.graph;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Files and their metadata over Microsoft Graph — OneDrive and SharePoint Online through
 * the same {@code driveItem} model (a OneDrive for Business drive <em>is</em> a SharePoint
 * document library under the hood, so everything here works on whichever the tenant has).
 * The read side is ProtoMolt input: {@link #listItemFields} returns the SharePoint column
 * values of a document — a data-rich JSON object that {@code infer-schema} turns into a
 * typed message. The write side is output: upload content, patch metadata columns.
 */
public final class GraphFiles {

    /** Simple-upload ceiling; larger files need an upload session (a later phase). */
    public static final int SIMPLE_UPLOAD_LIMIT = 4 * 1024 * 1024;

    private final GraphClient graph;

    public GraphFiles(GraphClient graph) {
        this.graph = Objects.requireNonNull(graph, "graph");
    }

    /** The signed-in user (delegated flows) — the cheapest connectivity probe. */
    public JsonNode me() throws IOException, InterruptedException {
        return graph.get("/me");
    }

    /** The signed-in user's OneDrive (its {@code driveType} says personal vs business). */
    public JsonNode meDrive() throws IOException, InterruptedException {
        return graph.get("/me/drive");
    }

    /** SharePoint site search; an empty result on a OneDrive-only tenant, not an error. */
    public JsonNode searchSites(String query) throws IOException, InterruptedException {
        return graph.get("/sites?search=" + URLEncoder.encode(query, StandardCharsets.UTF_8));
    }

    /** Document libraries (drives) of a SharePoint site. */
    public JsonNode drives(String siteId) throws IOException, InterruptedException {
        return graph.get("/sites/" + siteId + "/drives");
    }

    /** Children of a folder; {@code folderPath} null or "/" lists the drive root. */
    public JsonNode children(String driveId, String folderPath)
            throws IOException, InterruptedException {
        String base = "/drives/" + driveId + "/root";
        String path = folderPath == null || folderPath.isBlank() || folderPath.equals("/")
                ? base + "/children"
                : base + ":" + encodePath(folderPath) + ":/children";
        return graph.get(path);
    }

    public byte[] download(String driveId, String itemId)
            throws IOException, InterruptedException {
        return graph.getBytes("/drives/" + driveId + "/items/" + itemId + "/content");
    }

    /**
     * Uploads (or overwrites) a file by path. Content up to {@link #SIMPLE_UPLOAD_LIMIT};
     * the destination is always the caller's explicit drive and path.
     */
    public JsonNode upload(String driveId, String folderPath, String fileName, byte[] content,
                           String contentType) throws IOException, InterruptedException {
        if (content.length > SIMPLE_UPLOAD_LIMIT) {
            throw new IllegalArgumentException("Content is " + content.length + " bytes; the "
                    + "simple upload lane caps at " + SIMPLE_UPLOAD_LIMIT
                    + " - use an upload session for large files");
        }
        String path = (folderPath == null || folderPath.isBlank() || folderPath.equals("/")
                ? "" : normalize(folderPath)) + "/" + fileName;
        return graph.putBytes("/drives/" + driveId + "/root:" + encodePath(path) + ":/content",
                content, contentType == null ? "application/octet-stream" : contentType);
    }

    /**
     * The SharePoint list-item column values behind a document — titles, choice columns,
     * managed metadata, whatever the library declares. This is the metadata read lane.
     */
    public JsonNode listItemFields(String driveId, String itemId)
            throws IOException, InterruptedException {
        return graph.get("/drives/" + driveId + "/items/" + itemId
                + "/listItem?$expand=fields");
    }

    /**
     * Just the list-item columns behind a document — the {@code fields} object out of
     * {@link #listItemFields}, ready to hand straight to {@code infer-schema} as one sample.
     * Returns an empty object when the item has no list item (a personal-OneDrive file that
     * belongs to no document library), so a caller can sample a folder without null checks.
     * A {@code driveId} or {@code itemId} that does not resolve still fails.
     */
    public ObjectNode listItemFieldsOnly(String driveId, String itemId)
            throws IOException, InterruptedException {
        JsonNode fields;
        try {
            fields = listItemFields(driveId, itemId).path("fields");
        } catch (GraphClient.GraphApiException e) {
            // Graph answers 404 both for a file with no backing list item (a plain
            // personal-OneDrive file: no columns) and for an item that is not there at all;
            // only the item itself resolving tells the two apart.
            if (e.status() == 404 && itemExists(driveId, itemId)) {
                return JsonNodeFactory.instance.objectNode();
            }
            throw e;
        }
        return fields.isObject() ? (ObjectNode) fields : JsonNodeFactory.instance.objectNode();
    }

    /** Whether the driveItem resolves; any error resolving it leaves the caller's own to report. */
    private boolean itemExists(String driveId, String itemId)
            throws IOException, InterruptedException {
        try {
            graph.get("/drives/" + driveId + "/items/" + itemId);
            return true;
        } catch (GraphClient.GraphApiException e) {
            return false;
        }
    }

    /** Patches list-item columns; {@code fields} holds exactly the columns to change. */
    public JsonNode updateListItemFields(String driveId, String itemId, ObjectNode fields)
            throws IOException, InterruptedException {
        return graph.patch("/drives/" + driveId + "/items/" + itemId + "/listItem/fields",
                fields);
    }

    private static String normalize(String folderPath) {
        String cleaned = folderPath.replace('\\', '/');
        if (!cleaned.startsWith("/")) {
            cleaned = "/" + cleaned;
        }
        return cleaned.replaceAll("/+$", "");
    }

    private static String encodePath(String path) {
        StringBuilder encoded = new StringBuilder();
        for (String segment : normalize(path).split("/")) {
            if (segment.isEmpty()) {
                continue;
            }
            encoded.append('/').append(URLEncoder.encode(segment, StandardCharsets.UTF_8)
                    .replace("+", "%20"));
        }
        return encoded.toString();
    }
}
