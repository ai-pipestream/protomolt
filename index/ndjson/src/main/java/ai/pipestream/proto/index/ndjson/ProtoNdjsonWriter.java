package ai.pipestream.proto.index.ndjson;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.util.Objects;

/**
 * Encodes protobuf messages as NDJSON using the message descriptor (via {@link JsonFormat}).
 *
 * <p>Engine-agnostic output path: one JSON object per line. Search engines (Lucene /
 * OpenSearch / Solr) are separate ServiceLoader plugins that consume {@code IndexingPlan}.
 */
public final class ProtoNdjsonWriter {

    // The message body is proto3 JSON via JsonFormat; only the small bulk-action envelope (an
    // ordinary JSON object of index name and id) is built with Jackson, so no JSON is hand-escaped.
    private static final ObjectMapper JSON = new ObjectMapper();

    private final JsonFormat.Printer printer;

    public ProtoNdjsonWriter() {
        this(NdjsonOptions.defaults(), null);
    }

    public ProtoNdjsonWriter(NdjsonOptions options) {
        this(options, null);
    }

    /**
     * @throws IllegalArgumentException when {@code options.omitWhitespace()} is {@code false}:
     *         every output of this writer is line-oriented (NDJSON / bulk), and pretty-printed
     *         multi-line JSON is structurally invalid there.
     */
    public ProtoNdjsonWriter(NdjsonOptions options, DescriptorRegistry descriptorRegistry) {
        Objects.requireNonNull(options, "options");
        if (!options.omitWhitespace()) {
            throw new IllegalArgumentException(
                    "NdjsonOptions.omitWhitespace(false) would produce pretty-printed multi-line JSON, "
                            + "which is structurally invalid NDJSON/bulk output; use omitWhitespace(true)");
        }
        JsonFormat.Printer p = JsonFormat.printer();
        if (descriptorRegistry != null && !descriptorRegistry.registeredDescriptors().isEmpty()) {
            p = p.usingTypeRegistry(JsonFormat.TypeRegistry.newBuilder()
                    .add(descriptorRegistry.registeredDescriptors())
                    .build());
        }
        if (options.preservingProtoFieldNames()) {
            p = p.preservingProtoFieldNames();
        }
        if (options.includingDefaultValueFields()) {
            p = p.alwaysPrintFieldsWithNoPresence();
        }
        // Unconditional: the guard above has already rejected omitWhitespace(false).
        p = p.omittingInsignificantWhitespace();
        this.printer = p;
    }

    /**
     * Single-line JSON object for {@code message} (no trailing newline).
     */
    public String toJsonLine(Message message) {
        Objects.requireNonNull(message, "message");
        try {
            return printer.print(message);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException(
                    "Failed to encode " + message.getDescriptorForType().getFullName() + " as JSON", e);
        }
    }

    /**
     * Writes {@code message} as one NDJSON line (includes trailing {@code \n}).
     */
    public void writeLine(Appendable out, Message message) {
        try {
            out.append(toJsonLine(message)).append('\n');
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Writes each message as its own NDJSON line.
     */
    public void writeLines(Appendable out, Iterable<? extends Message> messages) {
        for (Message message : messages) {
            writeLine(out, message);
        }
    }

    /**
     * OpenSearch bulk {@code index} action + source document (two NDJSON lines).
     *
     * @param index required index name
     * @param id optional document id ({@code null} → let OpenSearch assign)
     */
    public void writeBulkIndex(Appendable out, String index, String id, Message document) {
        Objects.requireNonNull(index, "index");
        Objects.requireNonNull(document, "document");
        appendBulkPair(out, bulkIndexAction(index, id), objectSourceLine(document));
    }

    /**
     * OpenSearch bulk {@code create} action + source (fails if id exists).
     */
    public void writeBulkCreate(Appendable out, String index, String id, Message document) {
        Objects.requireNonNull(index, "index");
        Objects.requireNonNull(document, "document");
        appendBulkPair(out, bulkAction("create", index, id), objectSourceLine(document));
    }

    /**
     * Appends an action line and its source line as one atomic write, so an
     * {@link IOException} can never leave the bulk stream with a dangling action line.
     */
    private static void appendBulkPair(Appendable out, String action, String source) {
        String pair = action + '\n' + source + '\n';
        try {
            out.append(pair);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * OpenSearch bulk {@code delete} action (single line, no source).
     */
    public void writeBulkDelete(Appendable out, String index, String id) {
        Objects.requireNonNull(index, "index");
        Objects.requireNonNull(id, "id");
        if (id.isBlank()) {
            throw new IllegalArgumentException("Bulk delete requires a non-blank id");
        }
        try {
            out.append(bulkAction("delete", index, id)).append('\n');
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public String bulkIndexAction(String index, String id) {
        return bulkAction("index", index, id);
    }

    /**
     * Serialized source line for a bulk action, rejecting messages (e.g. well-known types
     * like {@code Timestamp}) that print as a JSON primitive instead of a JSON object.
     */
    private String objectSourceLine(Message document) {
        String json = toJsonLine(document);
        if (json.isEmpty() || json.charAt(0) != '{') {
            throw new IllegalArgumentException(
                    "Bulk _source must be a JSON object, but " + document.getDescriptorForType().getFullName()
                            + " serializes to the JSON value: " + json);
        }
        return json;
    }

    private static String bulkAction(String op, String index, String id) {
        ObjectNode action = JSON.createObjectNode();
        ObjectNode meta = action.putObject(op);
        meta.put("_index", index);
        if (id != null && !id.isBlank()) {
            meta.put("_id", id);
        }
        try {
            return JSON.writeValueAsString(action);
        } catch (JsonProcessingException e) {
            // An object of two strings cannot fail to serialize.
            throw new IllegalStateException("failed to serialize bulk action metadata", e);
        }
    }

    /** Convenience for callers that prefer {@link Writer}. */
    public void writeLine(Writer out, Message message) {
        writeLine((Appendable) out, message);
    }
}
