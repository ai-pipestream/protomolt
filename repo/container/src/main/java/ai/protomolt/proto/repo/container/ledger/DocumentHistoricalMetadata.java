package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.v1.DocumentPublicationRowKind;
import ai.protomolt.proto.repo.v1.DocumentRevisionMetadata;
import ai.protomolt.proto.repo.v1.DocumentSecurity;
import ai.protomolt.proto.repo.v1.HistoricalDocumentMetadata;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonParseException;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.function.Consumer;

/** Exact projection of immutable PostgreSQL JSONB snapshots, not a general JSON input API. */
final class DocumentHistoricalMetadata {
    static final int MAX_BYTES = 1024 * 1024;
    private static final ProtoValidator VALIDATOR = ProtoValidator.create();
    private static final Set<String> KEYS = Set.of("row_kind", "cluster_id", "account_id", "datasource_id",
            "connector_id", "content_type", "filename", "security", "delete_source_blobs_on_settle",
            "source_blob_delete_reason", "crawl_id", "created_at_epoch_micros", "updated_at_epoch_micros");

    private DocumentHistoricalMetadata() {}

    static HistoricalDocumentMetadata decode(int version, String encoded, String account) {
        if (version != 1) throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                "Historical metadata encoding version is unsupported");
        if (encoded == null) throw invalid("Historical metadata is missing");
        if (encoded.getBytes(StandardCharsets.UTF_8).length > MAX_BYTES)
            throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED,
                    "Historical metadata exceeds read capacity");
        try {
            var root = JsonParser.parseString(encoded);
            if (!root.isJsonObject()) throw invalid("Historical metadata must be an object");
            var json = root.getAsJsonObject();
            if (!json.keySet().equals(KEYS)) throw invalid("Historical metadata fields differ from encoding version");
            var kind = switch (string(json.get("row_kind"))) {
                case "INTAKE" -> DocumentPublicationRowKind.DOCUMENT_PUBLICATION_ROW_KIND_INTAKE;
                case "PIPELINE" -> DocumentPublicationRowKind.DOCUMENT_PUBLICATION_ROW_KIND_PIPELINE;
                default -> throw invalid("Historical metadata row kind is invalid");
            };
            var security = DocumentSecurity.newBuilder();
            if (!json.get("security").isJsonObject()) throw invalid("Historical metadata security must be an object");
            JsonFormat.parser().merge(json.get("security").toString(), security);
            var metadata = DocumentRevisionMetadata.newBuilder().setEncodingVersion(version).setRowKind(kind)
                    .setAccountId(string(json.get("account_id"))).setDatasourceId(string(json.get("datasource_id")))
                    .setSecurity(security).setCreatedAtEpochMicros(micros(json.get("created_at_epoch_micros")))
                    .setUpdatedAtEpochMicros(micros(json.get("updated_at_epoch_micros")));
            optional(json, "connector_id", metadata::setConnectorId);
            optional(json, "cluster_id", metadata::setClusterId);
            optional(json, "content_type", metadata::setContentType);
            optional(json, "filename", metadata::setFilename);
            optional(json, "crawl_id", metadata::setCrawlId);
            optional(json, "source_blob_delete_reason", metadata::setSourceBlobDeleteReason);
            var deletion = json.get("delete_source_blobs_on_settle");
            if (!deletion.isJsonPrimitive() || !deletion.getAsJsonPrimitive().isBoolean())
                throw invalid("Historical metadata deletion policy must be boolean");
            metadata.setDeleteSourceBlobsOnSettle(deletion.getAsBoolean());
            var result = HistoricalDocumentMetadata.newBuilder().setKnown(metadata).build();
            if (!metadata.getAccountId().equals(account) || !VALIDATOR.validate(result).valid())
                throw invalid("Historical metadata violates its recorded contract or ownership binding");
            return result;
        } catch (JsonParseException | InvalidProtocolBufferException | ArithmeticException | NumberFormatException malformed) {
            throw new RepositoryException(RepositoryException.Code.DATA_LOSS, "Historical metadata is malformed", malformed);
        }
    }

    private static String string(JsonElement value) {
        if (!value.isJsonPrimitive() || !value.getAsJsonPrimitive().isString())
            throw invalid("Historical metadata text has an invalid type");
        return value.getAsString();
    }
    private static void optional(JsonObject object, String key, Consumer<String> setter) {
        var value = object.get(key);
        if (!value.isJsonNull()) setter.accept(string(value));
    }
    private static long micros(JsonElement value) {
        if (!value.isJsonPrimitive() || !value.getAsJsonPrimitive().isNumber())
            throw invalid("Historical metadata timestamp must be numeric");
        return value.getAsBigDecimal().longValueExact();
    }
    private static RepositoryException invalid(String message) {
        return new RepositoryException(RepositoryException.Code.DATA_LOSS, message);
    }
}
