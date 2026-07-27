package ai.pipestream.proto.samples;

import ai.pipestream.proto.repo.v1.ChunkEmbedding;
import ai.pipestream.proto.repo.v1.Document;
import ai.pipestream.proto.repo.v1.OwnershipContext;
import ai.pipestream.proto.repo.v1.SemanticChunk;
import ai.pipestream.proto.repo.v1.SemanticProcessingResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.Timestamp;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Locale;
import java.util.UUID;

/**
 * Pure CourtListener JSONL → {@link Document} mapping for the court index sample.
 * One JSONL line is one opinion record (see {@code fixtures/court/opinions_sample.jsonl}).
 */
final class CourtDocuments {

    /** result_id of the single document-level embedding the sample attaches: {@value}. */
    static final String EMBEDDING_RESULT_ID = "court-sample-doc-embedding";

    private static final String DOC_ID_NAMESPACE = "courtlistener|";
    private static final String SOURCE_URI_TEMPLATE = "https://www.courtlistener.com/opinion/%d/%s/";

    private CourtDocuments() {
    }

    /** Maps one CourtListener opinion JSON object to a platform {@link Document}. */
    static Document toDocument(JsonNode opinion) {
        long opinionId = opinion.path("opinion_id").asLong();
        long clusterId = opinion.path("cluster_id").asLong();
        String caseName = text(opinion, "case_name");

        Document.Builder document = Document.newBuilder()
                .setDocId(docId(opinionId))
                .setOwnership(OwnershipContext.newBuilder()
                        .setAccountId("samples")
                        .setDatasourceId("courtlistener"));

        var searchMetadata = document.getSearchMetadataBuilder()
                .setDocumentType("court-opinion")
                .setLanguage("en")
                .setSourceUri(sourceUri(clusterId, caseName));
        if (!caseName.isBlank()) {
            searchMetadata.setTitle(caseName);
        }
        String body = text(opinion, "plain_text");
        if (!body.isBlank()) {
            searchMetadata.setBody(body);
        }
        String author = text(opinion, "author");
        if (author.isBlank()) {
            author = text(opinion, "judges");
        }
        if (!author.isBlank()) {
            searchMetadata.setAuthor(author);
        }
        String dateFiled = text(opinion, "date_filed");
        if (!dateFiled.isBlank()) {
            searchMetadata.setCreationDate(creationDate(dateFiled));
        }
        putMetadata(searchMetadata, opinion, "precedential_status");
        putMetadata(searchMetadata, opinion, "opinion_type");
        putMetadata(searchMetadata, opinion, "docket_id");
        putMetadata(searchMetadata, opinion, "nature_of_suit");
        return document.build();
    }

    /** Deterministic id: UUIDv5 (name-based, MD5) over {@code "courtlistener|<opinion_id>"}. */
    static String docId(long opinionId) {
        return UUID.nameUUIDFromBytes(
                (DOC_ID_NAMESPACE + opinionId).getBytes(StandardCharsets.UTF_8)).toString();
    }

    /** {@code https://www.courtlistener.com/opinion/<cluster_id>/<slug>/}. */
    static String sourceUri(long clusterId, String caseName) {
        return String.format(Locale.ROOT, SOURCE_URI_TEMPLATE, clusterId, slug(caseName));
    }

    /** Lowercase slug with every non-alphanumeric run collapsed to one dash. */
    static String slug(String caseName) {
        String slug = caseName.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]+", "-");
        return slug.replaceAll("^-+|-+$", "");
    }

    /**
     * Attaches one document-level embedding as a single {@link SemanticProcessingResult}
     * ({@value #EMBEDDING_RESULT_ID}) with one chunk carrying one {@link ChunkEmbedding}.
     */
    static Document withEmbedding(Document document, float[] vector, String model) {
        ChunkEmbedding.Builder embedding = ChunkEmbedding.newBuilder()
                .setEmbeddingId(EMBEDDING_RESULT_ID)
                .setChunkId(EMBEDDING_RESULT_ID)
                .setModel(model)
                .setDimensions(vector.length);
        for (float component : vector) {
            embedding.addVector(component);
        }
        SemanticChunk chunk = SemanticChunk.newBuilder()
                .setChunkId(EMBEDDING_RESULT_ID)
                .setChunkNumber(0)
                .addEmbeddings(embedding)
                .build();
        return document.toBuilder()
                .setSearchMetadata(document.getSearchMetadata().toBuilder()
                        .addSemanticResults(SemanticProcessingResult.newBuilder()
                                .setResultId(EMBEDDING_RESULT_ID)
                                .addChunks(chunk)))
                .build();
    }

    /** Vector of the first document-level embedding, or an empty array when none is attached. */
    static float[] firstDocumentEmbedding(Document document) {
        for (SemanticProcessingResult result : document.getSearchMetadata().getSemanticResultsList()) {
            for (SemanticChunk chunk : result.getChunksList()) {
                for (ChunkEmbedding embedding : chunk.getEmbeddingsList()) {
                    if (embedding.getVectorCount() > 0) {
                        float[] vector = new float[embedding.getVectorCount()];
                        for (int i = 0; i < vector.length; i++) {
                            vector[i] = embedding.getVector(i);
                        }
                        return vector;
                    }
                }
            }
        }
        return new float[0];
    }

    private static void putMetadata(
            ai.pipestream.proto.repo.v1.SearchMetadata.Builder searchMetadata,
            JsonNode opinion, String key) {
        String value = text(opinion, key);
        if (!value.isBlank()) {
            searchMetadata.putMetadata(key, value);
        }
    }

    private static Timestamp creationDate(String dateFiled) {
        long epochSecond = LocalDate.parse(dateFiled)
                .atStartOfDay(ZoneOffset.UTC).toEpochSecond();
        return Timestamp.newBuilder().setSeconds(epochSecond).build();
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.path(field);
        return value.isTextual() ? value.asText() : "";
    }
}
