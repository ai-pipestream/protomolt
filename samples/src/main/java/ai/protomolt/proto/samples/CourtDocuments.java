package ai.protomolt.proto.samples;

import ai.protomolt.proto.repo.semantic.v1.DerivedText;
import ai.protomolt.proto.repo.semantic.v1.EmbeddingSet;
import ai.protomolt.proto.repo.semantic.v1.OffsetUnit;
import ai.protomolt.proto.repo.semantic.v1.Segment;
import ai.protomolt.proto.repo.semantic.v1.SegmentVector;
import ai.protomolt.proto.repo.semantic.v1.Segmentation;
import ai.protomolt.proto.repo.semantic.v1.SegmentationDigestInput;
import ai.protomolt.proto.repo.semantic.v1.SegmentationKind;
import ai.protomolt.proto.repo.semantic.v1.Similarity;
import ai.protomolt.proto.repo.semantic.v1.SourceTextBinding;
import ai.protomolt.proto.repo.semantic.v1.TextSource;
import ai.protomolt.proto.repo.v1.ChunkEmbedding;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.OwnershipContext;
import ai.protomolt.proto.repo.v1.SemanticChunk;
import ai.protomolt.proto.repo.v1.SemanticProcessingResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.Timestamp;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.HexFormat;
import java.util.Locale;
import java.util.UUID;

/**
 * Pure CourtListener JSONL → {@link Document} mapping for the court index sample.
 * One JSONL line is one opinion record (see {@code fixtures/court/opinions_sample.jsonl}).
 */
final class CourtDocuments {

    /** Segmentation, segment, and embedding-set ID of the single document-level embedding: {@value}. */
    static final String EMBEDDING_RESULT_ID = "court-sample-doc-embedding";

    private static final String EMPTY_TEXT_SHA256 =
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
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
     * Attaches one document-level embedding as a single {@link Segmentation} and
     * {@link EmbeddingSet} ({@value #EMBEDDING_RESULT_ID}) with one segment carrying one
     * {@link SegmentVector}.
     */
    static Document withEmbedding(Document document, float[] vector, String model) {
        TextSource source = TextSource.newBuilder()
                .setDerived(DerivedText.newBuilder()
                        .setDerivationId(EMBEDDING_RESULT_ID)
                        .setText(""))
                .setBinding(SourceTextBinding.newBuilder()
                        .setResolvedTextSha256(EMPTY_TEXT_SHA256)
                        .setUtf8Length(0))
                .build();
        Segment segment = Segment.newBuilder()
                .setSegmentId(EMBEDDING_RESULT_ID)
                .setOrdinal(0)
                .setText("")
                .setStart(0)
                .setEnd(0)
                .build();
        SegmentationDigestInput digestInput = SegmentationDigestInput.newBuilder()
                .setEncodingVersion(1)
                .setKind(SegmentationKind.SEGMENTATION_KIND_PARAGRAPH)
                .setSource(source)
                .setOffsetUnit(OffsetUnit.OFFSET_UNIT_UTF8_BYTE)
                .addSegments(segment)
                .build();
        Segmentation segmentation = Segmentation.newBuilder()
                .setSegmentationId(EMBEDDING_RESULT_ID)
                .setContentDigest(sha256Hex(digestInput.toByteArray()))
                .setKind(SegmentationKind.SEGMENTATION_KIND_PARAGRAPH)
                .setSource(source)
                .setOffsetUnit(OffsetUnit.OFFSET_UNIT_UTF8_BYTE)
                .addSegments(segment)
                .build();
        SegmentVector.Builder segmentVector = SegmentVector.newBuilder()
                .setSegmentId(EMBEDDING_RESULT_ID);
        for (float component : vector) {
            segmentVector.addValues(component);
        }
        EmbeddingSet embeddingSet = EmbeddingSet.newBuilder()
                .setEmbeddingSetId(EMBEDDING_RESULT_ID)
                .setSegmentationId(EMBEDDING_RESULT_ID)
                .setSegmentationDigest(segmentation.getContentDigest())
                .setModel(model)
                .setDimensions(vector.length)
                .setSimilarity(Similarity.SIMILARITY_COSINE)
                .addVectors(segmentVector)
                .build();
        return document.toBuilder()
                .setSearchMetadata(document.getSearchMetadata().toBuilder()
                        .addSegmentations(segmentation)
                        .addEmbeddingSets(embeddingSet))
                .build();
    }

    /** Vector of the first document-level embedding, or an empty array when none is attached. */
    static float[] firstDocumentEmbedding(Document document) {
        for (EmbeddingSet embeddingSet : document.getSearchMetadata().getEmbeddingSetsList()) {
            for (SegmentVector segmentVector : embeddingSet.getVectorsList()) {
                if (segmentVector.getValuesCount() > 0) {
                    float[] vector = new float[segmentVector.getValuesCount()];
                    for (int i = 0; i < vector.length; i++) {
                        vector[i] = segmentVector.getValues(i);
                    }
                    return vector;
                }
            }
        }
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

    private static String sha256Hex(byte[] input) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(input));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    private static void putMetadata(
            ai.protomolt.proto.repo.v1.SearchMetadata.Builder searchMetadata,
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
