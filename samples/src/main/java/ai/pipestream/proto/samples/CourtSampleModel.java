package ai.pipestream.proto.samples;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Writes a small genuine Model2Vec model directory for the court sample — the same
 * WordPiece layout {@code StaticEmbeddingModel.load(Path)} reads, mirroring the
 * {@code Model2VecTestModel} util of the model2vec provider's own tests.
 *
 * <p>The vocabulary mixes common English stopwords with legal terms frequent in the
 * CourtListener corpus; every token vector is a deterministic (seeded by token text)
 * unit vector, so document embeddings are stable across runs and nearest neighbours
 * are driven by shared vocabulary. It is a demo model, not a semantic one — point the
 * sample at a real Model2Vec release for meaningful similarity.
 */
final class CourtSampleModel {

    static final int DIMENSION = 32;

    private static final List<String> TOKENS = tokens();

    private CourtSampleModel() {
    }

    /** Writes the model files into {@code directory}, creating it when missing. */
    static void write(Path directory) throws IOException {
        Files.createDirectories(directory);
        Files.write(directory.resolve("vocab.txt"), TOKENS);
        writeSafetensors(directory.resolve("model.safetensors"), vectors(TOKENS));
        // The extra fields mirror a published model's config; the loader reads only the booleans.
        Files.writeString(directory.resolve("config.json"),
                "{\"model_type\":\"model2vec\",\"normalize\":true,\"hidden_dim\":" + DIMENSION + "}");
        Files.writeString(directory.resolve("tokenizer_config.json"),
                "{\"do_lower_case\":true,\"tokenizer_class\":\"BertTokenizer\"}");
    }

    private static List<String> tokens() {
        List<String> tokens = new ArrayList<>(List.of("[PAD]", "[UNK]"));
        tokens.addAll(List.of(
                // frequent English stopwords (the pooling floor of every document)
                "the", "of", "and", "a", "in", "to", "is", "was", "for", "on", "that", "by",
                "this", "with", "as", "at", "an", "be", "are", "or", "it", "from", "we", "not",
                "have", "has", "were", "been", "their", "they", "he", "his", "her", "all", "no",
                "so", "if", "but", "which", "upon", "under", "between", "against", "other", "any",
                "such", "than", "its", "may", "would", "can", "will", "one", "also", "these",
                // legal vocabulary frequent in the CourtListener corpus
                "habeas", "corpus", "court", "district", "appeal", "appeals", "circuit",
                "petitioner", "respondent", "defendant", "plaintiff", "motion", "sentence",
                "sentencing", "trial", "judgment", "order", "case", "federal", "state", "judge",
                "jury", "conviction", "statute", "claim", "claims", "rights", "constitutional",
                "amendment", "evidence", "opinion", "filed", "united", "states", "v", "government",
                "law", "section", "rule", "act", "insurance", "contract", "damages", "puerto",
                "rico", "first", "summary", "dismiss", "dismissal", "review", "denied", "granted",
                "criminal", "civil", "procedure", "liability", "negligence", "patent", "immigration"));
        return tokens;
    }

    /** One deterministic unit vector per token (zero for the never-pooled special tokens). */
    private static float[][] vectors(List<String> tokens) {
        float[][] rows = new float[tokens.size()][];
        rows[0] = new float[DIMENSION]; // [PAD]
        rows[1] = new float[DIMENSION]; // [UNK]
        for (int row = 2; row < tokens.size(); row++) {
            Random random = new Random(tokens.get(row).hashCode());
            float[] vector = new float[DIMENSION];
            double norm = 0;
            for (int i = 0; i < vector.length; i++) {
                vector[i] = (float) random.nextGaussian();
                norm += vector[i] * (double) vector[i];
            }
            norm = Math.sqrt(norm);
            for (int i = 0; i < vector.length; i++) {
                vector[i] = (float) (vector[i] / norm);
            }
            rows[row] = vector;
        }
        return rows;
    }

    /** One F32 tensor named {@code embeddings}: header-length prefix, JSON header, row-major data. */
    private static void writeSafetensors(Path file, float[][] rows) throws IOException {
        int dimension = rows[0].length;
        ByteBuffer data = ByteBuffer.allocate(rows.length * dimension * Float.BYTES)
                .order(ByteOrder.LITTLE_ENDIAN);
        for (float[] row : rows) {
            for (float value : row) {
                data.putFloat(value);
            }
        }
        byte[] header = ("{\"embeddings\":{\"dtype\":\"F32\",\"shape\":[" + rows.length + ","
                + dimension + "],\"data_offsets\":[0," + data.capacity() + "]}}")
                .getBytes(StandardCharsets.UTF_8);
        ByteBuffer out = ByteBuffer.allocate(Long.BYTES + header.length + data.capacity())
                .order(ByteOrder.LITTLE_ENDIAN);
        out.putLong(header.length);
        out.put(header);
        out.put(data.array());
        Files.write(file, out.array());
    }
}
