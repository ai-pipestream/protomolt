package ai.protomolt.proto.systemtests;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * Writes a genuine Model2Vec model directory (the WordPiece layout
 * {@code StaticEmbeddingModel.load(Path)} reads) whose vocabulary is the
 * corpus's own words, each with a deterministic hash-derived unit vector. The
 * golden path embeds real court text with the real model2vec provider this
 * way: every corpus word is in vocabulary, so no chunk pools to the zero
 * vector cosine similarity cannot score, while the fixture stays
 * self-contained and byte-deterministic. It proves the chunk-and-embed lane,
 * not model quality.
 */
final class CorpusModel2Vec {

    static final int DIMENSION = 32;

    private CorpusModel2Vec() {
    }

    /** Writes the model files into {@code directory}. */
    static void write(Path directory, String corpus) throws IOException {
        // [PAD] and [UNK] rows stay zero; they are never pooled. The corpus
        // vocabulary follows in first-seen order, split the way the WordPiece
        // basic tokenizer does (lowercase, letters and digits only), so every
        // corpus word resolves to its own row.
        LinkedHashSet<String> vocabulary = new LinkedHashSet<>(List.of("[PAD]", "[UNK]"));
        for (String token : corpus.toLowerCase().split("[^\\p{L}\\p{N}]+")) {
            if (!token.isBlank()) {
                vocabulary.add(token);
            }
        }
        List<String> tokens = new ArrayList<>(vocabulary);
        float[][] vectors = new float[tokens.size()][];
        vectors[0] = new float[DIMENSION];
        vectors[1] = new float[DIMENSION];
        for (int row = 2; row < tokens.size(); row++) {
            vectors[row] = vectorFor(tokens.get(row));
        }
        Files.write(directory.resolve("vocab.txt"), tokens);
        writeSafetensors(directory.resolve("model.safetensors"), vectors);
        Files.writeString(directory.resolve("config.json"),
                "{\"model_type\":\"model2vec\",\"normalize\":true,\"hidden_dim\":" + DIMENSION + "}");
        Files.writeString(directory.resolve("tokenizer_config.json"),
                "{\"do_lower_case\":true,\"tokenizer_class\":\"BertTokenizer\"}");
    }

    /**
     * A unit vector derived from the token alone. {@code String.hashCode} is
     * specified by its Javadoc, so the vectors are stable across JVMs; the
     * murmur-style mixing spreads the per-component values so no row is zero
     * and distinct tokens get distinct directions.
     */
    private static float[] vectorFor(String token) {
        float[] row = new float[DIMENSION];
        double norm = 0;
        for (int i = 0; i < DIMENSION; i++) {
            int mixed = mix(token.hashCode() + 0x9E3779B9 * (i + 1));
            row[i] = mixed / (float) Integer.MAX_VALUE;
            norm += (double) row[i] * row[i];
        }
        float scale = (float) Math.sqrt(norm);
        for (int i = 0; i < DIMENSION; i++) {
            row[i] /= scale;
        }
        return row;
    }

    private static int mix(int h) {
        h ^= h >>> 16;
        h *= 0x85EBCA6B;
        h ^= h >>> 13;
        h *= 0xC2B2AE35;
        h ^= h >>> 16;
        return h == 0 ? 1 : h;
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
