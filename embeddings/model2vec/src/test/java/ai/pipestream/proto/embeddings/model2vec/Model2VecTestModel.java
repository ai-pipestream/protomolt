package ai.pipestream.proto.embeddings.model2vec;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Writes a tiny genuine Model2Vec model directory: the WordPiece layout
 * {@code StaticEmbeddingModel.load(Path)} reads, with a hand-crafted vocabulary whose
 * 4-dimensional vectors form clusters (dog/puppy/hound, cat/kitten, car/engine) over
 * near-zero filler words, so similarity assertions have known answers.
 *
 * <p>The four files are the ones the directory loader requires: {@code vocab.txt} (one token
 * per line, line number is the matrix row, {@code [UNK]} mandatory), {@code model.safetensors}
 * (8-byte little-endian header length, JSON header, row-major F32 tensor bytes),
 * {@code config.json} (boolean {@code normalize}), and {@code tokenizer_config.json}
 * (boolean {@code do_lower_case}).
 */
final class Model2VecTestModel {

    static final int DIMENSION = 4;

    // Row order is vocab.txt line order. [PAD] and [UNK] are never pooled; the filler words
    // (the, sat, on, mat, drove) sit near zero so content words dominate pooled sentences.
    private static final List<String> TOKENS = List.of(
            "[PAD]", "[UNK]",
            "dog", "puppy", "hound",
            "cat", "kitten",
            "car", "engine",
            "the", "sat", "on", "mat", "drove");

    private static final float[][] VECTORS = {
            {0f, 0f, 0f, 0f},               // [PAD]
            {0f, 0f, 0f, 0f},               // [UNK]
            {1.0f, 0.1f, 0.0f, 0.0f},       // dog
            {0.95f, 0.15f, 0.0f, 0.0f},     // puppy
            {0.9f, 0.2f, 0.05f, 0.0f},      // hound
            {0.1f, 1.0f, 0.0f, 0.0f},       // cat
            {0.15f, 0.95f, 0.05f, 0.0f},    // kitten
            {0.0f, 0.05f, 1.0f, 0.1f},      // car
            {0.05f, 0.0f, 0.95f, 0.15f},    // engine
            {0.01f, 0.01f, 0.01f, 0.01f},   // the
            {0.01f, 0.02f, 0.01f, 0.0f},    // sat
            {0.02f, 0.01f, 0.0f, 0.01f},    // on
            {0.01f, 0.0f, 0.02f, 0.01f},    // mat
            {0.0f, 0.01f, 0.01f, 0.02f},    // drove
    };

    private Model2VecTestModel() {
    }

    /** Writes the model files into {@code directory}. */
    static void write(Path directory) throws IOException {
        Files.write(directory.resolve("vocab.txt"), TOKENS);
        writeSafetensors(directory.resolve("model.safetensors"), VECTORS);
        // The extra fields mirror a published model's config; the loader reads only the booleans.
        Files.writeString(directory.resolve("config.json"),
                "{\"model_type\":\"model2vec\",\"normalize\":true,\"hidden_dim\":" + DIMENSION + "}");
        Files.writeString(directory.resolve("tokenizer_config.json"),
                "{\"do_lower_case\":true,\"tokenizer_class\":\"BertTokenizer\"}");
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
