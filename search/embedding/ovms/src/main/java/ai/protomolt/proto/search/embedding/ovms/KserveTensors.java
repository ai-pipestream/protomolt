package ai.protomolt.proto.search.embedding.ovms;

import inference.GrpcPredictV2.InferTensorContents;
import inference.GrpcPredictV2.ModelInferResponse;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

/** Shared KServe FP32 decoding for OVMS and compatible inference providers. */
public final class KserveTensors {
    private KserveTensors() {}

    /**
     * Decodes a named FP32 [batch, dimension] output without a protobuf/JSON conversion.
     * @param response the inference response
     * @param texts the expected batch size
     * @param outputName the required tensor name
     * @return vectors in request order
     */
    public static List<float[]> decode(ModelInferResponse response, int texts, String outputName) {
        for (int index = 0; index < response.getOutputsCount(); index++) {
            ModelInferResponse.InferOutputTensor output = response.getOutputs(index);
            if (!output.getName().equals(outputName)) {
                continue;
            }
            return vectors(response, index, output, texts);
        }
        throw new IllegalStateException("KServe response carries no output tensor named '"
                + outputName + "'");
    }

    private static List<float[]> vectors(ModelInferResponse response, int index,
            ModelInferResponse.InferOutputTensor output, int texts) {
        String name = output.getName();
        if (!"FP32".equals(output.getDatatype())) {
            throw new IllegalStateException("KServe output tensor '" + name
                    + "' has datatype '" + output.getDatatype() + "', expected FP32");
        }
        if (output.getShapeCount() != 2 || output.getShape(0) != texts) {
            throw new IllegalStateException("KServe output tensor '" + name + "' has shape "
                    + output.getShapeList() + ", expected [" + texts + ", dim]");
        }
        int dim = Math.toIntExact(output.getShape(1));
        if (texts <= 0 || dim <= 0) {
            throw new IllegalStateException("KServe tensor dimensions must be positive");
        }
        float[] flat = floats(response, index, output, Math.multiplyExact(texts, dim));
        List<float[]> vectors = new ArrayList<>(texts);
        for (int i = 0; i < texts; i++) {
            float[] vector = new float[dim];
            System.arraycopy(flat, i * dim, vector, 0, dim);
            vectors.add(vector);
        }
        return vectors;
    }

    private static float[] floats(ModelInferResponse response, int index,
            ModelInferResponse.InferOutputTensor output, int expected) {
        String name = output.getName();
        InferTensorContents contents = output.getContents();
        if (contents.getFp32ContentsCount() > 0) {
            if (contents.getFp32ContentsCount() != expected) {
                throw new IllegalStateException("KServe output tensor '" + name + "' carries "
                        + contents.getFp32ContentsCount()
                        + " fp32 values, expected " + expected);
            }
            float[] flat = new float[expected];
            for (int i = 0; i < expected; i++) {
                flat[i] = contents.getFp32Contents(i);
            }
            return flat;
        }
        // KServe servers commonly answer with raw contents: the flattened little-endian F32
        // payload of the tensor at the same index as 'outputs'. Read it through the
        // ByteString's own buffer rather than toByteArray(): a batch's tensor is
        // texts * dim * 4 bytes and the copy buys nothing, and the bulk FloatBuffer get
        // lands the whole vector in one intrinsified move instead of a getFloat() per
        // component.
        if (index < response.getRawOutputContentsCount()) {
            ByteBuffer buffer = response.getRawOutputContents(index).asReadOnlyByteBuffer()
                    .order(ByteOrder.LITTLE_ENDIAN);
            if (buffer.remaining() != Math.multiplyExact(expected, Float.BYTES)) {
                throw new IllegalStateException("KServe output tensor '" + name
                        + "' carries " + buffer.remaining() + " raw bytes, expected "
                        + Math.multiplyExact(expected, Float.BYTES));
            }
            float[] flat = new float[expected];
            buffer.asFloatBuffer().get(flat);
            return flat;
        }
        throw new IllegalStateException("KServe output tensor '" + name
                + "' carries neither fp32_contents nor raw_output_contents");
    }

}
