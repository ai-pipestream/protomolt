package ai.pipestream.proto.emit.parquet;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.xerial.snappy.Snappy;

import java.io.IOException;

/**
 * A write-side codec factory that never touches Hadoop. Parquet's default
 * {@code CodecFactory} materializes a Hadoop {@code Configuration} to look codecs up —
 * which drags the entire Hadoop client runtime onto the classpath of anyone who just wants
 * to write columnar bytes. Snappy compression is pure {@code snappy-java} here, so the
 * emitter runs with no Hadoop jars at all.
 */
final class HadoopFreeCodecs implements CompressionCodecFactory {

    @Override
    public BytesInputCompressor getCompressor(CompressionCodecName codecName) {
        return switch (codecName) {
            case UNCOMPRESSED -> new Passthrough();
            case SNAPPY -> new SnappyCompressor();
            default -> throw new IllegalArgumentException(
                    "Only SNAPPY and UNCOMPRESSED are supported without Hadoop: " + codecName);
        };
    }

    @Override
    public BytesInputDecompressor getDecompressor(CompressionCodecName codecName) {
        throw new UnsupportedOperationException("This factory only writes");
    }

    @Override
    public void release() {
    }

    private static final class SnappyCompressor implements BytesInputCompressor {
        @Override
        public BytesInput compress(BytesInput bytes) throws IOException {
            return BytesInput.from(Snappy.compress(bytes.toByteArray()));
        }

        @Override
        public CompressionCodecName getCodecName() {
            return CompressionCodecName.SNAPPY;
        }

        @Override
        public void release() {
        }
    }

    private static final class Passthrough implements BytesInputCompressor {
        @Override
        public BytesInput compress(BytesInput bytes) {
            return bytes;
        }

        @Override
        public CompressionCodecName getCodecName() {
            return CompressionCodecName.UNCOMPRESSED;
        }

        @Override
        public void release() {
        }
    }
}
