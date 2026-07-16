package ai.pipestream.proto.lake.iceberg;

import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.Util;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.stream.Stream;

/**
 * Column-level metrics for a data file, read from the Parquet footer the emitter already
 * produced in memory: per-field lower and upper bounds, value counts, and null counts, keyed by
 * the field ids stamped into the file. Iceberg query engines use these to skip files that cannot
 * match a predicate.
 *
 * <p>The footer is parsed straight from the in-memory bytes at the Thrift level. Iceberg's
 * {@code ParquetUtil.fileMetrics} and parquet-hadoop's {@code ParquetFileReader} both go through
 * {@code ParquetReadOptions}, whose builder instantiates Hadoop classes; reading the footer
 * directly with {@code Util.readFileMetaData} avoids them entirely, keeping the read Hadoop-free
 * the way the emitter's write path is. {@code HadoopFreeMetricsTest} enforces it.</p>
 */
final class IcebergMetrics {

    private IcebergMetrics() {
    }

    /** Metrics for the given Parquet bytes under {@code config}. */
    static Metrics forParquet(byte[] parquet, MetricsConfig config) {
        // Trailer: [file metadata Thrift][4-byte little-endian metadata length]["PAR1"].
        int length = parquet.length;
        int footerLength = (parquet[length - 8] & 0xff)
                | (parquet[length - 7] & 0xff) << 8
                | (parquet[length - 6] & 0xff) << 16
                | (parquet[length - 5] & 0xff) << 24;
        int footerStart = length - 8 - footerLength;
        try (ByteArrayInputStream in = new ByteArrayInputStream(
                parquet, footerStart, footerLength)) {
            FileMetaData thrift = Util.readFileMetaData(in);
            ParquetMetadata footer = new ParquetMetadataConverter().fromParquetMetadata(thrift);
            return ParquetUtil.footerMetrics(footer, Stream.empty(), config);
        } catch (IOException e) {
            throw new UncheckedIOException("Reading Parquet footer for metrics", e);
        }
    }
}
