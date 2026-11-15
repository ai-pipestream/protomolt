package ai.protomolt.proto.emit.parquet;

import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.hadoop.BloomFilterReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static ai.protomolt.proto.emit.parquet.ParquetShapeFixtures.GOLDEN_DOTTED_COLUMN_HADOOP;
import static ai.protomolt.proto.emit.parquet.ParquetShapeFixtures.PARQUET_VERSION_PREFIX;
import static ai.protomolt.proto.emit.parquet.ParquetShapeFixtures.goldenBytes;
import static ai.protomolt.proto.emit.parquet.ParquetShapeFixtures.readAllGroups;
import static ai.protomolt.proto.emit.parquet.ParquetShapeFixtures.readThriftFooter;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins {@code parquet-hadoop} 1.18.2 output for a literal dotted column {@code [a.b]} next to a
 * nested {@code [a, b]} leaf, written with the example Group writer rather than the protobuf
 * emitter. Parquet 1.18.2 keys writer bloom filters by {@code ColumnPath}, so the two columns
 * keep separate bloom filters; the test checks the bytes, the read-back and both filters.
 */
class ParquetHadoopDottedColumnGoldenTest {

    private static final String DOTTED_FIELD_NAME = "a.b";
    private static final String NESTED_GROUP_NAME = "a";
    private static final String NESTED_LEAF_NAME = "b";
    private static final String FLAT_DOT_VALUE = "flat-dot-value";
    private static final String NESTED_B_VALUE = "nested-b-value";
    private static final int BLOOM_FILTER_MAX_BYTES = 64;

    private static final MessageType DOTTED_COLLISION_SCHEMA = Types.buildMessage()
            .required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType())
            .named(DOTTED_FIELD_NAME)
            .optionalGroup()
                .required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType())
                .named(NESTED_LEAF_NAME)
            .named(NESTED_GROUP_NAME)
            .named("pq.golden.v1.DottedCollision");

    @Test
    void parquetHadoopDottedColumnAndNestedGroupCollisionMatchesPinned1182Golden(@TempDir Path dir)
            throws Exception {
        byte[] first = writeDottedCollisionParquet();
        byte[] second = writeDottedCollisionParquet();
        byte[] golden = goldenBytes(GOLDEN_DOTTED_COLUMN_HADOOP);

        assertThat(first).isEqualTo(second);
        assertThat(first).isEqualTo(golden);

        FileMetaData thriftFooter = readThriftFooter(first);
        assertThat(thriftFooter.getCreated_by()).startsWith(PARQUET_VERSION_PREFIX);
        assertThat(thriftFooter.getNum_rows()).isEqualTo(1L);

        List<Group> rows = readAllGroups(dir, GOLDEN_DOTTED_COLUMN_HADOOP, first);
        assertThat(rows).hasSize(1);
        Group row = rows.getFirst();
        assertThat(row.getString(DOTTED_FIELD_NAME, 0)).isEqualTo(FLAT_DOT_VALUE);
        assertThat(row.getGroup(NESTED_GROUP_NAME, 0).getString(NESTED_LEAF_NAME, 0))
                .isEqualTo(NESTED_B_VALUE);

        // Parquet 1.18.2 keys writer bloom filters by ColumnPath rather than toDotString(),
        // keeping [a.b] and [a, b] independent instead of overwriting [a.b]'s bloom filter.
        Path onDisk = dir.resolve("dotted-bloom-check.parquet");
        Files.write(onDisk, first);
        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(onDisk))) {
            BlockMetaData block = reader.getFooter().getBlocks().getFirst();
            BloomFilterReader bloomReader = reader.getBloomFilterDataReader(block);
            Map<ColumnPath, ColumnChunkMetaData> byPath = new LinkedHashMap<>();
            for (ColumnChunkMetaData column : block.getColumns()) {
                byPath.put(column.getPath(), column);
            }

            ColumnChunkMetaData flatChunk = byPath.get(ColumnPath.get(DOTTED_FIELD_NAME));
            ColumnChunkMetaData nestedChunk = byPath.get(
                    ColumnPath.get(NESTED_GROUP_NAME, NESTED_LEAF_NAME));
            assertThat(flatChunk).isNotNull();
            assertThat(nestedChunk).isNotNull();

            BloomFilter flatBloom = bloomReader.readBloomFilter(flatChunk);
            BloomFilter nestedBloom = bloomReader.readBloomFilter(nestedChunk);
            assertThat(flatBloom).isNotNull();
            assertThat(nestedBloom).isNotNull();
            assertThat(flatBloom.findHash(flatBloom.hash(Binary.fromString(FLAT_DOT_VALUE))))
                    .isTrue();
            assertThat(flatBloom.findHash(flatBloom.hash(Binary.fromString(NESTED_B_VALUE))))
                    .isFalse();
            assertThat(nestedBloom.findHash(nestedBloom.hash(Binary.fromString(NESTED_B_VALUE))))
                    .isTrue();
            assertThat(nestedBloom.findHash(nestedBloom.hash(Binary.fromString(FLAT_DOT_VALUE))))
                    .isFalse();
        }
    }

    private static byte[] writeDottedCollisionParquet() throws IOException {
        BufferOutputFile output = new BufferOutputFile();
        SimpleGroupFactory factory = new SimpleGroupFactory(DOTTED_COLLISION_SCHEMA);
        Group group = factory.newGroup();
        group.append(DOTTED_FIELD_NAME, FLAT_DOT_VALUE);
        group.addGroup(NESTED_GROUP_NAME).append(NESTED_LEAF_NAME, NESTED_B_VALUE);
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(output)
                .withType(DOTTED_COLLISION_SCHEMA)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withCodecFactory(new HadoopFreeCodecs())
                .withBloomFilterEnabled(true)
                .withMaxBloomFilterBytes(BLOOM_FILTER_MAX_BYTES)
                .withConf(new PlainParquetConfiguration())
                .build()) {
            writer.write(group);
        }
        return output.bytes();
    }

    private static final class BufferOutputFile implements OutputFile {
        private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        private byte[] bytes() {
            return buffer.toByteArray();
        }

        @Override
        public PositionOutputStream create(long blockSizeHint) {
            return new PositionOutputStream() {
                private long position;

                @Override
                public long getPos() {
                    return position;
                }

                @Override
                public void write(int b) {
                    buffer.write(b);
                    position++;
                }

                @Override
                public void write(byte[] bytes, int offset, int length) {
                    buffer.write(bytes, offset, length);
                    position += length;
                }
            };
        }

        @Override
        public PositionOutputStream createOrOverwrite(long blockSizeHint) {
            buffer.reset();
            return create(blockSizeHint);
        }

        @Override
        public boolean supportsBlockSize() {
            return false;
        }

        @Override
        public long defaultBlockSize() {
            return 0;
        }
    }
}
