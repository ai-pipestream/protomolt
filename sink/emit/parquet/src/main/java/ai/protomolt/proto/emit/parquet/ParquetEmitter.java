package ai.protomolt.proto.emit.parquet;

import ai.protomolt.proto.emit.Bundle;
import ai.protomolt.proto.meta.SensitivityMasker;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.ValidationResult;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Objects;

/**
 * The first data-plane renderer: protobuf messages as a Parquet file, driven entirely by
 * the descriptor — dynamic and generated messages alike, no code generation, no native
 * Hadoop, no filesystem. The file is produced in memory and handed back as bytes (or a
 * {@link Bundle} entry), so where the data lands is always the caller's explicit act
 * through a sink — this module never chooses a destination, matching the toolkit's
 * message-data disk policy.
 *
 * <p>Every message is validated against the constraint and CEL rules its descriptor declares
 * before it is written (see {@link ai.protomolt.proto.validate.ProtoValidator}); the write
 * fails fast on the first violation. {@link ParquetExportOptions#withoutValidation()} opts
 * out.</p>
 */
public final class ParquetEmitter {

    private ParquetEmitter() {
    }

    /** Writes one Parquet file of {@code messages} (all instances of {@code descriptor}). */
    public static byte[] toBytes(Descriptor descriptor, Iterable<? extends Message> messages)
            throws IOException {
        return toBytes(descriptor, messages, ProtoParquetSchemas.FieldIdResolver.NONE);
    }

    /**
     * Writes with column ids stamped into the file schema — how table formats (Iceberg)
     * identify columns natively, no name-mapping fallback involved.
     */
    public static byte[] toBytes(Descriptor descriptor, Iterable<? extends Message> messages,
                                 ProtoParquetSchemas.FieldIdResolver ids)
            throws IOException {
        return toBytes(descriptor, messages, ids, ParquetExportOptions.NONE);
    }

    /**
     * Writes an export: only the projected columns, each message validated against its
     * descriptor's declared rules and then masked per {@code options}. Validation runs on the
     * original message, before masking, and the write fails on the first offending message.
     * See {@link ParquetExportOptions}.
     *
     * @throws IOException when a message violates its descriptor's rules and
     *         {@code options.skipValidation()} is {@code false}
     */
    public static byte[] toBytes(Descriptor descriptor, Iterable<? extends Message> messages,
                                 ProtoParquetSchemas.FieldIdResolver ids,
                                 ParquetExportOptions options)
            throws IOException {
        Objects.requireNonNull(descriptor, "descriptor");
        Objects.requireNonNull(messages, "messages");
        Objects.requireNonNull(options, "options");
        // One validator per file, not per message: rule compilation and CEL environment
        // setup happen here, the loop only evaluates.
        ProtoValidator validator = options.skipValidation()
                ? null : ProtoValidator.forMessageType(descriptor);
        InMemoryOutputFile output = new InMemoryOutputFile();
        try (ParquetWriter<Message> writer = new Builder(output,
                new ProtoParquetWriteSupport(descriptor, ids, options.columns()))
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                // The non-Hadoop path, both halves: PlainParquetConfiguration keeps our
                // write support off Hadoop, and the codec factory keeps parquet's own
                // CodecFactory (which materializes a Hadoop Configuration) out of the
                // picture. Net effect: zero Hadoop classes load at run time.
                .withCodecFactory(new HadoopFreeCodecs())
                .withConf(new org.apache.parquet.conf.PlainParquetConfiguration())
                .build()) {
            int index = 0;
            for (Message message : messages) {
                if (!message.getDescriptorForType().getFullName()
                        .equals(descriptor.getFullName())) {
                    throw new IOException("Expected " + descriptor.getFullName() + " but got "
                            + message.getDescriptorForType().getFullName());
                }
                if (validator != null) {
                    ValidationResult result = validator.validate(message);
                    if (!result.valid()) {
                        throw validationFailure(descriptor, index, result);
                    }
                }
                writer.write(masked(message, options));
                index++;
            }
        }
        return output.bytes();
    }

    private static IOException validationFailure(Descriptor descriptor, int index,
                                                 ValidationResult result) {
        StringBuilder sb = new StringBuilder("Message ").append(index).append(" of type ")
                .append(descriptor.getFullName()).append(" failed validation:");
        for (ValidationResult.Violation violation : result.violations()) {
            sb.append(" [").append(violation.path()).append("] ")
                    .append(violation.ruleId()).append(": ").append(violation.message());
        }
        return new IOException(sb.toString());
    }

    private static Message masked(Message message, ParquetExportOptions options) {
        if (!options.masks()) {
            return message;
        }
        SensitivityMasker.Strategy strategy = options.maskStrategy();
        boolean keyed = strategy == SensitivityMasker.Strategy.ENCRYPT
                || strategy == SensitivityMasker.Strategy.DECRYPT;
        return keyed
                ? SensitivityMasker.mask(message, options.maskClasses(), strategy,
                        options.maskKey()).message()
                : SensitivityMasker.mask(message, options.maskClasses(), strategy).message();
    }

    /** The same file as a one-entry {@link Bundle}, ready for any sink. */
    public static Bundle bundle(String path, Descriptor descriptor,
                                Iterable<? extends Message> messages) throws IOException {
        return Bundle.builder().add(path, toBytes(descriptor, messages)).build();
    }

    /** An exported file (projection and masking applied) as a one-entry {@link Bundle}. */
    public static Bundle bundle(String path, Descriptor descriptor,
                                Iterable<? extends Message> messages,
                                ProtoParquetSchemas.FieldIdResolver ids,
                                ParquetExportOptions options) throws IOException {
        return Bundle.builder().add(path, toBytes(descriptor, messages, ids, options)).build();
    }

    private static final class Builder extends ParquetWriter.Builder<Message, Builder> {
        private final WriteSupport<Message> writeSupport;

        private Builder(OutputFile file, WriteSupport<Message> writeSupport) {
            super(file);
            this.writeSupport = writeSupport;
        }

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        protected WriteSupport<Message> getWriteSupport(Configuration conf) {
            return writeSupport;
        }

        @Override
        protected WriteSupport<Message> getWriteSupport(
                org.apache.parquet.conf.ParquetConfiguration conf) {
            return writeSupport;
        }
    }

    /** Parquet only needs position tracking on write, so a heap buffer is a valid file. */
    private static final class InMemoryOutputFile implements OutputFile {
        private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        byte[] bytes() {
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
