package ai.pipestream.proto.emit.parquet;

import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.io.LocalInputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Validate-on-write: the emitter enforces the constraint and CEL rules the descriptor
 * declares, on the original {@link DynamicMessage}, before anything is masked or written —
 * the write fails fast with every violation named, and
 * {@link ParquetExportOptions#withoutValidation()} is the explicit opt-out.
 */
class ParquetValidationTest {

    private static final String VALIDATE = "ai/pipestream/proto/validate/v1/validate.proto";

    private static final String PROTO = """
            syntax = "proto3";
            package pq.guard;
            import "ai/pipestream/proto/validate/v1/validate.proto";
            message Account {
              option (ai.pipestream.proto.validate.v1.message) = {
                cel: {
                  id: "account.within_limit"
                  message: "balance must not exceed the limit"
                  expression: "this.balance <= this.limit"
                }
              };
              string id = 1 [(ai.pipestream.proto.validate.v1.field) = {
                string: { min_len: 3 }
              }];
              int64 balance = 2;
              int64 limit = 3;
            }
            """;

    private static Descriptor accountType() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add(VALIDATE, resource(VALIDATE), "test")
                .add("pq/guard/account.proto", PROTO, "test").build());
        return compiled.descriptorFor("pq/guard/account.proto").orElseThrow()
                .findMessageTypeByName("Account");
    }

    private static DynamicMessage account(Descriptor type, String id, long balance, long limit) {
        return DynamicMessage.newBuilder(type)
                .setField(type.findFieldByName("id"), id)
                .setField(type.findFieldByName("balance"), balance)
                .setField(type.findFieldByName("limit"), limit)
                .build();
    }

    /** ParquetReader over a plain InputFile — no Hadoop filesystem anywhere in the test. */
    private static final class GroupBuilder extends ParquetReader.Builder<Group> {
        private GroupBuilder(org.apache.parquet.io.InputFile file) {
            super(file);
        }

        @Override
        protected ReadSupport<Group> getReadSupport() {
            return new GroupReadSupport();
        }
    }

    @Test
    void validMessagesWriteAndRoundTrip(@TempDir Path dir) throws Exception {
        Descriptor type = accountType();

        byte[] parquet = ParquetEmitter.toBytes(type, List.of(
                account(type, "abc", 50, 100),
                account(type, "def", 100, 100)));

        Path fileOnDisk = dir.resolve("accounts.parquet");
        Files.write(fileOnDisk, parquet);
        try (ParquetReader<Group> reader =
                     new GroupBuilder(new LocalInputFile(fileOnDisk)).build()) {
            Group first = reader.read();
            assertThat(first.getString("id", 0)).isEqualTo("abc");
            assertThat(first.getLong("balance", 0)).isEqualTo(50L);
            Group second = reader.read();
            assertThat(second.getString("id", 0)).isEqualTo("def");
            assertThat(reader.read()).isNull();
        }
    }

    @Test
    void constraintViolationFailsTheWrite() throws Exception {
        Descriptor type = accountType();

        assertThatThrownBy(() -> ParquetEmitter.toBytes(type, List.of(
                account(type, "abc", 50, 100),
                account(type, "x", 50, 100))))
                .isInstanceOf(IOException.class)
                // The second row offended; the failing field path and rule are named.
                .hasMessageContaining("Message 1")
                .hasMessageContaining("pq.guard.Account")
                .hasMessageContaining("[id]")
                .hasMessageContaining("string.min_len");
    }

    @Test
    void messageLevelCelViolationFailsTheWrite() throws Exception {
        Descriptor type = accountType();

        assertThatThrownBy(() -> ParquetEmitter.toBytes(type, List.of(
                account(type, "abc", 150, 100))))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Message 0")
                .hasMessageContaining("account.within_limit")
                .hasMessageContaining("balance must not exceed the limit");
    }

    @Test
    void withoutValidationWritesTheSameInvalidMessage() throws Exception {
        Descriptor type = accountType();
        DynamicMessage invalid = account(type, "x", 150, 100);

        byte[] parquet = ParquetEmitter.toBytes(type, List.of(invalid),
                ProtoParquetSchemas.FieldIdResolver.NONE,
                ParquetExportOptions.NONE.withoutValidation());

        assertThat(new String(parquet, 0, 4, StandardCharsets.US_ASCII)).isEqualTo("PAR1");
    }

    private static String resource(String path) {
        try (InputStream in = ParquetValidationTest.class.getClassLoader()
                .getResourceAsStream(path)) {
            if (in == null) {
                throw new IllegalStateException("missing test resource " + path);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
