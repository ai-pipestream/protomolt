package ai.protomolt.proto.emit.parquet;

import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.emit.parquet.v1.ParquetMaskStrategy;
import ai.protomolt.proto.emit.parquet.v1.ParquetRendererOptions;
import ai.protomolt.proto.emit.renderer.RenderFailure;
import ai.protomolt.proto.emit.renderer.RendererProvider;
import ai.protomolt.proto.emit.renderer.RendererRegistry;
import ai.protomolt.proto.emit.v1.RendererCapability;
import ai.protomolt.proto.projection.MessageProjection;
import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.ValidationResult;
import ai.protomolt.proto.validate.protovalidate.ProtovalidateRuleSource;
import ai.protomolt.proto.validate.source.ProtomoltRuleSource;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.UnknownFieldSet;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Qualification tests for {@link ParquetRendererProvider}, {@link ParquetRendererOptions}, and the
 * bounded cancellation/size-enforced {@link ParquetEmitter} write path.
 */
class ParquetRendererProviderTest {

    private static final String TEST_PROTO = """
            syntax = "proto3";
            package pq.rendition.test;

            import "google/protobuf/any.proto";
            import "google/protobuf/struct.proto";
            import "google/protobuf/timestamp.proto";
            import "ai/protomolt/proto/meta/v1/metadata.proto";
            import "ai/protomolt/proto/projection/v1/projection.proto";
            import "ai/protomolt/proto/validate/v1/validate.proto";

            enum MeasurementState {
              MEASUREMENT_STATE_UNSPECIFIED = 0;
              MEASUREMENT_STATE_READY = 1;
              MEASUREMENT_STATE_FAULT = 2;
            }

            message GeoPoint {
              double lat = 1;
              double lon = 2;
            }

            message TelemetryDocument {
              string doc_id = 1 [(ai.protomolt.proto.validate.v1.field).required = true];
              uint32 seq_u32 = 2;
              uint64 counter_u64 = 3;
              fixed64 digest_u64 = 4;
              optional string note = 5;
              optional int64 optional_zero = 6;
              string operator_email = 7 [(ai.protomolt.proto.meta.v1.field) = {sensitivity: "pii"}];
              optional string api_secret = 8 [(ai.protomolt.proto.meta.v1.field) = {sensitivity: "secret"}];
              MeasurementState state = 9;
              repeated string tags = 10;
              map<string, int64> counters = 11;
              GeoPoint origin = 12;
              GeoPoint destination = 13;
              google.protobuf.Timestamp captured_at = 14;
              google.protobuf.Struct metadata = 15;
              oneof detail {
                string summary_text = 16;
                int64 error_code = 17;
              }
            }

            message EmptyRecord {}

            message HasEmptyNested {
              string id = 1;
              EmptyRecord empty = 2;
            }

            message RecursiveNode {
              string name = 1;
              RecursiveNode parent = 2;
            }

            message CycleAlpha {
              string id = 1;
              CycleBeta beta = 2;
            }

            message CycleBeta {
              CycleAlpha alpha = 1;
            }

            message EnvelopeWithAny {
              string doc_id = 1;
              int64 version = 2;
              google.protobuf.Any raw_payload = 3;
            }

            message BatchItem {
              string item_id = 1;
              int64 quantity = 2;
              string reviewer_email = 3 [(ai.protomolt.proto.meta.v1.field) = {sensitivity: "pii"}];
              google.protobuf.Any opaque_attachment = 4;
            }

            message BatchContainer {
              string batch_id = 1 [(ai.protomolt.proto.validate.v1.field).required = true];
              repeated BatchItem items = 2;
              google.protobuf.Any batch_context = 3;
            }

            message ProjectedEnvelopeRow {
              option (ai.protomolt.proto.projection.v1.sources) = {
                source: "pq.rendition.test.EnvelopeWithAny"
              };
              string doc_id = 1 [
                (ai.protomolt.proto.validate.v1.field).required = true,
                (ai.protomolt.proto.projection.v1.from) = { paths: { path: "doc_id" } }
              ];
              int64 version = 2 [
                (ai.protomolt.proto.validate.v1.field) = { required: true, int64: { gte: 1 } },
                (ai.protomolt.proto.projection.v1.from) = { paths: { path: "version" } }
              ];
            }

            message ProjectedBatchRow {
              option (ai.protomolt.proto.projection.v1.sources) = {
                source: "pq.rendition.test.BatchItem"
              };
              string item_id = 1 [
                (ai.protomolt.proto.validate.v1.field).required = true,
                (ai.protomolt.proto.projection.v1.from) = { paths: { path: "item_id" } }
              ];
              int64 quantity = 2 [
                (ai.protomolt.proto.validate.v1.field) = { required: true, int64: { gt: 0 } },
                (ai.protomolt.proto.projection.v1.from) = { paths: { path: "quantity" } }
              ];
              int64 unit_divisor = 3 [
                (ai.protomolt.proto.projection.v1.from) = { cel: "100 / source.quantity" }
              ];
              string reviewer_email = 4 [
                (ai.protomolt.proto.meta.v1.field) = {sensitivity: "pii"},
                (ai.protomolt.proto.projection.v1.from) = { paths: { path: "reviewer_email" } }
              ];
            }
            """;

    private static FileDescriptor closedTestFile;
    private static DescriptorRegistry descriptorRegistry;

    @BeforeAll
    static void compileSchemas() throws Exception {
        String metaProto = readResource("ai/protomolt/proto/meta/v1/metadata.proto");
        String projectionProto = readResource("ai/protomolt/proto/projection/v1/projection.proto");
        String validateProto = readResource("ai/protomolt/proto/validate/v1/validate.proto");
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("ai/protomolt/proto/meta/v1/metadata.proto", metaProto, "meta")
                .add("ai/protomolt/proto/projection/v1/projection.proto", projectionProto, "proj")
                .add("ai/protomolt/proto/validate/v1/validate.proto", validateProto, "val")
                .add("pq/rendition/test/rendition_test.proto", TEST_PROTO, "test")
                .build());
        // Link from plain FileDescriptorSet bytes WITHOUT an ExtensionRegistry so tests prove
        // closed-descriptor reconstruction (matching RepositorySink's retained schema path)
        // preserves validation, sensitivity masking, and projection metadata.
        FileDescriptorSet unextended =
                FileDescriptorSet.parseFrom(compiled.descriptorSet().toByteArray());
        Map<String, FileDescriptor> linked = linkDescriptorSet(unextended);
        closedTestFile = linked.get("pq/rendition/test/rendition_test.proto");
        descriptorRegistry = DescriptorRegistry.create();
        for (FileDescriptor fd : linked.values()) {
            descriptorRegistry.registerFile(fd);
        }
    }

    @Test
    void optionsContractValidatesIdenticallyUnderProtomoltAndProtovalidate() {
        Descriptor desc = ParquetRendererOptions.getDescriptor();
        assertThat(desc.findFieldByName("skip_validation")).isNull();
        assertThat(desc.findFieldByName("mask_key")).isNull();
        assertThat(desc.findFieldByName("encryption_key")).isNull();

        ProtoValidator protomoltOnly =
                ProtoValidator.forMessageType(desc, List.of(new ProtomoltRuleSource()));
        ProtoValidator protovalidateOnly =
                ProtoValidator.forMessageType(desc, List.of(new ProtovalidateRuleSource()));

        // Default instance MUST pass both validators so ListSaveAsCapabilities probes succeed.
        Message dynamicDefault = DynamicMessage.getDefaultInstance(desc);
        assertBothValid(protomoltOnly, protovalidateOnly, dynamicDefault);
        assertBothValid(protomoltOnly, protovalidateOnly, ParquetRendererOptions.getDefaultInstance());

        ParquetRendererOptions valid = ParquetRendererOptions.newBuilder()
                .addColumns("doc_id")
                .addColumns("counter_u64")
                .addMaskedSensitivityClasses("pii")
                .addMaskedSensitivityClasses("secret_tier_2")
                .setMaskStrategy(ParquetMaskStrategy.PARQUET_MASK_STRATEGY_REDACT)
                .setMaxOutputBytes(4L * 1024 * 1024)
                .setMappingId("telemetry.v1")
                .setPolicyId("export-public")
                .build();
        assertBothValid(protomoltOnly, protovalidateOnly, valid);

        // Blank, dotted, or duplicate column names rejected by both rule sources.
        assertBothInvalid(protomoltOnly, protovalidateOnly,
                ParquetRendererOptions.newBuilder().addColumns("").build());
        assertBothInvalid(protomoltOnly, protovalidateOnly,
                ParquetRendererOptions.newBuilder().addColumns("origin.lat").build());
        assertBothInvalid(protomoltOnly, protovalidateOnly,
                ParquetRendererOptions.newBuilder().addColumns("doc_id").addColumns("doc_id").build());

        // Blank, malformed, or duplicate sensitivity classes rejected by both rule sources.
        assertBothInvalid(protomoltOnly, protovalidateOnly,
                ParquetRendererOptions.newBuilder().addMaskedSensitivityClasses("").build());
        assertBothInvalid(protomoltOnly, protovalidateOnly,
                ParquetRendererOptions.newBuilder().addMaskedSensitivityClasses("PII!").build());
        assertBothInvalid(protomoltOnly, protovalidateOnly,
                ParquetRendererOptions.newBuilder()
                        .addMaskedSensitivityClasses("pii")
                        .addMaskedSensitivityClasses("pii")
                        .build());

        // Out-of-bounds max_output_bytes, overlong IDs, and undefined enum values rejected.
        assertBothInvalid(protomoltOnly, protovalidateOnly,
                ParquetRendererOptions.newBuilder().setMaxOutputBytes(-1L).build());
        assertBothInvalid(protomoltOnly, protovalidateOnly,
                ParquetRendererOptions.newBuilder()
                        .setMaxOutputBytes(ParquetRendererProvider.HARD_MAX_OUTPUT_BYTES + 1L)
                        .build());
        assertBothInvalid(protomoltOnly, protovalidateOnly,
                ParquetRendererOptions.newBuilder().setMappingId("m".repeat(257)).build());
        assertBothInvalid(protomoltOnly, protovalidateOnly,
                ParquetRendererOptions.newBuilder().setPolicyId("p".repeat(257)).build());
        assertBothInvalid(protomoltOnly, protovalidateOnly,
                ParquetRendererOptions.newBuilder().setMaskStrategyValue(99).build());
    }

    @Test
    void serviceLoaderDiscoversProviderAndRendererRegistryQualifiesDefaultOptions()
            throws Exception {
        List<RendererProvider> discovered = ServiceLoader.load(RendererProvider.class)
                .stream()
                .map(ServiceLoader.Provider::get)
                .filter(p -> ParquetRendererProvider.PROVIDER_ID.equals(
                        p.capability().getProviderId()))
                .toList();
        assertThat(discovered).hasSize(1);

        RendererRegistry registry = RendererRegistry.load(
                ParquetRendererProvider.class.getClassLoader(),
                Set.of(ParquetRendererProvider.PROVIDER_ID));
        assertThat(registry.capabilities()).hasSize(1);
        RendererCapability cap = registry.capabilities().getFirst();
        assertThat(cap.getProviderId()).isEqualTo("ai.protomolt.emit.parquet.single");
        assertThat(cap.getProviderVersion()).isEqualTo("1.0");
        assertThat(cap.getSourceProfile()).isEqualTo("protobuf.single_message");
        assertThat(cap.getMediaType()).isEqualTo("application/vnd.apache.parquet");
        assertThat(cap.getExtension()).isEqualTo("parquet");
        assertThat(cap.getOptionsType())
                .isEqualTo("ai.protomolt.proto.emit.parquet.v1.ParquetRendererOptions");
        assertThat(cap.getOptionsFingerprint())
                .isEqualTo(DescriptorFingerprints.fingerprintOf(
                        ParquetRendererOptions.getDescriptor()));
        assertThat(cap.getLossDescription())
                .contains("non-authoritative")
                .contains("UINT_64")
                .contains("Struct");
        assertThat(cap.getMaxInputBytes()).isEqualTo(16 * 1024 * 1024);
        assertThat(cap.getMaxOutputBytes()).isEqualTo(32 * 1024 * 1024);

        Descriptor docDesc = closedTestFile.findMessageTypeByName("TelemetryDocument");
        DynamicMessage sample = sampleTelemetry(docDesc, false, true);
        Message defaultOptions = DynamicMessage.getDefaultInstance(
                discovered.getFirst().optionsDescriptor());
        registry.check(
                ParquetRendererProvider.PROVIDER_ID,
                sample,
                defaultOptions,
                (c, in, opt) -> {},
                () -> false);
        ByteString renderedViaRegistry = registry.render(
                ParquetRendererProvider.PROVIDER_ID,
                sample,
                defaultOptions,
                (c, in, opt) -> {},
                () -> false);
        assertThat(renderedViaRegistry.substring(0, 4).toStringUtf8()).isEqualTo("PAR1");
    }

    @Test
    void singleDocumentRenderRoundTripsUnsignedBoundariesPresenceAndSharedDagTypes(
            @TempDir Path dir) throws Exception {
        Descriptor docDesc = closedTestFile.findMessageTypeByName("TelemetryDocument");
        ParquetRendererProvider provider = new ParquetRendererProvider();

        // Row A: optional note unset, optional_zero explicitly set to 0, uint64 = 2^63
        // (Long.MIN_VALUE bit pattern), fixed64 = 2^64 - 1 (-1L bit pattern), oneof summary_text.
        DynamicMessage docA = sampleTelemetry(docDesc, false, true);
        byte[] bytesA = renderToBytes(provider, docA, ParquetRendererOptions.getDefaultInstance());
        Path fileA = dir.resolve("doc-a.parquet");
        Files.write(fileA, bytesA);

        try (ParquetFileReader footerReader = ParquetFileReader.open(new LocalInputFile(fileA))) {
            MessageType parquetSchema = footerReader.getFooter().getFileMetaData().getSchema();
            assertThat(parquetSchema.getType("counter_u64").getLogicalTypeAnnotation())
                    .isEqualTo(LogicalTypeAnnotation.intType(64, false));
            assertThat(parquetSchema.getType("digest_u64").getLogicalTypeAnnotation())
                    .isEqualTo(LogicalTypeAnnotation.intType(64, false));
            assertThat(parquetSchema.getType("seq_u32").asPrimitiveType().getPrimitiveTypeName())
                    .isEqualTo(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64);
        }

        List<Group> rowsA = readAllGroups(fileA);
        assertThat(rowsA).hasSize(1);
        Group rowA = rowsA.getFirst();
        assertThat(rowA.getString("doc_id", 0)).isEqualTo("doc-100");
        assertThat(rowA.getLong("seq_u32", 0)).isEqualTo(0xFFFF_FFFFL);
        assertThat(Long.toUnsignedString(rowA.getLong("counter_u64", 0)))
                .isEqualTo("9223372036854775808"); // 2^63
        assertThat(Long.toUnsignedString(rowA.getLong("digest_u64", 0)))
                .isEqualTo("18446744073709551615"); // 2^64 - 1
        assertThat(rowA.getFieldRepetitionCount("note")).isZero();
        assertThat(rowA.getFieldRepetitionCount("optional_zero")).isEqualTo(1);
        assertThat(rowA.getLong("optional_zero", 0)).isZero();
        assertThat(rowA.getString("operator_email", 0)).isEqualTo("ops@example.com");
        assertThat(rowA.getString("api_secret", 0)).isEqualTo("tok-live-secret");
        assertThat(rowA.getBinary("state", 0).toStringUsingUTF8())
                .isEqualTo("MEASUREMENT_STATE_READY");
        assertThat(rowA.getGroup("origin", 0).getDouble("lat", 0)).isEqualTo(37.7749);
        assertThat(rowA.getGroup("destination", 0).getDouble("lon", 0)).isEqualTo(-73.9855);
        assertThat(rowA.getLong("captured_at", 0)).isEqualTo(1_710_000_000_000_456L);
        assertThat(rowA.getBinary("metadata", 0).toStringUsingUTF8())
                .contains("\"env\":\"prod\"");
        assertThat(rowA.getString("summary_text", 0)).isEqualTo("nominal");
        assertThat(rowA.getFieldRepetitionCount("error_code")).isZero();

        // Row B: optional note explicitly set to empty string "", optional_zero unset.
        DynamicMessage docB = sampleTelemetry(docDesc, true, false);
        Path fileB = dir.resolve("doc-b.parquet");
        Files.write(
                fileB, renderToBytes(provider, docB, ParquetRendererOptions.getDefaultInstance()));
        Group rowB = readAllGroups(fileB).getFirst();
        assertThat(rowB.getFieldRepetitionCount("note")).isEqualTo(1);
        assertThat(rowB.getString("note", 0)).isEmpty();
        assertThat(rowB.getFieldRepetitionCount("optional_zero")).isZero();
    }

    @Test
    void columnProjectionAndSensitivityMaskingComposeWithoutKeyMaterial(@TempDir Path dir)
            throws Exception {
        Descriptor docDesc = closedTestFile.findMessageTypeByName("TelemetryDocument");
        DynamicMessage doc = sampleTelemetry(docDesc, true, true);
        ParquetRendererProvider provider = new ParquetRendererProvider();

        // Unspecified mask_strategy with non-empty masked_sensitivity_classes defaults to REDACT.
        ParquetRendererOptions redactOptions = ParquetRendererOptions.newBuilder()
                .addColumns("doc_id")
                .addColumns("operator_email")
                .addColumns("api_secret")
                .addColumns("counter_u64")
                .addMaskedSensitivityClasses("pii")
                .build();
        Path redactedFile = dir.resolve("redacted.parquet");
        Files.write(redactedFile, renderToBytes(provider, doc, redactOptions));
        Group redacted = readAllGroups(redactedFile).getFirst();
        assertThat(redacted.getType().getFieldCount()).isEqualTo(4);
        assertThat(redacted.getType().containsField("origin")).isFalse();
        assertThat(redacted.getString("doc_id", 0)).isEqualTo("doc-100");
        assertThat(redacted.getString("operator_email", 0)).isEqualTo("***");
        // "secret" class was not listed, so api_secret remains untouched.
        assertThat(redacted.getString("api_secret", 0)).isEqualTo("tok-live-secret");

        // REMOVE clears optional fields completely (0 occurrences in Parquet).
        ParquetRendererOptions removeOptions = ParquetRendererOptions.newBuilder()
                .addColumns("doc_id")
                .addColumns("operator_email")
                .addColumns("api_secret")
                .addMaskedSensitivityClasses("pii")
                .addMaskedSensitivityClasses("secret")
                .setMaskStrategy(ParquetMaskStrategy.PARQUET_MASK_STRATEGY_REMOVE)
                .build();
        Path removedFile = dir.resolve("removed.parquet");
        Files.write(removedFile, renderToBytes(provider, doc, removeOptions));
        Group removed = readAllGroups(removedFile).getFirst();
        assertThat(removed.getString("operator_email", 0)).isEmpty();
        assertThat(removed.getFieldRepetitionCount("api_secret")).isZero();

        // A message containing an opaque google.protobuf.Any field can be rendered when `columns`
        // explicitly projects out the Any field!
        Descriptor envDesc = closedTestFile.findMessageTypeByName("EnvelopeWithAny");
        DynamicMessage env = DynamicMessage.newBuilder(envDesc)
                .setField(envDesc.findFieldByName("doc_id"), "env-1")
                .setField(envDesc.findFieldByName("version"), 3L)
                .build();
        ParquetRendererOptions dropAnyColumns = ParquetRendererOptions.newBuilder()
                .addColumns("doc_id")
                .addColumns("version")
                .build();
        Path envFile = dir.resolve("env-projected.parquet");
        Files.write(envFile, renderToBytes(provider, env, dropAnyColumns));
        Group envRow = readAllGroups(envFile).getFirst();
        assertThat(envRow.getType().containsField("raw_payload")).isFalse();
        assertThat(envRow.getString("doc_id", 0)).isEqualTo("env-1");
        assertThat(envRow.getLong("version", 0)).isEqualTo(3L);
    }

    @Test
    void refusesUnsupportedSchemasUnknownFieldsUnrecognizedEnumsAndInvalidInputs()
            throws Exception {
        ParquetRendererProvider provider = new ParquetRendererProvider();
        ParquetRendererOptions defaults = ParquetRendererOptions.getDefaultInstance();

        // 1. Empty root message -> UNSUPPORTED
        Descriptor emptyDesc = closedTestFile.findMessageTypeByName("EmptyRecord");
        assertRenderFailure(
                RenderFailure.Kind.UNSUPPORTED,
                "zero columns",
                () -> provider.check(DynamicMessage.getDefaultInstance(emptyDesc), defaults));

        // 2. Empty nested message -> UNSUPPORTED
        Descriptor nestedEmptyDesc = closedTestFile.findMessageTypeByName("HasEmptyNested");
        DynamicMessage nestedEmpty = DynamicMessage.newBuilder(nestedEmptyDesc)
                .setField(nestedEmptyDesc.findFieldByName("id"), "x")
                .build();
        assertRenderFailure(
                RenderFailure.Kind.UNSUPPORTED,
                "Empty nested message",
                () -> provider.check(nestedEmpty, defaults));

        // 3. Direct recursion and mutual recursion -> UNSUPPORTED
        Descriptor recursiveDesc = closedTestFile.findMessageTypeByName("RecursiveNode");
        assertRenderFailure(
                RenderFailure.Kind.UNSUPPORTED,
                "Recursive message schema",
                () -> provider.check(
                        DynamicMessage.newBuilder(recursiveDesc)
                                .setField(recursiveDesc.findFieldByName("name"), "root")
                                .build(),
                        defaults));

        Descriptor cycleAlpha = closedTestFile.findMessageTypeByName("CycleAlpha");
        assertRenderFailure(
                RenderFailure.Kind.UNSUPPORTED,
                "Recursive message schema",
                () -> provider.check(
                        DynamicMessage.newBuilder(cycleAlpha)
                                .setField(cycleAlpha.findFieldByName("id"), "a")
                                .build(),
                        defaults));

        // 4. Opaque google.protobuf.Any column -> UNSUPPORTED
        Descriptor envDesc = closedTestFile.findMessageTypeByName("EnvelopeWithAny");
        assertRenderFailure(
                RenderFailure.Kind.UNSUPPORTED,
                "google.protobuf.Any",
                () -> provider.check(
                        DynamicMessage.newBuilder(envDesc)
                                .setField(envDesc.findFieldByName("doc_id"), "env-1")
                                .build(),
                        defaults));

        // 5. Unknown wire fields -> UNSUPPORTED
        Descriptor docDesc = closedTestFile.findMessageTypeByName("TelemetryDocument");
        DynamicMessage withUnknownWire = sampleTelemetry(docDesc, false, false).toBuilder()
                .setUnknownFields(UnknownFieldSet.newBuilder()
                        .addField(
                                999,
                                UnknownFieldSet.Field.newBuilder().addVarint(42L).build())
                        .build())
                .build();
        assertRenderFailure(
                RenderFailure.Kind.UNSUPPORTED,
                "unknown wire fields",
                () -> provider.check(withUnknownWire, defaults));

        // 6. Unrecognized open enum number -> UNSUPPORTED
        DynamicMessage validBase = sampleTelemetry(docDesc, false, false);
        // Tag for field 9 (state, varint): (9 << 3) | 0 = 72
        ByteArrayOutputStream rawEnumBytes = new ByteArrayOutputStream();
        validBase.writeTo(rawEnumBytes);
        com.google.protobuf.CodedOutputStream coded =
                com.google.protobuf.CodedOutputStream.newInstance(rawEnumBytes);
        coded.writeEnum(9, 777);
        coded.flush();
        DynamicMessage unknownEnumMsg =
                DynamicMessage.parseFrom(docDesc, rawEnumBytes.toByteArray());
        assertRenderFailure(
                RenderFailure.Kind.UNSUPPORTED,
                "Unrecognized enum number 777",
                () -> provider.check(unknownEnumMsg, defaults));

        // 7. Validation violation (blank doc_id violates validate.v1 not_empty) -> INVALID
        DynamicMessage invalidDoc = sampleTelemetry(docDesc, false, false).toBuilder()
                .clearField(docDesc.findFieldByName("doc_id"))
                .build();
        assertRenderFailure(
                RenderFailure.Kind.INVALID,
                "doc_id",
                () -> provider.check(invalidDoc, defaults));

        // 8. Unknown column name -> INVALID
        assertRenderFailure(
                RenderFailure.Kind.INVALID,
                "Unknown column 'no_such_col'",
                () -> provider.check(
                        validBase,
                        ParquetRendererOptions.newBuilder().addColumns("no_such_col").build()));

        // 9. Options max_output_bytes above provider ceiling -> INVALID
        ParquetRendererProvider tightProvider =
                ParquetRendererProvider.singleDocument(64 * 1024, 4096);
        assertRenderFailure(
                RenderFailure.Kind.INVALID,
                "exceeds provider max_output_bytes",
                () -> tightProvider.check(
                        validBase,
                        ParquetRendererOptions.newBuilder().setMaxOutputBytes(8192).build()));
    }

    @Test
    void enforcesInputLimitOutputByteCeilingAndCooperativeCancellation() throws Exception {
        Descriptor docDesc = closedTestFile.findMessageTypeByName("TelemetryDocument");
        DynamicMessage doc = sampleTelemetry(docDesc, true, true);

        // Input size limit refused before Parquet encoding begins.
        ParquetRendererProvider tinyInputProvider =
                ParquetRendererProvider.singleDocument(16, 64 * 1024);
        assertRenderFailure(
                RenderFailure.Kind.LIMIT,
                "max_input_bytes",
                () -> tinyInputProvider.check(doc, ParquetRendererOptions.getDefaultInstance()));

        // Output byte ceiling enforced inside BoundedStreamOutputFile BEFORE writing past cap.
        ParquetRendererProvider provider = new ParquetRendererProvider();
        ParquetRendererOptions tinyOutputCap =
                ParquetRendererOptions.newBuilder().setMaxOutputBytes(128).build();
        ByteArrayOutputStream cappedSink = new ByteArrayOutputStream();
        assertRenderFailure(
                RenderFailure.Kind.LIMIT,
                "max_output_bytes",
                () -> provider.render(doc, tinyOutputCap, cappedSink, () -> false));
        assertThat(cappedSink.size()).isLessThanOrEqualTo(128);

        // Cooperative cancellation before start.
        ByteArrayOutputStream preCancelledSink = new ByteArrayOutputStream();
        assertRenderFailure(
                RenderFailure.Kind.CANCELLED,
                "cancelled",
                () -> provider.render(
                        doc,
                        ParquetRendererOptions.getDefaultInstance(),
                        preCancelledSink,
                        () -> true));
        assertThat(preCancelledSink.size()).isZero();

        // Cooperative cancellation triggered mid-write during PositionOutputStream output.
        AtomicInteger writesObserved = new AtomicInteger();
        ByteArrayOutputStream midCancelledSink = new ByteArrayOutputStream();
        OutputStream trackingOut = new OutputStream() {
            @Override
            public void write(int b) {
                writesObserved.incrementAndGet();
                midCancelledSink.write(b);
            }

            @Override
            public void write(byte[] b, int off, int len) {
                writesObserved.incrementAndGet();
                midCancelledSink.write(b, off, len);
            }
        };
        assertRenderFailure(
                RenderFailure.Kind.CANCELLED,
                "cancelled",
                () -> provider.render(
                        doc,
                        ParquetRendererOptions.getDefaultInstance(),
                        trackingOut,
                        () -> writesObserved.get() >= 1));
    }

    @Test
    void declaredSingleAndRepeatedProjectionsRenderAndFailClosedOnAnyBadRow(@TempDir Path dir)
            throws Exception {
        // 1. Single-row declared MessageProjection maps away EnvelopeWithAny.raw_payload.
        Descriptor envDesc = closedTestFile.findMessageTypeByName("EnvelopeWithAny");
        Descriptor projEnvRowDesc = closedTestFile.findMessageTypeByName("ProjectedEnvelopeRow");
        MessageProjection singleProj =
                MessageProjection.forTarget(projEnvRowDesc, descriptorRegistry).orElseThrow();
        ParquetRendererProvider singleProjectedProvider = ParquetRendererProvider.projected(
                "ai.protomolt.emit.parquet.test.envelope_row",
                "pq.rendition.test.EnvelopeWithAny",
                envDesc,
                singleProj);

        DynamicMessage validEnv = DynamicMessage.newBuilder(envDesc)
                .setField(envDesc.findFieldByName("doc_id"), "env-42")
                .setField(envDesc.findFieldByName("version"), 7L)
                .build();
        Path envParquet = dir.resolve("projected-single.parquet");
        Files.write(
                envParquet,
                renderToBytes(
                        singleProjectedProvider,
                        validEnv,
                        ParquetRendererOptions.getDefaultInstance()));
        List<Group> singleRows = readAllGroups(envParquet);
        assertThat(singleRows).hasSize(1);
        assertThat(singleRows.getFirst().getString("doc_id", 0)).isEqualTo("env-42");
        assertThat(singleRows.getFirst().getLong("version", 0)).isEqualTo(7L);

        // Projected target validation rule (version >= 1) fails closed when version = 0.
        DynamicMessage zeroVersionEnv = DynamicMessage.newBuilder(envDesc)
                .setField(envDesc.findFieldByName("doc_id"), "env-bad")
                .setField(envDesc.findFieldByName("version"), 0L)
                .build();
        assertRenderFailure(
                RenderFailure.Kind.INVALID,
                "version",
                () -> singleProjectedProvider.check(
                        zeroVersionEnv, ParquetRendererOptions.getDefaultInstance()));

        // Wrong source descriptor fails closed with UNSUPPORTED.
        Descriptor docDesc = closedTestFile.findMessageTypeByName("TelemetryDocument");
        assertRenderFailure(
                RenderFailure.Kind.UNSUPPORTED,
                "expects source type",
                () -> singleProjectedProvider.check(
                        sampleTelemetry(docDesc, false, false),
                        ParquetRendererOptions.getDefaultInstance()));

        // 2. Multi-row declared repeated field projection.
        Descriptor batchDesc = closedTestFile.findMessageTypeByName("BatchContainer");
        Descriptor itemDesc = closedTestFile.findMessageTypeByName("BatchItem");
        FieldDescriptor itemsField = batchDesc.findFieldByName("items");
        Descriptor batchRowDesc = closedTestFile.findMessageTypeByName("ProjectedBatchRow");
        MessageProjection itemProj =
                MessageProjection.forTarget(batchRowDesc, descriptorRegistry).orElseThrow();

        ParquetRendererProvider repeatedProvider = ParquetRendererProvider.projectedRepeated(
                "ai.protomolt.emit.parquet.test.batch_items",
                "pq.rendition.test.BatchContainer#items",
                batchDesc,
                itemsField,
                itemProj);

        DynamicMessage batch = DynamicMessage.newBuilder(batchDesc)
                .setField(batchDesc.findFieldByName("batch_id"), "batch-9")
                .addRepeatedField(itemsField, batchItem(itemDesc, "item-1", 4L, "a@example.com"))
                .addRepeatedField(itemsField, batchItem(itemDesc, "item-2", 25L, "b@example.com"))
                .build();
        ParquetRendererOptions maskPii = ParquetRendererOptions.newBuilder()
                .addMaskedSensitivityClasses("pii")
                .build();
        Path batchParquet = dir.resolve("projected-batch.parquet");
        Files.write(batchParquet, renderToBytes(repeatedProvider, batch, maskPii));

        List<Group> batchRows = readAllGroups(batchParquet);
        assertThat(batchRows).hasSize(2);
        assertThat(batchRows.get(0).getString("item_id", 0)).isEqualTo("item-1");
        assertThat(batchRows.get(0).getLong("quantity", 0)).isEqualTo(4L);
        assertThat(batchRows.get(0).getLong("unit_divisor", 0)).isEqualTo(25L);
        assertThat(batchRows.get(0).getString("reviewer_email", 0)).isEqualTo("***");
        assertThat(batchRows.get(1).getString("item_id", 0)).isEqualTo("item-2");
        assertThat(batchRows.get(1).getLong("unit_divisor", 0)).isEqualTo(4L);

        // Empty repeated list produces a valid 0-row Parquet file with the target schema.
        DynamicMessage emptyBatch = DynamicMessage.newBuilder(batchDesc)
                .setField(batchDesc.findFieldByName("batch_id"), "batch-empty")
                .build();
        Path emptyBatchFile = dir.resolve("empty-batch.parquet");
        Files.write(
                emptyBatchFile,
                renderToBytes(
                        repeatedProvider,
                        emptyBatch,
                        ParquetRendererOptions.getDefaultInstance()));
        assertThat(readAllGroups(emptyBatchFile)).isEmpty();

        // CEL runtime failure (division by zero on quantity = 0) on row 1 fails closed with
        // UNSUPPORTED (never skipping failed rows like ingestion sinks!).
        DynamicMessage zeroQuantityBatch = DynamicMessage.newBuilder(batchDesc)
                .setField(batchDesc.findFieldByName("batch_id"), "batch-div0")
                .addRepeatedField(itemsField, batchItem(itemDesc, "item-ok", 5L, "a@example.com"))
                .addRepeatedField(itemsField, batchItem(itemDesc, "item-div0", 0L, "b@example.com"))
                .build();
        ByteArrayOutputStream failedBatchOut = new ByteArrayOutputStream();
        assertRenderFailure(
                RenderFailure.Kind.UNSUPPORTED,
                "Row 1 failed declared projection",
                () -> repeatedProvider.render(
                        zeroQuantityBatch,
                        ParquetRendererOptions.getDefaultInstance(),
                        failedBatchOut,
                        () -> false));
        assertThat(failedBatchOut.size()).isZero();

        // Target row validation failure (blank item_id on row 1) fails closed with INVALID.
        DynamicMessage blankItemBatch = DynamicMessage.newBuilder(batchDesc)
                .setField(batchDesc.findFieldByName("batch_id"), "batch-invalid-row")
                .addRepeatedField(itemsField, batchItem(itemDesc, "item-ok", 5L, "a@example.com"))
                .addRepeatedField(itemsField, batchItem(itemDesc, "", 5L, "b@example.com"))
                .build();
        assertRenderFailure(
                RenderFailure.Kind.INVALID,
                "Row 1",
                () -> repeatedProvider.check(
                        blankItemBatch, ParquetRendererOptions.getDefaultInstance()));
    }

    private static DynamicMessage batchItem(
            Descriptor itemDesc, String itemId, long quantity, String email) {
        DynamicMessage.Builder b = DynamicMessage.newBuilder(itemDesc)
                .setField(itemDesc.findFieldByName("quantity"), quantity)
                .setField(itemDesc.findFieldByName("reviewer_email"), email);
        if (!itemId.isEmpty()) {
            b.setField(itemDesc.findFieldByName("item_id"), itemId);
        }
        return b.build();
    }

    private static DynamicMessage sampleTelemetry(
            Descriptor docDesc, boolean setEmptyNote, boolean setOptionalZero) throws Exception {
        Descriptor geoDesc = docDesc.findFieldByName("origin").getMessageType();
        Descriptor tsDesc = docDesc.findFieldByName("captured_at").getMessageType();
        Descriptor structDesc = docDesc.findFieldByName("metadata").getMessageType();
        Descriptor mapEntry = docDesc.findFieldByName("counters").getMessageType();

        DynamicMessage.Builder structBuilder = DynamicMessage.newBuilder(structDesc);
        com.google.protobuf.util.JsonFormat.parser()
                .merge("{\"env\":\"prod\",\"tier\":1}", structBuilder);

        DynamicMessage.Builder builder = DynamicMessage.newBuilder(docDesc)
                .setField(docDesc.findFieldByName("doc_id"), "doc-100")
                .setField(docDesc.findFieldByName("seq_u32"), -1) // 0xFFFFFFFF
                .setField(docDesc.findFieldByName("counter_u64"), Long.MIN_VALUE) // 2^63
                .setField(docDesc.findFieldByName("digest_u64"), -1L) // 2^64 - 1
                .setField(docDesc.findFieldByName("operator_email"), "ops@example.com")
                .setField(docDesc.findFieldByName("api_secret"), "tok-live-secret")
                .setField(
                        docDesc.findFieldByName("state"),
                        docDesc.findFieldByName("state")
                                .getEnumType()
                                .findValueByName("MEASUREMENT_STATE_READY"))
                .addRepeatedField(docDesc.findFieldByName("tags"), "edge")
                .addRepeatedField(docDesc.findFieldByName("tags"), "verified")
                .addRepeatedField(
                        docDesc.findFieldByName("counters"),
                        DynamicMessage.newBuilder(mapEntry)
                                .setField(mapEntry.findFieldByName("key"), "packets")
                                .setField(mapEntry.findFieldByName("value"), 99L)
                                .build())
                .setField(
                        docDesc.findFieldByName("origin"),
                        DynamicMessage.newBuilder(geoDesc)
                                .setField(geoDesc.findFieldByName("lat"), 37.7749)
                                .setField(geoDesc.findFieldByName("lon"), -122.4194)
                                .build())
                .setField(
                        docDesc.findFieldByName("destination"),
                        DynamicMessage.newBuilder(geoDesc)
                                .setField(geoDesc.findFieldByName("lat"), 40.7580)
                                .setField(geoDesc.findFieldByName("lon"), -73.9855)
                                .build())
                .setField(
                        docDesc.findFieldByName("captured_at"),
                        DynamicMessage.newBuilder(tsDesc)
                                .setField(tsDesc.findFieldByName("seconds"), 1_710_000_000L)
                                .setField(tsDesc.findFieldByName("nanos"), 456_000)
                                .build())
                .setField(docDesc.findFieldByName("metadata"), structBuilder.build())
                .setField(docDesc.findFieldByName("summary_text"), "nominal");
        if (setEmptyNote) {
            builder.setField(docDesc.findFieldByName("note"), "");
        }
        if (setOptionalZero) {
            builder.setField(docDesc.findFieldByName("optional_zero"), 0L);
        }
        return builder.build();
    }

    private static byte[] renderToBytes(
            RendererProvider provider, Message input, Message options) throws IOException {
        provider.check(input, options);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        provider.render(input, options, out, () -> false);
        return out.toByteArray();
    }

    private static List<Group> readAllGroups(Path file) throws IOException {
        List<Group> rows = new ArrayList<>();
        try (ParquetReader<Group> reader = new GroupBuilder(new LocalInputFile(file)).build()) {
            Group current;
            while ((current = reader.read()) != null) {
                rows.add(current);
            }
        }
        return rows;
    }

    private static void assertBothValid(
            ProtoValidator protomolt, ProtoValidator protovalidate, Message msg) {
        ValidationResult r1 = protomolt.validate(msg);
        ValidationResult r2 = protovalidate.validate(msg);
        assertThat(r1.valid()).as("protomolt validate.v1 violations: %s", r1.violations()).isTrue();
        assertThat(r2.valid()).as("buf.validate violations: %s", r2.violations()).isTrue();
    }

    private static void assertBothInvalid(
            ProtoValidator protomolt, ProtoValidator protovalidate, Message msg) {
        assertThat(protomolt.validate(msg).valid())
                .as("expected protomolt validate.v1 to reject %s", msg)
                .isFalse();
        assertThat(protovalidate.validate(msg).valid())
                .as("expected buf.validate to reject %s", msg)
                .isFalse();
    }

    private static void assertRenderFailure(
            RenderFailure.Kind expectedKind,
            String expectedSubstring,
            org.assertj.core.api.ThrowableAssert.ThrowingCallable action) {
        assertThatThrownBy(action)
                .isInstanceOf(RenderFailure.class)
                .satisfies(t -> assertThat(((RenderFailure) t).kind()).isEqualTo(expectedKind))
                .hasMessageContaining(expectedSubstring);
    }

    private static String readResource(String path) throws IOException {
        try (InputStream in =
                     ParquetRendererProviderTest.class.getClassLoader().getResourceAsStream(path)) {
            if (in == null) {
                throw new IllegalStateException("Missing classpath resource: " + path);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static Map<String, FileDescriptor> linkDescriptorSet(FileDescriptorSet set)
            throws Exception {
        Map<String, FileDescriptorProto> byName = new LinkedHashMap<>();
        for (FileDescriptorProto proto : set.getFileList()) {
            byName.put(proto.getName(), proto);
        }
        Map<String, FileDescriptor> built = new LinkedHashMap<>();
        for (FileDescriptorProto proto : set.getFileList()) {
            buildFileDescriptor(proto, byName, built);
        }
        return built;
    }

    private static FileDescriptor buildFileDescriptor(
            FileDescriptorProto proto,
            Map<String, FileDescriptorProto> byName,
            Map<String, FileDescriptor> built) throws Exception {
        FileDescriptor existing = built.get(proto.getName());
        if (existing != null) {
            return existing;
        }
        FileDescriptor[] deps = new FileDescriptor[proto.getDependencyCount()];
        for (int i = 0; i < proto.getDependencyCount(); i++) {
            deps[i] = buildFileDescriptor(byName.get(proto.getDependency(i)), byName, built);
        }
        FileDescriptor linked = FileDescriptor.buildFrom(proto, deps);
        built.put(proto.getName(), linked);
        return linked;
    }

    private static final class GroupBuilder extends ParquetReader.Builder<Group> {
        private GroupBuilder(InputFile file) {
            super(file);
        }

        @Override
        protected ReadSupport<Group> getReadSupport() {
            return new GroupReadSupport();
        }
    }
}
