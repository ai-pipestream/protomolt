package ai.pipestream.proto.kafka.connect.opensearch;

import ai.pipestream.proto.index.opensearch.OpenSearchMappingGenerator;
import ai.pipestream.proto.index.spi.IndexFieldKind;
import ai.pipestream.proto.index.spi.IndexMapping;
import ai.pipestream.proto.kafka.wire.ConfluentWireFormat;
import ai.pipestream.proto.index.hints.FieldIndexHint;
import ai.pipestream.proto.index.hints.IndexFieldType;
import ai.pipestream.proto.index.hints.IndexingHintsProto;
import ai.pipestream.proto.validate.FieldRules;
import ai.pipestream.proto.validate.MessageRules;
import ai.pipestream.proto.validate.StringRules;
import ai.pipestream.proto.validate.ValidateProto;
import com.google.protobuf.Any;
import com.google.protobuf.AnyProto;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldOptions;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.DescriptorProtos.MessageOptions;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Adversarial pass over {@link OpenSearchSinkTask}: batch-level id collisions, every value
 * format's hostile input, the declared-rules gates (message-level {@code skip_when}, the
 * {@code google.protobuf.Any} payload gate and its {@code validate_payloads} opt-out),
 * document-id path shapes, and the task lifecycle edges. No HTTP: every test drives the
 * package-private {@link OpenSearchSinkTask.IndexClient} seam.
 */
class OpenSearchSinkTaskEdgeCaseTest {

    // ------------------------------------------------------------ batching / ids

    /**
     * At-least-once with a message-derived id means a redelivered record overwrites its own
     * document. Two records sharing an id inside ONE batch collapse to one map entry, which
     * is the same end state OpenSearch reaches from two same-_id index actions in one bulk
     * request (last one applied wins), so the collapse is not data loss.
     */
    @Test
    void duplicateDocumentIdsInOneBatchCollapseToTheLastRecord() throws Exception {
        OpenSearchSinkTaskTest.RecordingClient client = new OpenSearchSinkTaskTest.RecordingClient();
        OpenSearchSinkTask task = startedTask(client,
                Map.of(OpenSearchSinkConfig.DOCUMENT_ID_PATH, "doc_id"));

        task.put(List.of(
                record(doc(b -> b.set("doc_id", "d1").set("title", "first")).toByteArray(), 1),
                record(doc(b -> b.set("doc_id", "d1").set("title", "second")).toByteArray(), 2)));

        Map<String, Map<String, Object>> documents = client.writes.get(0).documents();
        assertThat(documents).containsOnlyKeys("d1");
        assertThat(documents.get("d1")).containsEntry("headline", "second");
    }

    @Test
    void aWhitespaceOnlyDocumentIdIsRejectedLikeAnUnsetOne() throws Exception {
        OpenSearchSinkTaskTest.RecordingClient client = new OpenSearchSinkTaskTest.RecordingClient();
        OpenSearchSinkTask task = startedTask(client,
                Map.of(OpenSearchSinkConfig.DOCUMENT_ID_PATH, "doc_id"));

        assertThatThrownBy(() -> task.put(List.of(
                record(doc(b -> b.set("doc_id", "   ").set("title", "Hello")).toByteArray(), 1))))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("doc_id");
        assertThat(client.writes).isEmpty();
    }

    /**
     * An int id stringifies, zero included: ids are read with {@code includeDefaults}, so
     * a legal {@code id = 0} is "0" rather than being mistaken for an unset field.
     */
    @Test
    void anIntegerIdPathStringifiesZeroIncluded() throws Exception {
        OpenSearchSinkTaskTest.RecordingClient client = new OpenSearchSinkTaskTest.RecordingClient();
        OpenSearchSinkTask task = startedTask(client,
                Map.of(OpenSearchSinkConfig.DOCUMENT_ID_PATH, "pages"));

        task.put(List.of(record(doc(b -> b.set("title", "Hello").set("pages", 7)).toByteArray(), 1)));
        assertThat(client.writes.get(0).documents()).containsOnlyKeys("7");

        task.put(List.of(record(doc(b -> b.set("title", "Hello").set("pages", 0)).toByteArray(), 2)));
        assertThat(client.writes.get(1).documents()).containsOnlyKeys("0");
    }

    @Test
    void aDottedIdPathResolvesAndAnUnsetParentIsADataError() throws Exception {
        OpenSearchSinkTaskTest.RecordingClient client = new OpenSearchSinkTaskTest.RecordingClient();
        OpenSearchSinkTask task = startedTask(client,
                Map.of(OpenSearchSinkConfig.DOCUMENT_ID_PATH, "child.code"));

        EdgeFixture fixture = EdgeFixture.create();
        DynamicMessage child = DynamicMessage.newBuilder(fixture.child)
                .setField(fixture.child.findFieldByName("code"), "c1")
                .build();
        task.put(List.of(record(
                doc(b -> b.set("title", "Hello").set("child", child)).toByteArray(), 1)));
        assertThat(client.writes.get(0).documents()).containsOnlyKeys("c1");

        assertThatThrownBy(() -> task.put(List.of(
                record(doc(b -> b.set("title", "Hello")).toByteArray(), 2))))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("child.code");
    }

    /**
     * A non-scalar document.id.path is refused, never stringified: a message value would be
     * an unstable text-format dump and a repeated value a bracketed list — an empty one,
     * "[]", would silently collapse every id-less record into one document.
     */
    @Test
    void nonScalarDocumentIdPathsAreRejectedRatherThanStringified() throws Exception {
        EdgeFixture fixture = EdgeFixture.create();
        DynamicMessage child = DynamicMessage.newBuilder(fixture.child)
                .setField(fixture.child.findFieldByName("code"), "c1")
                .build();

        OpenSearchSinkTaskTest.RecordingClient messageIdClient =
                new OpenSearchSinkTaskTest.RecordingClient();
        OpenSearchSinkTask messageIdTask = startedTask(messageIdClient,
                Map.of(OpenSearchSinkConfig.DOCUMENT_ID_PATH, "child"));
        assertThatThrownBy(() -> messageIdTask.put(List.of(record(
                doc(b -> b.set("title", "Hello").set("child", child)).toByteArray(), 1))))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("child");

        OpenSearchSinkTaskTest.RecordingClient listIdClient =
                new OpenSearchSinkTaskTest.RecordingClient();
        OpenSearchSinkTask listIdTask = startedTask(listIdClient,
                Map.of(OpenSearchSinkConfig.DOCUMENT_ID_PATH, "tags"));
        assertThatThrownBy(() -> listIdTask.put(List.of(record(
                doc(b -> b.set("title", "Hello").add("tags", "a").add("tags", "b")).toByteArray(), 1))))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("tags");
    }

    // ------------------------------------------------------------ value formats

    /**
     * A hand-built Confluent frame, both index encodings the spec allows: the lone zero byte
     * shortcut for {@code [0]} and an explicit zigzag-varint count+index pair. Neither the
     * schema id nor the message index is checked against the configured type — the payload is
     * always parsed as {@code message.type}, matching the Iceberg sink and the SMT codec.
     */
    @Test
    void confluentFramedRecordsDecodeWhicheverIndexEncodingTheyCarry() throws Exception {
        OpenSearchSinkTaskTest.RecordingClient client = new OpenSearchSinkTaskTest.RecordingClient();
        OpenSearchSinkTask task = startedTask(client,
                Map.of(OpenSearchSinkConfig.VALUE_FORMAT, "confluent",
                        OpenSearchSinkConfig.DOCUMENT_ID_PATH, "doc_id"));

        byte[] payload = doc(b -> b.set("doc_id", "d1").set("title", "Hello")).toByteArray();
        // messageIndex [0]: the single zero byte shortcut.
        task.put(List.of(record(confluentFrame(7, new byte[]{0}, payload), 1)));
        // messageIndex [1]: count 1 then index 1, both zigzag varints (Doc is the second
        // message declared in the fixture file).
        byte[] otherPayload = doc(b -> b.set("doc_id", "d2").set("title", "Hello")).toByteArray();
        task.put(List.of(record(confluentFrame(42, new byte[]{2, 2}, otherPayload), 2)));
        // A message index that points nowhere is still not consulted.
        byte[] thirdPayload = doc(b -> b.set("doc_id", "d3").set("title", "Hello")).toByteArray();
        task.put(List.of(record(confluentFrame(42, new byte[]{2, 14}, thirdPayload), 3)));

        assertThat(client.writes).hasSize(3);
        assertThat(client.writes.get(0).documents()).containsOnlyKeys("d1");
        assertThat(client.writes.get(1).documents()).containsOnlyKeys("d2");
        assertThat(client.writes.get(2).documents()).containsOnlyKeys("d3");
    }

    @Test
    void malformedConfluentFramesAreDataErrors() throws Exception {
        OpenSearchSinkTaskTest.RecordingClient client = new OpenSearchSinkTaskTest.RecordingClient();
        OpenSearchSinkTask task = startedTask(client,
                Map.of(OpenSearchSinkConfig.VALUE_FORMAT, "confluent"));

        // Non-zero magic byte.
        assertThatThrownBy(() -> task.put(List.of(record(new byte[]{1, 0, 0, 0, 7, 0}, 1))))
                .isInstanceOf(DataException.class);
        // Too short to hold the 5-byte prefix plus an index.
        assertThatThrownBy(() -> task.put(List.of(record(new byte[]{0, 0, 0, 0}, 2))))
                .isInstanceOf(DataException.class);
        // A non-byte[] value cannot be framed at all.
        assertThatThrownBy(() -> task.put(List.of(record("framed?", 3))))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("byte[]");
        assertThat(client.writes).isEmpty();
    }

    /** JSON is text: a String value (the StringConverter's output) is as valid as byte[]. */
    @Test
    void jsonValueFormatAcceptsAPlainStringValue() throws Exception {
        OpenSearchSinkTaskTest.RecordingClient client = new OpenSearchSinkTaskTest.RecordingClient();
        OpenSearchSinkTask task = startedTask(client,
                Map.of(OpenSearchSinkConfig.VALUE_FORMAT, "json",
                        OpenSearchSinkConfig.DOCUMENT_ID_PATH, "doc_id"));

        task.put(List.of(record("{\"doc_id\":\"d1\",\"title\":\"Hello\"}", 1)));

        assertThat(client.writes.get(0).documents()).containsOnlyKeys("d1");
        assertThat(client.writes.get(0).documents().get("d1")).containsEntry("headline", "Hello");
    }

    @Test
    void protobufBytesThatDoNotParseAreDataErrors() throws Exception {
        OpenSearchSinkTaskTest.RecordingClient client = new OpenSearchSinkTaskTest.RecordingClient();
        OpenSearchSinkTask task = startedTask(client, Map.of());

        // Tag for field 1 (length-delimited) claiming 5 bytes with only 1 present.
        assertThatThrownBy(() -> task.put(List.of(record(new byte[]{0x0A, 0x05, 0x61}, 1))))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("connect.edge.Doc");
    }

    /**
     * proto3 is lenient: an unrelated message whose field 1 is also a string parses cleanly as
     * Doc, so the wire cannot tell the sink it got the wrong type. The declared-rules gate is
     * what catches it — the required field the real type has is simply absent.
     */
    @Test
    void bytesThatParseAsTheWrongTypeAreCaughtByTheDeclaredRules() throws Exception {
        OpenSearchSinkTaskTest.RecordingClient client = new OpenSearchSinkTaskTest.RecordingClient();
        OpenSearchSinkTask task = startedTask(client, Map.of());
        EdgeFixture fixture = EdgeFixture.create();
        DynamicMessage foreign = DynamicMessage.newBuilder(fixture.inner)
                .setField(fixture.inner.findFieldByName("label"), "not-a-doc")
                .build();

        assertThatThrownBy(() -> task.put(List.of(record(foreign.toByteArray(), 1))))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("title");
        assertThat(client.writes).isEmpty();
    }

    /** Sinks in this repo reject tombstones (the Iceberg sink does the same); the SMTs pass
     * them through. The message has to be actionable, so it names the coordinates. */
    @Test
    void tombstonesAreDataErrorsNamingTheirCoordinates() throws Exception {
        OpenSearchSinkTaskTest.RecordingClient client = new OpenSearchSinkTaskTest.RecordingClient();
        OpenSearchSinkTask task = startedTask(client, Map.of());

        assertThatThrownBy(() -> task.put(List.of(record(null, 42))))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("null")
                .hasMessageContaining("orders")
                .hasMessageContaining("42");
    }

    // ------------------------------------------------------------ validation gates

    /** The message-level skip_when escape hatch reaches the sink: a self-declared draft
     * suspends its own field rules instead of failing the record. */
    @Test
    void aSkipWhenFlagSuspendsTheDeclaredFieldRules() throws Exception {
        OpenSearchSinkTaskTest.RecordingClient client = new OpenSearchSinkTaskTest.RecordingClient();
        OpenSearchSinkTask task = startedTask(client, Map.of());

        // title violates min_len 2, but the record declares itself a draft.
        task.put(List.of(record(doc(b -> b.set("title", "x").set("draft", true)).toByteArray(), 1)));
        assertThat(client.writes.get(0).documents().values().iterator().next())
                .containsEntry("headline", "x");

        assertThatThrownBy(() -> task.put(List.of(
                record(doc(b -> b.set("title", "x")).toByteArray(), 2))))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("title");
    }

    /**
     * The ServiceLoader-registered DeclaredRulesAnyPayloadValidator throws an unchecked
     * ValidationException from inside mapper.map; the task's RuntimeException arm has to turn
     * that into a DataException carrying the violation, not let it escape as a task failure.
     */
    @Test
    void anInvalidAnyPayloadIsADataErrorCarryingTheViolation() throws Exception {
        OpenSearchSinkTaskTest.RecordingClient client = new OpenSearchSinkTaskTest.RecordingClient();
        OpenSearchSinkTask task = startedTask(client, Map.of());
        Any invalid = packed("x");

        assertThatThrownBy(() -> task.put(List.of(
                record(doc(b -> b.set("title", "Hello").set("payload", invalid)).toByteArray(), 1))))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("payload.label")
                .hasMessageContaining("min_len");
        assertThat(client.writes).isEmpty();
    }

    /** validate_payloads:false on the Any field opts that field out of the gate end to end:
     * the payload still expands into the document, it is just not rule-checked. */
    @Test
    void validatePayloadsFalseOnTheFieldLetsTheInvalidPayloadThroughAndStillExpandsIt()
            throws Exception {
        OpenSearchSinkTaskTest.RecordingClient client = new OpenSearchSinkTaskTest.RecordingClient();
        OpenSearchSinkTask task = startedTask(client, Map.of());
        Any invalid = packed("x");

        task.put(List.of(
                record(doc(b -> b.set("title", "Hello").set("loose", invalid)).toByteArray(), 1)));

        assertThat(client.writes.get(0).documents().values().iterator().next())
                .containsEntry("loose_label", "x");
    }

    /**
     * validate=false is the operator's whole-surface switch: it suspends the Any payload
     * gate along with the top-level rules, so a topic with a misbehaving producer can be
     * drained without a schema edit. The schema-level validate_payloads opt-out remains
     * the per-field control.
     */
    @Test
    void validateFalseAlsoSuspendsTheAnyPayloadGate() throws Exception {
        OpenSearchSinkTaskTest.RecordingClient client = new OpenSearchSinkTaskTest.RecordingClient();
        OpenSearchSinkTask task = startedTask(client,
                Map.of(OpenSearchSinkConfig.VALIDATE, "false"));
        Any invalid = packed("x");

        task.put(List.of(
                record(doc(b -> b.set("title", "Hello").set("payload", invalid)).toByteArray(), 1)));

        assertThat(client.writes).hasSize(1);
        assertThat(client.writes.get(0).documents().values().iterator().next())
                .containsEntry("payload_label", "x");
    }

    // ------------------------------------------------------------ mapping / ensure index

    /** The mapping handed to ensureIndex is what the mapping generator renders, so a VECTOR hint
     * has to survive as a knn_vector property with its dimension — asserted off the captured
     * mapping, never over HTTP. */
    @Test
    void aVectorHintReachesEnsureIndexAsAKnnVectorMapping() throws Exception {
        OpenSearchSinkTaskTest.RecordingClient client = new OpenSearchSinkTaskTest.RecordingClient();
        startedTask(client, Map.of());

        IndexMapping mapping = client.ensured.get(0).mapping();
        assertThat(mapping.find("embedding")).hasValueSatisfying(field -> {
            assertThat(field.type()).isEqualTo(IndexFieldKind.VECTOR);
            assertThat(field.hint().vectorDims()).isEqualTo(4);
        });

        Map<String, Object> mappings = new OpenSearchMappingGenerator().generate(mapping);
        @SuppressWarnings("unchecked")
        Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");
        @SuppressWarnings("unchecked")
        Map<String, Object> embedding = (Map<String, Object>) properties.get("embedding");
        assertThat(embedding).containsEntry("type", "knn_vector").containsEntry("dimension", 4);
    }

    // ------------------------------------------------------------ lifecycle

    /**
     * Connect only honors RetriableException from put/poll — an exception out of start() fails
     * the task whatever its class — so a failed ensureIndex is a plain ConnectException that
     * names the index.
     */
    @Test
    void anEnsureIndexFailureAtStartIsAConnectException() {
        OpenSearchSinkTask task = new OpenSearchSinkTask();
        task.clientFactory = config -> new FailingEnsureClient();

        assertThatThrownBy(() -> task.start(props(Map.of())))
                .isInstanceOf(ConnectException.class)
                .isNotInstanceOf(RetriableException.class)
                .hasMessageContaining("docs")
                .hasMessageContaining("cluster unreachable");
    }

    @Test
    void stopBeforeStartIsANoOp() {
        assertThatCode(() -> new OpenSearchSinkTask().stop()).doesNotThrowAnyException();
    }

    /** Connect starts a task once, but a second start must at least rebind cleanly rather
     * than keep writing through the previous client. */
    @Test
    void aSecondStartRebindsTheClient() throws Exception {
        OpenSearchSinkTaskTest.RecordingClient first = new OpenSearchSinkTaskTest.RecordingClient();
        OpenSearchSinkTask task = startedTask(first, Map.of());
        OpenSearchSinkTaskTest.RecordingClient second = new OpenSearchSinkTaskTest.RecordingClient();
        task.clientFactory = config -> second;
        task.start(props(Map.of()));

        task.put(List.of(record(doc(b -> b.set("title", "Hello")).toByteArray(), 1)));

        assertThat(second.writes).hasSize(1);
        assertThat(first.writes).isEmpty();
    }

    /**
     * Nothing overrides flush/preCommit, so the framework's own offsets come straight back and
     * a failed put leaves them unadvanced: the batch is written by put or not at all.
     */
    @Test
    void aFailedPutWritesNothingAndPreCommitClaimsOnlyTheFrameworkOffsets() throws Exception {
        OpenSearchSinkTaskTest.RecordingClient client = new OpenSearchSinkTaskTest.RecordingClient();
        client.failWrites = true;
        OpenSearchSinkTask task = startedTask(client, Map.of());

        assertThatThrownBy(() -> task.put(List.of(
                record(doc(b -> b.set("title", "Hello")).toByteArray(), 1))))
                .isInstanceOf(RetriableException.class);
        assertThat(client.writes).isEmpty();

        Map<TopicPartition, OffsetAndMetadata> offsets =
                Map.of(new TopicPartition("orders", 0), new OffsetAndMetadata(1L));
        assertThat(task.preCommit(offsets)).isEqualTo(offsets);
    }

    // ---------------------------------------------------------------- fixture

    private static OpenSearchSinkTask startedTask(
            OpenSearchSinkTask.IndexClient client, Map<String, String> extra) throws Exception {
        OpenSearchSinkTask task = new OpenSearchSinkTask();
        task.clientFactory = config -> client;
        task.start(props(extra));
        return task;
    }

    private static Map<String, String> props(Map<String, String> extra) throws Exception {
        Map<String, String> props = new HashMap<>(Map.of(
                OpenSearchSinkConfig.DESCRIPTOR_SET, EdgeFixture.create().descriptorSetBase64(),
                OpenSearchSinkConfig.MESSAGE_TYPE, "connect.edge.Doc",
                OpenSearchSinkConfig.URL, "http://localhost:39999",
                OpenSearchSinkConfig.INDEX, "docs"));
        props.putAll(extra);
        return props;
    }

    private static SinkRecord record(Object value, long offset) {
        return new SinkRecord("orders", 0, null, null, null, value, offset);
    }

    /** {@code [0x00][4-byte big-endian schema id][message index][payload]}, byte by byte. */
    private static byte[] confluentFrame(int schemaId, byte[] messageIndex, byte[] payload) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(ConfluentWireFormat.MAGIC);
        out.write((schemaId >>> 24) & 0xFF);
        out.write((schemaId >>> 16) & 0xFF);
        out.write((schemaId >>> 8) & 0xFF);
        out.write(schemaId & 0xFF);
        out.writeBytes(messageIndex);
        out.writeBytes(payload);
        return out.toByteArray();
    }

    private static DynamicMessage doc(java.util.function.Consumer<DocBuilder> fields)
            throws Exception {
        DocBuilder builder = new DocBuilder(EdgeFixture.create().doc);
        fields.accept(builder);
        return builder.build();
    }

    /** An Any packing connect.edge.Inner, whose label declares required + min_len 3. */
    private static Any packed(String label) throws Exception {
        EdgeFixture fixture = EdgeFixture.create();
        return Any.pack(DynamicMessage.newBuilder(fixture.inner)
                .setField(fixture.inner.findFieldByName("label"), label)
                .build(), "type.googleapis.com/");
    }

    /** Terse field setting by name over a DynamicMessage builder. */
    private static final class DocBuilder {
        private final Descriptor descriptor;
        private final DynamicMessage.Builder builder;

        private DocBuilder(Descriptor descriptor) {
            this.descriptor = descriptor;
            this.builder = DynamicMessage.newBuilder(descriptor);
        }

        private DocBuilder set(String name, Object value) {
            builder.setField(field(name), value);
            return this;
        }

        private DocBuilder add(String name, Object value) {
            builder.addRepeatedField(field(name), value);
            return this;
        }

        private FieldDescriptor field(String name) {
            FieldDescriptor field = descriptor.findFieldByName(name);
            if (field == null) {
                throw new IllegalArgumentException("No field '" + name + "' on " + descriptor);
            }
            return field;
        }

        private DynamicMessage build() {
            return builder.build();
        }
    }

    private static final class FailingEnsureClient implements OpenSearchSinkTask.IndexClient {
        @Override
        public boolean ensureIndex(String index, IndexMapping mapping) throws IOException {
            throw new IOException("cluster unreachable");
        }

        @Override
        public void bulkWrite(String index, Map<String, Map<String, Object>> documentsById,
                              boolean refresh) {
            throw new IllegalStateException("not reached");
        }

        @Override
        public void close() {
        }
    }

    /**
     * A descriptor set carrying the hint and validation options the typed way, exactly as
     * protoc would compile them: doc_id KEYWORD; title TEXT named "headline" with
     * required+min_len(2); pages plain int32; payload a gated Any; loose an Any with
     * validate_payloads:false; draft the message-level skip_when flag; embedding a 4-dim
     * VECTOR; child a nested message; tags a repeated string. Inner declares
     * required+min_len(3) on label so a packed payload can be made to violate its rules.
     */
    record EdgeFixture(Descriptor doc, Descriptor inner, Descriptor child, FileDescriptorSet set) {

        static EdgeFixture create() throws Exception {
            FileDescriptorProto file = FileDescriptorProto.newBuilder()
                    .setName("connect/edge/doc.proto")
                    .setPackage("connect.edge")
                    .setSyntax("proto3")
                    .addDependency("google/protobuf/any.proto")
                    .addMessageType(DescriptorProto.newBuilder()
                            .setName("Inner")
                            .addField(FieldDescriptorProto.newBuilder()
                                    .setName("label")
                                    .setNumber(1)
                                    .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                    .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                    .setOptions(FieldOptions.newBuilder()
                                            .setExtension(ValidateProto.field,
                                                    FieldRules.newBuilder()
                                                            .setRequired(true)
                                                            .setString(StringRules.newBuilder()
                                                                    .setMinLen(3))
                                                            .build()))))
                    .addMessageType(DescriptorProto.newBuilder()
                            .setName("Doc")
                            .setOptions(MessageOptions.newBuilder()
                                    .setExtension(ValidateProto.message,
                                            MessageRules.newBuilder().setSkipWhen("draft").build()))
                            .addField(FieldDescriptorProto.newBuilder()
                                    .setName("doc_id")
                                    .setNumber(1)
                                    .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                    .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                    .setOptions(FieldOptions.newBuilder()
                                            .setExtension(IndexingHintsProto.index,
                                                    FieldIndexHint.newBuilder()
                                                            .setType(IndexFieldType.INDEX_FIELD_TYPE_KEYWORD)
                                                            .build())))
                            .addField(FieldDescriptorProto.newBuilder()
                                    .setName("title")
                                    .setNumber(2)
                                    .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                    .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                    .setOptions(FieldOptions.newBuilder()
                                            .setExtension(IndexingHintsProto.index,
                                                    FieldIndexHint.newBuilder()
                                                            .setType(IndexFieldType.INDEX_FIELD_TYPE_TEXT)
                                                            .setName("headline")
                                                            .build())
                                            .setExtension(ValidateProto.field,
                                                    FieldRules.newBuilder()
                                                            .setRequired(true)
                                                            .setString(StringRules.newBuilder()
                                                                    .setMinLen(2))
                                                            .build())))
                            .addField(FieldDescriptorProto.newBuilder()
                                    .setName("pages")
                                    .setNumber(3)
                                    .setType(FieldDescriptorProto.Type.TYPE_INT32)
                                    .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                            .addField(FieldDescriptorProto.newBuilder()
                                    .setName("payload")
                                    .setNumber(4)
                                    .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                                    .setTypeName(".google.protobuf.Any")
                                    .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                            .addField(FieldDescriptorProto.newBuilder()
                                    .setName("loose")
                                    .setNumber(5)
                                    .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                                    .setTypeName(".google.protobuf.Any")
                                    .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                    .setOptions(FieldOptions.newBuilder()
                                            .setExtension(IndexingHintsProto.index,
                                                    FieldIndexHint.newBuilder()
                                                            .setValidatePayloads(false)
                                                            .build())))
                            .addField(FieldDescriptorProto.newBuilder()
                                    .setName("draft")
                                    .setNumber(6)
                                    .setType(FieldDescriptorProto.Type.TYPE_BOOL)
                                    .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                            .addField(FieldDescriptorProto.newBuilder()
                                    .setName("embedding")
                                    .setNumber(7)
                                    .setType(FieldDescriptorProto.Type.TYPE_FLOAT)
                                    .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED)
                                    .setOptions(FieldOptions.newBuilder()
                                            .setExtension(IndexingHintsProto.index,
                                                    FieldIndexHint.newBuilder()
                                                            .setType(IndexFieldType.INDEX_FIELD_TYPE_VECTOR)
                                                            .setVectorDims(4)
                                                            .build())))
                            .addField(FieldDescriptorProto.newBuilder()
                                    .setName("child")
                                    .setNumber(8)
                                    .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                                    .setTypeName(".connect.edge.Child")
                                    .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                            .addField(FieldDescriptorProto.newBuilder()
                                    .setName("tags")
                                    .setNumber(9)
                                    .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                    .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED)))
                    .addMessageType(DescriptorProto.newBuilder()
                            .setName("Child")
                            .addField(FieldDescriptorProto.newBuilder()
                                    .setName("code")
                                    .setNumber(1)
                                    .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                    .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                    .build();
            FileDescriptorSet set = FileDescriptorSet.newBuilder()
                    .addFile(AnyProto.getDescriptor().toProto())
                    .addFile(file)
                    .build();
            FileDescriptor linked = FileDescriptor.buildFrom(
                    file, new FileDescriptor[]{AnyProto.getDescriptor()});
            return new EdgeFixture(
                    linked.findMessageTypeByName("Doc"),
                    linked.findMessageTypeByName("Inner"),
                    linked.findMessageTypeByName("Child"),
                    set);
        }

        String descriptorSetBase64() {
            return Base64.getEncoder().encodeToString(set.toByteArray());
        }
    }
}
