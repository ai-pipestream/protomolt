package ai.protomolt.proto.kafka.connect;

import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link RedactMessage} beyond the single {@code redact} case in {@link TransformsTest}: the
 * default {@code remove} strategy, class selection, the encrypt/decrypt round-trip with its
 * key requirements, and the pass-through shapes (no matching class, tombstones).
 */
class RedactMessageTest {

    private static final String CUSTOMER_PROTO = """
            syntax = "proto3";
            package redact.test;
            import "ai/pipestream/proto/meta/v1/metadata.proto";
            message Customer {
              string id = 1;
              string email = 2 [(ai.pipestream.proto.meta.v1.field) = {sensitivity: "pii"}];
              string ssn = 3 [(ai.pipestream.proto.meta.v1.field) = {sensitivity: "secret"}];
              int64 score = 4 [(ai.pipestream.proto.meta.v1.field) = {sensitivity: "pii"}];
            }
            """;

    private static final String KEY_1 =
            Base64.getEncoder().encodeToString(new byte[16]);
    private static final String KEY_2 =
            Base64.getEncoder().encodeToString(new byte[] {9, 9, 9, 9, 9, 9, 9, 9,
                    9, 9, 9, 9, 9, 9, 9, 9});

    private static Descriptor customerType;
    private static String descriptorSetBase64;

    private RedactMessage<SinkRecord> transform;

    @BeforeAll
    static void compile() throws Exception {
        String metadataProto;
        try (InputStream in = RedactMessageTest.class.getClassLoader()
                .getResourceAsStream("ai/protomolt/proto/meta/v1/metadata.proto")) {
            metadataProto = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("ai/protomolt/proto/meta/v1/metadata.proto", metadataProto, "test")
                .add("redact/test/customer.proto", CUSTOMER_PROTO, "test")
                .build());
        descriptorSetBase64 = Base64.getEncoder()
                .encodeToString(compiled.descriptorSet().toByteArray());
        FileDescriptor file = compiled.descriptorFor("redact/test/customer.proto").orElseThrow();
        customerType = file.findMessageTypeByName("Customer");
    }

    @AfterEach
    void close() {
        if (transform != null) {
            transform.close();
            transform = null;
        }
    }

    private RedactMessage<SinkRecord> redact(Map<String, String> overrides) {
        Map<String, String> props = new HashMap<>();
        props.put(ValueCodec.DESCRIPTOR_SET, descriptorSetBase64);
        props.put(ValueCodec.MESSAGE_TYPE, "redact.test.Customer");
        props.putAll(overrides);
        RedactMessage<SinkRecord> smt = new RedactMessage<>();
        smt.configure(props);
        transform = smt;
        return smt;
    }

    private static DynamicMessage customer(String id, String email, String ssn, long score) {
        return DynamicMessage.newBuilder(customerType)
                .setField(customerType.findFieldByName("id"), id)
                .setField(customerType.findFieldByName("email"), email)
                .setField(customerType.findFieldByName("ssn"), ssn)
                .setField(customerType.findFieldByName("score"), score)
                .build();
    }

    private static SinkRecord record(DynamicMessage customer) {
        return new SinkRecord("customers", 0, null, null, null, customer.toByteArray(), 0);
    }

    private static String field(DynamicMessage message, String name) {
        return (String) message.getField(customerType.findFieldByName(name));
    }

    private static long score(DynamicMessage message) {
        return (long) message.getField(customerType.findFieldByName("score"));
    }

    private static DynamicMessage decode(SinkRecord record) throws Exception {
        return DynamicMessage.parseFrom(customerType, (byte[]) record.value());
    }

    @Test
    void removeIsTheDefaultStrategyAndPiiTheDefaultClass() throws Exception {
        RedactMessage<SinkRecord> smt = redact(Map.of());
        DynamicMessage out = decode(smt.apply(record(customer("c-1", "pat@example.com",
                "111-22-3333", 42))));
        // pii fields are cleared; the secret-classed ssn is untouched by the default class list.
        assertThat(field(out, "email")).isEmpty();
        assertThat(score(out)).isZero();
        assertThat(field(out, "ssn")).isEqualTo("111-22-3333");
        assertThat(field(out, "id")).isEqualTo("c-1");
    }

    @Test
    void onlyTheConfiguredClassesAreMasked() throws Exception {
        RedactMessage<SinkRecord> smt = redact(Map.of(RedactMessage.CLASSES, "secret"));
        DynamicMessage out = decode(smt.apply(record(customer("c-1", "pat@example.com",
                "111-22-3333", 42))));
        assertThat(field(out, "ssn")).isEmpty();
        assertThat(field(out, "email")).isEqualTo("pat@example.com");
        assertThat(score(out)).isEqualTo(42L);
    }

    @Test
    void severalClassesMaskTogether() throws Exception {
        RedactMessage<SinkRecord> smt = redact(Map.of(RedactMessage.CLASSES, "pii,secret"));
        DynamicMessage out = decode(smt.apply(record(customer("c-1", "pat@example.com",
                "111-22-3333", 42))));
        assertThat(field(out, "email")).isEmpty();
        assertThat(field(out, "ssn")).isEmpty();
        assertThat(field(out, "id")).isEqualTo("c-1");
    }

    /**
     * A redacted number would still look plausible, so redact turns strings into {@code ***} and
     * clears everything else.
     */
    @Test
    void redactMasksStringsAndClearsNumbers() throws Exception {
        RedactMessage<SinkRecord> smt = redact(Map.of(RedactMessage.STRATEGY, "redact"));
        DynamicMessage out = decode(smt.apply(record(customer("c-1", "pat@example.com",
                "111-22-3333", 42))));
        assertThat(field(out, "email")).isEqualTo("***");
        assertThat(score(out)).isZero();
    }

    @Test
    void encryptThenDecryptRoundTripsTheValue() throws Exception {
        RedactMessage<SinkRecord> encrypt = redact(Map.of(
                RedactMessage.STRATEGY, "encrypt",
                RedactMessage.KEY, KEY_1));
        SinkRecord sealed = encrypt.apply(record(customer("c-1", "pat@example.com",
                "111-22-3333", 42)));
        DynamicMessage sealedMessage = decode(sealed);
        assertThat(field(sealedMessage, "email"))
                .isNotEqualTo("pat@example.com")
                .isNotBlank();
        // An int64 cannot hold ciphertext: non-string pii fields clear under encrypt.
        assertThat(score(sealedMessage)).isZero();

        transform = null;   // keep the encryptor open no longer than needed
        encrypt.close();

        RedactMessage<SinkRecord> decrypt = redact(Map.of(
                RedactMessage.STRATEGY, "decrypt",
                RedactMessage.KEY, KEY_1));
        DynamicMessage opened = decode(decrypt.apply(
                new SinkRecord("customers", 0, null, null, null, sealed.value(), 0)));
        assertThat(field(opened, "email")).isEqualTo("pat@example.com");
        assertThat(field(opened, "id")).isEqualTo("c-1");
    }

    @Test
    void encryptWithoutAKeyIsRejectedAtConfigure() {
        assertThatThrownBy(() -> redact(Map.of(RedactMessage.STRATEGY, "encrypt")))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("'" + RedactMessage.KEY + "'");
        assertThatThrownBy(() -> redact(Map.of(RedactMessage.STRATEGY, "decrypt")))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("'" + RedactMessage.KEY + "'");
    }

    /**
     * A wrong key must fail loudly, never silently emit garbage: the GCM tag binds the value to
     * the key (and to its field), so decryption refuses.
     */
    @Test
    void decryptWithTheWrongKeyFailsLoudly() {
        RedactMessage<SinkRecord> encrypt = redact(Map.of(
                RedactMessage.STRATEGY, "encrypt",
                RedactMessage.KEY, KEY_1));
        SinkRecord sealed = encrypt.apply(record(customer("c-1", "pat@example.com",
                "111-22-3333", 42)));
        encrypt.close();
        transform = null;

        RedactMessage<SinkRecord> decrypt = redact(Map.of(
                RedactMessage.STRATEGY, "decrypt",
                RedactMessage.KEY, KEY_2));
        assertThatThrownBy(() -> decrypt.apply(sealed))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Decryption failed");
    }

    /**
     * A schema with no field in the configured classes produces no masked paths, and the
     * transform returns the record instance it was given rather than a re-encoded copy.
     */
    @Test
    void recordsWithNoMatchingClassPassThroughUntouched() {
        RedactMessage<SinkRecord> smt = redact(Map.of(RedactMessage.CLASSES, "pci"));
        SinkRecord record = record(customer("c-1", "pat@example.com", "111-22-3333", 42));
        assertThat(smt.apply(record)).isSameAs(record);
    }

    @Test
    void tombstonesPassThrough() {
        RedactMessage<SinkRecord> smt = redact(Map.of());
        SinkRecord tombstone = new SinkRecord("customers", 0, null, null, null, null, 0);
        assertThat(smt.apply(tombstone)).isSameAs(tombstone);
    }
}
