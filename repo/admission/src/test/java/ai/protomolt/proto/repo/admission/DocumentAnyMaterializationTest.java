package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.descriptors.ClosedDescriptorSet;
import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.descriptors.MessageWireBudget;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.*;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.UnknownFieldSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;

import static ai.protomolt.proto.repo.admission.DocumentAnyMaterialization.*;
import static org.assertj.core.api.Assertions.*;

class DocumentAnyMaterializationTest {
    private static final String URL = "types.test/archive.Record";
    private static final Limits LIMITS = new Limits(100_000, 100, 10);

    @Test void preserveReturnsExactValueWithoutResolvingOrParsingMalformedUnknownData() {
        var original = Any.newBuilder().setTypeUrl("unknown-label")
                .setValue(ByteString.copyFrom(new byte[] {(byte) 0x80})).build();
        var result = read(selection(original, 1), original, Mode.PRESERVE, LIMITS,
                ignored -> { throw new AssertionError("preserve called resolver"); }, () -> {});
        assertThat(result).isInstanceOf(Preserved.class);
        assertThat(result.original()).isSameAs(original);
        assertThat(result.original().getValue()).isSameAs(original.getValue());
    }

    @Test void sameUrlCanUseDifferentOccurrenceBoundDefinitionsWithoutCrossContamination() throws Exception {
        var first = binding("old_label", false);
        var second = binding("new_label", false);
        var original = envelope(first, DynamicMessage.newBuilder(first.schema().type())
                .addRepeatedField(first.schema().type().findFieldByNumber(1), "stored").build());
        var calls = new AtomicInteger();
        Resolver resolver = occurrence -> {
            calls.incrementAndGet();
            return new Resolved(occurrence.prefix().getFirst().getFieldNumber() == 1 ? first : second);
        };
        var a = (Decoded) read(selection(original, 1), original, Mode.MATERIALIZE_IF_AVAILABLE, LIMITS, resolver, () -> {});
        var b = (Decoded) read(selection(original, 2), original, Mode.MATERIALIZE_IF_AVAILABLE, LIMITS, resolver, () -> {});
        assertThat(a.value().getAllFields().keySet()).extracting(Descriptors.FieldDescriptor::getName).containsExactly("old_label");
        assertThat(b.value().getAllFields().keySet()).extracting(Descriptors.FieldDescriptor::getName).containsExactly("new_label");
        assertThat(a.schema().getArtifactSha256()).isNotEqualTo(b.schema().getArtifactSha256());
        assertThat(a.original()).isSameAs(b.original());
        assertThat(calls).hasValue(2);
    }

    @ParameterizedTest
    @EnumSource(value = Reason.class, names = "MALFORMED_PAYLOAD", mode = EnumSource.Mode.EXCLUDE)
    void resolverOutcomesStayDistinctAndRetainOriginalBytes(Reason reason) {
        var original = Any.newBuilder().setTypeUrl(URL).setValue(ByteString.copyFromUtf8("opaque")).build();
        var supplied = Failure.of(reason);
        var result = (Failed) read(selection(original, 1), original, Mode.MATERIALIZE_IF_AVAILABLE, LIMITS,
                ignored -> new Unavailable(supplied), () -> {});
        assertThat(result.failure()).isSameAs(supplied);
        assertThat(result.original()).isSameAs(original);
    }

    @Test void nestedUnknownAnyRemainsOpaqueAndDecodeDoesNotValidateRequiredFields() throws Exception {
        var bound = binding("label", true);
        var child = Any.newBuilder().setTypeUrl("unknown/Child")
                .setValue(ByteString.copyFrom(new byte[] {(byte) 0x80})).build();
        var value = DynamicMessage.newBuilder(bound.schema().type())
                .setField(bound.schema().type().findFieldByNumber(2), child).buildPartial();
        assertThat(value.isInitialized()).isFalse(); // Required field 4 is absent.
        var original = envelope(bound, value);
        var calls = new AtomicInteger();
        var result = (Decoded) read(selection(original, 1), original, Mode.MATERIALIZE_IF_AVAILABLE, LIMITS,
                ignored -> { calls.incrementAndGet(); return new Resolved(bound); }, () -> {});
        assertThat(result.value().isInitialized()).isFalse();
        var nested = (DynamicMessage) result.value().getField(bound.schema().type().findFieldByNumber(2));
        assertThat(nested.getField(nested.getDescriptorForType().findFieldByName("value"))).isEqualTo(child.getValue());
        assertThat(calls).hasValue(1);
    }

    @Test void configuredBoundsAndMalformedBytesHaveDistinctOutcomes() throws Exception {
        var bound = binding("label", false);
        var leaf = DynamicMessage.newBuilder(bound.schema().type())
                .addRepeatedField(bound.schema().type().findFieldByNumber(1), "value").build();
        var original = envelope(bound, leaf);
        assertFailure(original, bound, new Limits(original.getValue().size() - 1, 100, 10), Reason.RESOURCE_LIMIT);
        assertThat(read(selection(original, 1), original, Mode.MATERIALIZE_IF_AVAILABLE,
                new Limits(original.getValue().size(), 1, 0), ignored -> new Resolved(bound), () -> {})).isInstanceOf(Decoded.class);
        var nested = envelope(bound, DynamicMessage.newBuilder(bound.schema().type())
                .setField(bound.schema().type().findFieldByNumber(3), leaf).build());
        assertFailure(nested, bound, new Limits(1000, 100, 0), Reason.RESOURCE_LIMIT);
        assertFailure(nested, bound, new Limits(1000, 1, 10), Reason.RESOURCE_LIMIT);
        assertThat(read(selection(nested, 1), nested, Mode.MATERIALIZE_IF_AVAILABLE,
                new Limits(1000, 2, 1), ignored -> new Resolved(bound), () -> {})).isInstanceOf(Decoded.class);
        var malformed = Any.newBuilder().setTypeUrl(URL).setValue(ByteString.copyFrom(new byte[] {0x0a, 5, 1})).build();
        var failed = assertFailure(malformed, bound, LIMITS, Reason.MALFORMED_PAYLOAD);
        assertThat(failed.failure().cause()).isPresent();
        assertThat(failed.original().getValue()).isSameAs(malformed.getValue());
    }

    @Test void deepestSupportedPayloadDecodesWithoutTheParsersDefaultLimitChangingItsOutcome() throws Exception {
        var bound = binding("label", false);
        var type = bound.schema().type();
        var child = DynamicMessage.newBuilder(type).addRepeatedField(type.findFieldByNumber(1), "leaf").build();
        for (int depth = 0; depth < 100; depth++)
            child = DynamicMessage.newBuilder(type).setField(type.findFieldByNumber(3), child).build();
        var original = envelope(bound, child).toBuilder().setUnknownFields(UnknownFieldSet.newBuilder()
                .addField(999, UnknownFieldSet.Field.newBuilder().addVarint(7).build()).build()).build();
        var result = read(selection(original, 1), original, Mode.MATERIALIZE_IF_AVAILABLE,
                new Limits(100_000, 101, 100), ignored -> new Resolved(bound), () -> {});
        assertThat(result).isInstanceOf(Decoded.class);
        assertThat(result.original()).isSameAs(original);
        assertThat(result.original().getUnknownFields().hasField(999)).isTrue();
        assertFailure(original, bound, new Limits(100_000, 101, 99), Reason.RESOURCE_LIMIT);
    }

    @Test void interruptedThreadIsNotConvertedToAnUnavailableDefinition() {
        var original = Any.newBuilder().setTypeUrl(URL).build();
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(() -> read(selection(original, 1), original, Mode.PRESERVE, LIMITS,
                    ignored -> { throw new AssertionError("interrupted call reached resolver"); }, () -> {}))
                    .isInstanceOf(CancellationException.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    @Test void rejectsMismatchedOccurrenceIdentityInBothModesBeforeLookup() {
        var original = Any.newBuilder().setTypeUrl(URL).setValue(ByteString.copyFromUtf8("data")).build();
        var match = selection(original, 1);
        for (var mode : Mode.values()) {
            for (var mismatch : List.of(
                    new DocumentSchemaAdmission.Selection(0, match.root(), "wrong/url", match.prefix(), match.valueSha256(), match.valueSizeBytes()),
                    new DocumentSchemaAdmission.Selection(0, match.root(), URL, match.prefix(), match.valueSha256(), 999),
                    new DocumentSchemaAdmission.Selection(0, match.root(), URL, match.prefix(), "a".repeat(64), match.valueSizeBytes()))) {
                assertThatThrownBy(() -> read(mismatch, original, mode, LIMITS,
                        ignored -> { throw new AssertionError("identity mismatch reached resolver"); }, () -> {}))
                        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Any differs");
            }
        }
    }

    @Test void wrongSchemaUrlIsExplicitAndUnclassifiedResolverExceptionsPropagate() throws Exception {
        var bound = binding("label", false);
        var original = Any.newBuilder().setTypeUrl("other-prefix/archive.Record").build();
        assertFailure(original, bound, LIMITS, Reason.CORRUPT_DEFINITION);
        var problem = new IllegalStateException("registry transport failed without classification");
        assertThatThrownBy(() -> read(selection(original, 1), original, Mode.MATERIALIZE_IF_AVAILABLE, LIMITS,
                ignored -> { throw problem; }, () -> {})).isSameAs(problem);
        assertThatThrownBy(() -> read(selection(original, 1), original, Mode.MATERIALIZE_IF_AVAILABLE, LIMITS,
                ignored -> null, () -> {})).isInstanceOf(NullPointerException.class);
    }

    @Test void cancellationAndRevocationAreRecheckedBeforeEveryOutcome() throws Exception {
        var bound = binding("label", false);
        var original = envelope(bound, DynamicMessage.newBuilder(bound.schema().type())
                .addRepeatedField(bound.schema().type().findFieldByNumber(1), "value").build());
        var stop = new CancellationException("revoked or cancelled");
        for (var mode : Mode.values()) {
            var total = new AtomicInteger();
            read(selection(original, 1), original, mode, LIMITS, ignored -> new Resolved(bound), total::incrementAndGet);
            for (int point = 1; point <= total.get(); point++) {
                int at = point;
                var checks = new AtomicInteger();
                assertThatThrownBy(() -> read(selection(original, 1), original, mode, LIMITS,
                        ignored -> new Resolved(bound), () -> { if (checks.incrementAndGet() == at) throw stop; })).isSameAs(stop);
            }
        }
        var lookedUp = new AtomicInteger();
        assertThatThrownBy(() -> read(selection(original, 1), original, Mode.MATERIALIZE_IF_AVAILABLE, LIMITS,
                ignored -> { lookedUp.incrementAndGet(); return new Unavailable(Failure.of(Reason.DEFINITION_MISSING)); },
                () -> { if (lookedUp.get() != 0) throw stop; })).isSameAs(stop);
    }

    @Test void hostLimitExceptionIsNotRelabeledByScannerCatch() throws Exception {
        var bound = binding("label", false);
        var original = envelope(bound, DynamicMessage.newBuilder(bound.schema().type())
                .addRepeatedField(bound.schema().type().findFieldByNumber(1), "value").build());
        var total = new AtomicInteger();
        read(selection(original, 1), original, Mode.MATERIALIZE_IF_AVAILABLE, LIMITS, ignored -> new Resolved(bound), total::incrementAndGet);
        for (int point = 1; point <= total.get(); point++) {
            int at = point;
            var checks = new AtomicInteger();
            var host = new MessageWireBudget.LimitExceededException("host has separate budget");
            assertThatThrownBy(() -> read(selection(original, 1), original, Mode.MATERIALIZE_IF_AVAILABLE, LIMITS,
                    ignored -> new Resolved(bound), () -> { if (checks.incrementAndGet() == at) throw host; })).isSameAs(host);
        }
    }

    private static Failed assertFailure(Any original, DocumentSchemaAssetBinding bound, Limits limits, Reason reason) {
        var result = (Failed) read(selection(original, 1), original, Mode.MATERIALIZE_IF_AVAILABLE,
                limits, ignored -> new Resolved(bound), () -> {});
        assertThat(result.failure().reason()).isEqualTo(reason);
        return result;
    }
    private static Any envelope(DocumentSchemaAssetBinding bound, DynamicMessage value) {
        return Any.newBuilder().setTypeUrl(bound.metadata().getTypeUrl()).setValue(value.toByteString()).build();
    }
    private static DocumentSchemaAdmission.Selection selection(Any original, int field) {
        // This pure unit seam does not authenticate a root/path; host traversal must do that.
        return new DocumentSchemaAdmission.Selection(0, DocumentSchemaRootLocator.getDefaultInstance(), original.getTypeUrl(),
                List.of(RepositorySchemaOccurrenceStep.newBuilder().setFieldNumber(field).build()),
                DocumentPartCodec.sha256Hex(original.getValue().toByteArray()), original.getValue().size());
    }
    private static DocumentSchemaAssetBinding binding(String label, boolean required) throws Exception {
        var message = DescriptorProto.newBuilder().setName("Record")
                .addField(FieldDescriptorProto.newBuilder().setName(label).setNumber(1).setType(FieldDescriptorProto.Type.TYPE_STRING)
                        .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED))
                .addField(FieldDescriptorProto.newBuilder().setName("payload").setNumber(2).setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                        .setTypeName(".google.protobuf.Any"))
                .addField(FieldDescriptorProto.newBuilder().setName("child").setNumber(3).setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                        .setTypeName(".archive.Record"));
        if (required) message.addField(FieldDescriptorProto.newBuilder().setName("required_value").setNumber(4)
                .setType(FieldDescriptorProto.Type.TYPE_STRING).setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED));
        var descriptor = Descriptors.FileDescriptor.buildFrom(FileDescriptorProto.newBuilder().setName("archive.proto")
                .setPackage("archive").setSyntax(required ? "proto2" : "proto3")
                .addDependency(Any.getDescriptor().getFile().getName()).addMessageType(message).build(),
                new Descriptors.FileDescriptor[] {Any.getDescriptor().getFile()}).getMessageTypes().getFirst();
        var closure = DescriptorFingerprints.closure(descriptor);
        var bytes = closure.toByteString();
        var metadata = RepositorySchemaAsset.newBuilder().setTypeUrl(URL)
                .setArtifactSha256(DocumentPartCodec.sha256Hex(bytes.toByteArray()))
                .setSchema(PublicationSchemaCondition.newBuilder().setTypeName("archive.Record")
                        .setDescriptorFingerprint(DescriptorFingerprints.fingerprint(closure)))
                .setCompilation(SchemaCompilationProvenance.newBuilder()
                        .setOrigin(SchemaCompilationOrigin.SCHEMA_COMPILATION_ORIGIN_IMPORTED_DESCRIPTOR)
                        .setEvidence(SchemaCompilerEvidence.SCHEMA_COMPILER_EVIDENCE_UNKNOWN)
                        .setUnknownCompilerReason("fixture did not compile source")
                        .setAdmissionRuntime(SchemaToolIdentity.newBuilder().setName("test").setVersion("1"))).build();
        return DocumentSchemaAssetBinding.bind(metadata, bytes, new ClosedDescriptorSet.Limits(100_000, 10, 20, 10), () -> {});
    }
}
