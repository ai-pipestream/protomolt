package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.descriptors.ClosedDescriptorSet;
import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.repo.v1.PublicationSchemaCondition;
import ai.protomolt.proto.validate.CelRule;
import ai.protomolt.proto.validate.FieldRules;
import ai.protomolt.proto.validate.MessageRules;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.RuleCompilationException;
import ai.protomolt.proto.validate.StringRules;
import ai.protomolt.proto.validate.ValidateProto;
import ai.protomolt.proto.validate.ValidationResult;
import ai.protomolt.proto.validate.source.ProtomoltRuleSource;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.*;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CancellationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DocumentPayloadCheckTest {
    private static final String URL = "type.protomolt.test/payload.Choice";
    private static final DocumentPayloadCheck.Limits LIMITS = new DocumentPayloadCheck.Limits(1024, 100, 10, 100, 1000);

    @Test
    void archivalMapCannotSkipAnOmittedAnyValue() throws Exception {
        var entry = DescriptorProto.newBuilder().setName("ItemsEntry")
                .setOptions(MessageOptions.newBuilder().setMapEntry(true))
                .addField(FieldDescriptorProto.newBuilder().setName("key").setNumber(1).setType(FieldDescriptorProto.Type.TYPE_STRING))
                .addField(FieldDescriptorProto.newBuilder().setName("value").setNumber(2).setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                        .setTypeName(".google.protobuf.Any"));
        var definition = DescriptorProto.newBuilder().setName("MapWrapper").addNestedType(entry)
                .addField(FieldDescriptorProto.newBuilder().setName("items").setNumber(1)
                        .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED).setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                        .setTypeName(".payload.MapWrapper.ItemsEntry"));
        var file = FileDescriptorProto.newBuilder().setName("map-wrapper.proto").setPackage("payload").setSyntax("proto3")
                .addDependency("google/protobuf/any.proto").addMessageType(definition).build();
        var schema = binding(FileDescriptor.buildFrom(file, new FileDescriptor[]{Any.getDescriptor().getFile()})
                .findMessageTypeByName("MapWrapper"));
        var field = schema.type().findFieldByNumber(1);
        var missing = DynamicMessage.newBuilder(field.getMessageType())
                .setField(field.getMessageType().findFieldByNumber(1), "missing").build();
        var data = DynamicMessage.newBuilder(schema.type()).addRepeatedField(field, missing).build();
        String url = "type.protomolt.test/payload.MapWrapper";
        var candidate = Any.newBuilder().setTypeUrl(url).setValue(data.toByteString()).build();
        assertThatThrownBy(() -> DocumentPayloadCheck.checkAssets(asset(schema, url), candidate, url, validator(), LIMITS,
                () -> {}, nested -> { throw new AssertionError("empty Any must not reach resolver"); }))
                .hasMessageContaining("invalid Any type URL");
    }

    @Test
    void rejectsAlteredAnyEnvelopeSemanticsBeforeResolution() throws Exception {
        var canonical = DescriptorProto.newBuilder().setName("Any")
                .addField(FieldDescriptorProto.newBuilder().setName("type_url").setNumber(1)
                        .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL).setType(FieldDescriptorProto.Type.TYPE_STRING))
                .addField(FieldDescriptorProto.newBuilder().setName("value").setNumber(2)
                        .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL).setType(FieldDescriptorProto.Type.TYPE_BYTES)).build();
        var renamed = canonical.toBuilder();
        renamed.getFieldBuilder(0).setName("other_url");
        var oneof = canonical.toBuilder().addOneofDecl(OneofDescriptorProto.newBuilder().setName("choice"));
        oneof.getFieldBuilder(0).setOneofIndex(0);
        var defaulted = canonical.toBuilder();
        defaulted.getFieldBuilder(0).setDefaultValue(URL);
        var required = canonical.toBuilder();
        required.getFieldBuilder(0).setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED);
        for (var definition : List.of(renamed.build(), oneof.build(), defaulted.build(), required.build())) {
            var file = FileDescriptorProto.newBuilder().setName("altered-any.proto")
                    .setPackage("google.protobuf").setSyntax("proto2").addMessageType(definition).build();
            var schema = binding(FileDescriptor.buildFrom(file, new FileDescriptor[0]).findMessageTypeByName("Any"));
            var data = DynamicMessage.newBuilder(schema.type()).setField(schema.type().findFieldByNumber(1), URL)
                    .setField(schema.type().findFieldByNumber(2), ByteString.EMPTY).build();
            var payload = Any.newBuilder().setTypeUrl("type.protomolt.test/google.protobuf.Any").setValue(data.toByteString()).build();
            assertThatThrownBy(() -> DocumentPayloadCheck.check(schema, payload, payload.getTypeUrl(), validator(), LIMITS,
                    () -> {}, url -> { throw new AssertionError("malformed envelope reached resolver"); }))
                    .hasMessageContaining("invalid Any envelope descriptor");
        }
    }

    @Test
    void rejectsAnAnyDescriptorWithAnExtraKnownFieldBeforeResolution() throws Exception {
        var definition=DescriptorProto.newBuilder().setName("Any")
                .addField(FieldDescriptorProto.newBuilder().setName("type_url").setNumber(1).setType(FieldDescriptorProto.Type.TYPE_STRING))
                .addField(FieldDescriptorProto.newBuilder().setName("value").setNumber(2).setType(FieldDescriptorProto.Type.TYPE_BYTES))
                .addField(FieldDescriptorProto.newBuilder().setName("extra").setNumber(3).setType(FieldDescriptorProto.Type.TYPE_STRING));
        var file=FileDescriptorProto.newBuilder().setName("custom-any.proto").setPackage("google.protobuf").setSyntax("proto3").addMessageType(definition).build();
        var schema=binding(FileDescriptor.buildFrom(file,new FileDescriptor[0]).findMessageTypeByName("Any"));
        var inner=binding(choice("true"));
        var data=DynamicMessage.newBuilder(schema.type()).setField(schema.type().findFieldByNumber(1),URL)
                .setField(schema.type().findFieldByNumber(2),candidate(inner,"valid","").getValue())
                .setField(schema.type().findFieldByNumber(3),"must not be skipped").build();
        var payload=Any.newBuilder().setTypeUrl("type.protomolt.test/google.protobuf.Any").setValue(data.toByteString()).build();
        var resolutions=new java.util.concurrent.atomic.AtomicInteger();
        assertThatThrownBy(()->DocumentPayloadCheck.check(schema,payload,payload.getTypeUrl(),validator(),LIMITS,()->{},
                url->{resolutions.incrementAndGet(); return inner;})).hasMessageContaining("invalid Any envelope descriptor");
        assertThat(resolutions.get()).isZero();
    }

    @Test
    void archivalAssetsRejectUnknownCandidateFields() throws Exception {
        var schema=binding(choice("true"));
        var known=candidate(schema,"valid","");
        var original=known.toBuilder().setValue(known.getValue().concat(ByteString.copyFrom(new byte[]{(byte)0xa0,6,1}))).build();
        assertThat(check(schema,original,LIMITS).original()).isSameAs(original);
        assertThatThrownBy(()->DocumentPayloadCheck.checkAssets(asset(schema,URL),original,URL,validator(),LIMITS,()->{},url->{throw new AssertionError(url);}))
                .hasMessageContaining("unknown candidate fields");
    }

    @Test
    void archivalAssetsRejectDuplicateMapKeysIncludingDefaults() throws Exception {
        var schema=binding(com.google.protobuf.Struct.getDescriptor());
        String url="type.protomolt.test/google.protobuf.Struct";
        var field=schema.type().findFieldByNumber(1);
        var entryType=field.getMessageType();
        for (String key:List.of("same","")) {
            var first=DynamicMessage.newBuilder(entryType).setField(entryType.findFieldByNumber(2),com.google.protobuf.Value.newBuilder().setStringValue("first").build());
            if (!key.isEmpty()) first.setField(entryType.findFieldByNumber(1),key);
            var second=DynamicMessage.newBuilder(entryType).setField(entryType.findFieldByNumber(1),key)
                    .setField(entryType.findFieldByNumber(2),com.google.protobuf.Value.newBuilder().setStringValue("second").build());
            var value=DynamicMessage.newBuilder(schema.type()).addRepeatedField(field,first.build()).addRepeatedField(field,second.build()).build();
            var original=Any.newBuilder().setTypeUrl(url).setValue(value.toByteString()).build();
            assertThatThrownBy(()->DocumentPayloadCheck.checkAssets(asset(schema,url),original,url,validator(),LIMITS,()->{},nested->{throw new AssertionError(nested);}))
                    .hasMessageContaining("duplicate map keys");
            var unique=value.toBuilder().setRepeatedField(field,1,second.setField(entryType.findFieldByNumber(1),"different").build()).build();
            var accepted=original.toBuilder().setValue(unique.toByteString()).build();
            assertThat(DocumentPayloadCheck.checkAssets(asset(schema,url),accepted,url,validator(),LIMITS,()->{},nested->{throw new AssertionError(nested);})
                    .payload().original()).isSameAs(accepted);
        }
    }

    @Test
    void archivalAssetUrlsMustMatchRootPolicyAndEveryNestedResolution() throws Exception {
        var wrapper = wrapper();
        var inner = binding(choice("(this.left != '') != (this.right != '')"));
        var payload = wrapped(wrapper, candidate(inner, "valid", ""), candidate(inner, "again", ""));
        var rootAsset = asset(wrapper, payload.getTypeUrl());
        var innerAsset = asset(inner, URL);
        var resolutions = new java.util.concurrent.atomic.AtomicInteger();
        var result = DocumentPayloadCheck.checkAssets(rootAsset, payload, payload.getTypeUrl(), validator(), LIMITS,
                () -> {}, url -> { resolutions.incrementAndGet(); return innerAsset; });
        assertThat(resolutions.get()).isEqualTo(1);
        assertThat(result.assets()).containsExactlyInAnyOrderEntriesOf(java.util.Map.of(payload.getTypeUrl(), rootAsset, URL, innerAsset));
        assertThat(result.payload().original()).isSameAs(payload);
        assertThatThrownBy(() -> result.assets().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> DocumentPayloadCheck.checkAssets(rootAsset, payload, "other/payload.Wrapper", validator(), LIMITS,
                () -> {}, url -> innerAsset)).hasMessageContaining("differs from host policy");
        assertThatThrownBy(() -> DocumentPayloadCheck.checkAssets(rootAsset, payload, payload.getTypeUrl(), validator(), LIMITS,
                () -> {}, url -> asset(inner, "other/payload.Choice"))).hasMessageContaining("asset type URL mismatch");
        assertThatThrownBy(() -> DocumentPayloadCheck.checkAssets(rootAsset, payload, payload.getTypeUrl(), validator(), LIMITS,
                () -> {}, url -> null)).hasMessageContaining("unresolved Any schema asset");
        var invalid = wrapped(wrapper, candidate(inner, "both", "filled"));
        assertThatThrownBy(() -> DocumentPayloadCheck.checkAssets(rootAsset, invalid, invalid.getTypeUrl(), validator(), LIMITS,
                () -> {}, url -> innerAsset)).isInstanceOf(ValidationResult.ValidationException.class);
    }

    private static DocumentSchemaAssetBinding asset(DocumentSchemaBinding schema, String url) {
        var metadata = ai.protomolt.proto.repo.v1.RepositorySchemaAsset.newBuilder()
                .setSchema(schema.condition()).setArtifactSha256(schema.artifactSha256()).setTypeUrl(url)
                .setCompilation(ai.protomolt.proto.repo.v1.SchemaCompilationProvenance.newBuilder()
                        .setOrigin(ai.protomolt.proto.repo.v1.SchemaCompilationOrigin.SCHEMA_COMPILATION_ORIGIN_IMPORTED_DESCRIPTOR)
                        .setEvidence(ai.protomolt.proto.repo.v1.SchemaCompilerEvidence.SCHEMA_COMPILER_EVIDENCE_UNKNOWN)
                        .setUnknownCompilerReason("Fixture producer did not report compiler")
                        .setAdmissionRuntime(ai.protomolt.proto.repo.v1.SchemaToolIdentity.newBuilder().setName("test-runtime").setVersion("1")))
                .build();
        return DocumentSchemaAssetBinding.bind(metadata, schema.artifact(),
                new ClosedDescriptorSet.Limits(4_000_000, 100, 1000, 100), () -> {});
    }

    @Test
    void validatesRealAnnotationsAgainstBoundRetainedSchema() throws Exception {
        var schema = binding(choice("(this.left != '') != (this.right != '')"));
        var original = candidate(schema, "correct", "");
        var checked = check(schema, original, LIMITS);
        assertThat(checked.schema()).isSameAs(schema);
        assertThat(checked.original()).isSameAs(original);
        assertThat(checked.decoded().getField(schema.type().findFieldByName("left"))).isEqualTo("correct");
        for (var invalid : List.of(candidate(schema, "x", ""), candidate(schema, "", ""),
                candidate(schema, "both", "filled"))) {
            assertThatThrownBy(() -> check(schema, invalid, LIMITS))
                    .isInstanceOf(ValidationResult.ValidationException.class);
        }
    }

    @Test
    void preservesNoncanonicalCandidateBytesAndCountsDuplicateTags() throws Exception {
        var schema = binding(choice("true"));
        var single = candidate(schema, "first", "");
        var bytes = single.getValue().concat(candidate(schema, "second", "").getValue());
        var original = single.toBuilder().setValue(bytes).build();
        var checked = check(schema, original, LIMITS);
        assertThat(checked.original().getValue()).isEqualTo(bytes);
        assertThat(checked.decoded().getField(schema.type().findFieldByName("left"))).isEqualTo("second");
        assertThatThrownBy(() -> check(schema, original, new DocumentPayloadCheck.Limits(1024, 1, 10, 100, 1000)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("wire value");
    }

    @Test
    void rejectsWrongTypeAndPrefixBeforeDecoding() throws Exception {
        var schema = binding(choice("true"));
        for (String wrong : List.of("payload.Choice", "other/payload.Choice", "type.protomolt.test/payload.Other")) {
            var candidate = Any.newBuilder().setTypeUrl(wrong).setValue(ByteString.copyFrom(new byte[]{0})).build();
            assertThatThrownBy(() -> check(schema, candidate, LIMITS))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("type URL");
        }
        assertThatThrownBy(() -> DocumentPayloadCheck.check(schema, candidate(schema, "valid", ""),
                "/payload.Choice", validator(), LIMITS, () -> {})).hasMessageContaining("accepted type URL");
    }

    @Test
    void rejectsOversizeMalformedAndUncompilableCandidates() throws Exception {
        var schema = binding(choice("true"));
        assertThatThrownBy(() -> check(schema, candidate(schema, "valid", ""),
                new DocumentPayloadCheck.Limits(1, 100, 10, 100, 1000)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("bytes");
        assertThatThrownBy(() -> check(schema, Any.newBuilder().setTypeUrl(URL)
                .setValue(ByteString.copyFrom(new byte[]{10, 100, 1})).build(), LIMITS))
                .isInstanceOf(InvalidProtocolBufferException.class);
        var invalidSchema = binding(choice("this.missing_field == 1"));
        assertThatThrownBy(() -> check(invalidSchema, candidate(invalidSchema, "valid", ""), LIMITS))
                .isInstanceOf(RuleCompilationException.class);
    }

    @Test
    void refusesExtensionSchemas() {
        for (Descriptor type : List.of(FieldOptions.getDescriptor())) {
            var schema = binding(type);
            String url = "type.protomolt.test/" + type.getFullName();
            var candidate = Any.newBuilder().setTypeUrl(url).build();
            assertThatThrownBy(() -> DocumentPayloadCheck.check(schema, candidate, url, validator(), LIMITS, () -> {}))
                    .isInstanceOf(UnsupportedOperationException.class);
        }
    }

    @Test
    void cancellationReturnsNoCheckedCandidate() throws Exception {
        var schema = binding(choice("true"));
        var stop = new CancellationException("cancel payload");
        assertThatThrownBy(() -> DocumentPayloadCheck.check(schema, candidate(schema, "valid", ""), URL,
                validator(), LIMITS, () -> { throw stop; })).isSameAs(stop);
    }

    @Test
    void acceptsUnsetNestedAnyAndChecksSchemaLimits() throws Exception {
        var file = FileDescriptorProto.newBuilder().setName("wrapper.proto").setPackage("payload").setSyntax("proto3")
                .addDependency(Any.getDescriptor().getFile().getName())
                .addMessageType(DescriptorProto.newBuilder().setName("Wrapper").addField(
                        FieldDescriptorProto.newBuilder().setName("nested").setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE).setTypeName(".google.protobuf.Any"))).build();
        var type = FileDescriptor.buildFrom(file, new FileDescriptor[]{Any.getDescriptor().getFile()})
                .findMessageTypeByName("Wrapper");
        var schema = binding(type);
        String url = "type.protomolt.test/payload.Wrapper";
        var empty = Any.newBuilder().setTypeUrl(url).build();
        assertThat(DocumentPayloadCheck.check(schema, empty, url, validator(), LIMITS, () -> {}).decoded().getAllFields()).isEmpty();
        var limited = new DocumentPayloadCheck.Limits(1024, 100, 10, 1, 1000);
        assertThatThrownBy(() -> DocumentPayloadCheck.check(schema, empty, url, validator(), limited, () -> {}))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("schema message count");
    }

    @Test
    void refusesInterruptedThreadWithoutRelyingOnHostControl() throws Exception {
        var schema = binding(choice("true"));
        var original = candidate(schema, "valid", "");
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(() -> check(schema, original, LIMITS)).isInstanceOf(CancellationException.class);
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void preservesUnknownCandidateBytesWithoutClaimingTheirValidation() throws Exception {
        var schema = binding(choice("true"));
        var valid = candidate(schema, "valid", "");
        // Unknown length-delimited field 99 with one opaque byte.
        var bytes = valid.getValue().concat(ByteString.copyFrom(new byte[]{(byte) 0x9a, 0x06, 0x01, 0x7a}));
        var checked = check(schema, valid.toBuilder().setValue(bytes).build(), LIMITS);
        assertThat(checked.original().getValue()).isEqualTo(bytes);
        assertThat(checked.decoded().getUnknownFields().hasField(99)).isTrue();
    }

    @Test
    void resolvesRepeatedAnyOnceAndValidatesEveryEmbeddedPayload() throws Exception {
        var wrapper = wrapper();
        var inner = binding(choice("(this.left != '') != (this.right != '')"));
        var calls = new java.util.concurrent.atomic.AtomicInteger();
        var payload = wrapped(wrapper, candidate(inner, "first", ""), candidate(inner, "second", ""));
        var checked = DocumentPayloadCheck.check(wrapper, payload, payload.getTypeUrl(), validator(), LIMITS, () -> {}, url -> {
            assertThat(url).isEqualTo(URL);
            calls.incrementAndGet();
            return inner;
        });
        assertThat(calls.get()).isEqualTo(1);
        assertThat(checked.resolvedSchemas()).containsEntry(URL, inner);
        assertThat(checked.original()).isSameAs(payload);
        var invalid = wrapped(wrapper, candidate(inner, "first", ""), candidate(inner, "x", ""));
        assertThatThrownBy(() -> DocumentPayloadCheck.check(wrapper, invalid, invalid.getTypeUrl(), validator(), LIMITS,
                () -> {}, url -> inner)).isInstanceOf(ValidationResult.ValidationException.class);
    }

    @Test
    void unresolvedWrongBindingAndResolverFailureCannotProduceCheckedPayload() throws Exception {
        var wrapper = wrapper();
        var inner = binding(choice("true"));
        var payload = wrapped(wrapper, candidate(inner, "first", ""));
        assertThatThrownBy(() -> DocumentPayloadCheck.check(wrapper, payload, payload.getTypeUrl(), validator(), LIMITS, () -> {}))
                .hasMessageContaining("unresolved Any");
        assertThatThrownBy(() -> DocumentPayloadCheck.check(wrapper, payload, payload.getTypeUrl(), validator(), LIMITS, () -> {}, url -> null))
                .hasMessageContaining("unresolved Any");
        assertThatThrownBy(() -> DocumentPayloadCheck.check(wrapper, payload, payload.getTypeUrl(), validator(), LIMITS, () -> {}, url -> wrapper))
                .hasMessageContaining("type URL");
        var failure = new IllegalStateException("registry unavailable");
        assertThatThrownBy(() -> DocumentPayloadCheck.check(wrapper, payload, payload.getTypeUrl(), validator(), LIMITS,
                () -> {}, url -> { throw failure; })).isSameAs(failure);
    }

    @Test
    void nestedWorkSharesBytesWireValuesAndDepthBudgets() throws Exception {
        var wrapper = wrapper();
        var inner = binding(choice("true"));
        var payload = wrapped(wrapper, candidate(inner, "first", ""));
        var bytes = new DocumentPayloadCheck.Limits(payload.getValue().size(), 100, 10, 100, 1000);
        assertThatThrownBy(() -> DocumentPayloadCheck.check(wrapper, payload, payload.getTypeUrl(), validator(), bytes,
                () -> {}, url -> inner)).hasMessageContaining("aggregate payload bytes");
        var wire = new DocumentPayloadCheck.Limits(1024, 3, 10, 100, 1000);
        assertThatThrownBy(() -> DocumentPayloadCheck.check(wrapper, payload, payload.getTypeUrl(), validator(), wire,
                () -> {}, url -> inner)).hasMessageContaining("wire value");
        var depth = new DocumentPayloadCheck.Limits(1024, 100, 1, 100, 1000);
        assertThatThrownBy(() -> DocumentPayloadCheck.check(wrapper, payload, payload.getTypeUrl(), validator(), depth,
                () -> {}, url -> inner)).hasMessageContaining("depth");
    }

    @Test
    void recursivelyResolvesAnyInsideResolvedPayload() throws Exception {
        var wrapper = wrapper();
        var inner = binding(choice("true"));
        var nested = wrapped(wrapper, candidate(inner, "valid", ""));
        var payload = wrapped(wrapper, nested);
        var checked = DocumentPayloadCheck.check(wrapper, payload, payload.getTypeUrl(), validator(), LIMITS,
                () -> {}, url -> inner);
        assertThat(checked.resolvedSchemas()).hasSize(2);
        var invalid = wrapped(wrapper, wrapped(wrapper, candidate(inner, "x", "")));
        assertThatThrownBy(() -> DocumentPayloadCheck.check(wrapper, invalid, invalid.getTypeUrl(), validator(), LIMITS,
                () -> {}, url -> inner)).isInstanceOf(ValidationResult.ValidationException.class);
    }

    @Test
    void validatesMapAnyValues() throws Exception {
        var entry = DescriptorProto.newBuilder().setName("ItemsEntry").setOptions(MessageOptions.newBuilder().setMapEntry(true))
                .addField(FieldDescriptorProto.newBuilder().setName("key").setNumber(1).setType(FieldDescriptorProto.Type.TYPE_STRING))
                .addField(FieldDescriptorProto.newBuilder().setName("value").setNumber(2).setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                        .setTypeName(".google.protobuf.Any"));
        var file = FileDescriptorProto.newBuilder().setName("map.proto").setPackage("payload").setSyntax("proto3")
                .addDependency("google/protobuf/any.proto").addMessageType(DescriptorProto.newBuilder().setName("MapWrapper")
                        .addNestedType(entry).addField(FieldDescriptorProto.newBuilder().setName("items").setNumber(1)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED).setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setTypeName(".payload.MapWrapper.ItemsEntry"))).build();
        var schema = binding(FileDescriptor.buildFrom(file, new FileDescriptor[]{Any.getDescriptor().getFile()})
                .findMessageTypeByName("MapWrapper"));
        var inner = binding(choice("true"));
        var items = schema.type().findFieldByName("items");
        var entryType = items.getMessageType();
        var value = candidate(inner, "x", "");
        var mapEntry = DynamicMessage.newBuilder(entryType).setField(entryType.findFieldByName("key"), "one")
                .setField(entryType.findFieldByName("value"), DynamicMessage.parseFrom(entryType.findFieldByName("value").getMessageType(), value.toByteString())).build();
        var payload = Any.newBuilder().setTypeUrl("type.protomolt.test/payload.MapWrapper")
                .setValue(DynamicMessage.newBuilder(schema.type()).addRepeatedField(items, mapEntry).build().toByteString()).build();
        assertThatThrownBy(() -> DocumentPayloadCheck.check(schema, payload, payload.getTypeUrl(), validator(), LIMITS,
                () -> {}, url -> inner)).isInstanceOf(ValidationResult.ValidationException.class);
    }

    @Test
    void resolvesSourceOnlyTypeAtRuntimeAndReusesRetainedDefinitionOffline() throws Exception {
        var wrapper = wrapper();
        String typeUrl = "type.protomolt.test/runtime.NewType";
        var embedded = Any.newBuilder().setTypeUrl(typeUrl)
                .setValue(ByteString.copyFrom(new byte[]{10, 2, 'o', 'k'})).build();
        var payload = wrapped(wrapper, embedded);
        var checked = DocumentPayloadCheck.check(wrapper, payload, payload.getTypeUrl(), validator(), LIMITS, () -> {}, url -> {
            assertThat(url).isEqualTo(typeUrl);
            try { var compiled = new ai.protomolt.proto.sources.ProtoSourceCompiler().compile(
                    ai.protomolt.proto.sources.ProtoSourceSet.builder().add("runtime.proto",
                            "syntax = \"proto3\"; package runtime; message NewType { string value = 1; }", "test definition").build());
            return binding(compiled.descriptorFor("runtime.proto").orElseThrow().findMessageTypeByName("NewType"));
            } catch (ai.protomolt.proto.sources.ProtoCompilationException e) {
                throw new IllegalStateException("runtime schema compilation failed", e);
            }
        });
        var retained = checked.resolvedSchemas().get(typeUrl);
        // Fresh descriptor graph reconstructed only from retained bytes, no class or live registry.
        var reconstructed = DocumentSchemaBinding.bind(retained.condition(), retained.artifact(),
                new ClosedDescriptorSet.Limits(4_000_000, 100, 1000, 100), () -> {});
        var offline = DocumentPayloadCheck.check(wrapper, payload, payload.getTypeUrl(), validator(), LIMITS,
                () -> {}, url -> reconstructed);
        assertThat(offline.original()).isEqualTo(payload);
        assertThat(offline.resolvedSchemas().get(typeUrl).type().getFullName()).isEqualTo("runtime.NewType");
    }

    private static DocumentSchemaBinding wrapper() throws Exception {
        var file = FileDescriptorProto.newBuilder().setName("wrapper.proto").setPackage("payload").setSyntax("proto3")
                .addDependency("google/protobuf/any.proto").addMessageType(DescriptorProto.newBuilder().setName("Wrapper")
                        .addField(FieldDescriptorProto.newBuilder().setName("items").setNumber(1)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED).setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setTypeName(".google.protobuf.Any"))).build();
        return binding(FileDescriptor.buildFrom(file, new FileDescriptor[]{Any.getDescriptor().getFile()})
                .findMessageTypeByName("Wrapper"));
    }

    private static Any wrapped(DocumentSchemaBinding wrapper, Any... values) throws Exception {
        var data = DynamicMessage.newBuilder(wrapper.type());
        var field = wrapper.type().findFieldByName("items");
        for (Any value : values) data.addRepeatedField(field, DynamicMessage.parseFrom(field.getMessageType(), value.toByteString()));
        return Any.newBuilder().setTypeUrl("type.protomolt.test/payload.Wrapper").setValue(data.build().toByteString()).build();
    }

    private static DocumentPayloadCheck check(DocumentSchemaBinding schema, Any candidate, DocumentPayloadCheck.Limits limits)
            throws InvalidProtocolBufferException {
        return DocumentPayloadCheck.check(schema, candidate, URL, validator(), limits, () -> {});
    }

    private static ProtoValidator validator() {
        return ProtoValidator.create(List.of(new ProtomoltRuleSource()));
    }

    private static Any candidate(DocumentSchemaBinding binding, String left, String right) {
        var type = binding.type();
        var data = DynamicMessage.newBuilder(type).setField(type.findFieldByName("left"), left)
                .setField(type.findFieldByName("right"), right).build();
        return Any.newBuilder().setTypeUrl(URL).setValue(data.toByteString()).build();
    }

    private static DocumentSchemaBinding binding(Descriptor type) {
        var set = DescriptorFingerprints.closure(type);
        var condition = PublicationSchemaCondition.newBuilder().setTypeName(type.getFullName())
                .setDescriptorFingerprint(DescriptorFingerprints.fingerprint(set)).build();
        return DocumentSchemaBinding.bind(condition, set.toByteString(),
                new ClosedDescriptorSet.Limits(4_000_000, 100, 1000, 100), () -> {});
    }

    private static Descriptor choice(String expression) throws Exception {
        var message = DescriptorProto.newBuilder().setName("Choice")
                .setOptions(MessageOptions.newBuilder().setExtension(ValidateProto.message,
                        MessageRules.newBuilder().addCel(CelRule.newBuilder().setId("choice.exclusive")
                                .setExpression(expression).setMessage("choose exactly one")).build()));
        for (String field : List.of("left", "right")) {
            message.addField(FieldDescriptorProto.newBuilder().setName(field).setNumber(message.getFieldCount() + 1)
                    .setType(FieldDescriptorProto.Type.TYPE_STRING).setOptions(FieldOptions.newBuilder()
                            .setExtension(ValidateProto.field, FieldRules.newBuilder().setIgnoreIfZero(true)
                                    .setString(StringRules.newBuilder().setMinLen(3)).build())));
        }
        var file = FileDescriptorProto.newBuilder().setName("choice.proto").setPackage("payload").setSyntax("proto3")
                .addDependency(ValidateProto.getDescriptor().getName()).addMessageType(message).build();
        return FileDescriptor.buildFrom(file, new FileDescriptor[]{ValidateProto.getDescriptor()})
                .findMessageTypeByName("Choice");
    }
}
