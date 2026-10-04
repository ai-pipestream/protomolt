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
    void contextualResolutionDoesNotReuseRootPolicyAndRejectsConflictingAssetMetadata() throws Exception {
        var root = wrapper();
        var nested = wrapped(root);
        var payload = wrapped(root, nested, nested);
        var rootAsset = asset(root, payload.getTypeUrl());
        var calls = new java.util.concurrent.atomic.AtomicInteger();
        var checked = DocumentPayloadCheck.checkContextualAssets(rootAsset, payload, payload.getTypeUrl(),
                validator(), LIMITS, () -> {}, request -> {
                    calls.incrementAndGet();
                    assertThat(request.typeUrl()).isEqualTo(payload.getTypeUrl());
                    return rootAsset;
                }, DocumentSchemaOccurrences.Limits.DEFAULT);
        assertThat(calls.get()).isEqualTo(2);
        assertThat(checked.assets()).hasSize(1);
        assertThat(checked.payload().occurrences()).hasSize(3);
        var changedMetadata = rootAsset.metadata().toBuilder().setCompilation(rootAsset.metadata().getCompilation()
                .toBuilder().setUnknownCompilerReason("Different producer assertion")).build();
        var conflicting = DocumentSchemaAssetBinding.bind(changedMetadata, root.artifact(),
                new ClosedDescriptorSet.Limits(4_000_000, 100, 1000, 100), () -> {});
        assertThatThrownBy(() -> DocumentPayloadCheck.checkContextualAssets(rootAsset, payload, payload.getTypeUrl(),
                validator(), LIMITS, () -> {}, request -> conflicting, DocumentSchemaOccurrences.Limits.DEFAULT))
                .hasMessageContaining("conflicting schema asset metadata");
    }

    @Test
    void contextualResolutionKeepsSameUrlVersionsSeparateThroughValidationAndProjection() throws Exception {
        var root = wrapper();
        var left = binding(choice("this.left != '' && this.right == ''"));
        var right = binding(choice("this.right != '' && this.left == ''"));
        var first = candidate(left, "left", "");
        var second = candidate(right, "", "right");
        var digests = List.of(digest(first.getValue()), digest(second.getValue()));
        var payload = wrapped(root, first, second);
        var rootAsset = asset(root, payload.getTypeUrl());
        var requests = new java.util.ArrayList<DocumentPayloadCheck.ResolutionRequest>();
        java.util.function.Function<DocumentPayloadCheck.ResolutionRequest, DocumentSchemaAssetBinding> resolver = request -> {
            requests.add(request);
            assertThat(request.typeUrl()).isEqualTo(URL);
            assertThat(request.prefix()).hasSize(3);
            assertThat(request.prefix().get(0)).isInstanceOf(DocumentSchemaOccurrences.Boundary.class);
            assertThat(request.prefix().get(1)).isEqualTo(new DocumentSchemaOccurrences.Field(1));
            int index = ((DocumentSchemaOccurrences.Index) request.prefix().get(2)).index();
            var value = index == 0 ? first : second;
            assertThat(request.valueSha256()).isEqualTo(digests.get(index));
            assertThat(request.valueSizeBytes()).isEqualTo(value.getValue().size());
            return asset(index == 0 ? left : right, request.typeUrl());
        };
        var result = DocumentPayloadCheck.checkContextualAssets(rootAsset, payload, payload.getTypeUrl(),
                validator(), LIMITS, () -> {}, resolver, DocumentSchemaOccurrences.Limits.DEFAULT);
        assertThat(requests).hasSize(2);
        assertThatThrownBy(() -> requests.getFirst().prefix().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThat(result.assets()).hasSize(3);
        var paths = DocumentSchemaOccurrenceProjection.project(result.payload(), () -> {});
        assertThat(paths).hasSize(3);
        assertThat(paths.get(1).getSteps(3).getAnyBoundary().getResolved().getArtifactSha256()).isEqualTo(left.artifactSha256());
        assertThat(paths.get(2).getSteps(3).getAnyBoundary().getResolved().getArtifactSha256()).isEqualTo(right.artifactSha256());
        assertThatThrownBy(() -> DocumentPayloadCheck.checkContextualAssets(rootAsset, payload, payload.getTypeUrl(),
                validator(), LIMITS, () -> {}, request -> asset(left, request.typeUrl()), DocumentSchemaOccurrences.Limits.DEFAULT))
                .isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> DocumentPayloadCheck.checkContextualAssets(rootAsset, payload, payload.getTypeUrl(),
                validator(), LIMITS, () -> {}, request -> null, DocumentSchemaOccurrences.Limits.DEFAULT))
                .hasMessageContaining("unresolved Any schema asset");
        var outage = new IllegalStateException("registry unavailable");
        assertThatThrownBy(() -> DocumentPayloadCheck.checkContextualAssets(rootAsset, payload, payload.getTypeUrl(),
                validator(), LIMITS, () -> {}, request -> { throw outage; }, DocumentSchemaOccurrences.Limits.DEFAULT))
                .isSameAs(outage);
    }

    @Test
    void projectsMeasuredBoundaryIdentityAndRejectsUncheckedEvidence() throws Exception {
        var root = wrapper();
        var inner = binding(choice("true"));
        var leaf = candidate(inner, "valid", "");
        var candidate = wrapped(root, leaf);
        var checked = DocumentPayloadCheck.checkAssets(asset(root, candidate.getTypeUrl()), candidate, candidate.getTypeUrl(),
                validator(), LIMITS, () -> {}, url -> asset(inner, url)).payload();
        var paths = DocumentSchemaOccurrenceProjection.project(checked, () -> {});
        assertThat(paths).hasSize(2);
        var boundary = paths.get(1).getSteps(3).getAnyBoundary();
        assertThat(boundary.getTypeUrl()).isEqualTo(URL);
        assertThat(boundary.getValueSizeBytes()).isEqualTo(leaf.getValue().size());
        assertThat(boundary.getValueSha256()).isEqualTo(digest(leaf.getValue()));
        assertThat(boundary.getResolved().getArtifactSha256()).isEqualTo(inner.artifactSha256());
        assertThat(boundary.getResolved().getSchema()).isEqualTo(inner.condition());
        for (var path : paths) {
            assertThat(validator().validate(path).valid()).isTrue();
            assertThat(ai.protomolt.proto.repo.v1.RepositorySchemaOccurrencePath.parseFrom(path.toByteString())).isEqualTo(path);
        }
        assertThatThrownBy(() -> paths.clear()).isInstanceOf(UnsupportedOperationException.class);
        var permissive = DocumentPayloadCheck.check(root, candidate, candidate.getTypeUrl(), validator(), LIMITS, () -> {}, url -> inner);
        assertThatThrownBy(() -> DocumentSchemaOccurrenceProjection.project(permissive, () -> {}))
                .hasMessageContaining("strict archival occurrence evidence required");
        var calls = new java.util.concurrent.atomic.AtomicInteger();
        assertThatThrownBy(() -> DocumentSchemaOccurrenceProjection.project(checked, () -> {
            if (calls.incrementAndGet() == 4) throw new CancellationException("stop projection");
        })).isInstanceOf(CancellationException.class);
        assertThat(DocumentSchemaOccurrenceProjection.project(checked, () -> {})).isEqualTo(paths);
    }

    @Test
    void projectsEveryMapKeyTypeWithoutLosingUnsignedBits() throws Exception {
        var inner = binding(choice("true"));
        var leaf = candidate(inner, "valid", "");
        var types = List.of(FieldDescriptorProto.Type.TYPE_STRING, FieldDescriptorProto.Type.TYPE_BOOL,
                FieldDescriptorProto.Type.TYPE_INT32, FieldDescriptorProto.Type.TYPE_SINT32, FieldDescriptorProto.Type.TYPE_SFIXED32,
                FieldDescriptorProto.Type.TYPE_UINT32, FieldDescriptorProto.Type.TYPE_FIXED32,
                FieldDescriptorProto.Type.TYPE_INT64, FieldDescriptorProto.Type.TYPE_SINT64, FieldDescriptorProto.Type.TYPE_SFIXED64,
                FieldDescriptorProto.Type.TYPE_UINT64, FieldDescriptorProto.Type.TYPE_FIXED64);
        for (var type : types) {
            Object key = switch (type) {
                case TYPE_STRING -> "";
                case TYPE_BOOL -> false;
                case TYPE_INT32, TYPE_SINT32, TYPE_SFIXED32, TYPE_UINT32, TYPE_FIXED32 -> -1;
                default -> -1L;
            };
            var root = mapWrapper(false, type);
            var field = root.type().findFieldByNumber(1);
            var entry = DynamicMessage.newBuilder(field.getMessageType())
                    .setField(field.getMessageType().findFieldByNumber(1), key)
                    .setField(field.getMessageType().findFieldByNumber(2), leaf).build();
            String url = "type.protomolt.test/payload.MapWrapper";
            var candidate = Any.newBuilder().setTypeUrl(url).setValue(DynamicMessage.newBuilder(root.type())
                    .addRepeatedField(field, entry).build().toByteString()).build();
            var checked = DocumentPayloadCheck.checkAssets(asset(root, url), candidate, url, validator(), LIMITS,
                    () -> {}, nested -> asset(inner, nested)).payload();
            var projected = DocumentSchemaOccurrenceProjection.project(checked, () -> {}).get(1).getSteps(2).getMapKey();
            assertThat(projected.getType().name()).isEqualTo("REPOSITORY_OCCURRENCE_KEY_" + type.name());
            switch (type) {
                case TYPE_STRING -> { assertThat(projected.hasStringValue()).isTrue(); assertThat(projected.getStringValue()).isEmpty(); }
                case TYPE_BOOL -> { assertThat(projected.hasBoolValue()).isTrue(); assertThat(projected.getBoolValue()).isFalse(); }
                case TYPE_UINT32, TYPE_FIXED32 -> assertThat(projected.getUnsignedValue()).isEqualTo(4294967295L);
                case TYPE_UINT64, TYPE_FIXED64 -> assertThat(Long.toUnsignedString(projected.getUnsignedValue())).isEqualTo("18446744073709551615");
                default -> assertThat(projected.getSignedValue()).isEqualTo(-1L);
            }
        }
    }

    @Test
    void archivalOccurrencesDistinguishEqualRepeatedValuesAndBindExactBytes() throws Exception {
        var root = wrapper();
        var inner = binding(choice("true"));
        var item = candidate(inner, "same", "");
        var candidate = wrapped(root, item, item);
        var result = DocumentPayloadCheck.checkAssets(asset(root, candidate.getTypeUrl()), candidate, candidate.getTypeUrl(),
                validator(), LIMITS, () -> {}, url -> asset(inner, url));
        var occurrences = result.payload().occurrences();
        assertThat(occurrences).hasSize(3);
        var rootBoundary = new DocumentSchemaOccurrences.Boundary(candidate.getTypeUrl(), digest(candidate.getValue()), root.artifactSha256(), candidate.getValue().size());
        var childBoundary = new DocumentSchemaOccurrences.Boundary(URL, digest(item.getValue()), inner.artifactSha256(), item.getValue().size());
        assertThat(occurrences.get(0).path()).containsExactly(rootBoundary);
        for (int i = 0; i < 2; i++) {
            assertThat(occurrences.get(i + 1).path()).containsExactly(rootBoundary,
                    new DocumentSchemaOccurrences.Field(1), new DocumentSchemaOccurrences.Index(i), childBoundary);
        }
        assertThat(result.payload().original()).isSameAs(candidate);
        assertThatThrownBy(() -> occurrences.clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> occurrences.get(0).path().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThat(DocumentPayloadCheck.check(root, candidate, candidate.getTypeUrl(), validator(), LIMITS, () -> {}, url -> inner)
                .occurrences()).isEmpty();
    }

    @Test
    void archivalEvidenceEnforcesAggregateLimitsBeforeReturningAResult() throws Exception {
        var root = wrapper();
        var inner = binding(choice("true"));
        var candidate = wrapped(root, candidate(inner, "same", ""), candidate(inner, "same", ""));
        int text = candidate.getTypeUrl().getBytes(java.nio.charset.StandardCharsets.UTF_8).length * 3
                + URL.getBytes(java.nio.charset.StandardCharsets.UTF_8).length * 2;
        var exact = new DocumentSchemaOccurrences.Limits(3, 9, text);
        assertThat(DocumentPayloadCheck.checkAssets(asset(root, candidate.getTypeUrl()), candidate, candidate.getTypeUrl(),
                validator(), LIMITS, () -> {}, url -> asset(inner, url), exact).payload().occurrences()).hasSize(3);
        for (var bound : List.of(new DocumentSchemaOccurrences.Limits(2, 9, text),
                new DocumentSchemaOccurrences.Limits(3, 8, text), new DocumentSchemaOccurrences.Limits(3, 9, text - 1))) {
            assertThatThrownBy(() -> DocumentPayloadCheck.checkAssets(asset(root, candidate.getTypeUrl()), candidate, candidate.getTypeUrl(),
                    validator(), LIMITS, () -> {}, url -> asset(inner, url), bound)).hasMessageContaining("limit exceeded");
        }
    }

    private static String digest(ByteString bytes) throws Exception {
        return java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray()));
    }

    private static DocumentSchemaBinding mapWrapper() throws Exception {
        return mapWrapper(false);
    }

    private static DocumentSchemaBinding mapWrapper(boolean extraField) throws Exception {
        return mapWrapper(extraField, FieldDescriptorProto.Type.TYPE_STRING);
    }

    private static DocumentSchemaBinding mapWrapper(boolean extraField, FieldDescriptorProto.Type keyType) throws Exception {
        var entry = DescriptorProto.newBuilder().setName("ItemsEntry")
                .setOptions(MessageOptions.newBuilder().setMapEntry(true))
                .addField(FieldDescriptorProto.newBuilder().setName("key").setNumber(1).setType(keyType))
                .addField(FieldDescriptorProto.newBuilder().setName("value").setNumber(2).setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                        .setTypeName(".google.protobuf.Any"));
        if (extraField) entry.addField(FieldDescriptorProto.newBuilder().setName("extra").setNumber(3)
                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE).setTypeName(".google.protobuf.Any"));
        var definition = DescriptorProto.newBuilder().setName("MapWrapper").addNestedType(entry)
                .addField(FieldDescriptorProto.newBuilder().setName("items").setNumber(1)
                        .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED).setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                        .setTypeName(".payload.MapWrapper.ItemsEntry"));
        var file = FileDescriptorProto.newBuilder().setName("map-wrapper.proto").setPackage("payload").setSyntax("proto3")
                .addDependency("google/protobuf/any.proto").addMessageType(definition).build();
        return binding(FileDescriptor.buildFrom(file, new FileDescriptor[]{Any.getDescriptor().getFile()})
                .findMessageTypeByName("MapWrapper"));
    }

    @Test
    void archivalOccurrencesRetainUnsignedMapKeysAndNestedBoundaries() throws Exception {
        var root = mapWrapper(false, FieldDescriptorProto.Type.TYPE_UINT64);
        var middle = wrapper();
        var inner = binding(choice("true"));
        var leaf = candidate(inner, "valid", "");
        var nested = wrapped(middle, leaf);
        var field = root.type().findFieldByNumber(1);
        var data = DynamicMessage.newBuilder(root.type());
        for (long key : new long[]{0L, -1L}) {
            data.addRepeatedField(field, DynamicMessage.newBuilder(field.getMessageType())
                    .setField(field.getMessageType().findFieldByNumber(1), key)
                    .setField(field.getMessageType().findFieldByNumber(2), nested).build());
        }
        String url = "type.protomolt.test/payload.MapWrapper";
        var candidate = Any.newBuilder().setTypeUrl(url).setValue(data.build().toByteString()).build();
        var result = DocumentPayloadCheck.checkAssets(asset(root, url), candidate, url, validator(), LIMITS, () -> {},
                requested -> asset(requested.equals(URL) ? inner : middle, requested));
        assertThat(result.payload().occurrences()).hasSize(5);
        for (int i = 0; i < 2; i++) {
            var path = result.payload().occurrences().get(2 + i * 2).path();
            assertThat(path).containsExactly(
                    new DocumentSchemaOccurrences.Boundary(url, digest(candidate.getValue()), root.artifactSha256(), candidate.getValue().size()),
                    new DocumentSchemaOccurrences.Field(1),
                    new DocumentSchemaOccurrences.MapKey(com.google.protobuf.Descriptors.FieldDescriptor.Type.UINT64, i == 0 ? 0L : -1L),
                    new DocumentSchemaOccurrences.Boundary(nested.getTypeUrl(), digest(nested.getValue()), middle.artifactSha256(), nested.getValue().size()),
                    new DocumentSchemaOccurrences.Field(1), new DocumentSchemaOccurrences.Index(0),
                    new DocumentSchemaOccurrences.Boundary(URL, digest(leaf.getValue()), inner.artifactSha256(), leaf.getValue().size()));
        }
    }

    @Test
    void archivalEvidenceRejectsNoncanonicalMapEntryDescriptors() throws Exception {
        var root = mapWrapper(true);
        String url = "type.protomolt.test/payload.MapWrapper";
        var candidate = Any.newBuilder().setTypeUrl(url).build();
        assertThatThrownBy(() -> DocumentPayloadCheck.checkAssets(asset(root, url), candidate, url, validator(), LIMITS,
                () -> {}, nested -> { throw new AssertionError("malformed map reached resolver"); }))
                .hasMessageContaining("invalid map entry descriptor");
    }

    @Test
    void archivalMapCannotSkipAnOmittedAnyValue() throws Exception {
        var schema = mapWrapper();
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
    void archivalMapPathsUseTypedKeysAndChargeUtf8Bytes() throws Exception {
        var root = mapWrapper();
        var inner = binding(choice("true"));
        var item = candidate(inner, "same", "");
        var field = root.type().findFieldByNumber(1);
        String url = "type.protomolt.test/payload.MapWrapper";
        var entries = new java.util.ArrayList<DynamicMessage>();
        for (String key : List.of("", "é水")) {
            entries.add(DynamicMessage.newBuilder(field.getMessageType())
                    .setField(field.getMessageType().findFieldByNumber(1), key)
                    .setField(field.getMessageType().findFieldByNumber(2), item).build());
        }
        int text = 3 * url.getBytes(java.nio.charset.StandardCharsets.UTF_8).length
                + 2 * URL.getBytes(java.nio.charset.StandardCharsets.UTF_8).length
                + "é水".getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        for (var order : List.of(entries, List.of(entries.get(1), entries.get(0)))) {
            var data = DynamicMessage.newBuilder(root.type());
            order.forEach(entry -> data.addRepeatedField(field, entry));
            var candidate = Any.newBuilder().setTypeUrl(url).setValue(data.build().toByteString()).build();
            var result = DocumentPayloadCheck.checkAssets(asset(root, url), candidate, url, validator(), LIMITS,
                    () -> {}, nested -> asset(inner, nested), new DocumentSchemaOccurrences.Limits(3, 9, text));
            assertThat(result.payload().occurrences()).hasSize(3);
            for (int i = 0; i < 2; i++) {
                var path = result.payload().occurrences().get(i + 1).path();
                assertThat(path).hasSize(4);
                assertThat(path.get(1)).isEqualTo(new DocumentSchemaOccurrences.Field(1));
                assertThat(path.get(2)).isEqualTo(new DocumentSchemaOccurrences.MapKey(
                        com.google.protobuf.Descriptors.FieldDescriptor.Type.STRING,
                        order.get(i).getField(field.getMessageType().findFieldByNumber(1))));
                assertThat(path.get(3)).isEqualTo(new DocumentSchemaOccurrences.Boundary(URL, digest(item.getValue()), inner.artifactSha256(), item.getValue().size()));
            }
            assertThatThrownBy(() -> DocumentPayloadCheck.checkAssets(asset(root, url), candidate, url, validator(), LIMITS,
                    () -> {}, nested -> asset(inner, nested), new DocumentSchemaOccurrences.Limits(3, 9, text - 1)))
                    .hasMessageContaining("text limit exceeded");
        }
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
        assertThat(result.assets()).containsExactlyInAnyOrderEntriesOf(java.util.Map.of(
                new DocumentPayloadCheck.SchemaKey(payload.getTypeUrl(), wrapper.artifactSha256()), rootAsset,
                new DocumentPayloadCheck.SchemaKey(URL, inner.artifactSha256()), innerAsset));
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
        assertThat(checked.resolvedSchemas()).containsEntry(new DocumentPayloadCheck.SchemaKey(URL, inner.artifactSha256()), inner);
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
        var retained = checked.resolvedSchemas().entrySet().stream()
                .filter(entry -> entry.getKey().typeUrl().equals(typeUrl)).findFirst().orElseThrow().getValue();
        // Fresh descriptor graph reconstructed only from retained bytes, no class or live registry.
        var reconstructed = DocumentSchemaBinding.bind(retained.condition(), retained.artifact(),
                new ClosedDescriptorSet.Limits(4_000_000, 100, 1000, 100), () -> {});
        var offline = DocumentPayloadCheck.check(wrapper, payload, payload.getTypeUrl(), validator(), LIMITS,
                () -> {}, url -> reconstructed);
        assertThat(offline.original()).isEqualTo(payload);
        assertThat(offline.resolvedSchemas().get(new DocumentPayloadCheck.SchemaKey(typeUrl, reconstructed.artifactSha256()))
                .type().getFullName()).isEqualTo("runtime.NewType");
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
