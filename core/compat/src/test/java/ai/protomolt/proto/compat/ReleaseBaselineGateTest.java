package ai.protomolt.proto.compat;

import ai.protomolt.proto.compat.ActionEnvelopeSnapshot.ActionEnvelopeEntry;
import ai.protomolt.proto.compat.JavaApiSnapshot.ClassEntry;
import ai.protomolt.proto.compat.JavaApiSnapshot.ConstructorEntry;
import ai.protomolt.proto.compat.JavaApiSnapshot.FieldEntry;
import ai.protomolt.proto.compat.JavaApiSnapshot.MethodEntry;
import ai.protomolt.proto.compat.JavaApiSnapshot.TypeKind;
import ai.protomolt.proto.compat.JavaApiSnapshot.Visibility;
import ai.protomolt.proto.compat.RestRouteSnapshot.RestRouteEntry;
import ai.protomolt.proto.compat.StoredFormatSnapshot.StoredFormatEntry;
import ai.protomolt.proto.descriptors.ApiStability;
import ai.protomolt.proto.descriptors.StabilityTier;
import ai.protomolt.proto.meta.DescriptorMetadata;
import ai.protomolt.proto.meta.FieldMeta;
import ai.protomolt.proto.meta.MessageMeta;
import ai.protomolt.proto.meta.MetadataProto;
import ai.protomolt.proto.meta.StabilityLevel;
import ai.protomolt.proto.meta.StabilityOption;
import ai.protomolt.proto.meta.StabilityProto;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.UnknownFieldSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ReleaseBaselineGateTest {

    @Test
    void javaApiSurfaceRejectsStableBreaksAndPermitsAdditionsAndExperimentalChanges() {
        ClassEntry baselineClass = new ClassEntry(
                "ai.protomolt.example.OrderClient",
                TypeKind.CLASS,
                Visibility.PUBLIC,
                false,
                false,
                "",
                List.of("java.io.Closeable"),
                StabilityTier.STABLE,
                List.of(new ConstructorEntry(List.of("java.lang.String"), Visibility.PUBLIC, StabilityTier.STABLE)),
                List.of(
                        new MethodEntry(
                                "submitOrder",
                                List.of("java.lang.String"),
                                "int",
                                Visibility.PUBLIC,
                                false,
                                false,
                                false,
                                false,
                                StabilityTier.STABLE),
                        new MethodEntry(
                                "experimentalPreview",
                                List.of("int"),
                                "java.lang.String",
                                Visibility.PUBLIC,
                                false,
                                false,
                                false,
                                false,
                                StabilityTier.EXPERIMENTAL)),
                List.of(new FieldEntry(
                        "DEFAULT_TIMEOUT_MS",
                        "long",
                        Visibility.PUBLIC,
                        true,
                        true,
                        StabilityTier.STABLE)));

        JavaApiSnapshot baseline = new JavaApiSnapshot(List.of(baselineClass));

        // 1. Compatible evolution: add a new stable method, add a new class, and remove an EXPERIMENTAL method.
        ClassEntry compatibleUpdatedClass = new ClassEntry(
                "ai.protomolt.example.OrderClient",
                TypeKind.CLASS,
                Visibility.PUBLIC,
                false,
                false,
                "",
                List.of("java.io.Closeable"),
                StabilityTier.STABLE,
                List.of(
                        new ConstructorEntry(List.of("java.lang.String"), Visibility.PUBLIC, StabilityTier.STABLE),
                        new ConstructorEntry(List.of("java.lang.String", "int"), Visibility.PUBLIC, StabilityTier.STABLE)),
                List.of(
                        new MethodEntry(
                                "submitOrder",
                                List.of("java.lang.String"),
                                "int",
                                Visibility.PUBLIC,
                                false,
                                false,
                                false,
                                false,
                                StabilityTier.DEPRECATED),
                        new MethodEntry(
                                "submitOrderV2",
                                List.of("java.lang.String"),
                                "long",
                                Visibility.PUBLIC,
                                false,
                                false,
                                false,
                                false,
                                StabilityTier.STABLE)),
                List.of(new FieldEntry(
                        "DEFAULT_TIMEOUT_MS",
                        "long",
                        Visibility.PUBLIC,
                        true,
                        true,
                        StabilityTier.STABLE)));

        ClassEntry addedClass = new ClassEntry(
                "ai.protomolt.example.OrderReceipt",
                TypeKind.RECORD,
                Visibility.PUBLIC,
                true,
                false,
                "java.lang.Record",
                List.of(),
                StabilityTier.STABLE,
                List.of(),
                List.of(),
                List.of());

        List<SurfaceChange> compatibleChanges = JavaApiCompatibilityChecker.compare(
                baseline,
                new JavaApiSnapshot(List.of(compatibleUpdatedClass, addedClass)));
        assertThat(compatibleChanges.stream().filter(SurfaceChange::violatesGate)).isEmpty();
        assertThat(compatibleChanges)
                .anyMatch(c -> c.ruleId().equals(JavaApiCompatibilityChecker.JAVA_METHOD_REMOVED)
                        && c.stability() == StabilityTier.EXPERIMENTAL
                        && !c.violatesGate());

        // 2. Breaking changes on STABLE declarations: return type change, visibility reduction, field type change.
        ClassEntry brokenClass = new ClassEntry(
                "ai.protomolt.example.OrderClient",
                TypeKind.CLASS,
                Visibility.PUBLIC,
                true, // added final on extensible class
                false,
                "",
                List.of(), // removed Closeable interface
                StabilityTier.STABLE,
                List.of(new ConstructorEntry(List.of("java.lang.String"), Visibility.PACKAGE_PRIVATE, StabilityTier.STABLE)),
                List.of(new MethodEntry(
                        "submitOrder",
                        List.of("java.lang.String"),
                        "long", // changed return type from int to long
                        Visibility.PROTECTED, // reduced visibility from PUBLIC to PROTECTED
                        false,
                        false,
                        false,
                        false,
                        StabilityTier.STABLE)),
                List.of(new FieldEntry(
                        "DEFAULT_TIMEOUT_MS",
                        "int", // changed field type from long to int
                        Visibility.PUBLIC,
                        true,
                        true,
                        StabilityTier.STABLE)));

        List<SurfaceChange> breakingChanges = JavaApiCompatibilityChecker.compare(
                baseline,
                new JavaApiSnapshot(List.of(brokenClass)));
        List<String> violatingRules = breakingChanges.stream()
                .filter(SurfaceChange::violatesGate)
                .map(SurfaceChange::ruleId)
                .toList();
        assertThat(violatingRules).contains(
                JavaApiCompatibilityChecker.JAVA_TYPE_FINAL_ADDED,
                JavaApiCompatibilityChecker.JAVA_SUPERTYPE_REMOVED,
                JavaApiCompatibilityChecker.JAVA_CONSTRUCTOR_VISIBILITY_REDUCED,
                JavaApiCompatibilityChecker.JAVA_METHOD_RETURN_TYPE_CHANGED,
                JavaApiCompatibilityChecker.JAVA_METHOD_VISIBILITY_REDUCED,
                JavaApiCompatibilityChecker.JAVA_FIELD_TYPE_CHANGED);
    }

    @Test
    void protobufSurfaceEnforcesStableRulesAndAllowsExperimentalAndAdditiveEvolution() {
        DescriptorProtos.FieldOptions expOptions = DescriptorProtos.FieldOptions.newBuilder()
                .setExtension(
                        StabilityProto.fieldStability,
                        StabilityOption.newBuilder()
                                .setLevel(StabilityLevel.STABILITY_LEVEL_EXPERIMENTAL)
                                .build())
                .build();

        FileDescriptorSet baselineSet = singleFileSet(
                "example/v1/order.proto",
                "example.v1",
                DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("Order")
                        .addField(field("order_id", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                        .addField(field("amount_cents", 2, DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64))
                        .addField(field("experimental_tag", 3, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
                                .toBuilder()
                                .setOptions(expOptions)
                                .build())
                        .build());

        // Compatible: add optional field 4, and change type of EXPERIMENTAL field 3.
        FileDescriptorSet compatibleSet = singleFileSet(
                "example/v1/order.proto",
                "example.v1",
                DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("Order")
                        .addField(field("order_id", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                        .addField(field("amount_cents", 2, DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64))
                        .addField(field("experimental_tag", 3, DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32)
                                .toBuilder()
                                .setOptions(expOptions)
                                .build())
                        .addField(field("currency", 4, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                        .build());

        List<SurfaceChange> compatibleDiff = ProtobufSurfaceCompatibilityChecker.compare(
                ProtobufSurfaceSnapshot.of(baselineSet),
                ProtobufSurfaceSnapshot.of(compatibleSet));
        assertThat(compatibleDiff.stream().filter(SurfaceChange::violatesGate)).isEmpty();

        // Breaking: change STABLE field 2 type from INT64 to STRING, and downgrade message stability.
        FileDescriptorSet breakingSet = singleFileSet(
                "example/v1/order.proto",
                "example.v1",
                DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("Order")
                        .setOptions(DescriptorProtos.MessageOptions.newBuilder()
                                .setExtension(
                                        StabilityProto.messageStability,
                                        StabilityOption.newBuilder()
                                                .setLevel(StabilityLevel.STABILITY_LEVEL_EXPERIMENTAL)
                                                .build())
                                .build())
                        .addField(field("order_id", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                        .addField(field("amount_cents", 2, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                        .build());

        List<SurfaceChange> breakingDiff = ProtobufSurfaceCompatibilityChecker.compare(
                ProtobufSurfaceSnapshot.of(baselineSet),
                ProtobufSurfaceSnapshot.of(breakingSet));
        assertThat(breakingDiff.stream().filter(SurfaceChange::violatesGate).map(SurfaceChange::ruleId))
                .contains(
                        ChangeRules.FIELD_TYPE_CHANGED,
                        ProtobufSurfaceCompatibilityChecker.PROTO_STABILITY_DOWNGRADED);
    }

    @Test
    void storedFormatSurfaceDetectsPinMismatchUnreservedFieldRemovalAndAllowsAdditiveEvolution() {
        FileDescriptorSet v1Closure = singleFileSet(
                "repo/v1/manifest.proto",
                "repo.v1",
                DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("Manifest")
                        .addField(field("manifest_id", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                        .addField(field("revision", 2, DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64))
                        .build());

        FileDescriptorSet v2CompatibleClosure = singleFileSet(
                "repo/v1/manifest.proto",
                "repo.v1",
                DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("Manifest")
                        .addField(field("manifest_id", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                        .addField(field("revision", 2, DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64))
                        .addField(field("checksum", 3, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                        .build());

        FileDescriptorSet v2UnreservedRemovalClosure = singleFileSet(
                "repo/v1/manifest.proto",
                "repo.v1",
                DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("Manifest")
                        .addField(field("manifest_id", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                        .build());

        StoredFormatSnapshot baseline = new StoredFormatSnapshot(List.of(
                new StoredFormatEntry(
                        "repo.manifest.v1",
                        "repo.v1.Manifest",
                        1,
                        StabilityTier.STABLE,
                        CompatibilityMode.BACKWARD,
                        "",
                        "",
                        v1Closure),
                new StoredFormatEntry(
                        "repo.pinned-container.v1",
                        "repo.v1.Manifest",
                        1,
                        StabilityTier.STABLE,
                        CompatibilityMode.FULL,
                        "fp-1",
                        "pinned-sha-1",
                        v1Closure)));

        // Compatible evolution on unpinned stored format (backward mode + optional field added)
        StoredFormatSnapshot compatibleCurrent = new StoredFormatSnapshot(List.of(
                new StoredFormatEntry(
                        "repo.manifest.v1",
                        "repo.v1.Manifest",
                        2,
                        StabilityTier.STABLE,
                        CompatibilityMode.BACKWARD,
                        "",
                        "",
                        v2CompatibleClosure),
                new StoredFormatEntry(
                        "repo.pinned-container.v1",
                        "repo.v1.Manifest",
                        1,
                        StabilityTier.STABLE,
                        CompatibilityMode.FULL,
                        "fp-1",
                        "pinned-sha-1",
                        v1Closure)));

        assertThat(StoredFormatCompatibilityChecker.compare(baseline, compatibleCurrent)
                .stream()
                .filter(SurfaceChange::violatesGate))
                .isEmpty();

        // Breaking: unreserved field removal on repo.manifest.v1 and pin change on repo.pinned-container.v1
        StoredFormatSnapshot breakingCurrent = new StoredFormatSnapshot(List.of(
                new StoredFormatEntry(
                        "repo.manifest.v1",
                        "repo.v1.Manifest",
                        1,
                        StabilityTier.STABLE,
                        CompatibilityMode.BACKWARD,
                        "",
                        "",
                        v2UnreservedRemovalClosure),
                new StoredFormatEntry(
                        "repo.pinned-container.v1",
                        "repo.v1.Manifest",
                        1,
                        StabilityTier.STABLE,
                        CompatibilityMode.FULL,
                        "fp-2",
                        "pinned-sha-2",
                        v2CompatibleClosure)));

        List<String> brokenRules = StoredFormatCompatibilityChecker.compare(baseline, breakingCurrent)
                .stream()
                .filter(SurfaceChange::violatesGate)
                .map(SurfaceChange::ruleId)
                .toList();
        assertThat(brokenRules).contains(
                ChangeRules.FIELD_REMOVED_NOT_RESERVED,
                StoredFormatCompatibilityChecker.STORED_FORMAT_CLOSURE_PIN_CHANGED);
    }

    @Test
    void restRouteSurfaceDetectsRouteVerbAndParameterBreaksAndAllowsNewRoutes() {
        RestRouteEntry getSubjectRoute = new RestRouteEntry(
                "registry.subject.versions",
                "/subjects/{subject}/versions",
                List.of("GET", "POST"),
                List.of("subject"),
                List.of(),
                "application/json",
                "application/json",
                "RegisterSchemaRequest",
                "SchemaVersionList",
                List.of(200, 404),
                false,
                StabilityTier.STABLE);

        RestRouteSnapshot baseline = new RestRouteSnapshot(List.of(getSubjectRoute));

        RestRouteEntry addedRoute = new RestRouteEntry(
                "registry.health",
                "/v1/health",
                List.of("GET"),
                List.of(),
                List.of(),
                "",
                "application/json",
                "",
                "HealthResponse",
                List.of(200),
                false,
                StabilityTier.STABLE);

        // Compatible: add new route
        List<SurfaceChange> compatible = RestRouteCompatibilityChecker.compare(
                baseline,
                new RestRouteSnapshot(List.of(getSubjectRoute, addedRoute)));
        assertThat(compatible.stream().filter(SurfaceChange::violatesGate)).isEmpty();

        // Breaking: remove POST verb, change {subject} path param to {id}, add required query param, require auth
        RestRouteEntry mutatedRoute = new RestRouteEntry(
                "registry.subject.versions",
                "/subjects/{id}/versions",
                List.of("GET"),
                List.of("id"),
                List.of("tenant"),
                "application/json",
                "application/json",
                "RegisterSchemaRequestV2",
                "SchemaVersionList",
                List.of(404),
                true,
                StabilityTier.STABLE);

        List<String> violations = RestRouteCompatibilityChecker.compare(
                        baseline,
                        new RestRouteSnapshot(List.of(mutatedRoute)))
                .stream()
                .filter(SurfaceChange::violatesGate)
                .map(SurfaceChange::ruleId)
                .toList();
        assertThat(violations).contains(
                RestRouteCompatibilityChecker.REST_PATH_TEMPLATE_CHANGED,
                RestRouteCompatibilityChecker.REST_HTTP_METHOD_REMOVED,
                RestRouteCompatibilityChecker.REST_REQUIRED_QUERY_PARAM_ADDED,
                RestRouteCompatibilityChecker.REST_REQUEST_TYPE_CHANGED,
                RestRouteCompatibilityChecker.REST_SUCCESS_STATUS_REMOVED,
                RestRouteCompatibilityChecker.REST_AUTH_REQUIREMENT_ADDED);
    }

    @Test
    void actionEnvelopeSurfaceDetectsContractBreaksAndAllowsCompatibleAdditions() {
        ActionEnvelopeEntry validateAction = new ActionEnvelopeEntry(
                "validate-message",
                "schema:read",
                false,
                "ai.protomolt.ValidateRequest",
                "ai.protomolt.ValidateResponse",
                List.of("payload"),
                Map.of("payload", "TYPE_STRING", "strict", "TYPE_BOOL"),
                Map.of("valid", "TYPE_BOOL"),
                StabilityTier.STABLE);

        ActionEnvelopeSnapshot baseline = new ActionEnvelopeSnapshot(List.of(validateAction));

        // Compatible: add optional input field, add output field, add new action
        ActionEnvelopeEntry evolvedValidateAction = new ActionEnvelopeEntry(
                "validate-message",
                "schema:read",
                false,
                "ai.protomolt.ValidateRequest",
                "ai.protomolt.ValidateResponse",
                List.of("payload"),
                Map.of("payload", "TYPE_STRING", "strict", "TYPE_BOOL", "dialect", "TYPE_STRING"),
                Map.of("valid", "TYPE_BOOL", "violation_count", "TYPE_INT32"),
                StabilityTier.STABLE);

        ActionEnvelopeEntry newAction = new ActionEnvelopeEntry(
                "diff-schemas",
                "schema:read",
                false,
                "ai.protomolt.DiffRequest",
                "ai.protomolt.DiffResponse",
                List.of(),
                Map.of("old_schema", "TYPE_STRING"),
                Map.of("compatible", "TYPE_BOOL"),
                StabilityTier.STABLE);

        assertThat(ActionEnvelopeCompatibilityChecker.compare(
                        baseline,
                        new ActionEnvelopeSnapshot(List.of(evolvedValidateAction, newAction)))
                .stream()
                .filter(SurfaceChange::violatesGate))
                .isEmpty();

        // Breaking: change requiredScope, add required input field, remove input field, change output field type
        ActionEnvelopeEntry brokenValidateAction = new ActionEnvelopeEntry(
                "validate-message",
                "schema:admin",
                true,
                "ai.protomolt.ValidateRequest",
                "ai.protomolt.ValidateResponse",
                List.of("payload", "strict"),
                Map.of("payload", "TYPE_STRING"),
                Map.of("valid", "TYPE_STRING"),
                StabilityTier.STABLE);

        List<String> violations = ActionEnvelopeCompatibilityChecker.compare(
                        baseline,
                        new ActionEnvelopeSnapshot(List.of(brokenValidateAction)))
                .stream()
                .filter(SurfaceChange::violatesGate)
                .map(SurfaceChange::ruleId)
                .toList();
        assertThat(violations).contains(
                ActionEnvelopeCompatibilityChecker.ACTION_SCOPE_CHANGED,
                ActionEnvelopeCompatibilityChecker.ACTION_STREAMING_CHANGED,
                ActionEnvelopeCompatibilityChecker.ACTION_REQUIRED_INPUT_ADDED,
                ActionEnvelopeCompatibilityChecker.ACTION_INPUT_FIELD_REMOVED,
                ActionEnvelopeCompatibilityChecker.ACTION_OUTPUT_FIELD_TYPE_CHANGED);
    }

    @Test
    void releaseBaselineManifestWaiversAndEndToEndAdvancementGateWork(@TempDir Path tempDir) throws Exception {
        // 1. Verify UNRELEASED_INITIAL_SNAPSHOT refuses to claim an external release tag
        assertThatThrownBy(() -> new ReleaseBaselineManifest(
                ReleaseBaselineManifest.SCHEMA_VERSION,
                ReleaseBaselineStatus.UNRELEASED_INITIAL_SNAPSHOT,
                "0.1.0-SNAPSHOT",
                "v0.1.0",
                "Invalid claim",
                List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("do not claim a released stability guarantee");

        // 2. Verify RELEASED_BASELINE requires a non-blank releaseTag
        assertThatThrownBy(() -> ReleaseBaselineManifest.released("1.0.0", "", "Missing tag", List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires a non-blank releaseTag");

        // 3. Verify wildcard waivers are rejected
        assertThatThrownBy(() -> new CompatibilityWaiver(
                "WAIVER-WILDCARD",
                CompatibilitySurface.ACTION_ENVELOPE,
                "*",
                "validate-message",
                "PR#827",
                "Wildcard not allowed"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("wildcard");

        // 4. End-to-end save, load, breaking rejection, waiver approval, and baseline advancement
        ActionEnvelopeEntry v1Action = new ActionEnvelopeEntry(
                "inspect-descriptor",
                "schema:read",
                false,
                "ai.protomolt.InspectRequest",
                "ai.protomolt.InspectResponse",
                List.of("type_name"),
                Map.of("type_name", "TYPE_STRING"),
                Map.of("fqn", "TYPE_STRING"),
                StabilityTier.STABLE);

        ReleaseBaselineBundle initialBundle = new ReleaseBaselineBundle(
                ReleaseBaselineManifest.unreleasedInitial("0.1.0-SNAPSHOT", List.of()),
                new JavaApiSnapshot(List.of()),
                ProtobufSurfaceSnapshot.of(FileDescriptorSet.getDefaultInstance()),
                new StoredFormatSnapshot(List.of()),
                new RestRouteSnapshot(List.of()),
                new ActionEnvelopeSnapshot(List.of(v1Action)));

        ReleaseBaselineReport initReport = ReleaseBaselineGate.advanceBaseline(tempDir, initialBundle, false);
        assertThat(initReport.isCompatible()).isTrue();
        assertThat(initReport.claimsExternalReleaseGuarantee()).isFalse();

        ReleaseBaselineBundle loaded = ReleaseBaselineGate.load(tempDir);
        assertThat(loaded).isEqualTo(initialBundle);

        // Candidate removes 'inspect-descriptor' without a waiver -> advanceBaseline fails closed
        ReleaseBaselineBundle unwaivedBreakBundle = new ReleaseBaselineBundle(
                ReleaseBaselineManifest.unreleasedInitial("0.1.0-SNAPSHOT", List.of()),
                new JavaApiSnapshot(List.of()),
                ProtobufSurfaceSnapshot.of(FileDescriptorSet.getDefaultInstance()),
                new StoredFormatSnapshot(List.of()),
                new RestRouteSnapshot(List.of()),
                new ActionEnvelopeSnapshot(List.of()));

        assertThatThrownBy(() -> ReleaseBaselineGate.advanceBaseline(tempDir, unwaivedBreakBundle, false))
                .isInstanceOf(IncompatibleSurfaceException.class)
                .hasMessageContaining("ACTION_REMOVED")
                .hasMessageContaining("inspect-descriptor");

        // Candidate declares an explicit reviewed waiver for removing 'inspect-descriptor' -> succeeds
        CompatibilityWaiver removalWaiver = new CompatibilityWaiver(
                "WAIVER-827-ACTION-01",
                CompatibilitySurface.ACTION_ENVELOPE,
                ActionEnvelopeCompatibilityChecker.ACTION_REMOVED,
                "inspect-descriptor",
                "PR#827",
                "Retired duplicate inspect-descriptor verb in favor of describe-type");

        ReleaseBaselineBundle waivedBreakBundle = new ReleaseBaselineBundle(
                ReleaseBaselineManifest.unreleasedInitial("0.1.0-SNAPSHOT", List.of(removalWaiver)),
                new JavaApiSnapshot(List.of()),
                ProtobufSurfaceSnapshot.of(FileDescriptorSet.getDefaultInstance()),
                new StoredFormatSnapshot(List.of()),
                new RestRouteSnapshot(List.of()),
                new ActionEnvelopeSnapshot(List.of()));

        ReleaseBaselineReport waivedReport = ReleaseBaselineGate.advanceBaseline(tempDir, waivedBreakBundle, false);
        assertThat(waivedReport.isCompatible()).isTrue();
        assertThat(waivedReport.waivedChanges()).hasSize(1);
        assertThat(waivedReport.matchedWaivers()).containsExactly(removalWaiver);

        // Spent waiver is retired from manifest.json on advancement, so re-adding and removing fails closed
        ReleaseBaselineBundle postAdvanceLoaded = ReleaseBaselineGate.load(tempDir);
        assertThat(postAdvanceLoaded.manifest().waivers()).isEmpty();
        assertThat(ReleaseBaselineGate.evaluate(postAdvanceLoaded, unwaivedBreakBundle).isCompatible()).isTrue();

        // Unused waiver fails closed
        CompatibilityWaiver unusedWaiver = new CompatibilityWaiver(
                "WAIVER-UNUSED-01",
                CompatibilitySurface.ACTION_ENVELOPE,
                ActionEnvelopeCompatibilityChecker.ACTION_REMOVED,
                "non-existent-action",
                "PR#827",
                "Unused waiver must fail closed");
        ReleaseBaselineBundle bundleWithUnusedWaiver = new ReleaseBaselineBundle(
                ReleaseBaselineManifest.unreleasedInitial("0.1.0-SNAPSHOT", List.of(unusedWaiver)),
                new JavaApiSnapshot(List.of()),
                ProtobufSurfaceSnapshot.of(FileDescriptorSet.getDefaultInstance()),
                new StoredFormatSnapshot(List.of()),
                new RestRouteSnapshot(List.of()),
                new ActionEnvelopeSnapshot(List.of()));
        ReleaseBaselineReport unusedReport = ReleaseBaselineGate.evaluate(postAdvanceLoaded, bundleWithUnusedWaiver);
        assertThat(unusedReport.isCompatible()).isFalse();
        assertThat(unusedReport.unusedWaivers()).containsExactly(unusedWaiver);
        assertThatThrownBy(unusedReport::throwIfIncompatible)
                .isInstanceOf(IncompatibleSurfaceException.class)
                .hasMessageContaining("WAIVER-UNUSED-01")
                .satisfies(ex -> assertThat(((IncompatibleSurfaceException) ex).unusedWaivers())
                        .containsExactly(unusedWaiver));
    }

    @ApiStability(StabilityTier.STABLE)
    public static final class SampleOuterContract {
        public static final class Builder {
            public SampleOuterContract build() {
                return new SampleOuterContract();
            }
        }

        protected record NestedResult(String id) {
        }

        static final class PackagePrivateHelper {
        }

        private static final class PrivateHelper {
        }
    }

    @Test
    void javaApiSnapshotCapturesExportedNestedTypesRecursively() {
        JavaApiSnapshot snapshot = JavaApiSnapshot.capture(SampleOuterContract.class);
        Map<String, ClassEntry> byName = snapshot.classesByName();

        assertThat(byName).containsKey(SampleOuterContract.class.getName());
        assertThat(byName).containsKey(SampleOuterContract.Builder.class.getName());
        assertThat(byName).containsKey(SampleOuterContract.Builder.class.getCanonicalName());
        assertThat(byName).containsKey(SampleOuterContract.NestedResult.class.getName());
        assertThat(byName).doesNotContainKey(SampleOuterContract.PackagePrivateHelper.class.getName());
        assertThat(byName).doesNotContainKey(SampleOuterContract.PrivateHelper.class.getName());
    }

    @Test
    void loadRejectsTamperedProtobufStabilityJson(@TempDir Path tempDir) throws Exception {
        FileDescriptorSet descriptorSet = singleFileSet(
                "example/v1/order.proto",
                "example.v1",
                DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("Order")
                        .addField(field("order_id", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                        .build());
        ReleaseBaselineBundle bundle = new ReleaseBaselineBundle(
                ReleaseBaselineManifest.unreleasedInitial("0.1.0-SNAPSHOT", List.of()),
                new JavaApiSnapshot(List.of()),
                ProtobufSurfaceSnapshot.of(descriptorSet),
                new StoredFormatSnapshot(List.of()),
                new RestRouteSnapshot(List.of()),
                new ActionEnvelopeSnapshot(List.of()));
        ReleaseBaselineGate.save(tempDir, bundle);

        Path stabilityPath = tempDir.resolve(ReleaseBaselineGate.PROTOBUF_STABILITY_FILE);
        String original = Files.readString(stabilityPath);
        Files.writeString(stabilityPath, original.replace("\"STABLE\"", "\"EVOLVING\""));

        assertThatThrownBy(() -> ReleaseBaselineGate.load(tempDir))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(ReleaseBaselineGate.PROTOBUF_STABILITY_FILE);
    }

    @Test
    void malformedStabilityOptionFailsClosedInsteadOfFallingBack() {
        UnknownFieldSet malformed = UnknownFieldSet.newBuilder()
                .addField(StabilityProto.fileStability.getNumber(),
                        UnknownFieldSet.Field.newBuilder()
                                .addLengthDelimited(ByteString.copyFrom(new byte[] {0x01}))
                                .build())
                .build();
        DescriptorProtos.FileDescriptorProto file = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setSyntax("proto3")
                .setName("broken/stability.proto")
                .setPackage("broken")
                .setOptions(DescriptorProtos.FileOptions.newBuilder().setUnknownFields(malformed))
                .build();

        assertThatThrownBy(() -> ProtobufSurfaceSnapshot.of(
                FileDescriptorSet.newBuilder().addFile(file).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(StabilityProto.fileStability.getDescriptor().getFullName());

        // Unrecognized StabilityLevel enum wire number (e.g. 99) must also fail closed
        DescriptorProtos.FileDescriptorProto unrecognizedLevelFile = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setSyntax("proto3")
                .setName("broken/unrecognized_level.proto")
                .setPackage("broken")
                .setOptions(DescriptorProtos.FileOptions.newBuilder()
                        .setExtension(
                                StabilityProto.fileStability,
                                StabilityOption.newBuilder().setLevelValue(99).build())
                        .build())
                .build();
        assertThatThrownBy(() -> ProtobufSurfaceSnapshot.of(
                FileDescriptorSet.newBuilder().addFile(unrecognizedLevelFile).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("99");

        // Unknown labels["stability"] value on message or field metadata must fail closed
        DescriptorProtos.FileDescriptorProto bogusMessageLabelFile = singleFileSet(
                "broken/bogus_msg_label.proto",
                "broken",
                DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("BrokenMsg")
                        .setOptions(DescriptorProtos.MessageOptions.newBuilder()
                                .setExtension(
                                        MetadataProto.message,
                                        MessageMeta.newBuilder()
                                                .putLabels(DescriptorMetadata.STABILITY_LABEL_KEY, "bogus")
                                                .build())
                                .build())
                        .build())
                .getFile(0);
        assertThatThrownBy(() -> ProtobufSurfaceSnapshot.of(
                FileDescriptorSet.newBuilder().addFile(bogusMessageLabelFile).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("bogus");

        DescriptorProtos.FileDescriptorProto bogusFieldLabelFile = singleFileSet(
                "broken/bogus_field_label.proto",
                "broken",
                DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("BrokenFieldMsg")
                        .addField(field("id", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
                                .toBuilder()
                                .setOptions(DescriptorProtos.FieldOptions.newBuilder()
                                        .setExtension(
                                                MetadataProto.field,
                                                FieldMeta.newBuilder()
                                                        .putLabels(DescriptorMetadata.STABILITY_LABEL_KEY, "typo-tier")
                                                        .build())
                                        .build())
                                .build())
                        .build())
                .getFile(0);
        assertThatThrownBy(() -> ProtobufSurfaceSnapshot.of(
                FileDescriptorSet.newBuilder().addFile(bogusFieldLabelFile).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("typo-tier");

        // Legal forward-compatible unknown fields inside StabilityOption must NOT be rejected
        StabilityOption forwardOption = StabilityOption.newBuilder()
                .setLevel(StabilityLevel.STABILITY_LEVEL_EVOLVING)
                .setUnknownFields(UnknownFieldSet.newBuilder()
                        .addField(
                                77,
                                UnknownFieldSet.Field.newBuilder()
                                        .addLengthDelimited(ByteString.copyFromUtf8("future"))
                                        .build())
                        .build())
                .build();
        DescriptorProtos.FileOptions unparsedForwardOptions;
        try {
            unparsedForwardOptions = DescriptorProtos.FileOptions.parseFrom(
                    DescriptorProtos.FileOptions.newBuilder()
                            .setExtension(StabilityProto.fileStability, forwardOption)
                            .build()
                            .toByteString());
        } catch (Exception e) {
            throw new AssertionError(e);
        }
        DescriptorProtos.FileDescriptorProto forwardFile = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setSyntax("proto3")
                .setName("ok/forward.proto")
                .setPackage("ok.v1")
                .setOptions(unparsedForwardOptions)
                .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("Ping").build())
                .build();
        ProtobufSurfaceSnapshot forwardSnapshot = ProtobufSurfaceSnapshot.of(
                FileDescriptorSet.newBuilder().addFile(forwardFile).build());
        assertThat(forwardSnapshot.stabilityForPath("ok.v1.Ping")).isEqualTo(StabilityTier.EVOLVING);
    }

    private static DescriptorProtos.FieldDescriptorProto field(
            String name,
            int number,
            DescriptorProtos.FieldDescriptorProto.Type type) {
        return DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setName(name)
                .setNumber(number)
                .setType(type)
                .build();
    }

    private static FileDescriptorSet singleFileSet(
            String fileName,
            String pkg,
            DescriptorProtos.DescriptorProto message) {
        return FileDescriptorSet.newBuilder()
                .addFile(DescriptorProtos.FileDescriptorProto.newBuilder()
                        .setSyntax("proto3")
                        .setName(fileName)
                        .setPackage(pkg)
                        .addMessageType(message)
                        .build())
                .build();
    }
}
