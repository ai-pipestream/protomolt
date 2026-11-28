package ai.protomolt.proto.projection;

import ai.protomolt.proto.descriptors.ClosedDescriptorSet;
import ai.protomolt.proto.descriptors.MessageFingerprints;
import ai.protomolt.proto.projection.fixture.ProjectionExpectedError;
import ai.protomolt.proto.projection.fixture.ProjectionExpectedSuccess;
import ai.protomolt.proto.projection.fixture.ProjectionFixtureBundle;
import ai.protomolt.proto.projection.fixture.ProjectionFixtureCase;
import ai.protomolt.proto.projection.fixture.ProjectionPhase;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.util.FieldMaskUtil;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Executes the checked-in language-neutral binary conformance bundle
 * ({@code fixtures/map-projection-conformance.binpb}) strictly from raw
 * {@code FileDescriptorSet} bytes and {@link DynamicMessage} payloads (#981, #988).
 */
class ProjectionLanguageNeutralFixturesTest {

    private static final String FIXTURE_RESOURCE = "/fixtures/map-projection-conformance.binpb";
    private static final ClosedDescriptorSet.Limits DESCRIPTOR_LIMITS =
            new ClosedDescriptorSet.Limits(2 * 1024 * 1024, 64, 256, 32);

    @Test
    void checkedInFixtureBundleMatchesDeterministicCatalogAndExecutesFromRawDescriptors()
            throws Exception {
        byte[] expectedBundleBytes = ProjectionFixtureCatalog.buildDeterministicBundleBytes();
        String writeTarget = System.getenv("PROTOMOLT_WRITE_PROJECTION_FIXTURES");
        if (writeTarget != null && !writeTarget.isBlank()) {
            Path path = Path.of(writeTarget);
            Files.createDirectories(path.getParent());
            Files.write(path, expectedBundleBytes);
        }

        byte[] checkedInBytes;
        try (InputStream in = getClass().getResourceAsStream(FIXTURE_RESOURCE)) {
            assertThat(in)
                    .as("Checked-in conformance fixture %s must exist", FIXTURE_RESOURCE)
                    .isNotNull();
            checkedInBytes = in.readAllBytes();
        }
        assertThat(checkedInBytes).isEqualTo(expectedBundleBytes);

        ProjectionFixtureBundle bundle = ProjectionFixtureBundle.parseFrom(
                checkedInBytes, ExtensionRegistry.getEmptyRegistry());
        assertThat(bundle.getCasesCount()).isGreaterThanOrEqualTo(16);

        // Link descriptors strictly from the raw FileDescriptorSet bytes in the fixture,
        // without pre-registering ProjectionProto extensions or referencing generated classes.
        List<FileDescriptor> linkedFiles = ClosedDescriptorSet.load(
                bundle.getDescriptorSetBytes(), DESCRIPTOR_LIMITS);
        Map<String, Descriptor> typesByName = indexDescriptors(linkedFiles);

        for (ProjectionFixtureCase testCase : bundle.getCasesList()) {
            runFixtureCase(testCase, typesByName);
        }
    }

    private static void runFixtureCase(
            ProjectionFixtureCase testCase, Map<String, Descriptor> typesByName) throws Exception {
        Descriptor targetType = requireType(typesByName, testCase.getTargetType());
        Descriptor sourceType = requireType(typesByName, testCase.getSourceType());
        List<Descriptor> resolvable = testCase.getResolvableSourceTypesList().stream()
                .map(name -> requireType(typesByName, name))
                .toList();
        SourceResolver resolver = SourceResolver.of(resolvable);

        if (testCase.getOutcomeCase() == ProjectionFixtureCase.OutcomeCase.ERROR
                && testCase.getError().getPhase() == ProjectionPhase.PROJECTION_PHASE_COMPILE) {
            ProjectionExpectedError err = testCase.getError();
            assertThatThrownBy(() -> MessageProjection.forTarget(targetType, resolver))
                    .as("case %s (%s)", testCase.getName(), testCase.getDescription())
                    .isInstanceOf(ProjectionException.class)
                    .hasMessageContaining(err.getMessageContains());
            return;
        }

        MessageProjection projection = MessageProjection.forTarget(targetType, resolver)
                .orElseThrow(() -> new AssertionError(
                        "Expected compiled projection for case " + testCase.getName()));
        DynamicMessage dynamicSource = DynamicMessage.parseFrom(
                sourceType, testCase.getSourceBytes(), ExtensionRegistry.getEmptyRegistry());

        if (testCase.getOutcomeCase() == ProjectionFixtureCase.OutcomeCase.ERROR) {
            ProjectionExpectedError err = testCase.getError();
            assertThatThrownBy(() -> projection.project(dynamicSource))
                    .as("case %s (%s)", testCase.getName(), testCase.getDescription())
                    .isInstanceOf(ProjectionException.class)
                    .hasMessageContaining(err.getMessageContains());
            return;
        }

        ProjectionExpectedSuccess expected = testCase.getSuccess();
        DynamicMessage projected = projection.project(dynamicSource);
        byte[] actualDeterministicBytes = MessageFingerprints.deterministicBytes(projected);
        assertThat(actualDeterministicBytes)
                .as("deterministic bytes for case %s", testCase.getName())
                .isEqualTo(expected.getDeterministicTargetBytes().toByteArray());

        assertThat(projection.targetMask())
                .as("targetMask for case %s", testCase.getName())
                .isEqualTo(expected.getTargetMask());
        assertThat(FieldMaskUtil.isValid(targetType, projection.targetMask()))
                .as("targetMask validity for case %s", testCase.getName())
                .isTrue();

        MessageProjection.SourceMask sourceMask = projection.sourceMask(sourceType);
        assertThat(sourceMask.fieldMask())
                .as("sourceMask for case %s", testCase.getName())
                .isEqualTo(expected.getSourceMask());
        assertThat(sourceMask.complete())
                .as("sourceMask.complete for case %s", testCase.getName())
                .isEqualTo(expected.getSourceMaskComplete());
        assertThat(FieldMaskUtil.isValid(sourceType, sourceMask.fieldMask()))
                .as("sourceMask validity for case %s", testCase.getName())
                .isTrue();

        DynamicMessage wireReloaded = DynamicMessage.parseFrom(
                targetType, actualDeterministicBytes, ExtensionRegistry.getEmptyRegistry());
        for (String fieldName : expected.getPresentFieldsList()) {
            FieldDescriptor fd = targetType.findFieldByName(fieldName);
            assertThat(fd).as("field %s on %s", fieldName, targetType.getFullName()).isNotNull();
            if (fd.isRepeated()) {
                assertThat(wireReloaded.getRepeatedFieldCount(fd))
                        .as("case %s expected non-empty field %s", testCase.getName(), fieldName)
                        .isGreaterThan(0);
            } else {
                assertThat(wireReloaded.hasField(fd) || !wireReloaded.getField(fd).equals(fd.getDefaultValue()))
                        .as("case %s expected present field %s", testCase.getName(), fieldName)
                        .isTrue();
            }
        }
        for (String fieldName : expected.getAbsentFieldsList()) {
            FieldDescriptor fd = targetType.findFieldByName(fieldName);
            assertThat(fd).as("field %s on %s", fieldName, targetType.getFullName()).isNotNull();
            if (fd.isRepeated()) {
                assertThat(wireReloaded.getRepeatedFieldCount(fd))
                        .as("case %s expected empty field %s", testCase.getName(), fieldName)
                        .isZero();
            } else {
                assertThat(wireReloaded.hasField(fd))
                        .as("case %s expected absent field %s", testCase.getName(), fieldName)
                        .isFalse();
            }
        }
    }

    private static Descriptor requireType(Map<String, Descriptor> types, String fullName) {
        Descriptor d = types.get(fullName);
        assertThat(d).as("Descriptor %s in fixture closure", fullName).isNotNull();
        return d;
    }

    private static Map<String, Descriptor> indexDescriptors(List<FileDescriptor> files) {
        Map<String, Descriptor> byName = new LinkedHashMap<>();
        for (FileDescriptor file : files) {
            for (Descriptor msg : file.getMessageTypes()) {
                indexMessage(msg, byName);
            }
        }
        return byName;
    }

    private static void indexMessage(Descriptor msg, Map<String, Descriptor> out) {
        out.put(msg.getFullName(), msg);
        for (Descriptor nested : msg.getNestedTypes()) {
            indexMessage(nested, out);
        }
    }
}
