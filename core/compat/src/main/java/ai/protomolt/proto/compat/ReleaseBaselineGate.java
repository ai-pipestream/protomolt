package ai.protomolt.proto.compat;

import ai.protomolt.proto.compat.StoredFormatSnapshot.StoredFormatEntry;
import ai.protomolt.proto.descriptors.ApiStability;
import ai.protomolt.proto.descriptors.StabilityTier;
import ai.protomolt.proto.meta.DescriptorMetadata;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.ExtensionRegistry;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Release-baseline compatibility gate and snapshot serializer for all five
 * {@link CompatibilitySurface}s.
 *
 * <p>Evaluates a current {@link ReleaseBaselineBundle} against a recorded baseline bundle,
 * failing closed on any unwaived incompatible change to a {@link StabilityTier#STABLE} or
 * {@link StabilityTier#DEPRECATED} declaration while permitting compatible additions and
 * changes to {@link StabilityTier#EVOLVING}, {@link StabilityTier#EXPERIMENTAL}, or
 * {@link StabilityTier#INTERNAL} declarations.</p>
 */
@ApiStability(StabilityTier.STABLE)
public final class ReleaseBaselineGate {

    public static final String MANIFEST_FILE = "manifest.json";
    public static final String JAVA_API_FILE = "java-api.json";
    public static final String PROTOBUF_DESCRIPTORS_FILE = "protobuf-descriptors.binpb";
    public static final String PROTOBUF_STABILITY_FILE = "protobuf-stability.json";
    public static final String STORED_FORMATS_FILE = "stored-formats.json";
    public static final String REST_ROUTES_FILE = "rest-routes.json";
    public static final String ACTION_ENVELOPES_FILE = "action-envelopes.json";

    private static final ExtensionRegistry EXTENSIONS = ExtensionRegistry.newInstance();
    private static final ObjectMapper MAPPER = createMapper();

    static {
        DescriptorMetadata.registerExtensions(EXTENSIONS);
    }

    private ReleaseBaselineGate() {
    }

    /**
     * Evaluates {@code current} against {@code baseline} using the waivers declared in
     * both {@code baseline.manifest()} and {@code current.manifest()}.
     *
     * @param baseline recorded baseline bundle
     * @param current  current candidate bundle
     * @return the compatibility evaluation report
     */
    public static ReleaseBaselineReport evaluate(
            ReleaseBaselineBundle baseline,
            ReleaseBaselineBundle current) {
        return evaluate(baseline, current, List.of());
    }

    /**
     * Evaluates {@code current} against {@code baseline} with additional reviewed waivers.
     *
     * @param baseline          recorded baseline bundle
     * @param current           current candidate bundle
     * @param additionalWaivers extra reviewed waivers to apply alongside manifest waivers
     * @return the compatibility evaluation report
     */
    public static ReleaseBaselineReport evaluate(
            ReleaseBaselineBundle baseline,
            ReleaseBaselineBundle current,
            List<CompatibilityWaiver> additionalWaivers) {
        Objects.requireNonNull(baseline, "baseline");
        Objects.requireNonNull(current, "current");
        Objects.requireNonNull(additionalWaivers, "additionalWaivers");

        List<SurfaceChange> allChanges = new ArrayList<>();
        allChanges.addAll(JavaApiCompatibilityChecker.compare(baseline.javaApi(), current.javaApi()));
        allChanges.addAll(ProtobufSurfaceCompatibilityChecker.compare(baseline.protobuf(), current.protobuf()));
        allChanges.addAll(StoredFormatCompatibilityChecker.compare(
                baseline.storedFormats(), current.storedFormats()));
        allChanges.addAll(RestRouteCompatibilityChecker.compare(baseline.restRoutes(), current.restRoutes()));
        allChanges.addAll(ActionEnvelopeCompatibilityChecker.compare(
                baseline.actionEnvelopes(), current.actionEnvelopes()));

        Map<String, CompatibilityWaiver> waiversById = new LinkedHashMap<>();
        for (CompatibilityWaiver waiver : baseline.manifest().waivers()) {
            waiversById.put(waiver.id(), waiver);
        }
        for (CompatibilityWaiver waiver : current.manifest().waivers()) {
            waiversById.put(waiver.id(), waiver);
        }
        for (CompatibilityWaiver waiver : additionalWaivers) {
            waiversById.put(waiver.id(), waiver);
        }
        List<CompatibilityWaiver> effectiveWaivers = List.copyOf(waiversById.values());

        List<SurfaceChange> waivedChanges = new ArrayList<>();
        List<SurfaceChange> unwaivedViolations = new ArrayList<>();
        Set<CompatibilityWaiver> matchedWaivers = new LinkedHashSet<>();

        for (SurfaceChange change : allChanges) {
            if (!change.violatesGate()) {
                continue;
            }
            CompatibilityWaiver matching = findMatchingWaiver(change, effectiveWaivers);
            if (matching != null) {
                waivedChanges.add(change);
                matchedWaivers.add(matching);
            } else {
                unwaivedViolations.add(change);
            }
        }

        List<CompatibilityWaiver> unusedWaivers = new ArrayList<>();
        for (CompatibilityWaiver waiver : effectiveWaivers) {
            if (!matchedWaivers.contains(waiver)) {
                unusedWaivers.add(waiver);
            }
        }

        return new ReleaseBaselineReport(
                baseline.manifest(),
                allChanges,
                waivedChanges,
                unwaivedViolations,
                new ArrayList<>(matchedWaivers),
                unusedWaivers);
    }

    /**
     * Writes a {@link ReleaseBaselineBundle} deterministically to {@code baselineDir}.
     *
     * @param baselineDir target directory
     * @param bundle      bundle to persist
     * @throws IOException if writing any baseline file fails
     */
    public static void save(Path baselineDir, ReleaseBaselineBundle bundle) throws IOException {
        Objects.requireNonNull(baselineDir, "baselineDir");
        Objects.requireNonNull(bundle, "bundle");
        Files.createDirectories(baselineDir);

        writeJson(baselineDir.resolve(MANIFEST_FILE), bundle.manifest());
        writeJson(baselineDir.resolve(JAVA_API_FILE), bundle.javaApi());
        Files.write(
                baselineDir.resolve(PROTOBUF_DESCRIPTORS_FILE),
                bundle.protobuf().descriptorSet().toByteArray());
        writeJson(baselineDir.resolve(PROTOBUF_STABILITY_FILE), bundle.protobuf().elementStability());
        writeJson(
                baselineDir.resolve(STORED_FORMATS_FILE),
                StoredFormatsWire.fromSnapshot(bundle.storedFormats()));
        writeJson(baselineDir.resolve(REST_ROUTES_FILE), bundle.restRoutes());
        writeJson(baselineDir.resolve(ACTION_ENVELOPES_FILE), bundle.actionEnvelopes());
    }

    /**
     * Loads a {@link ReleaseBaselineBundle} from {@code baselineDir}.
     *
     * @param baselineDir directory containing the baseline files
     * @return loaded and validated baseline bundle
     * @throws IOException if any required baseline file is missing or malformed
     */
    public static ReleaseBaselineBundle load(Path baselineDir) throws IOException {
        Objects.requireNonNull(baselineDir, "baselineDir");
        ReleaseBaselineManifest manifest = MAPPER.readValue(
                baselineDir.resolve(MANIFEST_FILE).toFile(),
                ReleaseBaselineManifest.class);
        JavaApiSnapshot javaApi = MAPPER.readValue(
                baselineDir.resolve(JAVA_API_FILE).toFile(),
                JavaApiSnapshot.class);
        byte[] protoBytes = Files.readAllBytes(baselineDir.resolve(PROTOBUF_DESCRIPTORS_FILE));
        FileDescriptorSet descriptorSet = FileDescriptorSet.parseFrom(protoBytes, EXTENSIONS);
        Map<String, StabilityTier> protoStability = MAPPER.readValue(
                baselineDir.resolve(PROTOBUF_STABILITY_FILE).toFile(),
                new TypeReference<Map<String, StabilityTier>>() {});
        ProtobufSurfaceSnapshot protobuf = new ProtobufSurfaceSnapshot(descriptorSet, protoStability);
        StoredFormatsWire storedWire = MAPPER.readValue(
                baselineDir.resolve(STORED_FORMATS_FILE).toFile(),
                StoredFormatsWire.class);
        RestRouteSnapshot restRoutes = MAPPER.readValue(
                baselineDir.resolve(REST_ROUTES_FILE).toFile(),
                RestRouteSnapshot.class);
        ActionEnvelopeSnapshot actionEnvelopes = MAPPER.readValue(
                baselineDir.resolve(ACTION_ENVELOPES_FILE).toFile(),
                ActionEnvelopeSnapshot.class);
        return new ReleaseBaselineBundle(
                manifest,
                javaApi,
                protobuf,
                storedWire.toSnapshot(),
                restRoutes,
                actionEnvelopes);
    }

    /**
     * Advances the recorded baseline in {@code baselineDir} to {@code candidateBundle}.
     *
     * <p>If an existing baseline is present in {@code baselineDir}, this method first
     * evaluates {@code candidateBundle} against it. Unless {@code allowMajorReleaseBreak}
     * is {@code true}, any unwaived breaking change on a {@link StabilityTier#STABLE} or
     * {@link StabilityTier#DEPRECATED} surface causes {@link IncompatibleSurfaceException}
     * to be thrown before any file is overwritten.</p>
     *
     * @param baselineDir            target baseline directory
     * @param candidateBundle        new baseline bundle to write
     * @param allowMajorReleaseBreak whether to permit unwaived breaking changes (only at a major release boundary)
     * @return the evaluation report against the prior baseline (or against {@code candidateBundle} itself when initializing)
     * @throws IOException                  if reading or writing baseline files fails
     * @throws IncompatibleSurfaceException if unwaived breaking changes exist and {@code allowMajorReleaseBreak} is {@code false}
     */
    public static ReleaseBaselineReport advanceBaseline(
            Path baselineDir,
            ReleaseBaselineBundle candidateBundle,
            boolean allowMajorReleaseBreak) throws IOException {
        Objects.requireNonNull(baselineDir, "baselineDir");
        Objects.requireNonNull(candidateBundle, "candidateBundle");

        Path manifestPath = baselineDir.resolve(MANIFEST_FILE);
        ReleaseBaselineReport report;
        if (Files.exists(manifestPath)) {
            ReleaseBaselineBundle existing = load(baselineDir);
            report = evaluate(existing, candidateBundle);
            if (!allowMajorReleaseBreak) {
                report.throwIfIncompatible();
            }
        } else {
            report = evaluate(candidateBundle, candidateBundle);
        }
        save(baselineDir, candidateBundle);
        return report;
    }

    private static CompatibilityWaiver findMatchingWaiver(
            SurfaceChange change,
            List<CompatibilityWaiver> waivers) {
        for (CompatibilityWaiver waiver : waivers) {
            if (waiver.covers(change)) {
                return waiver;
            }
        }
        return null;
    }

    private static void writeJson(Path path, Object value) throws IOException {
        DefaultPrettyPrinter printer = new DefaultPrettyPrinter();
        DefaultIndenter indenter = new DefaultIndenter("  ", "\n");
        printer.indentObjectsWith(indenter);
        printer.indentArraysWith(indenter);
        String json = MAPPER.writer(printer).writeValueAsString(value) + "\n";
        Files.writeString(path, json);
    }

    private static ObjectMapper createMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
        mapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
        return mapper;
    }

    private record StoredFormatsWire(List<StoredFormatWireEntry> formats) {
        static StoredFormatsWire fromSnapshot(StoredFormatSnapshot snapshot) {
            List<StoredFormatWireEntry> entries = new ArrayList<>();
            for (StoredFormatEntry entry : snapshot.formats()) {
                entries.add(new StoredFormatWireEntry(
                        entry.formatId(),
                        entry.rootTypeName(),
                        entry.formatVersion(),
                        entry.stability(),
                        entry.compatibilityMode(),
                        entry.closureFingerprint(),
                        entry.pinnedClosureSha256(),
                        Base64.getEncoder().encodeToString(entry.descriptorClosure().toByteArray())));
            }
            return new StoredFormatsWire(entries);
        }

        StoredFormatSnapshot toSnapshot() throws IOException {
            List<StoredFormatEntry> entries = new ArrayList<>();
            if (formats != null) {
                for (StoredFormatWireEntry wire : formats) {
                    byte[] raw = Base64.getDecoder().decode(wire.descriptorClosureBase64());
                    FileDescriptorSet closure = FileDescriptorSet.parseFrom(raw, EXTENSIONS);
                    entries.add(new StoredFormatEntry(
                            wire.formatId(),
                            wire.rootTypeName(),
                            wire.formatVersion(),
                            wire.stability(),
                            wire.compatibilityMode(),
                            wire.closureFingerprint(),
                            wire.pinnedClosureSha256(),
                            closure));
                }
            }
            return new StoredFormatSnapshot(entries);
        }
    }

    private record StoredFormatWireEntry(
            String formatId,
            String rootTypeName,
            int formatVersion,
            StabilityTier stability,
            CompatibilityMode compatibilityMode,
            String closureFingerprint,
            String pinnedClosureSha256,
            String descriptorClosureBase64) {
    }
}
