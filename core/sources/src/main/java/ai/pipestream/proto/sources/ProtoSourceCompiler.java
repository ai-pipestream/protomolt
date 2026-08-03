package ai.pipestream.proto.sources;

import ai.pipestream.proto.descriptors.GoogleDescriptorLoader;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.squareup.wire.schema.Location;
import com.squareup.wire.schema.ProtoFile;
import com.squareup.wire.schema.Schema;
import com.squareup.wire.schema.SchemaLoader;
import com.squareup.wire.schema.internal.SchemaEncoder;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Compiles a {@link ProtoSourceSet} to runtime descriptors using Square Wire's schema library —
 * no {@code protoc} binary involved.
 *
 * <p>Sources are staged on an in-memory filesystem — compilation never touches disk — and
 * linked with Wire (which supplies the bundled {@code google/protobuf} imports;
 * {@code field_mask.proto}, which Wire does not bundle, is supplied here). The pipeline then
 * splits structure from options:</p>
 *
 * <ul>
 *   <li><b>Structure</b> is encoded by Wire's {@code SchemaEncoder} from an option-free twin of
 *   the sources ({@link OptionStrippingRewriter}). The encoder cannot be trusted with options:
 *   it keys an intermediate map by the option field's simple name (same-named extension families
 *   collide and only the last survives), and its option-value coercion has no map branch, so a
 *   single map-valued option (e.g. {@code labels} on the meta annotation family) aborts the
 *   whole encode with {@code "not implemented: map<...>"}. With options stripped it does the one
 *   job it is good at — message structure — and neither defect can fire.</li>
 *   <li><b>Options</b> are re-encoded from the linked model of the original sources
 *   ({@link LinkedOptionsRepair} + {@link LinkedOptionsEncoder}), whose {@code Options} maps are
 *   keyed by {@code ProtoMember} and fully intact.</li>
 * </ul>
 *
 * <p>The encoded files are built into linked {@link FileDescriptor}s; well-known-type imports
 * the encoder cannot supply are linked from protobuf-java's runtime by
 * {@link GoogleDescriptorLoader}'s fallback.</p>
 *
 * <p>Instances are stateless and thread-safe. This is the single compilation pipeline behind
 * every text-based descriptor source (schema-registry loaders, gatherers).</p>
 */
public final class ProtoSourceCompiler {

    /** {@code field_mask.proto} is a well-known import that Wire does not bundle. */
    private static final String FIELD_MASK_PATH = "google/protobuf/field_mask.proto";

    /** Wire bundles {@code descriptor.proto}; the option-encoder universe needs it on demand. */
    private static final String DESCRIPTOR_PROTO_PATH = "google/protobuf/descriptor.proto";

    private static final String FIELD_MASK_PROTO = """
            syntax = "proto3";
            package google.protobuf;
            message FieldMask {
              repeated string paths = 1;
            }
            """;

    /**
     * Compiles every file in the set plus the imports Wire can supply.
     *
     * @throws ProtoCompilationException on parse or link failure, unresolvable imports,
     *         or unsafe source paths
     */
    public CompiledProtos compile(ProtoSourceSet sources) throws ProtoCompilationException {
        if (sources.isEmpty()) {
            return new CompiledProtos(FileDescriptorSet.getDefaultInstance(), List.of(), Map.of());
        }
        try (FileSystem memory = Jimfs.newFileSystem(Configuration.unix())) {
            Path dir = memory.getPath("/protos");
            writeSources(sources, dir);
            Schema linked = link(memory, dir);
            Path structuralDir = memory.getPath("/structural");
            writeStructuralSources(sources, structuralDir);
            Schema structural = link(memory, structuralDir);
            Map<String, FileDescriptorProto> protos = new LinkedHashMap<>();
            SchemaEncoder encoder = new SchemaEncoder(structural);
            for (String path : sources.paths()) {
                encodeWithDependencies(path, structural, encoder, protos);
            }
            FileDescriptorSet set = repairOptions(linked, protos);
            return linkDescriptors(sources, set);
        } catch (IOException e) {
            throw new ProtoCompilationException("Failed to stage sources in memory", e);
        }
    }

    /**
     * Writes the option-free twin of every source: same structure, no option statements, so
     * {@code SchemaEncoder} never sees an option payload. Standard options
     * ({@code java_package}, {@code deprecated}, ...) are stripped too — they are restored from
     * the linked model by the repair pass like every other option.
     */
    private static void writeStructuralSources(ProtoSourceSet sources, Path dir)
            throws ProtoCompilationException {
        boolean needsFieldMask = !sources.contains(FIELD_MASK_PATH);
        try {
            for (ProtoSource source : sources.sources()) {
                Path target = dir.resolve(source.path()).normalize();
                if (!target.startsWith(dir)) {
                    throw new ProtoCompilationException("Unsafe source path: " + source.path()
                            + " (origin: " + source.origin() + ")");
                }
                Files.createDirectories(target.getParent());
                Files.writeString(target, OptionStrippingRewriter.strip(source.path(), source.content()));
            }
            if (needsFieldMask) {
                Path fieldMask = dir.resolve(FIELD_MASK_PATH);
                Files.createDirectories(fieldMask.getParent());
                Files.writeString(fieldMask, FIELD_MASK_PROTO);
            }
        } catch (IOException e) {
            throw new ProtoCompilationException("Failed to stage sources for compilation", e);
        }
    }

    private static void writeSources(ProtoSourceSet sources, Path dir) throws ProtoCompilationException {
        boolean needsFieldMask = !sources.contains(FIELD_MASK_PATH);
        try {
            for (ProtoSource source : sources.sources()) {
                Path target = dir.resolve(source.path()).normalize();
                if (!target.startsWith(dir)) {
                    throw new ProtoCompilationException("Unsafe source path: " + source.path()
                            + " (origin: " + source.origin() + ")");
                }
                Files.createDirectories(target.getParent());
                Files.writeString(target, source.content());
            }
            if (needsFieldMask) {
                Path fieldMask = dir.resolve(FIELD_MASK_PATH);
                Files.createDirectories(fieldMask.getParent());
                Files.writeString(fieldMask, FIELD_MASK_PROTO);
            }
        } catch (IOException e) {
            throw new ProtoCompilationException("Failed to stage sources for compilation", e);
        }
    }

    private static Schema link(FileSystem fileSystem, Path dir) throws ProtoCompilationException {
        try {
            SchemaLoader loader = new SchemaLoader(fileSystem);
            loader.setLoadExhaustively(true);
            loader.initRoots(List.of(Location.get(dir.toString())), List.of());
            return loader.loadSchema();
        } catch (Exception e) {
            throw new ProtoCompilationException("Failed to link proto sources: " + e.getMessage(), e);
        }
    }

    /**
     * Encodes {@code path} and, transitively, every import Wire has a linked file for. Imports
     * Wire cannot supply (well-known types resolved from protobuf-java's runtime) are left to
     * {@link GoogleDescriptorLoader}'s well-known-type fallback.
     */
    private static void encodeWithDependencies(String path, Schema schema, SchemaEncoder encoder,
                                               Map<String, FileDescriptorProto> out)
            throws ProtoCompilationException {
        if (out.containsKey(path)) {
            return;
        }
        ProtoFile protoFile = schema.protoFile(path);
        if (protoFile == null) {
            return;
        }
        FileDescriptorProto proto;
        try {
            proto = FileDescriptorProto.parseFrom(encoder.encode(protoFile).toByteArray());
        } catch (IOException | RuntimeException e) {
            // SchemaEncoder also throws raw runtime exceptions on structures it does not
            // support; compile failures must surface as ProtoCompilationException either way.
            throw new ProtoCompilationException("Failed to encode " + path
                    + (e.getMessage() == null ? "" : ": " + e.getMessage()), e);
        }
        out.put(path, proto);
        for (String dependency : proto.getDependencyList()) {
            encodeWithDependencies(dependency, schema, encoder, out);
        }
    }

    /**
     * Writes every element's options from Wire's linked model into the option-free structural
     * set and returns the completed set.
     *
     * <p>The structural encode carries no options at all (see
     * {@link OptionStrippingRewriter}), so this pass is the single source of every option
     * payload in the compiled set — standard options ({@code java_package},
     * {@code deprecated}, ...) and custom extensions alike. The linked {@code Options} maps are
     * keyed by {@code ProtoMember} and fully intact; {@link LinkedOptionsRepair} walks the
     * model and the encoded tree in parallel and sets each non-empty options payload with bytes
     * re-encoded by {@link LinkedOptionsEncoder}. Extension descriptors resolve against
     * {@link FileDescriptor}s built from the encoded structure, which is already correct.</p>
     */
    private static FileDescriptorSet repairOptions(Schema schema, Map<String, FileDescriptorProto> protos)
            throws ProtoCompilationException {
        Map<String, FileDescriptorProto> universe = new LinkedHashMap<>(protos);
        if (!universe.containsKey(DESCRIPTOR_PROTO_PATH)) {
            // The option encoder builds options messages against the same descriptor universe as
            // the extension definitions, so it needs the google.protobuf.*Options types even
            // when no source imports descriptor.proto. Wire bundles the file and links it
            // whenever any option exists (linking options requires it); when it is absent no
            // element carries options and the encoder never looks the types up.
            ProtoFile descriptorFile = schema.protoFile(DESCRIPTOR_PROTO_PATH);
            if (descriptorFile != null) {
                universe.put(DESCRIPTOR_PROTO_PATH, encodeFile(schema, descriptorFile));
            }
        }
        FileDescriptorSet structural = FileDescriptorSet.newBuilder().addAllFile(universe.values()).build();
        LinkedOptionsEncoder optionsEncoder;
        try {
            optionsEncoder = LinkedOptionsEncoder.over(GoogleDescriptorLoader.fromDescriptorSet(structural));
        } catch (Exception e) {
            throw new ProtoCompilationException("Failed to build descriptors for option repair", e);
        }
        Map<String, FileDescriptorProto> repaired = new LinkedHashMap<>();
        for (Map.Entry<String, FileDescriptorProto> entry : protos.entrySet()) {
            ProtoFile protoFile = schema.protoFile(entry.getKey());
            repaired.put(entry.getKey(), protoFile == null
                    ? entry.getValue()
                    : LinkedOptionsRepair.repair(protoFile, entry.getValue(), optionsEncoder));
        }
        return FileDescriptorSet.newBuilder().addAllFile(repaired.values()).build();
    }

    /** Encodes one linked file with {@code SchemaEncoder} (used for Wire-bundled files whose
     *  structure the option-encoder universe needs but which no source imported). */
    private static FileDescriptorProto encodeFile(Schema schema, ProtoFile protoFile)
            throws ProtoCompilationException {
        try {
            return FileDescriptorProto.parseFrom(
                    new SchemaEncoder(schema).encode(protoFile).toByteArray());
        } catch (IOException | RuntimeException e) {
            throw new ProtoCompilationException("Failed to encode " + protoFile.getLocation().getPath()
                    + (e.getMessage() == null ? "" : ": " + e.getMessage()), e);
        }
    }

    private static CompiledProtos linkDescriptors(ProtoSourceSet sources, FileDescriptorSet set)
            throws ProtoCompilationException {
        List<FileDescriptor> descriptors;
        try {
            descriptors = GoogleDescriptorLoader.fromDescriptorSet(set);
        } catch (Exception e) {
            throw new ProtoCompilationException("Failed to build descriptors: " + e.getMessage(), e);
        }
        Map<String, FileDescriptor> byPath = new HashMap<>();
        for (FileDescriptor descriptor : descriptors) {
            byPath.put(descriptor.getName(), descriptor);
        }
        for (String path : sources.paths()) {
            if (!byPath.containsKey(path)) {
                throw new ProtoCompilationException("Compiled set does not contain " + path);
            }
        }
        return new CompiledProtos(set, descriptors, byPath);
    }

}
