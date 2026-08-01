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
 * {@code field_mask.proto}, which Wire does not bundle, is supplied here), encoded to
 * {@code FileDescriptorProto}s and built into linked {@link FileDescriptor}s.
 * Well-known-type imports the encoder cannot supply are linked from protobuf-java's runtime
 * by {@link GoogleDescriptorLoader}'s fallback.</p>
 *
 * <p>Wire's {@code SchemaEncoder} encodes structure correctly but mangles options whose
 * extension fields share a simple name across packages (its intermediate JSON map is keyed by
 * simple name, so same-named option families collide and only the last survives). The encoded
 * files therefore pass through {@link LinkedOptionsRepair}, which re-encodes every element's
 * options from Wire's linked model — keyed by {@code ProtoMember} and fully intact — before
 * the set is built.</p>
 *
 * <p>Instances are stateless and thread-safe. This is the single compilation pipeline behind
 * every text-based descriptor source (schema-registry loaders, gatherers).</p>
 */
public final class ProtoSourceCompiler {

    /** {@code field_mask.proto} is a well-known import that Wire does not bundle. */
    private static final String FIELD_MASK_PATH = "google/protobuf/field_mask.proto";
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
            Schema schema = link(memory, dir);
            Map<String, FileDescriptorProto> protos = new LinkedHashMap<>();
            SchemaEncoder encoder = new SchemaEncoder(schema);
            for (String path : sources.paths()) {
                encodeWithDependencies(path, schema, encoder, protos);
            }
            FileDescriptorSet set = repairOptions(schema, protos);
            return linkDescriptors(sources, set);
        } catch (IOException e) {
            throw new ProtoCompilationException("Failed to stage sources in memory", e);
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
            // SchemaEncoder also throws raw runtime exceptions (e.g. NPE on an enum option
            // identifier it cannot resolve); compile failures must surface as
            // ProtoCompilationException either way.
            throw new ProtoCompilationException("Failed to encode " + path
                    + (e.getMessage() == null ? "" : ": " + e.getMessage()), e);
        }
        out.put(path, proto);
        for (String dependency : proto.getDependencyList()) {
            encodeWithDependencies(dependency, schema, encoder, out);
        }
    }

    /**
     * Re-encodes every element's options from Wire's linked model and returns the repaired set.
     *
     * <p>{@code SchemaEncoder} encodes structure correctly but mangles options: it routes the
     * linked options through a JSON-style map keyed by the option field's simple name, so custom
     * option extensions that share a field name across packages (the repo's annotation families
     * all declare extensions named {@code field} / {@code message} under different numbers)
     * collide — only the last family survives with content. The linked {@code Options} maps are
     * intact, so {@link LinkedOptionsRepair} rebuilds every non-empty options payload from them.
     * Extension descriptors resolve against {@link FileDescriptor}s built from the encoded
     * structure, which is already correct — only option payloads are affected.</p>
     */
    private static FileDescriptorSet repairOptions(Schema schema, Map<String, FileDescriptorProto> protos)
            throws ProtoCompilationException {
        FileDescriptorSet structural = FileDescriptorSet.newBuilder().addAllFile(protos.values()).build();
        LinkedOptionsEncoder optionsEncoder;
        try {
            optionsEncoder = LinkedOptionsEncoder.over(GoogleDescriptorLoader.fromDescriptorSet(structural));
        } catch (Exception e) {
            throw new ProtoCompilationException("Failed to build descriptors for option repair", e);
        }
        if (optionsEncoder.isEmpty()) {
            return structural;
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
