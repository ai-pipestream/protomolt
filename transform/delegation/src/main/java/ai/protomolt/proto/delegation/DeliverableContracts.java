package ai.protomolt.proto.delegation;

import ai.protomolt.proto.delegation.v1.DeliverableContract;
import ai.protomolt.proto.delegation.v1.TaskSpec;
import ai.protomolt.proto.http.jsonschema.ProtoJsonSchemaGenerator;
import ai.protomolt.proto.meta.DescriptorMetadata;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.ValidationResult;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Links the descriptor set a {@link DeliverableContract} carries and judges a candidate's
 * deliverable against it.
 *
 * <p>A contract travels with the offer, so the rules a deliverable is held to are the rules
 * the coordinator declared when it opened the task, not whatever the running process happens
 * to have on its classpath. The set is parsed with the house option extensions registered, so
 * the declared field rules and message CEL inside it are readable rather than unknown fields,
 * and the deliverable is unpacked into a {@link DynamicMessage} and validated exactly the way
 * {@code DeclaredRulesAnyPayloadValidator} validates a payload unpacked from an
 * {@code Any} on the index write path: every violation is reported with its path prefixed by
 * the field the Any sits in.
 *
 * <p>Linking a set is not free and a transcript is reduced from the beginning on every
 * appended frame, so the linked descriptor and its validator are cached by the exact bytes of
 * the set and the type it names. A set that does not link is cached as its failure too: a
 * broken contract must not pay for linking once per frame of the replay either.
 */
public final class DeliverableContracts {

    /** The Any type URL prefix a packed deliverable carries. */
    static final String TYPE_URL_PREFIX = "type.googleapis.com/";

    /** Cache bound; a full cache is cleared rather than evicted entry by entry. */
    private static final int MAX_CACHED_CONTRACTS = 128;

    private static final Map<Key, Linked> LINKED = new ConcurrentHashMap<>();

    /**
     * One linked contract.
     *
     * @param descriptor the deliverable message type, resolved from the contract's set
     * @param validator the validator built for that type's declared rules
     */
    public record Compiled(Descriptor descriptor, ProtoValidator validator) {

        public Compiled {
            Objects.requireNonNull(descriptor, "descriptor");
            Objects.requireNonNull(validator, "validator");
        }
    }

    private record Key(ByteString descriptorSet, String typeName) {
    }

    /** A linked contract or, when {@code failure} is set, why it did not link. */
    private record Linked(Descriptor descriptor, ProtoValidator validator, String failure) {
    }

    private DeliverableContracts() {
    }

    /**
     * Links {@code contract} and resolves the message it names.
     *
     * @param contract the contract carried by a task spec
     * @return the linked descriptor and the validator built for it
     * @throws IllegalArgumentException when the set does not link or does not declare the type
     */
    public static Compiled compile(DeliverableContract contract) {
        Objects.requireNonNull(contract, "contract");
        Key key = new Key(contract.getDescriptorSet(), contract.getTypeName());
        Linked linked = LINKED.get(key);
        if (linked == null) {
            linked = link(contract);
            if (LINKED.size() >= MAX_CACHED_CONTRACTS) {
                LINKED.clear();
            }
            LINKED.put(key, linked);
        }
        if (linked.failure() != null) {
            throw new IllegalArgumentException(linked.failure());
        }
        return new Compiled(linked.descriptor(), linked.validator());
    }

    /**
     * The spec a worker sees: a contract that arrived without a schema gets one rendered from
     * its own descriptor set, so a worker reads the deliverable's shape and bounds without a
     * toolchain, a registry, or a shared build.
     *
     * @param spec the spec as the coordinator's caller wrote it
     * @return the same spec when it declares no contract, otherwise one carrying the schema
     * @throws IllegalArgumentException when the contract does not link
     */
    public static TaskSpec rendered(TaskSpec spec) {
        Objects.requireNonNull(spec, "spec");
        if (!spec.hasContract()) {
            return spec;
        }
        Compiled compiled = compile(spec.getContract());
        if (!spec.getContract().getJsonSchema().isEmpty()) {
            return spec;
        }
        return spec.toBuilder()
                .setContract(spec.getContract().toBuilder()
                        .setJsonSchema(ProtoJsonSchemaGenerator.create()
                                .generateJson(compiled.descriptor())))
                .build();
    }

    /**
     * Every way {@code result} fails {@code contract}, as sentences a finding can carry.
     *
     * @param contract the contract the task's spec declared
     * @param result the deliverable the candidate carried
     * @return the problems in report order; empty means the deliverable satisfies the contract
     */
    public static List<String> check(DeliverableContract contract, Any result) {
        Objects.requireNonNull(contract, "contract");
        Objects.requireNonNull(result, "result");
        Compiled compiled;
        try {
            compiled = compile(contract);
        } catch (IllegalArgumentException e) {
            return List.of("the task's deliverable contract does not link: " + e.getMessage()
                    + "; the deliverable cannot be judged");
        }
        String expected = contract.getTypeName();
        if (!result.getTypeUrl().endsWith("/" + expected)) {
            return List.of("the deliverable is a " + typeName(result.getTypeUrl())
                    + " but the task's contract names " + expected);
        }
        DynamicMessage deliverable;
        try {
            deliverable = DynamicMessage.parseFrom(compiled.descriptor(), result.getValue());
        } catch (InvalidProtocolBufferException e) {
            return List.of("the deliverable does not parse as " + expected + ": "
                    + e.getMessage());
        }
        ValidationResult validation = compiled.validator().validate(deliverable);
        List<String> problems = new ArrayList<>(validation.violations().size());
        for (ValidationResult.Violation violation : validation.violations()) {
            problems.add("the deliverable violates " + violation.ruleId() + " at "
                    + joinPath("result", violation.path()) + ": " + violation.message());
        }
        return List.copyOf(problems);
    }

    /** The message name at the end of an Any type URL. */
    static String typeName(String typeUrl) {
        int slash = typeUrl.lastIndexOf('/');
        return slash < 0 ? typeUrl : typeUrl.substring(slash + 1);
    }

    /** A violation path under the field the deliverable sits in; either side empty is dropped. */
    private static String joinPath(String field, String path) {
        return path.isEmpty() ? field : field + "." + path;
    }

    private static Linked link(DeliverableContract contract) {
        FileDescriptorSet set;
        try {
            set = FileDescriptorSet.parseFrom(contract.getDescriptorSet(), extensions());
        } catch (InvalidProtocolBufferException e) {
            return new Linked(null, null, "the contract's descriptor_set is not a serialized"
                    + " FileDescriptorSet: " + e.getMessage());
        }
        Map<String, FileDescriptorProto> byName = new LinkedHashMap<>();
        set.getFileList().forEach(file -> byName.put(file.getName(), file));
        Map<String, FileDescriptor> built = new LinkedHashMap<>();
        try {
            for (FileDescriptorProto file : set.getFileList()) {
                build(file, byName, built, new LinkedHashSet<>());
            }
        } catch (DescriptorValidationException | IllegalArgumentException e) {
            return new Linked(null, null,
                    "the contract's descriptor_set does not link: " + e.getMessage());
        }
        for (FileDescriptor file : built.values()) {
            Descriptor descriptor = message(file, contract.getTypeName());
            if (descriptor != null) {
                return new Linked(descriptor,
                        ProtoValidator.forMessageType(descriptor), null);
            }
        }
        return new Linked(null, null, "the contract's descriptor_set declares no message named "
                + contract.getTypeName());
    }

    /** The house option extensions, so declared rules survive the parse as options. */
    private static ExtensionRegistry extensions() {
        ExtensionRegistry registry = ExtensionRegistry.newInstance();
        ValidationResult.registerExtensions(registry);
        DescriptorMetadata.registerExtensions(registry);
        return registry;
    }

    private static FileDescriptor build(FileDescriptorProto file,
                                        Map<String, FileDescriptorProto> byName,
                                        Map<String, FileDescriptor> built,
                                        Set<String> building)
            throws DescriptorValidationException {
        FileDescriptor done = built.get(file.getName());
        if (done != null) {
            return done;
        }
        if (!building.add(file.getName())) {
            throw new IllegalArgumentException(
                    "the files import each other in a cycle at " + file.getName());
        }
        List<FileDescriptor> dependencies = new ArrayList<>(file.getDependencyCount());
        for (String dependency : file.getDependencyList()) {
            FileDescriptorProto declared = byName.get(dependency);
            if (declared == null) {
                FileDescriptor known = wellKnown(dependency);
                if (known == null) {
                    throw new IllegalArgumentException(file.getName() + " imports "
                            + dependency + ", which the descriptor set does not carry");
                }
                dependencies.add(known);
                continue;
            }
            dependencies.add(build(declared, byName, built, building));
        }
        FileDescriptor descriptor = FileDescriptor.buildFrom(file,
                dependencies.toArray(new FileDescriptor[0]));
        building.remove(file.getName());
        built.put(file.getName(), descriptor);
        return descriptor;
    }

    /**
     * A well-known file the runtime already carries. A descriptor set produced by protoc
     * without {@code --include_imports} still names these, and refusing them would refuse
     * every contract whose deliverable holds a timestamp.
     */
    private static FileDescriptor wellKnown(String dependency) {
        return switch (dependency) {
            case "google/protobuf/any.proto" -> Any.getDescriptor().getFile();
            case "google/protobuf/timestamp.proto" ->
                    com.google.protobuf.Timestamp.getDescriptor().getFile();
            case "google/protobuf/duration.proto" ->
                    com.google.protobuf.Duration.getDescriptor().getFile();
            case "google/protobuf/struct.proto" ->
                    com.google.protobuf.Struct.getDescriptor().getFile();
            case "google/protobuf/wrappers.proto" ->
                    com.google.protobuf.StringValue.getDescriptor().getFile();
            case "google/protobuf/empty.proto" ->
                    com.google.protobuf.Empty.getDescriptor().getFile();
            case "google/protobuf/field_mask.proto" ->
                    com.google.protobuf.FieldMask.getDescriptor().getFile();
            case "google/protobuf/descriptor.proto" ->
                    com.google.protobuf.DescriptorProtos.getDescriptor();
            default -> null;
        };
    }

    /** The named message in one file, nested types included. */
    private static Descriptor message(FileDescriptor file, String fullName) {
        String pkg = file.getPackage();
        if (!pkg.isEmpty() && !fullName.startsWith(pkg + ".")) {
            return null;
        }
        String relative = pkg.isEmpty() ? fullName : fullName.substring(pkg.length() + 1);
        String[] parts = relative.split("\\.");
        Descriptor current = file.findMessageTypeByName(parts[0]);
        for (int i = 1; current != null && i < parts.length; i++) {
            current = current.findNestedTypeByName(parts[i]);
        }
        return current;
    }
}
