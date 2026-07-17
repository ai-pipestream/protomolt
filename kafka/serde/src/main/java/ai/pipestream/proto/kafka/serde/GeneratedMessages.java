package ai.pipestream.proto.kafka.serde;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Parses payloads into the generated Java classes an application already compiled, when they are
 * on the classpath, and into {@link DynamicMessage} otherwise.
 *
 * <p>A consumer that gets a {@code DynamicMessage} back reads fields by descriptor; one that gets
 * its own generated {@code Order} back just calls {@code getId()}. Nothing has to be configured
 * for this: the packaged descriptor set records each file's Java options ({@code java_package},
 * {@code java_multiple_files}, {@code java_outer_classname}), which is exactly what protoc used
 * to name the classes, so the class name is derived and looked up once per type. A type whose
 * class is not on the classpath — descriptor-set-only deployments, or types resolved from a
 * registry that this application never compiled — quietly stays dynamic.</p>
 */
final class GeneratedMessages {

    private static final Logger LOG = LoggerFactory.getLogger(GeneratedMessages.class);

    private final List<FileDescriptor> packaged;
    private final boolean enabled;
    private final ClassLoader loader;
    // Default instances by type full name; empty marks a type proven absent from the classpath.
    private final ConcurrentMap<String, Optional<Message>> defaults = new ConcurrentHashMap<>();

    GeneratedMessages(List<FileDescriptor> packaged, boolean enabled, ClassLoader loader) {
        this.packaged = packaged;
        this.enabled = enabled;
        this.loader = loader;
    }

    /**
     * The payload as the generated class for {@code type}'s name when one exists, and as a
     * {@link DynamicMessage} under {@code type} otherwise. The generated class parses by its own
     * compiled schema; a writer who has since added fields still round-trips, because protobuf
     * keeps what the class does not know as unknown fields.
     */
    Message parse(Descriptor type, byte[] payload) throws InvalidProtocolBufferException {
        Message prototype = enabled ? defaultInstanceFor(type.getFullName()) : null;
        return prototype != null
                ? prototype.getParserForType().parseFrom(payload)
                : DynamicMessage.parseFrom(type, payload);
    }

    private Message defaultInstanceFor(String fullName) {
        return defaults.computeIfAbsent(fullName, this::locate).orElse(null);
    }

    /**
     * The class name is derived from the <em>packaged</em> file declaring the type, not from
     * whatever file resolved it: the packaged descriptor set describes the build this
     * application's classes came from.
     */
    private Optional<Message> locate(String fullName) {
        Descriptor type = SerdeDescriptors.findMessageType(packaged, fullName);
        if (type == null) {
            return Optional.empty();
        }
        String className = binaryClassName(type);
        try {
            Class<?> clazz = Class.forName(className, true, loader);
            if (!Message.class.isAssignableFrom(clazz)) {
                LOG.debug("{} exists but is not a protobuf Message; {} stays dynamic",
                        className, fullName);
                return Optional.empty();
            }
            return Optional.of((Message) clazz.getMethod("getDefaultInstance").invoke(null));
        } catch (ClassNotFoundException e) {
            // The expected case for descriptor-set-only deployments: no generated class exists.
            LOG.debug("No generated class {} on the classpath; {} stays dynamic",
                    className, fullName);
            return Optional.empty();
        } catch (ReflectiveOperationException | LinkageError e) {
            // The class exists but cannot be used - a version clash, say. Louder than the
            // not-found case, because someone put that class there expecting it to be returned.
            LOG.warn("Generated class {} could not be loaded; {} stays dynamic: {}",
                    className, fullName, e.toString());
            return Optional.empty();
        }
    }

    /** The binary name protoc gave {@code type}'s generated class, nested classes {@code $}-joined. */
    static String binaryClassName(Descriptor type) {
        FileDescriptor file = type.getFile();
        var options = file.getOptions();
        String javaPackage = options.hasJavaPackage() ? options.getJavaPackage()
                : file.getPackage();
        Deque<String> names = new ArrayDeque<>();
        for (Descriptor current = type; current != null; current = current.getContainingType()) {
            names.addFirst(current.getName());
        }
        String nested = String.join("$", names);
        if (options.getJavaMultipleFiles()) {
            return javaPackage.isEmpty() ? nested : javaPackage + "." + nested;
        }
        String outer = options.hasJavaOuterClassname() ? options.getJavaOuterClassname()
                : outerClassName(file);
        return (javaPackage.isEmpty() ? outer : javaPackage + "." + outer) + "$" + nested;
    }

    /**
     * protoc's default outer class name: the file's base name camel-cased (letters after an
     * underscore or digit are capitalized, separators dropped), with {@code OuterClass} appended
     * when a top-level type in the file already claims the name.
     */
    private static String outerClassName(FileDescriptor file) {
        String base = file.getName();
        base = base.substring(base.lastIndexOf('/') + 1);
        if (base.endsWith(".proto")) {
            base = base.substring(0, base.length() - ".proto".length());
        }
        StringBuilder out = new StringBuilder(base.length());
        boolean capitalize = true;
        for (char c : base.toCharArray()) {
            if (Character.isDigit(c)) {
                out.append(c);
                capitalize = true;
            } else if (Character.isLetter(c)) {
                out.append(capitalize ? Character.toUpperCase(c) : c);
                capitalize = false;
            } else {
                capitalize = true;
            }
        }
        String name = out.toString();
        boolean claimed = file.getMessageTypes().stream().anyMatch(m -> m.getName().equals(name))
                || file.getEnumTypes().stream().anyMatch(e -> e.getName().equals(name))
                || file.getServices().stream().anyMatch(s -> s.getName().equals(name));
        return claimed ? name + "OuterClass" : name;
    }
}
