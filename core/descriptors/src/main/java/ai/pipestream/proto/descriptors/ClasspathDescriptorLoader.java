package ai.pipestream.proto.descriptors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Message;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A descriptor loader that resolves descriptors from generated Protobuf classes on the classpath.
 *
 * <p>Resolution is on demand and by message type name: the fully qualified proto type name is
 * mapped to candidate generated Java class names, and the first candidate that is a
 * {@link Message} contributes its file descriptor. Types whose generated code lives in an outer
 * holder class (a file compiled without {@code java_multiple_files}) or under a {@code java_package}
 * that differs from the proto package are not resolvable this way; use a descriptor-set loader for
 * those.</p>
 */
public class ClasspathDescriptorLoader implements DescriptorLoader {

    private static final Logger LOG = LoggerFactory.getLogger(ClasspathDescriptorLoader.class);

    @Override
    public List<FileDescriptor> loadDescriptors() throws DescriptorLoadException {
        // Classpath scanning for ALL descriptors is expensive and usually not needed
        // since we prefer on-demand resolution via loadDescriptorForType(typeName).
        return Collections.emptyList();
    }

    @Override
    public FileDescriptor loadDescriptor(String fileName) throws DescriptorLoadException {
        // This loader has no proto-file index; a caller passing a type name is still served.
        return loadDescriptorForType(fileName);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Overrides the interface default, which enumerates {@link #loadDescriptors()} and would
     * therefore never match: this loader does not enumerate.</p>
     */
    @Override
    public FileDescriptor loadDescriptorForType(String fullTypeName) throws DescriptorLoadException {
        if (fullTypeName == null || fullTypeName.isEmpty()) {
            return null;
        }
        for (String candidate : candidateClassNames(fullTypeName)) {
            FileDescriptor fd = resolveFromClass(candidate);
            if (fd != null) {
                LOG.debug("Resolved descriptor for {} from classpath class {}", fullTypeName, candidate);
                return fd;
            }
        }
        LOG.trace("No generated class on the classpath defines type {}", fullTypeName);
        return null;
    }

    /**
     * Java class names that may hold the generated code for a proto type name. Message nesting is
     * expressed with {@code $} in binary class names, and the split between package and nesting is
     * not recoverable from the proto name alone, so every split point is tried, outermost first.
     */
    private static List<String> candidateClassNames(String fullTypeName) {
        String[] parts = fullTypeName.split("\\.");
        List<String> candidates = new ArrayList<>(parts.length);
        candidates.add(fullTypeName);
        for (int split = parts.length - 1; split >= 1; split--) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < parts.length; i++) {
                if (i > 0) {
                    sb.append(i < split ? '.' : '$');
                }
                sb.append(parts[i]);
            }
            String candidate = sb.toString();
            if (!candidate.equals(fullTypeName)) {
                candidates.add(candidate);
            }
        }
        return candidates;
    }

    private FileDescriptor resolveFromClass(String className) {
        try {
            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            if (loader == null) {
                loader = getClass().getClassLoader();
            }
            Class<?> clazz = Class.forName(className, false, loader);

            if (Message.class.isAssignableFrom(clazz)) {
                Method getDescriptorMethod = clazz.getMethod("getDescriptor");
                Object descriptor = getDescriptorMethod.invoke(null);

                if (descriptor instanceof com.google.protobuf.Descriptors.Descriptor d) {
                    return d.getFile();
                }
            }
        } catch (ClassNotFoundException | NoClassDefFoundError e) {
            // Normal case if the class is not on the classpath.
            LOG.trace("Class {} not found on classpath for descriptor resolution", className);
        } catch (Exception e) {
            LOG.warn("Failed to resolve descriptor from class {}: {}", className, e.getMessage());
        }
        return null;
    }

    @Override
    public boolean isAvailable() {
        return true;
    }

    @Override
    public String getLoaderType() {
        return "Classpath Class Resolver";
    }
}
