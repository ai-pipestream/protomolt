package ai.pipestream.proto.descriptors;

import com.google.protobuf.Descriptors.FileDescriptor;

import java.util.List;

/**
 * Generic interface for loading Protocol Buffer descriptors from various sources.
 */
public interface DescriptorLoader {

    /**
     * Loads all available file descriptors from this loader's source.
     *
     * @return A list of FileDescriptors
     * @throws DescriptorLoadException if loading fails
     */
    List<FileDescriptor> loadDescriptors() throws DescriptorLoadException;

    /**
     * Loads a specific file descriptor by name.
     *
     * @param fileName The proto file name (e.g., "my_types.proto")
     * @return The FileDescriptor, or null if not found
     * @throws DescriptorLoadException if loading fails
     */
    FileDescriptor loadDescriptor(String fileName) throws DescriptorLoadException;

    /**
     * Loads the file descriptor that defines the given message type. Note the distinction from
     * {@link #loadDescriptor(String)}, which looks up by proto FILE name: this looks up by the
     * fully qualified name of a message TYPE contained in a file (nested types included).
     *
     * @param fullTypeName The fully qualified message type name (e.g., "my.pkg.MyType")
     * @return The FileDescriptor containing the type, or null if no loaded file defines it
     * @throws DescriptorLoadException if loading fails
     */
    default FileDescriptor loadDescriptorForType(String fullTypeName) throws DescriptorLoadException {
        for (FileDescriptor fd : loadDescriptors()) {
            for (com.google.protobuf.Descriptors.Descriptor message : fd.getMessageTypes()) {
                if (containsType(message, fullTypeName)) {
                    return fd;
                }
            }
        }
        return null;
    }

    private static boolean containsType(com.google.protobuf.Descriptors.Descriptor message, String fullTypeName) {
        if (message.getFullName().equals(fullTypeName)) {
            return true;
        }
        for (com.google.protobuf.Descriptors.Descriptor nested : message.getNestedTypes()) {
            if (containsType(nested, fullTypeName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks if this loader is available and can load descriptors.
     */
    boolean isAvailable();

    /**
     * Gets a human-readable name for this loader type.
     */
    String getLoaderType();

    /**
     * Exception thrown when descriptor loading fails.
     */
    class DescriptorLoadException extends Exception {
        public DescriptorLoadException(String message) {
            super(message);
        }

        public DescriptorLoadException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
