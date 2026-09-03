package ai.protomolt.proto.helpers;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import com.google.protobuf.*;
import com.google.protobuf.Descriptors.Descriptor;

/**
 * Utility class for handling google.protobuf.Any messages.
 * Provides packing, unpacking, and type URL management for Any fields.
 */
public class AnyHandler {

    private static final String TYPE_URL_PREFIX = "type.googleapis.com/";

    private final DescriptorRegistry descriptorRegistry;
    private final TypeConverter typeConverter;

    /**
     * Creates a new AnyHandler with the specified descriptor registry.
     *
     * @param descriptorRegistry Registry for looking up message descriptors
     */
    public AnyHandler(DescriptorRegistry descriptorRegistry) {
        this.descriptorRegistry = descriptorRegistry;
        this.typeConverter = new TypeConverter();
    }

    /**
     * Creates a new AnyHandler with an empty descriptor registry.
     */
    public AnyHandler() {
        this(new DescriptorRegistry());
    }

    /**
     * Packs a message into a google.protobuf.Any.
     *
     * @param message The message to pack
     * @return An Any containing the packed message
     */
    public Any pack(Message message) {
        return Any.pack(message);
    }

    /**
     * Unpacks an Any message to a concrete message type.
     * Uses the descriptor registry to resolve the type.
     *
     * @param any The Any message to unpack
     * @return The unpacked message
     * @throws InvalidProtocolBufferException if unpacking fails
     */
    public Message unpack(Any any) throws InvalidProtocolBufferException {
        String typeUrl = any.getTypeUrl();
        String typeName = extractTypeName(typeUrl);

        // Try to find a registered descriptor
        Descriptor descriptor = descriptorRegistry.findDescriptorByFullName(typeName);

        if (descriptor != null) {
            return DynamicMessage.parseFrom(descriptor, any.getValue());
        }

        // If no descriptor found, try to unpack as DynamicMessage
        // This will fail if the descriptor is not available
        throw new InvalidProtocolBufferException(
            "Cannot unpack Any: descriptor not found for type " + typeName);
    }

    /**
     * Unpacks an Any message to a DynamicMessage using the provided descriptor.
     *
     * @param any The Any message to unpack
     * @param descriptor The descriptor to use for unpacking
     * @return The unpacked DynamicMessage
     * @throws InvalidProtocolBufferException if unpacking fails
     */
    public DynamicMessage unpackToDynamic(Any any, Descriptor descriptor) throws InvalidProtocolBufferException {
        return DynamicMessage.parseFrom(descriptor, any.getValue());
    }

    /**
     * Checks if an Any message contains a specific type.
     *
     * @param any The Any message to check
     * @param descriptor The descriptor of the expected type
     * @return true if the Any contains the specified type
     */
    public boolean is(Any any, Descriptor descriptor) {
        String typeUrl = any.getTypeUrl();
        String typeName = extractTypeName(typeUrl);
        return descriptor.getFullName().equals(typeName);
    }

    /**
     * Checks if an Any message contains a specific type by full name.
     *
     * @param any The Any message to check
     * @param fullTypeName The full type name (e.g., "ai.protomolt.data.v1.SearchMetadata")
     * @return true if the Any contains the specified type
     */
    public boolean is(Any any, String fullTypeName) {
        String typeUrl = any.getTypeUrl();
        String typeName = extractTypeName(typeUrl);
        return fullTypeName.equals(typeName);
    }

    /**
     * Gets the type name from an Any message.
     *
     * @param any The Any message
     * @return The full type name
     */
    public String getTypeName(Any any) {
        return extractTypeName(any.getTypeUrl());
    }

    /**
     * Creates a type URL from a type name.
     *
     * @param typeName The full type name
     * @return The type URL
     */
    public String createTypeUrl(String typeName) {
        if (typeName.startsWith(TYPE_URL_PREFIX)) {
            return typeName;
        }
        return TYPE_URL_PREFIX + typeName;
    }

    /**
     * Extracts the type name from a type URL.
     * Handles both short form (type.googleapis.com/...) and full URLs.
     *
     * @param typeUrl The type URL
     * @return The extracted type name
     */
    private String extractTypeName(String typeUrl) {
        int lastSlash = typeUrl.lastIndexOf('/');
        if (lastSlash >= 0) {
            return typeUrl.substring(lastSlash + 1);
        }
        return typeUrl;
    }

    /**
     * Unpacks an Any to a Struct representation.
     * This is useful for accessing Any content in a generic way.
     *
     * @param any The Any message to unpack
     * @return A Struct representation of the message
     * @throws InvalidProtocolBufferException if unpacking fails
     */
    public Struct unpackToStruct(Any any) throws InvalidProtocolBufferException {
        Message message = unpack(any);
        return typeConverter.messageToStruct(message);
    }

    /**
     * Packs a Struct into an Any with a specified type name.
     * The Struct is first converted to a DynamicMessage.
     *
     * @param struct The Struct to pack
     * @param typeName The full type name for the Any
     * @return An Any containing the packed message
     * @throws IllegalArgumentException if the descriptor is not found
     */
    public Any packStruct(Struct struct, String typeName) {
        Descriptor descriptor = descriptorRegistry.findDescriptorByFullName(typeName);
        if (descriptor == null) {
            throw new IllegalArgumentException("Descriptor not found for type: " + typeName);
        }

        DynamicMessage message = typeConverter.structToMessage(struct, descriptor);
        return Any.pack(message);
    }

    /**
     * Safely unpacks an Any, returning null if unpacking fails.
     *
     * @param any The Any message to unpack
     * @return The unpacked message, or null if unpacking fails
     */
    public Message unpackSafe(Any any) {
        try {
            return unpack(any);
        } catch (InvalidProtocolBufferException e) {
            return null;
        }
    }

    /**
     * Gets the descriptor registry used by this handler.
     *
     * @return The descriptor registry
     */
    public DescriptorRegistry getDescriptorRegistry() {
        return descriptorRegistry;
    }
}
