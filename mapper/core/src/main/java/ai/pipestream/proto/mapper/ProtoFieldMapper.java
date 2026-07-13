package ai.pipestream.proto.mapper;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.helpers.AnyHandler;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;

import java.util.List;

/**
 * Maps fields between protobuf messages using text rules or path accessors.
 */
public interface ProtoFieldMapper {

    DescriptorRegistry getDescriptorRegistry();

    AnyHandler getAnyHandler();

    Object getValue(MessageOrBuilder source, String path) throws MappingException;

    /**
     * As {@link #getValue(MessageOrBuilder, String)}, but when {@code includeDefaults} is
     * {@code true} a proto3 implicit-presence leaf field at its default value
     * ({@code false} / {@code 0} / {@code ""}) is returned as that default instead of
     * {@code null}. Fields with explicit presence (optional, message, oneof members) still
     * return {@code null} when unset.
     */
    default Object getValue(MessageOrBuilder source, String path, boolean includeDefaults)
            throws MappingException {
        return getValue(source, path);
    }

    void setValue(Message.Builder targetBuilder, String path, Object value) throws MappingException;

    void appendValue(Message.Builder targetBuilder, String path, Object value) throws MappingException;

    void clearField(Message.Builder targetBuilder, String path) throws MappingException;

    void mapInPlace(Message.Builder builder, List<String> rules) throws MappingException;

    void map(Message source, Message.Builder targetBuilder, List<String> rules) throws MappingException;
}
