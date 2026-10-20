package ai.protomolt.proto.mapper;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.helpers.AnyHandler;
import ai.protomolt.proto.helpers.TypeConverter;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;

import java.util.List;

/**
 * A streamlined utility for dynamically mapping fields between Protocol Buffer messages.
 *
 * <p>This class delegates dot-notation path reading and writing to {@link FieldAccessor},
 * path segmentation and literal parsing to {@link FieldPathResolver}, and
 * {@code google.protobuf.Struct} operations to {@link StructValueCodec}.
 *
 * <p><b>Features:</b>
 * <ul>
 * <li>Handles literal value assignments (strings, booleans, numbers, null).</li>
 * <li>Correctly reads from and writes to `google.protobuf.Struct` fields, handling type conversions from DynamicMessage.</li>
 * <li>Supports `google.protobuf.Any` field packing and unpacking.</li>
 * <li>Provides type conversion between different protobuf types.</li>
 * <li>Manages proto descriptors for dynamic message handling.</li>
 * <li>Provides precise error messages for invalid paths.</li>
 * </ul>
 */
public class ProtoFieldMapperImpl implements ProtoFieldMapper {

    private final TextRuleParser ruleParser = new TextRuleParser();
    private final FieldAccessor fieldAccessor;
    private final DescriptorRegistry descriptorRegistry;
    private final AnyHandler anyHandler;
    private final TypeConverter typeConverter;

    /**
     * CDI constructor — receives the application-scoped DescriptorRegistry
     * which has CDI-injected loaders (Apicurio, Google, etc.) already wired.
     *
     * @param descriptorRegistry The descriptor registry to use
     */
    public ProtoFieldMapperImpl(DescriptorRegistry descriptorRegistry) {
        this.descriptorRegistry = descriptorRegistry;
        this.anyHandler = new AnyHandler(descriptorRegistry);
        this.typeConverter = new TypeConverter();
        this.fieldAccessor = new FieldAccessor(anyHandler, typeConverter);
    }

    /**
     * Gets the descriptor registry for this mapper.
     * Use this to register descriptors for Any field handling.
     *
     * @return The descriptor registry
     */
    @Override
    public DescriptorRegistry getDescriptorRegistry() {
        return descriptorRegistry;
    }

    /**
     * Gets the AnyHandler for this mapper.
     * Use this for manual Any field operations.
     *
     * @return The AnyHandler
     */
    @Override
    public AnyHandler getAnyHandler() {
        return anyHandler;
    }

    /**
     * Reads a value from a protobuf message/builder using a dot-notation path.
     *
     * <p>This exposes the same path semantics as the rule engine, but without needing to build
     * a string rule (useful for services that already have a mapping model).
     * @param source the source
     * @param path the path
     * @return the value
     * @throws MappingException if the operation fails
     */
    @Override
    public Object getValue(MessageOrBuilder source, String path) throws MappingException {
        return fieldAccessor.getValue(source, path, path);
    }

    /**
     * Reads a value like {@link #getValue(MessageOrBuilder, String)}, optionally surfacing the
     * default value ({@code false} / {@code 0} / {@code ""}) of proto3 implicit-presence leaf
     * fields instead of {@code null}.
     * @param source the source
     * @param path the path
     * @param includeDefaults when true, implicit-presence leaves at default return the default
     * @return the value
     * @throws MappingException if the operation fails
     */
    @Override
    public Object getValue(MessageOrBuilder source, String path, boolean includeDefaults)
            throws MappingException {
        return fieldAccessor.getValue(source, path, path, includeDefaults);
    }

    /**
     * Writes a value to a protobuf builder using a dot-notation path.
     * @param targetBuilder the target builder
     * @param path the path
     * @param value the value
     * @throws MappingException if the operation fails
     */
    @Override
    public void setValue(Message.Builder targetBuilder, String path, Object value) throws MappingException {
        fieldAccessor.setValue(targetBuilder, path, value, path);
    }

    /**
     * Appends a value to a repeated field using a dot-notation path.
     * @param targetBuilder the target builder
     * @param path the path
     * @param value the value
     * @throws MappingException if the operation fails
     */
    @Override
    public void appendValue(Message.Builder targetBuilder, String path, Object value) throws MappingException {
        fieldAccessor.appendValue(targetBuilder, path, value, path);
    }

    /**
     * Clears a field using a dot-notation path.
     * @param targetBuilder the target builder
     * @param path the path
     * @throws MappingException if the operation fails
     */
    @Override
    public void clearField(Message.Builder targetBuilder, String path) throws MappingException {
        fieldAccessor.clearField(targetBuilder, path, path);
    }

    /**
     * Executes the rule syntax directly against a single mutable builder.
     *
     * <p>Unlike {@link #map(Message, Message.Builder, List)}, this reads from the same builder
     * as it is being mutated, so later rules can depend on earlier rules without repeatedly
     * building intermediate immutable messages.</p>
     * @param builder the builder
     * @param rules the rules
     * @throws MappingException if the operation fails
     */
    @Override
    public void mapInPlace(Message.Builder builder, List<String> rules) throws MappingException {
        executeRules(builder, builder, rules);
    }

    /**
     * Maps fields from a source message to a target message builder based on a list of rules.
     *
     * @param source The source protobuf message to read values from
     * @param targetBuilder The target message builder to write values to
     * @param rules List of mapping rules in string format
     * @throws MappingException If any rule cannot be applied due to invalid paths or type mismatches
     */
    @Override
    public void map(Message source, Message.Builder targetBuilder, List<String> rules) throws MappingException {
        executeRules(source, targetBuilder, rules);
    }

    private void executeRules(MessageOrBuilder source, Message.Builder targetBuilder, List<String> rules)
            throws MappingException {
        List<TextMappingRule> parsedRules = ruleParser.parse(rules);

        for (TextMappingRule rule : parsedRules) {
            try {
                Object value = null;
                if (rule.sourcePath() != null) {
                    value = fieldAccessor.getValue(source, rule.sourcePath(), rule.originalRule());
                }

                switch (rule.operation()) {
                    case ASSIGN -> fieldAccessor.setValue(
                            targetBuilder, rule.targetPath(), value, rule.originalRule());
                    case APPEND -> fieldAccessor.appendValue(
                            targetBuilder, rule.targetPath(), value, rule.originalRule());
                    case CLEAR -> fieldAccessor.clearField(
                            targetBuilder, rule.targetPath(), rule.originalRule());
                }
            } catch (Exception e) {
                if (e instanceof MappingException mappingException) {
                    throw mappingException;
                }
                throw new MappingException(
                        "Failed to execute rule: " + rule.originalRule(), e, rule.originalRule());
            }
        }
    }
}
