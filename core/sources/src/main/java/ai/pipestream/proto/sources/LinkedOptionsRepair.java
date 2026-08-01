package ai.pipestream.proto.sources;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumOptions;
import com.google.protobuf.DescriptorProtos.EnumValueDescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumValueOptions;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldOptions;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileOptions;
import com.google.protobuf.DescriptorProtos.MessageOptions;
import com.google.protobuf.DescriptorProtos.MethodDescriptorProto;
import com.google.protobuf.DescriptorProtos.MethodOptions;
import com.google.protobuf.DescriptorProtos.OneofDescriptorProto;
import com.google.protobuf.DescriptorProtos.OneofOptions;
import com.google.protobuf.DescriptorProtos.ServiceDescriptorProto;
import com.google.protobuf.DescriptorProtos.ServiceOptions;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import com.squareup.wire.schema.EnumConstant;
import com.squareup.wire.schema.EnumType;
import com.squareup.wire.schema.Extend;
import com.squareup.wire.schema.Field;
import com.squareup.wire.schema.MessageType;
import com.squareup.wire.schema.OneOf;
import com.squareup.wire.schema.Options;
import com.squareup.wire.schema.ProtoFile;
import com.squareup.wire.schema.Rpc;
import com.squareup.wire.schema.Service;
import com.squareup.wire.schema.Type;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Repairs the options on an encoded {@link FileDescriptorProto} from Wire's linked model.
 *
 * <p>Wire's {@code SchemaEncoder} gets the message structure right but mangles options whose
 * extension fields share a simple name across packages (it keys an intermediate JSON map by
 * simple name, so same-named families collide and all but the last are emitted as empty
 * extension entries). The linked {@link Options} maps are intact, so this walk visits every
 * element of the file in parallel — Wire model on one side, encoded descriptor tree on the
 * other, matched by name (fields by number) — and replaces each non-empty options payload
 * with bytes re-encoded by {@link LinkedOptionsEncoder}.</p>
 *
 * <p>Any structural mismatch between the two trees fails loud: it means the encoder changed
 * shape underneath this pass, and silently keeping the old bytes would hide it.</p>
 */
final class LinkedOptionsRepair {

    private LinkedOptionsRepair() {
    }

    static FileDescriptorProto repair(ProtoFile wireFile, FileDescriptorProto encoded,
                                      LinkedOptionsEncoder encoder) throws OptionEncodingException {
        String path = wireFile.getLocation().getPath();
        FileDescriptorProto.Builder file = encoded.toBuilder();
        if (encoder.hasOptions(wireFile.getOptions())) {
            file.setOptions(encode(wireFile.getOptions(), "google.protobuf.FileOptions",
                    FileOptions.parser(), encoder, "file " + path));
        }

        Map<String, DescriptorProto.Builder> messages = indexByName(file.getMessageTypeBuilderList(),
                DescriptorProto.Builder::getName);
        Map<String, EnumDescriptorProto.Builder> enums = indexByName(file.getEnumTypeBuilderList(),
                EnumDescriptorProto.Builder::getName);
        for (Type type : wireFile.getTypes()) {
            if (type instanceof MessageType message) {
                repairMessage(message, require(messages, message.getName(), "message", path), encoder, path);
            } else if (type instanceof EnumType enumType) {
                repairEnum(enumType, require(enums, enumType.getName(), "enum", path), encoder, path);
            }
        }
        repairExtensionFields(wireFile.getExtendList(), file.getExtensionBuilderList(), encoder, path);

        Map<String, ServiceDescriptorProto.Builder> services = indexByName(file.getServiceBuilderList(),
                ServiceDescriptorProto.Builder::getName);
        for (Service service : wireFile.getServices()) {
            repairService(service, require(services, service.name(), "service", path), encoder, path);
        }
        return file.build();
    }

    private static void repairMessage(MessageType message, DescriptorProto.Builder builder,
                                      LinkedOptionsEncoder encoder, String path)
            throws OptionEncodingException {
        String context = "message " + builder.getName() + " in " + path;
        if (encoder.hasOptions(message.getOptions())) {
            builder.setOptions(encode(message.getOptions(), "google.protobuf.MessageOptions",
                    MessageOptions.parser(), encoder, context));
        }

        Map<Integer, FieldDescriptorProto.Builder> fields = new HashMap<>();
        for (FieldDescriptorProto.Builder field : builder.getFieldBuilderList()) {
            fields.put(field.getNumber(), field);
        }
        for (Field field : message.getFieldsAndOneOfFields()) {
            if (encoder.hasOptions(field.getOptions())) {
                FieldDescriptorProto.Builder target = fields.get(field.getTag());
                if (target == null) {
                    throw new OptionEncodingException(context + ": encoded descriptor has no field #"
                            + field.getTag() + " (" + field.getName() + ")");
                }
                target.setOptions(encode(field.getOptions(), "google.protobuf.FieldOptions",
                        FieldOptions.parser(), encoder, "field " + field.getName() + " of " + context));
            }
        }

        Map<String, OneofDescriptorProto.Builder> oneofs = indexByName(builder.getOneofDeclBuilderList(),
                OneofDescriptorProto.Builder::getName);
        for (OneOf oneOf : message.getOneOfs()) {
            if (encoder.hasOptions(oneOf.getOptions())) {
                OneofDescriptorProto.Builder target = require(oneofs, oneOf.getName(), "oneof", path);
                target.setOptions(encode(oneOf.getOptions(), "google.protobuf.OneofOptions",
                        OneofOptions.parser(), encoder, "oneof " + oneOf.getName() + " of " + context));
            }
        }

        Map<String, DescriptorProto.Builder> nestedMessages = indexByName(builder.getNestedTypeBuilderList(),
                DescriptorProto.Builder::getName);
        Map<String, EnumDescriptorProto.Builder> nestedEnums = indexByName(builder.getEnumTypeBuilderList(),
                EnumDescriptorProto.Builder::getName);
        for (Type nested : message.getNestedTypes()) {
            if (nested instanceof MessageType nestedMessage) {
                repairMessage(nestedMessage, require(nestedMessages, nestedMessage.getName(), "message", path),
                        encoder, path);
            } else if (nested instanceof EnumType nestedEnum) {
                repairEnum(nestedEnum, require(nestedEnums, nestedEnum.getName(), "enum", path), encoder, path);
            }
        }
        repairExtensionFields(message.getNestedExtendList(), builder.getExtensionBuilderList(), encoder, path);
    }

    private static void repairEnum(EnumType enumType, EnumDescriptorProto.Builder builder,
                                   LinkedOptionsEncoder encoder, String path)
            throws OptionEncodingException {
        String context = "enum " + builder.getName() + " in " + path;
        if (encoder.hasOptions(enumType.getOptions())) {
            builder.setOptions(encode(enumType.getOptions(), "google.protobuf.EnumOptions",
                    EnumOptions.parser(), encoder, context));
        }
        Map<String, EnumValueDescriptorProto.Builder> values = indexByName(builder.getValueBuilderList(),
                EnumValueDescriptorProto.Builder::getName);
        for (EnumConstant constant : enumType.getConstants()) {
            if (encoder.hasOptions(constant.getOptions())) {
                EnumValueDescriptorProto.Builder target = require(values, constant.getName(), "enum value", path);
                target.setOptions(encode(constant.getOptions(), "google.protobuf.EnumValueOptions",
                        EnumValueOptions.parser(), encoder, "enum value " + constant.getName() + " of " + context));
            }
        }
    }

    private static void repairService(Service service, ServiceDescriptorProto.Builder builder,
                                      LinkedOptionsEncoder encoder, String path)
            throws OptionEncodingException {
        String context = "service " + builder.getName() + " in " + path;
        if (encoder.hasOptions(service.options())) {
            builder.setOptions(encode(service.options(), "google.protobuf.ServiceOptions",
                    ServiceOptions.parser(), encoder, context));
        }
        Map<String, MethodDescriptorProto.Builder> methods = indexByName(builder.getMethodBuilderList(),
                MethodDescriptorProto.Builder::getName);
        for (Rpc rpc : service.rpcs()) {
            if (encoder.hasOptions(rpc.getOptions())) {
                MethodDescriptorProto.Builder target = require(methods, rpc.getName(), "method", path);
                target.setOptions(encode(rpc.getOptions(), "google.protobuf.MethodOptions",
                        MethodOptions.parser(), encoder, "method " + rpc.getName() + " of " + context));
            }
        }
    }

    /** Options on extension field declarations themselves (e.g. {@code [deprecated = true]}). */
    private static void repairExtensionFields(List<Extend> extendBlocks,
                                              List<FieldDescriptorProto.Builder> extensionBuilders,
                                              LinkedOptionsEncoder encoder, String path)
            throws OptionEncodingException {
        if (extendBlocks.isEmpty()) {
            return;
        }
        Map<Integer, FieldDescriptorProto.Builder> byNumber = new HashMap<>();
        for (FieldDescriptorProto.Builder field : extensionBuilders) {
            byNumber.put(field.getNumber(), field);
        }
        for (Extend extend : extendBlocks) {
            for (Field field : extend.getFields()) {
                if (encoder.hasOptions(field.getOptions())) {
                    FieldDescriptorProto.Builder target = byNumber.get(field.getTag());
                    if (target == null) {
                        throw new OptionEncodingException("extend " + extend.getName() + " in " + path
                                + ": encoded descriptor has no extension field #" + field.getTag());
                    }
                    target.setOptions(encode(field.getOptions(), "google.protobuf.FieldOptions",
                            FieldOptions.parser(), encoder,
                            "extension field " + field.getName() + " of extend " + extend.getName()
                                    + " in " + path));
                }
            }
        }
    }

    private static <M extends Message> M encode(Options options, String optionsType, Parser<M> parser,
                                                LinkedOptionsEncoder encoder, String context)
            throws OptionEncodingException {
        try {
            return parser.parseFrom(encoder.encode(options, optionsType, context));
        } catch (InvalidProtocolBufferException e) {
            throw new OptionEncodingException(context + ": re-encoded options do not parse as "
                    + optionsType, e);
        }
    }

    private static <B> Map<String, B> indexByName(List<B> builders, Function<B, String> name) {
        Map<String, B> byName = HashMap.newHashMap(builders.size());
        for (B builder : builders) {
            byName.put(name.apply(builder), builder);
        }
        return byName;
    }

    private static <B> B require(Map<String, B> byName, String name, String kind, String path)
            throws OptionEncodingException {
        B builder = byName.get(name);
        if (builder == null) {
            throw new OptionEncodingException("encoded descriptor for " + path + " has no " + kind
                    + " named " + name + " — encoder/model mismatch");
        }
        return builder;
    }
}
