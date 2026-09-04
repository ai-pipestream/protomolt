package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.descriptors.DescriptorIdentity;
import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.mesh.MeshValidation;
import ai.protomolt.proto.mesh.SchemaIdentityResolver;
import ai.protomolt.proto.mesh.runtime.v1.TypedPayload;
import ai.protomolt.proto.mesh.v1.SchemaReference;
import com.google.protobuf.Any;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import java.util.Objects;

/** Exact-schema operations shared by flow compilation and processor boundaries. */
final class RuntimeSchemas {

    private RuntimeSchemas() {
    }

    static DescriptorIdentity identity(SchemaReference schema) {
        MeshValidation.validate(schema);
        return new DescriptorIdentity(
                schema.getTypeName(), schema.getDescriptorFingerprint());
    }

    static boolean same(SchemaReference first, SchemaReference second) {
        return identity(first).equals(identity(second));
    }

    static SchemaReference reference(Descriptor descriptor) {
        DescriptorIdentity identity = DescriptorIdentity.of(descriptor);
        return SchemaReference.newBuilder()
                .setTypeName(identity.typeName())
                .setDescriptorFingerprint(identity.fingerprint())
                .build();
    }

    static Descriptor resolve(DescriptorRegistry registry, SchemaReference schema) {
        return new SchemaIdentityResolver(registry).resolve(schema);
    }

    static TypedPayload pack(Message message) {
        Objects.requireNonNull(message, "message");
        return TypedPayload.newBuilder()
                .setSchema(reference(message.getDescriptorForType()))
                .setPayload(Any.pack(message))
                .build();
    }

    static DynamicMessage unpack(DescriptorRegistry registry, TypedPayload typed) {
        Objects.requireNonNull(typed, "typed");
        if (!typed.hasSchema() || !typed.hasPayload()) {
            throw new IllegalArgumentException(
                    "typed payload requires both schema and protobuf Any bytes");
        }
        Descriptor descriptor = resolve(registry, typed.getSchema());
        String typeName = typeNameOf(typed.getPayload().getTypeUrl());
        if (!typeName.equals(typed.getSchema().getTypeName())) {
            throw new IllegalArgumentException("typed payload Any names " + typeName
                    + " but its exact schema names " + typed.getSchema().getTypeName());
        }
        try {
            return DynamicMessage.parseFrom(descriptor, typed.getPayload().getValue());
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("typed payload is not valid protobuf binary for "
                    + descriptor.getFullName(), e);
        }
    }

    private static String typeNameOf(String typeUrl) {
        int slash = typeUrl.lastIndexOf('/');
        if (slash < 0 || slash == typeUrl.length() - 1) {
            throw new IllegalArgumentException(
                    "typed payload Any type URL must end with a protobuf type name: " + typeUrl);
        }
        return typeUrl.substring(slash + 1);
    }
}
