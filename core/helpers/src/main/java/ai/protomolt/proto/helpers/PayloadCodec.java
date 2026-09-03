package ai.protomolt.proto.helpers;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Struct;

/**
 * Pack/unpack helpers for {@link Any} and {@link Struct} payloads.
 */
public final class PayloadCodec {

    private final AnyHandler anyHandler;
    private final TypeConverter typeConverter;

    public PayloadCodec(DescriptorRegistry registry) {
        this.anyHandler = new AnyHandler(registry);
        this.typeConverter = new TypeConverter();
    }

    public PayloadCodec() {
        this(new DescriptorRegistry());
    }

    public Any pack(Message message) {
        return anyHandler.pack(message);
    }

    public Message unpack(Any any) throws InvalidProtocolBufferException {
        return anyHandler.unpack(any);
    }

    public Message unpackSafe(Any any) {
        return anyHandler.unpackSafe(any);
    }

    public Struct toStruct(Message message) {
        return typeConverter.messageToStruct(message);
    }

    public Struct unpackToStruct(Any any) throws InvalidProtocolBufferException {
        return anyHandler.unpackToStruct(any);
    }

    public AnyHandler anyHandler() {
        return anyHandler;
    }

    public TypeConverter typeConverter() {
        return typeConverter;
    }
}
