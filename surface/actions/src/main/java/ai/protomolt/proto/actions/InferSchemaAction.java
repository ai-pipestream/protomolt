package ai.protomolt.proto.actions;

import ai.protomolt.proto.shapes.SchemaInferrer;
import ai.protomolt.proto.shapes.ShapeSynthesizer;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * Struct-to-proto: reverse-engineers a message type from data-rich JSON samples and returns
 * it exactly like the other shape verbs — registrable proto source plus the linked
 * descriptor set.
 */
final class InferSchemaAction implements ProtoAction {

    @Override
    public String name() {
        return "infer-schema";
    }

    @Override
    public String requiredScope() {
        return Scopes.SCHEMA_READ;
    }

    @Override
    public String description() {
        return "Infers a proto definition from one or more JSON sample documents: objects "
                + "become nested messages, arrays become repeated fields with element "
                + "inference, numbers become int64 when integral across every sample and "
                + "double otherwise, and anything genuinely dynamic (mixed types, empty "
                + "arrays, null-only keys) falls back to google.protobuf.Value. Keys are "
                + "sanitized to field names with json_name preserving the original, so the "
                + "inferred schema parses the very documents it came from. Returns proto "
                + "source and a descriptor set, ready to register or feed other verbs.";
    }

    @Override
    public Descriptor requestType() {
        return CatalogContract.request("InferSchemaRequest");
    }

    @Override
    public Descriptor responseType() {
        return CatalogContract.response("InferSchemaResponse");
    }

    @Override
    public Message execute(Message input, ActionContext context) throws ActionException {
        String name = Fields.string(input, "name");
        // Samples are structures, and the message requires at least one, so what arrives
        // here is a list of documents that already parsed.
        List<Struct> samples = new ArrayList<>();
        for (Message sample : Fields.<Message>list(input, "samples")) {
            samples.add(CatalogContract.as(sample, Struct.getDefaultInstance(), name()));
        }
        ShapeSynthesizer.SynthesizedShape shape;
        try {
            shape = new SchemaInferrer().infer(name, samples);
        } catch (IllegalArgumentException e) {
            throw Inputs.invalidInput(e.getMessage(), "/samples");
        }
        return Reply.of(responseType())
                .set("type", shape.type().getFullName())
                .set("file", shape.file().getName())
                .set("protoSource", shape.protoSource())
                .set("descriptorSetBase64",
                        Base64.getEncoder().encodeToString(shape.descriptorSet().toByteArray()))
                .build();
    }
}
