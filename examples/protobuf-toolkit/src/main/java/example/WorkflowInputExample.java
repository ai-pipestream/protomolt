package example;

import ai.protomolt.proto.projection.MessageProjection;
import ai.protomolt.proto.projection.SourceResolver;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import example.contract.Contact;
import example.contract.SourceContact;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.UUID;

/** Projects and validates application data before preparing a starter workflow input. */
public final class WorkflowInputExample {
    private WorkflowInputExample() { }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            throw new IllegalArgumentException("Expected SOURCE_JSON OPERATION_UUID OUTPUT_JSON");
        }
        String operationId = UUID.fromString(args[1]).toString();
        if (!operationId.equals(args[1])) {
            throw new IllegalArgumentException("Use a canonical lowercase operation UUID");
        }
        var source = SourceContact.newBuilder();
        JsonFormat.parser().merge(Files.readString(Path.of(args[0])), source);
        var projection = MessageProjection.forTarget(Contact.getDescriptor(),
                SourceResolver.of(SourceContact.getDescriptor())).orElseThrow();
        var projected = projection.project(source.build());
        ProtoValidator.forMessageType(Contact.getDescriptor()).validate(projected).throwIfInvalid();
        var contact = Contact.parseFrom(projected.toByteString());
        var input = Struct.newBuilder()
                .putFields("operationId", Value.newBuilder().setStringValue(operationId).build())
                .putFields("content", Value.newBuilder()
                        .setStringValue(contact.getName() + " <" + contact.getEmail() + ">").build())
                .build();
        // Never silently replace a saved operation with changed content.
        Files.writeString(Path.of(args[2]), JsonFormat.printer().print(input) + "\n",
                StandardOpenOption.CREATE_NEW);
        System.out.println("Projected contact passed runtime validation; wrote " + args[2]);
        System.out.println("The coordinator must still validate this JSON against the workflow input contract.");
    }
}
