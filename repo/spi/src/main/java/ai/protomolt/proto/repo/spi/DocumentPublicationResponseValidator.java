package ai.protomolt.proto.repo.spi;

import ai.protomolt.proto.repo.v1.PublishDocumentResponse;
import ai.protomolt.proto.validate.ProtoValidator;
import java.util.Objects;

/** Response shape and command correspondence; durability and authorization belong to the repository. */
public final class DocumentPublicationResponseValidator {
    public static final int MAX_BYTES=DocumentPublicationResultCodec.MAX_BYTES+16;
    private static final ProtoValidator VALIDATOR=ProtoValidator.create();
    private DocumentPublicationResponseValidator() {}

    public static void requireValid(DocumentPublicationCommand command, String principal, PublishDocumentResponse response) {
        Objects.requireNonNull(command); Objects.requireNonNull(principal);
        if (response==null || response.getSerializedSize()>MAX_BYTES
                || !response.getUnknownFields().asMap().isEmpty() || !VALIDATOR.validate(response).valid())
            throw new IllegalArgumentException("Invalid publication response");
        if (response.hasCommitted()) command.requireResult(response.getCommitted(),principal,response.getCommitted().getOwnerGeneration());
        else if (response.hasRejected()) command.requireRejection(response.getRejected(),principal,response.getRejected().getOwnerGeneration());
        else throw new IllegalArgumentException("Publication outcome is absent");
    }
}
