package ai.protomolt.proto.samples;

import ai.protomolt.proto.samples.authoring.v1.AuthoringFixtureServiceGrpc;
import ai.protomolt.proto.samples.authoring.v1.NormalizeTextRequest;
import ai.protomolt.proto.samples.authoring.v1.NormalizeTextResponse;
import ai.protomolt.proto.samples.authoring.v1.WriteRecordRequest;
import ai.protomolt.proto.samples.authoring.v1.WriteRecordResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Objects;

/** The external unary service used by the scripted authoring demonstration. */
public final class AuthoringFixtureService
        extends AuthoringFixtureServiceGrpc.AuthoringFixtureServiceImplBase {
    private final FileSystemFixtureRecordRepository records;

    public AuthoringFixtureService(FileSystemFixtureRecordRepository records) {
        this.records = Objects.requireNonNull(records, "records");
    }

    @Override
    public void normalizeText(NormalizeTextRequest request,
            StreamObserver<NormalizeTextResponse> observer) {
        try {
            FixtureValidation.validate(request);
            String normalized = normalize(request.getText());
            NormalizeTextResponse response = NormalizeTextResponse.newBuilder()
                    .setText(normalized).build();
            FixtureValidation.validate(response);
            observer.onNext(response);
            observer.onCompleted();
        } catch (IllegalArgumentException e) {
            observer.onError(Status.INVALID_ARGUMENT
                    .withDescription("invalid normalization request or result")
                    .asRuntimeException());
        } catch (RuntimeException e) {
            observer.onError(Status.INTERNAL.withDescription("normalization failed")
                    .asRuntimeException());
        }
    }

    @Override
    public void writeRecord(WriteRecordRequest request,
            StreamObserver<WriteRecordResponse> observer) {
        try {
            WriteRecordResponse response = records.writeOrMatch(request);
            FixtureValidation.validate(response);
            observer.onNext(response);
            observer.onCompleted();
        } catch (FixtureRecordConflictException e) {
            observer.onError(Status.ALREADY_EXISTS
                    .withDescription("operation ID is bound to different content")
                    .asRuntimeException());
        } catch (IllegalArgumentException e) {
            observer.onError(Status.INVALID_ARGUMENT
                    .withDescription("invalid record request")
                    .asRuntimeException());
        } catch (IOException | RuntimeException e) {
            observer.onError(Status.INTERNAL.withDescription("record storage failed")
                    .asRuntimeException());
        }
    }

    static String normalize(String input) {
        String lines = input.replace("\r\n", "\n");
        int start = 0;
        int end = lines.length();
        while (start < end && edgeWhitespace(lines.charAt(start))) start++;
        while (end > start && edgeWhitespace(lines.charAt(end - 1))) end--;
        return lines.substring(start, end);
    }

    private static boolean edgeWhitespace(char value) {
        return value == ' ' || value == '\t' || value == '\r' || value == '\n';
    }
}
