package ai.protomolt.proto.repo.archive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.repo.sink.v1.PublishRepositorySinkRequest;
import ai.protomolt.proto.repo.sink.v1.SinkDestination;
import ai.protomolt.proto.repo.archive.v1.RenditionDescriptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

/** A publication with no conversion writes protobuf, and may not be labelled as anything else. */
class RepositorySinkRequestsTest {

    private static PublishRepositorySinkRequest directPublication(String mediaType) {
        return PublishRepositorySinkRequest.newBuilder()
                .setDestination(SinkDestination.newBuilder()
                        .setRendition(RenditionDescriptor.newBuilder()
                                .setName("state")
                                .setMediaType(mediaType)))
                .build();
    }

    @Test
    void directPublicationRefusesANonProtobufDestinationMediaType() {
        for (String mediaType : new String[] {"application/pdf", "text/markdown"}) {
            assertThatThrownBy(() -> RepositorySinkRequests.validateDestinationRenditionRules(
                            directPublication(mediaType)))
                    .as(mediaType)
                    .isInstanceOfSatisfying(StatusRuntimeException.class, refused -> {
                        assertThat(refused.getStatus().getCode())
                                .isEqualTo(Status.Code.INVALID_ARGUMENT);
                        assertThat(refused.getStatus().getDescription())
                                .contains("destination.rendition.media_type");
                    });
        }
    }

    @Test
    void directPublicationAcceptsAnEmptyOrProtobufDestinationMediaType() {
        for (String mediaType : new String[] {"", RepositorySink.PROTOBUF_MEDIA_TYPE}) {
            assertThatCode(() -> RepositorySinkRequests.validateDestinationRenditionRules(
                            directPublication(mediaType)))
                    .as("'%s'", mediaType)
                    .doesNotThrowAnyException();
        }
    }
}
