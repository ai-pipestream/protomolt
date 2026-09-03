package ai.protomolt.proto.acquire.confluence;

import ai.protomolt.proto.acquire.confluence.v1.ConfluenceServiceGrpc;
import ai.protomolt.proto.acquire.confluence.v1.ListSpacesRequest;
import ai.protomolt.proto.acquire.confluence.v1.ListSpacesResponse;
import ai.protomolt.proto.acquire.confluence.v1.Space;
import ai.protomolt.proto.validate.ProtoValidator;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * One cheap read against the real Confluence workspace, proving the facade
 * works against live Atlassian, not just the fake. Gated on credentials
 * (CONFLUENCE_EMAIL + CONFLUENCE_API_TOKEN, or the CONFLUENCE_USER /
 * CONFLUENCE_TOKEN aliases) and excluded from the default test task; run it
 * with {@code ./gradlew :protomolt-acquire-confluence:liveSmokeTest}. The
 * token is never printed.
 */
class ConfluenceLiveSmokeIT {

    private static final String DEFAULT_BASE_URL = "https://pipestreamai.atlassian.net/wiki";

    private static String credential(String canonical, String alias) {
        String value = System.getenv(canonical);
        if (value == null || value.isBlank()) {
            value = System.getenv(alias);
        }
        return value == null || value.isBlank() ? null : value;
    }

    @Test
    void listSpacesAgainstTheLiveWorkspace() throws Exception {
        String email = credential(ConfluenceConnectorConfig.ENV_EMAIL,
                ConfluenceConnectorConfig.ENV_EMAIL_ALIAS);
        String token = credential(ConfluenceConnectorConfig.ENV_API_TOKEN,
                ConfluenceConnectorConfig.ENV_API_TOKEN_ALIAS);
        assumeTrue(email != null && token != null,
                "no live Confluence credentials in the environment; skipping");
        String baseUrl = System.getenv(ConfluenceConnectorConfig.ENV_BASE_URL);
        if (baseUrl == null || baseUrl.isBlank()) {
            baseUrl = DEFAULT_BASE_URL;
        }

        ConfluenceConnectorConfig config = ConfluenceConnectorConfig.builder()
                .baseUrl(baseUrl)
                .email(email)
                .apiToken(token)
                .pageSize(10)
                .build();
        ConfluenceGrpcService service = new ConfluenceGrpcService(config,
                new ConfluenceClient(config), ConfluenceGrpcService.DEFAULT_ATTACHMENT_MAX_BYTES);
        Server server = InProcessServerBuilder.forName("confluence-live-smoke")
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .addService(service)
                .build().start();
        ManagedChannel channel = InProcessChannelBuilder.forName("confluence-live-smoke").build();
        try {
            ListSpacesResponse response = ConfluenceServiceGrpc.newBlockingStub(channel)
                    .listSpaces(ListSpacesRequest.newBuilder().setLimit(1).build());

            // Shape, not just connectivity: limit honored, identities present,
            // and the platform validation rules hold on live data.
            assertThat(response.getSpacesCount()).isEqualTo(1);
            Space space = response.getSpaces(0);
            assertThat(space.getId()).isNotBlank();
            assertThat(space.getKey()).isNotBlank();
            assertThat(ProtoValidator.create().validate(space).violations()).isEmpty();
            System.out.println("[ConfluenceLiveSmokeIT] ListSpaces limit=1 -> key="
                    + space.getKey() + " name=" + space.getName()
                    + " type=" + space.getType() + " status=" + space.getStatus());
        } finally {
            channel.shutdownNow();
            server.shutdownNow();
        }
    }
}
