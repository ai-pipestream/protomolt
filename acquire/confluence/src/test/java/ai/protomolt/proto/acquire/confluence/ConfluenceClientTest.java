package ai.protomolt.proto.acquire.confluence;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Client behavior against the fake: basic auth goes out, query params are
 * encoded, cursor pagination follows {@code _links.next} across pages, 429 is
 * retried after {@code Retry-After}, and attachment bytes download from a
 * relative URL.
 */
class ConfluenceClientTest {

    private FakeConfluenceServer fake;
    private ConfluenceClient client;

    @BeforeEach
    void startFake() throws Exception {
        fake = FakeConfluenceServer.start();
        // No politeness gap in tests.
        client = new ConfluenceClient(fake.baseUrl(), "bot@pipestream.ai", "token-123",
                Duration.ZERO);
    }

    @AfterEach
    void stopFake() {
        fake.close();
    }

    @Test
    void sendsBasicAuthAndAcceptHeaders() throws Exception {
        fake.stub("/wiki/api/v2/spaces", ConfluenceFixtures.emptyListJson());

        client.get("/api/v2/spaces");

        List<FakeConfluenceServer.RecordedRequest> requests = fake.requestsTo("/wiki/api/v2/spaces");
        assertThat(requests).hasSize(1);
        String expected = "Basic " + Base64.getEncoder()
                .encodeToString("bot@pipestream.ai:token-123".getBytes());
        assertThat(requests.get(0).authorization()).isEqualTo(expected);
    }

    @Test
    void encodesQueryParams() throws Exception {
        fake.stub("/wiki/api/v2/pages", ConfluenceFixtures.emptyListJson());

        client.get("/api/v2/pages", Map.of("space-id", "100", "title", "a b&c"));

        assertThat(fake.requestsTo("/wiki/api/v2/pages").get(0).query())
                .contains("space-id=100")
                .contains("title=a+b%26c");
    }

    @Test
    void paginatesAcrossThreePagesViaLinksNext() throws Exception {
        fake.stub("/wiki/api/v2/pages",
                ConfluenceFixtures.pageListJson("c2",
                        ConfluenceFixtures.pageJson("1", "100", "One", "2024-03-01T00:00:00Z")));
        fake.stub("/wiki/api/v2/pages?cursor=c2",
                ConfluenceFixtures.pageListJson("c3",
                        ConfluenceFixtures.pageJson("2", "100", "Two", "2024-03-02T00:00:00Z")));
        fake.stub("/wiki/api/v2/pages?cursor=c3",
                ConfluenceFixtures.pageListJson(null,
                        ConfluenceFixtures.pageJson("3", "100", "Three", "2024-03-03T00:00:00Z")));

        ConfluenceClient.ResultPage first = client.getPage("/api/v2/pages", Map.of());
        assertThat(first.body().path("results").get(0).path("id").asText()).isEqualTo("1");
        assertThat(first.nextUrl()).endsWith("/wiki/api/v2/pages?cursor=c2");

        ConfluenceClient.ResultPage second = client.getPage(first.nextUrl(), Map.of());
        assertThat(second.body().path("results").get(0).path("id").asText()).isEqualTo("2");
        assertThat(second.nextUrl()).endsWith("cursor=c3");

        ConfluenceClient.ResultPage third = client.getPage(second.nextUrl(), Map.of());
        assertThat(third.body().path("results").get(0).path("id").asText()).isEqualTo("3");
        assertThat(third.nextUrl()).isNull();

        assertThat(fake.requestsTo("/wiki/api/v2/pages")).hasSize(3);
    }

    @Test
    void retries429AfterRetryAfter() throws Exception {
        fake.stubOnce("/wiki/api/v2/spaces", new FakeConfluenceServer.Stub(429,
                "{\"message\":\"rate limited\"}", Map.of("Retry-After", "0")));
        fake.stub("/wiki/api/v2/spaces", ConfluenceFixtures.emptyListJson());

        JsonNode body = client.get("/api/v2/spaces");

        assertThat(body.path("results").isArray()).isTrue();
        assertThat(fake.requestsTo("/wiki/api/v2/spaces")).hasSize(2);
    }

    @Test
    void failsAfterExhaustingThrottleRetries() {
        for (int i = 0; i < 6; i++) {
            fake.stubOnce("/wiki/api/v2/spaces", new FakeConfluenceServer.Stub(429,
                    "{}", Map.of("Retry-After", "0")));
        }

        assertThatThrownBy(() -> client.get("/api/v2/spaces"))
                .isInstanceOf(ConfluenceClient.ConfluenceApiException.class)
                .satisfies(e -> assertThat(
                        ((ConfluenceClient.ConfluenceApiException) e).status()).isEqualTo(429));
    }

    @Test
    void surfacesApiErrorsWithStatus() {
        fake.stub("/wiki/api/v2/pages/999",
                new FakeConfluenceServer.Stub(404, "{\"message\":\"no page\"}", Map.of()));

        assertThatThrownBy(() -> client.get("/api/v2/pages/999"))
                .isInstanceOf(ConfluenceClient.ConfluenceApiException.class)
                .hasMessageContaining("404");
    }

    @Test
    void downloadsAttachmentBytesFromRelativeUrl() throws Exception {
        byte[] png = new byte[]{1, 2, 3, 4};
        fake.stub("/wiki/download/attachments/a1/diagram.png",
                new FakeConfluenceServer.Stub(200, new String(png, java.nio.charset.StandardCharsets.ISO_8859_1), Map.of()));

        byte[] bytes = client.downloadAttachmentBytes("/download/attachments/a1/diagram.png");
        assertThat(new String(bytes, java.nio.charset.StandardCharsets.ISO_8859_1))
                .isEqualTo(new String(png, java.nio.charset.StandardCharsets.ISO_8859_1));
    }
}
