package ai.pipestream.proto.config.registry;

import ai.pipestream.proto.config.ConfigSource;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Optional;

/**
 * The registry plug: config documents read from the git-backed registry
 * over its native HTTP surface. The registry is the writer's door — it
 * gates every put (and every get) against the registered type's declared
 * rules — and this source just reads: the served payload is the typed
 * message's bytes, the version is the git commit that last touched the
 * document, and a missing document is emptiness, never an error. GitOps
 * with typed protobuf instead of YAML.
 */
public final class RegistryConfigSource implements ConfigSource {

    private static final ObjectMapper JSON = new ObjectMapper();

    private final URI base;
    private final String apiToken;
    private final HttpClient http;

    /**
     * @param baseUrl the registry's native route prefix, e.g.
     *        {@code http://registry:8081/protomolt}
     * @param apiToken the registry's shared secret, or null when the
     *        registry runs open
     */
    public RegistryConfigSource(String baseUrl, String apiToken) {
        if (baseUrl == null || baseUrl.isBlank()) {
            throw new IllegalArgumentException("baseUrl must not be blank");
        }
        this.base = URI.create(baseUrl.endsWith("/")
                ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl);
        this.apiToken = apiToken;
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    @Override
    public Optional<Fetched> fetch(String subject) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(base + "/configs/"
                        + URLEncoder.encode(subject, StandardCharsets.UTF_8)))
                .timeout(Duration.ofSeconds(30))
                .GET();
        if (apiToken != null) {
            request.header("api_token", apiToken);
        }
        HttpResponse<String> response =
                http.send(request.build(), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() == 404) {
            return Optional.empty();
        }
        if (response.statusCode() != 200) {
            throw new IllegalStateException("the registry answered " + response.statusCode()
                    + " for config '" + subject + "': " + response.body());
        }
        JsonNode body = JSON.readTree(response.body());
        return Optional.of(new Fetched(
                body.path("version").asText(),
                Base64.getDecoder().decode(body.path("payloadBase64").asText())));
    }
}
