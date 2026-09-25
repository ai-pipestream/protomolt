package ai.protomolt.proto.serve;

import ai.protomolt.proto.receipt.RecordVerifier;
import ai.protomolt.proto.receipt.TrustSnapshot;
import ai.protomolt.proto.receipt.WorkRecords;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/** Exports one starter task and its verified local artifact bytes to a ZIP on stdout. */
public final class StarterRecordExport {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final int MAX_BYTES = 64 * 1024 * 1024;

    private StarterRecordExport() { }

    /** Run inside the starter's serve container; no credential enters the command line. */
    public static void main(String[] args) throws Exception {
        if (args.length != 1) throw new IllegalArgumentException("usage: StarterRecordExport <task-uuid>");
        String taskId = UUID.fromString(args[0]).toString();
        HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        String token = Files.readString(Path.of("/run/console/token")).strip();
        HttpResponse<InputStream> login = client.send(HttpRequest.newBuilder(
                        URI.create("http://127.0.0.1:8080/api/task-session"))
                .timeout(Duration.ofSeconds(30)).header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(JSON.createObjectNode().put("token", token).toString()))
                .build(), HttpResponse.BodyHandlers.ofInputStream());
        try (InputStream body = login.body()) {
            if (login.statusCode() != 200) throw new IllegalStateException("starter console login failed");
        }
        String cookie = login.headers().firstValue("set-cookie")
                .orElseThrow(() -> new IllegalStateException("console returned no session cookie")).split(";", 2)[0];
        try {
            HttpResponse<InputStream> response = client.send(HttpRequest.newBuilder(
                            URI.create("http://127.0.0.1:8080/api/tasks/" + taskId + "/record"))
                    .timeout(Duration.ofSeconds(30)).header("Cookie", cookie)
                    .POST(HttpRequest.BodyPublishers.ofString("{}")).build(),
                    HttpResponse.BodyHandlers.ofInputStream());
            try (InputStream body = response.body()) {
                byte[] bytes = body.readNBytes(MAX_BYTES + 1);
                if (bytes.length > MAX_BYTES) throw new IllegalStateException("record export exceeds 64 MiB");
                JsonNode snapshot = JSON.readTree(bytes);
                if (response.statusCode() != 200) throw new IllegalStateException(
                        "record export refused: " + snapshot.path("error").asText());
                writeSnapshot(snapshot, Files.readAllBytes(Path.of("/run/identity/trust.binpb")),
                        Path.of("/data/evidence"), System.out);
            }
        } finally {
            client.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:8080/api/task-session"))
                            .timeout(Duration.ofSeconds(10)).header("Cookie", cookie).DELETE().build(),
                    HttpResponse.BodyHandlers.discarding());
        }
    }

    static void writeSnapshot(JsonNode snapshot, byte[] trustBytes, Path evidence,
                              OutputStream output) throws Exception {
        byte[] record = Base64.getDecoder().decode(snapshot.path("recordBase64").asText());
        byte[] transcript = Base64.getDecoder().decode(snapshot.path("transcriptBase64").asText());
        TrustSnapshot trust = TrustSnapshot.parseFrom(trustBytes);
        var authenticated = RecordVerifier.verify(record, trust);
        if (!authenticated.verified()) {
            throw new IllegalArgumentException("record signature/trust refused: " + authenticated.refusal());
        }
        // Authenticate references before reading worker-controlled files. Never follow links.
        Map<String, byte[]> artifacts = new LinkedHashMap<>();
        String transcriptDigest = WorkRecords.sha256Hex(transcript);
        boolean transcriptReferenced = false;
        long total = record.length + (long) transcript.length + trustBytes.length;
        for (var reference : authenticated.manifest().getArtifactsList()) {
            String digest = reference.getSha256();
            if (artifacts.containsKey(digest)) continue;
            byte[] bytes;
            if (digest.equals(transcriptDigest)) {
                bytes = transcript;
                transcriptReferenced = true;
            } else {
                if (reference.getSizeBytes() > MAX_BYTES || total + reference.getSizeBytes() > MAX_BYTES) {
                    throw new IllegalArgumentException("artifact export exceeds 64 MiB");
                }
                Path source = evidence.resolve(digest);
                if (!Files.isRegularFile(source, LinkOption.NOFOLLOW_LINKS)) {
                    throw new IllegalArgumentException("missing local artifact: " + digest);
                }
                try (InputStream input = Files.newInputStream(source, LinkOption.NOFOLLOW_LINKS)) {
                    bytes = input.readNBytes((int) Math.min(MAX_BYTES, reference.getSizeBytes()) + 1);
                }
                total += bytes.length;
            }
            artifacts.put(digest, bytes);
        }
        if (!transcriptReferenced) throw new IllegalArgumentException("exported transcript is not referenced by the record");
        var complete = RecordVerifier.verify(record, trust, artifacts);
        if (!complete.verified()) {
            throw new IllegalArgumentException("artifact verification refused: " + complete.refusal());
        }
        // No output until every reference has authenticated and rehashed successfully.
        try (ZipOutputStream zip = new ZipOutputStream(output)) {
            put(zip, "record.binpb", record);
            put(zip, "trust.binpb", trustBytes);
            for (var entry : artifacts.entrySet()) put(zip, "artifacts/" + entry.getKey(), entry.getValue());
        }
    }

    private static void put(ZipOutputStream zip, String name, byte[] bytes) throws Exception {
        ZipEntry entry = new ZipEntry(name);
        entry.setTime(0);
        zip.putNextEntry(entry);
        zip.write(bytes);
        zip.closeEntry();
    }
}
