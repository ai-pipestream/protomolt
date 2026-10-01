package ai.protomolt.proto.samples;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.protomolt.proto.samples.authoring.v1.NormalizeTextRequest;
import ai.protomolt.proto.samples.authoring.v1.NormalizeTextResponse;
import ai.protomolt.proto.samples.authoring.v1.WriteRecordRequest;
import ai.protomolt.proto.samples.authoring.v1.WriteRecordResponse;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;


/** Runtime and durable-write behavior for the external authoring fixture service. */
class AuthoringFixtureServiceTest {
    private static final String ID = "aa123456-1234-4234-8234-123456789abc";

    @TempDir Path temporaryDirectory;
    private Server server;
    private ManagedChannel channel;

    @AfterEach
    void stopServer() throws Exception {
        if (channel != null) {
            channel.shutdownNow();
            channel.awaitTermination(5, TimeUnit.SECONDS);
        }
        if (server != null) {
            server.shutdownNow();
            server.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void normalizeTextAppliesOnlyDocumentedTransformationsInOrder() throws Exception {
        FixtureClient client = startTcp(new FileSystemFixtureRecordRepository(temporaryDirectory));

        assertEquals("Alpha\nBeta", client.normalize(" \tAlpha\r\nBeta\t ").getText());
        assertEquals("A\rB", client.normalize(" A\rB ").getText());
        assertEquals("\u00a0Élodie\u00a0", client.normalize("\u00a0Élodie\u00a0").getText());
        assertEquals("first  second", client.normalize("first  second").getText());
    }

    @Test
    void invalidNormalizeAndWriteRequestsHaveNoStoredEffects() throws Exception {
        FixtureClient client = startTcp(new FileSystemFixtureRecordRepository(temporaryDirectory));

        assertStatus(Status.Code.INVALID_ARGUMENT,
                () -> client.normalize(" \t\r\n "));
        assertStatus(Status.Code.INVALID_ARGUMENT,
                () -> client.write(WriteRecordRequest.getDefaultInstance()));
        assertTrue(new FileSystemFixtureRecordRepository(temporaryDirectory).find(ID).isEmpty());
        try (var files = Files.list(temporaryDirectory)) {
            assertFalse(files.anyMatch(path -> path.getFileName().toString().endsWith(".pb")));
        }
    }

    @Test
    void exactRetryReturnsPersistedResponseAndChangedContentConflicts() throws Exception {
        FileSystemFixtureRecordRepository repository = new FileSystemFixtureRecordRepository(temporaryDirectory);
        FixtureClient client = startTcp(repository);
        WriteRecordRequest request = request(ID, "exact bytes\né");

        WriteRecordResponse first = client.write(request);
        WriteRecordResponse retry = client.write(request);
        assertEquals(first, retry);
        assertEquals(ID, first.getOperationId());
        assertEquals(sha256(request.getContentBytes().toByteArray()), first.getContentSha256());
        assertEquals(request, repository.find(ID).orElseThrow().getRequest());
        assertStatus(Status.Code.ALREADY_EXISTS, () -> client.write(request(ID, "changed content")));
        assertEquals(request, repository.find(ID).orElseThrow().getRequest());
    }

    @Test
    void concurrentEqualWritesShareOneStoredResult() throws Exception {
        FileSystemFixtureRecordRepository repository = new FileSystemFixtureRecordRepository(temporaryDirectory);
        FixtureClient client = startTcp(repository);
        assertConcurrentWrites(client, request(ID, "same exact content"), request(ID, "same exact content"));
        assertEquals("same exact content", repository.find(ID).orElseThrow().getRequest().getContent());
    }

    @Test
    void concurrentDifferentWritesHaveOneWinnerAndOneConflict() throws Exception {
        FileSystemFixtureRecordRepository repository = new FileSystemFixtureRecordRepository(temporaryDirectory);
        FixtureClient client = startTcp(repository);
        WriteRecordRequest left = request(ID, "content alpha");
        WriteRecordRequest right = request(ID, "content beta");
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            var first = executor.submit(() -> concurrentWrite(client, left, ready, start));
            var second = executor.submit(() -> concurrentWrite(client, right, ready, start));
            assertTrue(ready.await(5, TimeUnit.SECONDS));
            start.countDown();
            List<Object> results = List.of(first.get(10, TimeUnit.SECONDS), second.get(10, TimeUnit.SECONDS));
            assertEquals(1, results.stream().filter(WriteRecordResponse.class::isInstance).count());
            assertEquals(1, results.stream().filter(Status.Code.class::isInstance).count());
            assertTrue(results.contains(Status.Code.ALREADY_EXISTS));
            String persisted = repository.find(ID).orElseThrow().getRequest().getContent();
            assertTrue(persisted.equals(left.getContent()) || persisted.equals(right.getContent()));
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void restartServesThePreviouslyCommittedResponse() throws Exception {
        WriteRecordRequest request = request(ID, "survives service restart");
        WriteRecordResponse original;
        {
            FixtureClient firstProcess = startTcp(new FileSystemFixtureRecordRepository(temporaryDirectory));
            original = firstProcess.write(request);
            stopCurrentServer();
        }

        FixtureClient restarted = startTcp(new FileSystemFixtureRecordRepository(temporaryDirectory));
        assertEquals(original, restarted.write(request));
        assertStatus(Status.Code.ALREADY_EXISTS, () -> restarted.write(request(ID, "different")));
    }

    @Test
    void corruptStoredRecordFailsClosed() throws Exception {
        FileSystemFixtureRecordRepository repository = new FileSystemFixtureRecordRepository(temporaryDirectory);
        FixtureClient client = startTcp(repository);
        client.write(request(ID, "committed"));
        Files.write(temporaryDirectory.resolve(ID + ".pb"), new byte[] {0x0f, 0x00, 0x7f});

        assertStatus(Status.Code.INTERNAL, () -> client.write(request(ID, "committed")));
        assertEquals(3, Files.size(temporaryDirectory.resolve(ID + ".pb")));
    }

    @Test
    void failureBeforePublishNeverReturnsSuccessOrLeavesARecord() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        FileSystemFixtureRecordRepository repository = new FileSystemFixtureRecordRepository(
                temporaryDirectory, () -> {
                    attempts.incrementAndGet();
                    throw new IOException("injected pre-publish failure");
                });
        FixtureClient client = startTcp(repository);

        assertStatus(Status.Code.INTERNAL, () -> client.write(request(ID, "must not be acknowledged")));
        assertEquals(1, attempts.get());
        assertTrue(repository.find(ID).isEmpty());
        assertFalse(Files.exists(temporaryDirectory.resolve(ID + ".pb")));
        try (var files = Files.list(temporaryDirectory)) {
            assertFalse(files.anyMatch(path -> path.getFileName().toString().endsWith(".tmp")));
        }
    }

    @Test
    void uncertainReplyAfterPublishIsRecoveredFromStoredRecord() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        FileSystemFixtureRecordRepository repository = new FileSystemFixtureRecordRepository(
                temporaryDirectory, () -> {}, () -> {
                    if (attempts.incrementAndGet() == 1) {
                        throw new IOException("injected directory sync failure");
                    }
                });
        FixtureClient client = startTcp(repository);
        WriteRecordRequest request = request(ID, "committed before reply failure");

        assertStatus(Status.Code.INTERNAL, () -> client.write(request));
        assertTrue(Files.exists(temporaryDirectory.resolve(ID + ".pb")));
        stopCurrentServer();

        FixtureClient restarted = startTcp(new FileSystemFixtureRecordRepository(temporaryDirectory));
        WriteRecordResponse recovered = restarted.write(request);
        assertEquals(ID, recovered.getOperationId());
        assertEquals(sha256(request.getContentBytes().toByteArray()), recovered.getContentSha256());
        assertEquals(request, new FileSystemFixtureRecordRepository(temporaryDirectory)
                .find(ID).orElseThrow().getRequest());
    }

    private FixtureClient startTcp(FileSystemFixtureRecordRepository repository) throws Exception {
        server = ServerBuilder.forPort(0).addService(new AuthoringFixtureService(repository)).build().start();
        channel = io.grpc.ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort())
                .usePlaintext().build();
        return new FixtureClient(channel);
    }

    private void stopCurrentServer() throws Exception {
        channel.shutdownNow();
        channel.awaitTermination(5, TimeUnit.SECONDS);
        channel = null;
        server.shutdownNow();
        server.awaitTermination(5, TimeUnit.SECONDS);
        server = null;
    }

    private static Object concurrentWrite(FixtureClient client, WriteRecordRequest request,
            CountDownLatch ready, CountDownLatch start) throws Exception {
        ready.countDown();
        if (!start.await(5, TimeUnit.SECONDS)) throw new AssertionError("write start timed out");
        try {
            return client.write(request);
        } catch (StatusRuntimeException e) {
            return e.getStatus().getCode();
        }
    }

    private static void assertConcurrentWrites(FixtureClient client, WriteRecordRequest a,
            WriteRecordRequest b) throws Exception {
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            var first = executor.submit(() -> concurrentWrite(client, a, ready, start));
            var second = executor.submit(() -> concurrentWrite(client, b, ready, start));
            assertTrue(ready.await(5, TimeUnit.SECONDS));
            start.countDown();
            Object firstResult = first.get(10, TimeUnit.SECONDS);
            Object secondResult = second.get(10, TimeUnit.SECONDS);
            assertInstanceOf(WriteRecordResponse.class, firstResult);
            assertEquals(firstResult, secondResult);
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private static WriteRecordRequest request(String id, String content) {
        return WriteRecordRequest.newBuilder().setOperationId(id)
                .setContentBytes(ByteString.copyFrom(content, StandardCharsets.UTF_8)).build();
    }

    private static String sha256(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }

    private static void assertStatus(Status.Code code, ThrowingRunnable action) {
        StatusRuntimeException error = assertThrows(StatusRuntimeException.class, action::run);
        assertEquals(code, error.getStatus().getCode());
    }

    @FunctionalInterface
    private interface ThrowingRunnable { void run() throws Exception; }

    private static final class FixtureClient {
        private final io.grpc.ManagedChannel channel;

        private FixtureClient(ManagedChannel channel) { this.channel = channel; }

        private NormalizeTextResponse normalize(String text) {
            return ai.protomolt.proto.samples.authoring.v1.AuthoringFixtureServiceGrpc
                    .newBlockingStub(channel).normalizeText(NormalizeTextRequest.newBuilder().setText(text).build());
        }

        private WriteRecordResponse write(WriteRecordRequest request) {
            return ai.protomolt.proto.samples.authoring.v1.AuthoringFixtureServiceGrpc
                    .newBlockingStub(channel).writeRecord(request);
        }
    }
}
