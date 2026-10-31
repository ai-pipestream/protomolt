package ai.protomolt.proto.repo.accession;

import static ai.protomolt.proto.repo.accession.AccessionFixture.*;
import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.authz.grpc.CallerContexts;
import ai.protomolt.proto.repo.stack.Stack;
import ai.protomolt.proto.repo.v2.*;
import io.grpc.*;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.time.Clock;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** Real HTTP/2 cancellation must unblock stack I/O without waiting for the gRPC callback to return. */
class StreamingReadCancellationIT {
    @ParameterizedTest
    @CsvSource({"typed,false", "typed,true", "opaque,false", "opaque,true", "blob,false", "blob,true"})
    void cancellationClosesBlockedStackRead(String kind, boolean deadline) throws Exception {
        try (var fixture = new AccessionFixture(); var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var account = "acct-" + UUID.randomUUID();
            var address = address(account, "cancel-read");
            boolean blob = kind.equals("blob");
            boolean opaque = kind.equals("opaque");
            if (opaque) saveOpaque(fixture, account, address);
            else fixture.saves.save(processAuthority(), create("save", address,
                    document(account, "cancel-read", "cancel")).build());
            var stored = fixture.catalog.manifest(address, 1).orElseThrow();
            var part = stored.getPartsList().stream().filter(p -> p.getEntry().getSelector().getPart()
                    == (blob ? DocumentPart.DOCUMENT_PART_BLOBS : DocumentPart.DOCUMENT_PART_CORE))
                    .findFirst().orElseThrow();
            var target = blob ? part.getBlobRefs(0).getRef() : part.getRef();
            var entered = new CountDownLatch(1);
            var closed = new CountDownLatch(1);
            var released = new CountDownLatch(1);
            var unwind = new CountDownLatch(1);
            var outstanding = new AtomicLong();
            var stack = new BlockingStack(fixture.stack, target, entered, closed, unwind, new AtomicBoolean());
            var reads = new DocumentReads(fixture.catalog,
                    new DocumentSaves.Drives("default", Map.of("default", stack)), limits(), bytes -> {
                        outstanding.addAndGet(bytes);
                        return () -> { if (outstanding.addAndGet(-bytes) == 0) released.countDown(); };
                    }, Clock.systemUTC());
            var service = new AccessionGrpcService(fixture.saves, fixture.uploads, reads, ignored -> processAuthority());
            var interceptor = new ServerInterceptor() {
                @Override public <Q, S> ServerCall.Listener<Q> interceptCall(ServerCall<Q, S> call,
                        Metadata headers, ServerCallHandler<Q, S> next) {
                    return Contexts.interceptCall(Context.current().withValue(CallerContexts.CALLER,
                            new Caller(processAuthority().principalName(), Set.of(), true)), call, headers, next);
                }
            };
            var server = NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0))
                    .executor(executor).addService(ServerInterceptors.intercept(service, interceptor)).build().start();
            var channel = NettyChannelBuilder.forAddress("127.0.0.1", server.getPort()).usePlaintext().build();
            try {
                var stub = DocumentServiceGrpc.newStub(channel);
                if (deadline) stub = stub.withDeadlineAfter(5, TimeUnit.SECONDS);
                var documentRequest = ReadDocumentRequest.newBuilder().setAddress(address).setAllowOpaque(opaque)
                        .setSelection(PartSelection.newBuilder().addSelectors(
                                PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE))).build();
                var call = new AtomicReference<ClientCallStreamObserver<?>>();
                var terminal = new CountDownLatch(1);
                var failure = new AtomicReference<Throwable>();
                if (blob) {
                    stub.readBlob(ReadBlobRequest.newBuilder().setAddress(address).setBlobId("b1").build(),
                            observer(call, terminal, failure));
                } else {
                    stub.readDocument(documentRequest,
                            observer(call, terminal, failure));
                }
                assertThat(entered.await(10, TimeUnit.SECONDS)).as("server entered the blocked provider read").isTrue();
                long expectedReservation = opaque ? 2L * DocumentStreams.CHUNK_SIZE : target.getLength();
                if (!blob) assertThat(outstanding.get()).isEqualTo(expectedReservation);
                if (!deadline) call.get().cancel("cancel while the provider read is blocked", null);
                assertThat(terminal.await(10, TimeUnit.SECONDS)).isTrue();
                assertThat(Status.fromThrowable(failure.get()).getCode())
                        .isEqualTo(deadline ? Status.Code.DEADLINE_EXCEEDED : Status.Code.CANCELLED);
                assertThat(closed.await(3, TimeUnit.SECONDS)).as("cancellation closes the blocked stream").isTrue();
                if (!blob) assertThat(outstanding.get())
                        .as("cancellation cannot release bytes still owned by the blocked reader")
                        .isEqualTo(expectedReservation);
                unwind.countDown();
                if (!blob) {
                    assertThat(released.await(3, TimeUnit.SECONDS)).as("reader unwinds and releases its reservation").isTrue();
                    assertThat(outstanding.get()).isZero();
                }
                // The same host remains usable; successful data still matches the retained object.
                var next = DocumentServiceGrpc.newBlockingStub(channel).withDeadlineAfter(10, TimeUnit.SECONDS);
                var actual = com.google.protobuf.ByteString.newOutput();
                if (blob) {
                    next.readBlob(ReadBlobRequest.newBuilder().setAddress(address).setBlobId("b1").build())
                            .forEachRemaining(frame -> { if (frame.hasData()) write(actual, frame.getData()); });
                } else {
                    var complete = new AtomicBoolean();
                    next.readDocument(documentRequest).forEachRemaining(frame -> {
                        if (frame.hasPartData()) write(actual, frame.getPartData().getData());
                        if (frame.hasComplete()) complete.set(true);
                    });
                    assertThat(complete).isTrue();
                }
                try (var expected = fixture.stack.open(target)) {
                    assertThat(actual.toByteString()).isEqualTo(com.google.protobuf.ByteString.readFrom(expected));
                }
                assertThat(outstanding.get()).isZero();
            } finally {
                // Rescue the deliberately blocked provider even when the regression assertion fails.
                closed.countDown();
                unwind.countDown();
                channel.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
                server.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
            }
        }
    }

    private static void write(com.google.protobuf.ByteString.Output output, com.google.protobuf.ByteString bytes) {
        try { bytes.writeTo(output); } catch (IOException failure) { throw new AssertionError(failure); }
    }

    private static void saveOpaque(AccessionFixture fixture, String account,
            ai.protomolt.proto.repo.v1.NodeAddress address) {
        String url = "type.test/opaque.Widget";
        var credential = UUID.randomUUID();
        fixture.catalog.registerCredential(ISSUER, credential, "chunker");
        fixture.opaqueStepGrant(account, credential, "chunker", true, "grant-producer", url);
        var doc = ai.protomolt.proto.repo.v1.Document.newBuilder().setDocId("cancel-read")
                .setOwnership(ownership(account, "chunker"))
                .setStructuredData(com.google.protobuf.Any.newBuilder().setTypeUrl(url)
                        .setValue(com.google.protobuf.ByteString.copyFromUtf8("opaque bytes"))).build();
        fixture.opaqueSaves.save(writer(account, "chunker", credential, 1), SaveDocumentRequest.newBuilder()
                .setOperationId("save").setTarget(DocumentTarget.newBuilder().setAddress(address))
                .setExpected(ExpectedRevision.newBuilder().setNone(true)).setDocument(doc)
                .addWrites(put(DocumentPart.DOCUMENT_PART_CORE)).setOwner(stepOwner("chunker"))
                .setValidationMode(ValidationMode.VALIDATION_MODE_OPAQUE_ALLOWED)
                .addOpaqueAttestations(OpaqueAttestation.newBuilder().setTypeUrl(url)
                        .setOccurrence(OccurrenceLocator.newBuilder()
                                .setPart(PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE))
                                .addSteps(ai.protomolt.proto.repo.v1.RepositorySchemaOccurrenceStep.newBuilder()
                                        .setFieldNumber(4))))
                .build());
    }

    private static <Q, S> ClientResponseObserver<Q, S> observer(
            AtomicReference<ClientCallStreamObserver<?>> call, CountDownLatch terminal,
            AtomicReference<Throwable> failure) {
        return new ClientResponseObserver<>() {
            @Override public void beforeStart(ClientCallStreamObserver<Q> stream) { call.set(stream); }
            @Override public void onNext(S value) {}
            @Override public void onError(Throwable error) { failure.set(error); terminal.countDown(); }
            @Override public void onCompleted() { terminal.countDown(); }
        };
    }

    private record BlockingStack(Stack delegate, StackRef target, CountDownLatch entered,
            CountDownLatch closed, CountDownLatch unwind, AtomicBoolean blocked) implements Stack {
        @Override public String stackId() { return delegate.stackId(); }
        @Override public StackRef put(InputStream body, long length) throws IOException { return delegate.put(body, length); }
        @Override public StackRef put(byte[] body) throws IOException { return delegate.put(body); }
        @Override public InputStream open(StackRef ref) throws IOException {
            var source = delegate.open(ref);
            if (!ref.equals(target) || !blocked.compareAndSet(false, true)) return source;
            return new java.io.FilterInputStream(source) {
                private final AtomicBoolean done = new AtomicBoolean();
                @Override public int read(byte[] bytes, int offset, int length) throws IOException {
                    entered.countDown();
                    try {
                        if (!closed.await(30, TimeUnit.SECONDS)) throw new IOException("test read timed out");
                        if (!unwind.await(30, TimeUnit.SECONDS)) throw new IOException("test unwind timed out");
                    } catch (InterruptedException error) {
                        Thread.currentThread().interrupt();
                        throw new IOException(error);
                    }
                    throw new IOException("closed blocked read");
                }
                @Override public void close() throws IOException {
                    if (done.compareAndSet(false, true)) {
                        closed.countDown();
                        source.close();
                    }
                }
            };
        }
        @Override public InputStream openRange(StackRef ref, long offset, long length) throws IOException {
            return delegate.openRange(ref, offset, length);
        }
        @Override public long head(StackRef ref) throws IOException { return delegate.head(ref); }
        @Override public void delete(StackRef ref) throws IOException { delegate.delete(ref); }
    }
}
