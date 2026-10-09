package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.v1.PublicationObjectIdentity;
import ai.protomolt.proto.registry.SchemaRegistryStore;
import com.google.protobuf.ByteString;
import java.lang.reflect.*;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/** Holds only a successful real provider call; interrupts do not pretend its worker exited. */
final class ManagedHistoricalWorkerGate {
    enum Kind { PUT, GET, SCHEMA }
    final AtomicBoolean armed = new AtomicBoolean();
    final CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1), exited = new CountDownLatch(1);
    private final Kind kind;
    private volatile ByteString schema;
    private ByteString expectedSchema;
    private volatile PublicationObjectIdentity source;
    private volatile BlobStore.PutSpec written;
    private volatile BlobStore.GetResult observed;
    private BlobStore actual;

    ManagedHistoricalWorkerGate(Kind kind) { this.kind = kind; }
    void source(PublicationObjectIdentity value) { source = value; }
    BlobStore wrap(BlobStore store) {
        actual = store;
        return (BlobStore) Proxy.newProxyInstance(BlobStore.class.getClassLoader(), new Class<?>[] {BlobStore.class}, (proxy, method, args) -> {
            boolean target = kind == Kind.GET ? method.getName().equals("getBounded") && source != null
                    && source.getNamespace().equals(args[0]) && source.getObjectKey().equals(args[1])
                    && Objects.equals(source.getProviderVersion().isEmpty() ? null : source.getProviderVersion(), args[2])
                    : kind == Kind.PUT && method.getName().equals("put");
            boolean held = target && armed.compareAndSet(true, false);
            try {
                var result = method.invoke(store, args);
                if (held) {
                    if (kind == Kind.GET) observed = (BlobStore.GetResult) result;
                    else written = (BlobStore.PutSpec) args[0];
                    entered.countDown();
                    awaitRelease();
                }
                return result;
            } catch (InvocationTargetException failure) { throw failure.getCause(); }
            finally { if (held) exited.countDown(); }
        });
    }
    SchemaRegistryStore wrapRegistry(SchemaRegistryStore store, String digest, ByteString expected) {
        expectedSchema = expected;
        return (SchemaRegistryStore) Proxy.newProxyInstance(SchemaRegistryStore.class.getClassLoader(),
                new Class<?>[] {SchemaRegistryStore.class}, (proxy, method, args) -> {
                    boolean held = kind == Kind.SCHEMA && method.getName().equals("descriptorSet")
                            && digest.equals(args[0]) && armed.compareAndSet(true, false);
                    try {
                        var result = method.invoke(store, args);
                        if (held) {
                            if (!(result instanceof Optional<?> value) || value.isEmpty() || !expected.equals(value.get()))
                                throw new AssertionError("Real Git must return the selected descriptor bytes");
                            schema = (ByteString) ((Optional<?>) result).orElseThrow();
                            entered.countDown();
                            awaitRelease();
                        }
                        return result;
                    } catch (InvocationTargetException failure) { throw failure.getCause(); }
                    finally { if (held) exited.countDown(); }
                });
    }
    void verifyBytes() {
        if (kind == Kind.SCHEMA) {
            if (schema == null || !schema.equals(expectedSchema)) throw new AssertionError("Held Git bytes differ from selected schema");
        } else if (kind == Kind.GET) {
            if (observed == null || observed.data().length != source.getSizeBytes()
                    || !DocumentPartCodec.sha256Hex(observed.data()).equals(source.getSha256()))
                throw new AssertionError("Held historical GET must contain exact real source bytes");
        } else {
            if (written == null) throw new AssertionError("Actual PUT must have completed");
            var bytes = actual.getBounded(written.bucket(), written.key(), null, 1024 * 1024).data();
            if (!DocumentPartCodec.sha256Hex(bytes).equals(written.sha256Hex()))
                throw new AssertionError("Real Redis must retain the cancelled upload bytes");
        }
    }
    private void awaitRelease() {
        boolean interrupted = false;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        try {
            while (release.getCount() != 0) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) throw new IllegalStateException("Historical provider gate timed out");
                try { release.await(remaining, TimeUnit.NANOSECONDS); }
                catch (InterruptedException expected) { interrupted = true; }
            }
        } finally { if (interrupted) Thread.currentThread().interrupt(); }
    }
}
