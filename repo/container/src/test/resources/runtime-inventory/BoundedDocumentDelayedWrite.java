package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import java.lang.reflect.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/** Arms the parent transport proxy for the next exact upload; delegates the real PUT unchanged. */
final class BoundedDocumentDelayedWrite {
    private final AtomicBoolean armed=new AtomicBoolean();
    private volatile BlobStore.PutSpec written;
    private volatile byte[] bytes;
    void arm() { armed.set(true); }
    BlobStore wrap(BlobStore actual,String prefix) {
        return (BlobStore)Proxy.newProxyInstance(BlobStore.class.getClassLoader(),new Class<?>[]{BlobStore.class},
                (proxy,method,args) -> {
                    if (method.getName().equals("put") && armed.compareAndSet(true,false)) {
                        written=(BlobStore.PutSpec)args[0];
                        if (!(args[1] instanceof byte[] body)) throw new AssertionError("Expected bounded byte upload");
                        bytes=body.clone();
                        var encoder=Base64.getUrlEncoder().withoutPadding();
                        String physical="protomolt:redis:v3-create-only:"
                                +encoder.encodeToString(prefix.getBytes(StandardCharsets.UTF_8))+":"
                                +encoder.encodeToString(written.bucket().getBytes(StandardCharsets.UTF_8))+":"
                                +encoder.encodeToString(written.key().getBytes(StandardCharsets.UTF_8));
                        Path control=Path.of(System.getenv("PROTOMOLT_TEST_DELAYED_CONTROL"));
                        Path temporary=Files.createTempFile(control,"arm-",".tmp");
                        Files.writeString(temporary,physical);
                        Files.move(temporary,control.resolve("arm"),StandardCopyOption.ATOMIC_MOVE);
                    }
                    try { return method.invoke(actual,args); }
                    catch (InvocationTargetException failure) { throw failure.getCause(); }
                });
    }
    void assertAbsent(BlobStore store) {
        if (written==null || armed.get()) throw new AssertionError("Delayed upload did not reach provider");
        try { store.getBounded(written.bucket(),written.key(),null,1024*1024); throw new AssertionError("Delayed bytes arrived early"); }
        catch (BlobStore.BlobNotFoundException expected) { }
    }
    void save(Path directory) throws Exception {
        if (written==null || bytes==null) throw new AssertionError("No delayed upload captured");
        var values=new Properties();
        values.setProperty("namespace",written.bucket()); values.setProperty("key",written.key());
        values.setProperty("sha256",written.sha256Hex());
        Files.write(directory.resolve("delayed-put-body.bin"),bytes);
        try (var output=Files.newOutputStream(directory.resolve("delayed-put.properties"))) { values.store(output,"Original delayed request"); }
    }
    void assertDistinct(ai.protomolt.proto.repo.v1.PublishDocumentRequest... committed) {
        String digest=ai.protomolt.proto.repo.codec.DocumentPartCodec.sha256Hex(bytes);
        for (var request:committed) for (var payload:request.getPayloadsList()) {
            if (digest.equals(ai.protomolt.proto.repo.codec.DocumentPartCodec.sha256Hex(payload.getContent().toByteArray())))
                throw new AssertionError("Delayed payload equals committed bytes");
        }
    }
    static boolean timedOut(Throwable failure) {
        for (Throwable cause=failure;cause!=null;cause=cause.getCause())
            if (cause instanceof java.net.SocketTimeoutException) return true;
        return false;
    }
}
