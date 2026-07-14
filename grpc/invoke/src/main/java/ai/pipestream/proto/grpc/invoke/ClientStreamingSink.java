package ai.pipestream.proto.grpc.invoke;

import com.google.protobuf.DynamicMessage;
import io.grpc.Status;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;

import java.util.concurrent.CountDownLatch;

/**
 * Response side of {@link DynamicGrpcCalls#callClientStreaming}: tracks transport readiness so
 * the sender can honor flow control, and blocks for the single response.
 */
final class ClientStreamingSink {

    private final Object readiness = new Object();
    private final CountDownLatch done = new CountDownLatch(1);
    private volatile ClientCallStreamObserver<DynamicMessage> call;
    private volatile DynamicMessage response;
    private volatile Status failure;

    ClientResponseObserver<DynamicMessage, DynamicMessage> observer() {
        return new ClientResponseObserver<>() {
            @Override
            public void beforeStart(ClientCallStreamObserver<DynamicMessage> requestStream) {
                call = requestStream;
                requestStream.setOnReadyHandler(() -> {
                    synchronized (readiness) {
                        readiness.notifyAll();
                    }
                });
            }

            @Override
            public void onNext(DynamicMessage value) {
                response = value;
            }

            @Override
            public void onError(Throwable t) {
                failure = Status.fromThrowable(t);
                done.countDown();
                synchronized (readiness) {
                    readiness.notifyAll();
                }
            }

            @Override
            public void onCompleted() {
                done.countDown();
                synchronized (readiness) {
                    readiness.notifyAll();
                }
            }
        };
    }

    /** Blocks until the transport can absorb another message (or the call has ended). */
    void awaitReady() {
        ClientCallStreamObserver<DynamicMessage> active = call;
        if (active == null) {
            return;
        }
        synchronized (readiness) {
            while (!active.isReady() && done.getCount() > 0) {
                try {
                    readiness.wait(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    /** Blocks for the response; rethrows the call's failure. */
    DynamicMessage awaitResponse() {
        try {
            done.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Status.CANCELLED.withDescription("interrupted awaiting the response")
                    .asRuntimeException();
        }
        Status status = failure;
        if (status != null) {
            throw status.asRuntimeException();
        }
        DynamicMessage value = response;
        if (value == null) {
            throw Status.INTERNAL.withDescription("call completed without a response")
                    .asRuntimeException();
        }
        return value;
    }
}
