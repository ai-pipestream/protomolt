package ai.pipestream.proto.grpc.invoke;

import com.google.protobuf.DynamicMessage;
import io.grpc.Status;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/** Bounded response and flow-control side of a finite dynamic bidi call. */
final class BidiStreamingSink {

    private final int maxResponses;
    private final Object readiness = new Object();
    private final Object responsesLock = new Object();
    private final CountDownLatch done = new CountDownLatch(1);
    private final List<DynamicMessage> responses = new ArrayList<>();
    private volatile ClientCallStreamObserver<DynamicMessage> call;
    private volatile Status failure;

    BidiStreamingSink(int maxResponses) {
        this.maxResponses = maxResponses;
    }

    ClientResponseObserver<DynamicMessage, DynamicMessage> observer() {
        return new ClientResponseObserver<>() {
            @Override
            public void beforeStart(ClientCallStreamObserver<DynamicMessage> requestStream) {
                call = requestStream;
                requestStream.disableAutoRequestWithInitial(maxResponses);
                requestStream.setOnReadyHandler(() -> {
                    synchronized (readiness) {
                        readiness.notifyAll();
                    }
                });
            }

            @Override
            public void onNext(DynamicMessage value) {
                boolean full;
                synchronized (responsesLock) {
                    responses.add(value);
                    full = responses.size() >= maxResponses;
                }
                if (full) {
                    ClientCallStreamObserver<DynamicMessage> active = call;
                    if (active != null) {
                        active.cancel("bounded response limit reached", null);
                    }
                    done.countDown();
                    synchronized (readiness) {
                        readiness.notifyAll();
                    }
                }
            }

            @Override
            public void onError(Throwable throwable) {
                Status status = Status.fromThrowable(throwable);
                // Cancellation after the bounded prefix is deliberate, not a transport
                // failure. The caller decides whether reaching the cap means overflow.
                synchronized (responsesLock) {
                    if (responses.size() < maxResponses) {
                        failure = status;
                    }
                }
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

    boolean isDone() {
        return done.getCount() == 0;
    }

    void awaitReady() {
        ClientCallStreamObserver<DynamicMessage> active = call;
        if (active == null) {
            return;
        }
        synchronized (readiness) {
            while (!active.isReady() && !isDone()) {
                try {
                    readiness.wait(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    failure = Status.CANCELLED
                            .withDescription("interrupted sending bidi request stream");
                    done.countDown();
                    return;
                }
            }
        }
    }

    List<DynamicMessage> awaitResponses() {
        try {
            done.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Status.CANCELLED
                    .withDescription("interrupted awaiting bidi responses")
                    .asRuntimeException();
        }
        Status status = failure;
        if (status != null) {
            throw status.asRuntimeException();
        }
        synchronized (responsesLock) {
            return List.copyOf(responses);
        }
    }
}
