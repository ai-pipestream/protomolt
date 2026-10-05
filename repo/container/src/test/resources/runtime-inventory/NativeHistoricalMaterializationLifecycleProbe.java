package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.authz.grpc.CallerContexts;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.service.DocumentHistoryMaterializationGrpcService;
import ai.protomolt.proto.repo.history.grpc.HistoricalOccurrenceClient;
import ai.protomolt.proto.repo.admission.DocumentHistoricalResponseMaterialization;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import io.grpc.*;
import io.grpc.inprocess.*;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

/** Holds a real server send after real storage/verification, never a simulated successful backend. */
public final class NativeHistoricalMaterializationLifecycleProbe {
    private static final class Gate {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicInteger successfulSends = new AtomicInteger();
        volatile ServerCall<?, ?> call;
    }

    static void run(HistoricalMaterializationRepository repository, ReadHistoricalOccurrenceRequest request,
            HistoricalMaterializationRepository.Limits limits) throws Exception {
        var budget = new PayloadBudget(32L * 1024 * 1024);
        var active = new AtomicReference<Gate>();
        var service = new DocumentHistoryMaterializationGrpcService(repository,
                caller -> new RepositoryCaller(caller.name(), false, Set.of(request.getAddress().getAccountId()), Set.of()),
                limits, budget, 1);
        ServerInterceptor gated = new ServerInterceptor() {
            public <Q, S> ServerCall.Listener<Q> interceptCall(ServerCall<Q, S> call, Metadata headers, ServerCallHandler<Q, S> next) {
                var selected = active.get();
                var observed = new ForwardingServerCall.SimpleForwardingServerCall<Q, S>(call) {
                    @Override public void sendMessage(S response) {
                        if (selected != null) {
                            selected.call = call; selected.entered.countDown();
                            try { require(selected.release.await(15, TimeUnit.SECONDS), "held send released"); }
                            catch (InterruptedException interrupted) {
                                Thread.currentThread().interrupt(); throw new IllegalStateException("held send interrupted", interrupted);
                            }
                        }
                        super.sendMessage(response);
                        if (selected != null && !call.isCancelled()) selected.successfulSends.incrementAndGet();
                    }
                };
                return Contexts.interceptCall(Context.current().withValue(CallerContexts.CALLER, Caller.scoped("owner", Set.of())),
                        observed, headers, next);
            }
        };
        String name = InProcessServerBuilder.generateName();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var server = InProcessServerBuilder.forName(name).executor(executor).intercept(gated).addService(service).build().start();
            var channel = InProcessChannelBuilder.forName(name).build();
            try {
                for (boolean deadline : new boolean[]{false, true}) {
                    var gate = new Gate(); active.set(gate);
                    var stub = DocumentHistoryMaterializationServiceGrpc.newFutureStub(channel)
                            .withDeadlineAfter(deadline ? 3 : 10, TimeUnit.SECONDS);
                    var future = stub.readHistoricalOccurrence(request);
                    try {
                        require(gate.entered.await(5, TimeUnit.SECONDS), "producer reached real send");
                        long held = budget.reservedBytes(); require(held > 0, "snapshot reserved before held send");
                        if (deadline) {
                            try { future.get(5, TimeUnit.SECONDS); throw new AssertionError("deadline returned content"); }
                            catch (ExecutionException failure) {
                                require(Status.fromThrowable(failure.getCause()).getCode() == Status.Code.DEADLINE_EXCEEDED, "deadline status");
                            }
                        } else {
                            require(future.cancel(true), "client cancellation accepted");
                            try { future.get(); throw new AssertionError("cancelled call returned content"); }
                            catch (CancellationException expected) { require(future.isCancelled(), "client observes cancellation"); }
                        }
                        await(() -> gate.call.isCancelled(), "server observed cancelled call");
                        require(budget.reservedBytes() == held, "termination cannot release a producing snapshot");
                        try {
                            DocumentHistoryMaterializationServiceGrpc.newBlockingStub(channel).withDeadlineAfter(2, TimeUnit.SECONDS)
                                    .readHistoricalOccurrence(request);
                            throw new AssertionError("cancelled producer released call slot early");
                        } catch (StatusRuntimeException full) {
                            require(full.getStatus().getCode() == Status.Code.RESOURCE_EXHAUSTED, "held producer retains call capacity");
                        }
                    } finally { active.set(null); gate.release.countDown(); }
                    await(() -> budget.reservedBytes() == 0, "snapshot released after producer exit");
                    require(gate.successfulSends.get() == 0, "cancelled held send did not succeed");
                    var retry = DocumentHistoryMaterializationServiceGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS)
                            .readHistoricalOccurrence(request);
                    require(retry.getRevisionId().equals(request.getRevisionId()), "call capacity recovered for real retry");
                    await(() -> budget.reservedBytes() == 0, "retry snapshot released");
                }
                for (int mode = 0; mode < 5; mode++) {
                    int selectedMode = mode;
                    var gate = new Gate(); active.set(gate);
                    var clientBudget = new PayloadBudget(32L * 1024 * 1024);
                    var client = new HistoricalOccurrenceClient(DocumentHistoryMaterializationServiceGrpc.newFutureStub(channel),
                            clientBudget, new DocumentHistoricalResponseMaterialization.Limits(8 * 1024 * 1024, 100_000, 100),
                            java.time.Duration.ofSeconds(mode == 1 ? 2 : 10), 1);
                    var stop = new java.util.concurrent.atomic.AtomicBoolean();
                    var worker = new AtomicReference<Thread>();
                    var interruptedAtExit = new java.util.concurrent.atomic.AtomicBoolean();
                    RuntimeException injected = mode == 4 ? capacityFailure() : new IllegalArgumentException("host control failure");
                    var control = new RepositoryReadControl() {
                        public boolean isCancelled() { return selectedMode == 0 && stop.get(); }
                        public long remainingNanos() { return Long.MAX_VALUE; }
                        @Override public void check() {
                            if (selectedMode >= 3 && stop.get()) throw injected;
                            RepositoryReadControl.super.check();
                        }
                    };
                    var reading = executor.submit(() -> {
                        worker.set(Thread.currentThread());
                        try (var result = client.read(request, control)) {
                            throw new AssertionError("held client unexpectedly returned content");
                        } finally { interruptedAtExit.set(Thread.currentThread().isInterrupted()); }
                    });
                    try {
                        require(gate.entered.await(5, TimeUnit.SECONDS), "client reached held real send");
                        if (mode == 2) worker.get().interrupt(); else if (mode != 1) stop.set(true);
                        try { reading.get(5, TimeUnit.SECONDS); throw new AssertionError("client stop ignored"); }
                        catch (ExecutionException failure) {
                            var cause = failure.getCause();
                            if (mode == 0) require(cause instanceof RepositoryException
                                    && ((RepositoryException) cause).code() == RepositoryException.Code.CANCELLED, "local cancellation preserved");
                            else if (mode >= 3) require(cause == injected, "host control failure preserved");
                            else if (mode == 2 && cause instanceof RepositoryException local)
                                require(local.code() == RepositoryException.Code.CANCELLED, "interrupted local control cancellation");
                            else require(Status.fromThrowable(cause).getCode() == (mode == 1
                                    ? Status.Code.DEADLINE_EXCEEDED : Status.Code.CANCELLED), "client terminal status");
                        }
                        if (mode == 2) require(interruptedAtExit.get(), "client preserves interrupt flag");
                        require(clientBudget.reservedBytes() == 0, "stopped client releases bytes");
                        await(() -> gate.call.isCancelled(), "stopped client cancels real call");
                    } finally { active.set(null); gate.release.countDown(); }
                    await(() -> budget.reservedBytes() == 0, "stopped producer drained");
                    try (var retry = client.read(request, RepositoryReadControl.NONE)) {
                        require(retry.view(RepositoryReadControl.NONE).response().getRevisionId().equals(request.getRevisionId()),
                                "client slot recovered after stopped read");
                    }
                    require(clientBudget.reservedBytes() == 0, "retry client bytes drained");
                    await(() -> budget.reservedBytes() == 0, "retry producer drained");
                }
            } finally {
                var remaining = active.getAndSet(null); if (remaining != null) remaining.release.countDown();
                channel.shutdownNow(); server.shutdownNow();
                require(channel.awaitTermination(5, TimeUnit.SECONDS) && server.awaitTermination(5, TimeUnit.SECONDS), "lifecycle transport drained");
            }
        }
        require(budget.reservedBytes() == 0, "lifecycle budget drained");
        System.out.println("NATIVE_HISTORICAL_MATERIALIZATION_LIFECYCLE_OK");
    }

    private static RuntimeException capacityFailure() {
        try (var ignored = new PayloadBudget(1).reserve(2)) { throw new AssertionError("expected real budget refusal"); }
        catch (PayloadBudget.CapacityExceededException failure) { return failure; }
    }

    private static void await(BooleanSupplier condition, String message) throws InterruptedException {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean() && System.nanoTime() < until) Thread.sleep(10);
        require(condition.getAsBoolean(), message);
    }
    private static void require(boolean value, String message) { if (!value) throw new AssertionError(message); }
}
