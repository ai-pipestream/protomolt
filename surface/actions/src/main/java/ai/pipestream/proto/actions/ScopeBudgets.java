package ai.pipestream.proto.actions;

import ai.pipestream.proto.actions.Caller.Budget;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * The spending ledger behind {@link Caller.Budget}: one per node, shared by every
 * enforcement point that checks scopes (the action catalog, the scope interceptor, the
 * registry's route table, the search console's login boundary), so a principal's budget
 * is one allowance across the node's transports rather than one per surface. The ledger
 * is pure mechanism — the limits ride the resolved caller, so a re-scoped policy changes
 * budgets with no rewiring, and a caller without a budget on the scope passes untouched.
 *
 * <p>Rate is a fixed one-minute window per principal and scope: admitted requests count,
 * refused ones do not, and the window resets sixty seconds after its first admitted
 * request. Payload is checked before rate, and an oversize refusal spends nothing.
 * Refusals name the principal, the scope, and the exhausted limit — never a credential.
 *
 * <p>Thread-safe. The ledger holds one small entry per (principal, scope) pair that has
 * actually spent, pruned as windows expire.
 */
public final class ScopeBudgets {

    private static final long WINDOW_MILLIS = 60_000L;

    private static final class Window {
        long startMillis;
        int admitted;
    }

    private final Map<String, Window> windows = new ConcurrentHashMap<>();
    private final LongSupplier clock;

    public ScopeBudgets() {
        this(System::currentTimeMillis);
    }

    ScopeBudgets(LongSupplier clock) {
        this.clock = clock;
    }

    /**
     * Spends one request by {@code caller} on {@code scope}, answering empty when the
     * caller is within budget and the named refusal when it is not.
     *
     * @param caller the resolved caller
     * @param scope the scope the operation requires
     * @param payloadBytes the request payload size, or a negative value on a surface
     *        that does not know it (payload caps do not apply there)
     * @return empty when admitted; the refusal message otherwise
     */
    public Optional<String> refuse(Caller caller, String scope, long payloadBytes) {
        Budget budget = caller.budgets().get(scope);
        if (budget == null) {
            return Optional.empty();
        }
        if (budget.maxPayloadBytes() > 0 && payloadBytes > budget.maxPayloadBytes()) {
            return Optional.of("caller '" + caller.name() + "' payload of " + payloadBytes
                    + " bytes exceeds its " + budget.maxPayloadBytes()
                    + "-byte budget for '" + scope + "'");
        }
        if (budget.requestsPerMinute() == 0) {
            return Optional.empty();
        }
        long now = clock.getAsLong();
        Window window = windows.computeIfAbsent(caller.name() + "\0" + scope,
                key -> new Window());
        synchronized (window) {
            if (now - window.startMillis >= WINDOW_MILLIS) {
                window.startMillis = now;
                window.admitted = 0;
            }
            if (window.admitted >= budget.requestsPerMinute()) {
                return Optional.of("caller '" + caller.name() + "' exhausted its "
                        + budget.requestsPerMinute() + "-per-minute budget for '"
                        + scope + "'");
            }
            window.admitted++;
        }
        pruneExpired(now);
        return Optional.empty();
    }

    private void pruneExpired(long now) {
        windows.entrySet().removeIf(entry -> {
            synchronized (entry.getValue()) {
                return now - entry.getValue().startMillis >= WINDOW_MILLIS;
            }
        });
    }
}
