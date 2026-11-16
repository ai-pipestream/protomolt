package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.blob.spi.OpenedBlobStore;
import ai.protomolt.proto.repo.service.DriveStoreIdentities.StoreIdentity;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Per-identity single-flight pool for {@link OpenedBlobStore} handles.
 *
 * <p>Provider initialization executes outside map bucket locks and outside per-slot
 * monitors so slow opens on one backend never block unrelated resolves or pin virtual
 * threads, while concurrent opens on the same canonical {@link StoreIdentity} share
 * one initialization flight.
 */
final class SingleFlightStoreHandles {
    private final ConcurrentHashMap<StoreIdentity, HandleSlot> slots = new ConcurrentHashMap<>();

    void preseed(StoreIdentity identity, OpenedBlobStore handle) {
        slots.put(identity, HandleSlot.preseeded(handle));
    }

    HandleLease acquire(StoreIdentity identity, Supplier<OpenedBlobStore> opener) {
        while (true) {
            HandleSlot existing = slots.get(identity);
            if (existing != null) {
                HandleLease joined = existing.tryJoin(slots, identity);
                if (joined != null) {
                    return joined;
                }
                continue;
            }
            HandleSlot created = new HandleSlot();
            HandleSlot raced = slots.putIfAbsent(identity, created);
            if (raced != null) {
                HandleLease joined = raced.tryJoin(slots, identity);
                if (joined != null) {
                    return joined;
                }
                continue;
            }
            try {
                OpenedBlobStore opened = opener.get();
                created.complete(opened);
                return new HandleLease(slots, identity, created, opened, true);
            } catch (RuntimeException | Error failure) {
                created.markAborted();
                slots.remove(identity, created);
                created.fail(failure);
                throw failure;
            }
        }
    }

    void clear() {
        slots.clear();
    }

    private static final class HandleSlot {
        private final CompletableFuture<OpenedBlobStore> future;
        private OpenedBlobStore resolved;
        private int uncommittedAcquirers;
        private boolean committed;
        private boolean aborted;

        HandleSlot() {
            this.future = new CompletableFuture<>();
            this.uncommittedAcquirers = 1;
        }

        private HandleSlot(OpenedBlobStore handle) {
            this.future = CompletableFuture.completedFuture(handle);
            this.resolved = handle;
            this.committed = true;
        }

        static HandleSlot preseeded(OpenedBlobStore handle) {
            return new HandleSlot(handle);
        }

        HandleLease tryJoin(ConcurrentHashMap<StoreIdentity, HandleSlot> map, StoreIdentity identity) {
            boolean counted;
            synchronized (this) {
                if (aborted) {
                    return null;
                }
                if (committed) {
                    return new HandleLease(map, identity, this, resolved, false);
                }
                uncommittedAcquirers++;
                counted = true;
            }
            OpenedBlobStore opened = await();
            return new HandleLease(map, identity, this, opened, counted);
        }

        void complete(OpenedBlobStore handle) {
            synchronized (this) {
                this.resolved = handle;
            }
            future.complete(handle);
        }

        void fail(Throwable failure) {
            future.completeExceptionally(failure);
        }

        synchronized void markAborted() {
            this.aborted = true;
        }

        void commit(OwnedResources owned, OpenedBlobStore handle) {
            boolean register = false;
            synchronized (this) {
                if (!committed) {
                    committed = true;
                    uncommittedAcquirers = 0;
                    register = true;
                }
            }
            if (register) {
                owned.add(handle);
            }
        }

        void abort(
                ConcurrentHashMap<StoreIdentity, HandleSlot> map,
                StoreIdentity identity,
                OpenedBlobStore handle,
                Throwable cause) {
            boolean closeNow = false;
            synchronized (this) {
                if (committed) {
                    return;
                }
                uncommittedAcquirers--;
                if (uncommittedAcquirers <= 0) {
                    aborted = true;
                    map.remove(identity, this);
                    closeNow = true;
                }
            }
            if (closeNow) {
                try {
                    handle.close();
                } catch (Exception cleanup) {
                    cause.addSuppressed(cleanup);
                }
            }
        }

        private OpenedBlobStore await() {
            try {
                return future.join();
            } catch (CompletionException wrapped) {
                Throwable cause = wrapped.getCause();
                if (cause instanceof RuntimeException runtime) {
                    throw runtime;
                }
                if (cause instanceof Error error) {
                    throw error;
                }
                throw wrapped;
            }
        }
    }

    static final class HandleLease {
        private final ConcurrentHashMap<StoreIdentity, HandleSlot> map;
        private final StoreIdentity identity;
        private final HandleSlot slot;
        private final OpenedBlobStore handle;
        private boolean pendingDecision;

        HandleLease(
                ConcurrentHashMap<StoreIdentity, HandleSlot> map,
                StoreIdentity identity,
                HandleSlot slot,
                OpenedBlobStore handle,
                boolean pendingDecision) {
            this.map = map;
            this.identity = identity;
            this.slot = slot;
            this.handle = handle;
            this.pendingDecision = pendingDecision;
        }

        OpenedBlobStore handle() {
            return handle;
        }

        void commit(OwnedResources owned) {
            pendingDecision = false;
            slot.commit(owned, handle);
        }

        void abort(Throwable cause) {
            if (!pendingDecision) {
                return;
            }
            pendingDecision = false;
            slot.abort(map, identity, handle, cause);
        }
    }
}
