package ai.protomolt.proto.repo.container.ledger;

/** Durable protection only; the shared local handle proves drain before calling these operations. */
sealed interface DocumentReadProtection<P> permits DocumentReadPins.Captured {
    P plan();
    void release(Tx tx);
    void recover(Tx tx);
    boolean confirmReleased(Tx tx);
}
