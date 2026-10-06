package ai.protomolt.proto.repo.container.ledger;

import com.google.protobuf.ByteString;

/** Fixed-size preparation digests plus the complete immutable handoff identity. */
record DocumentSuccessorFingerprint(RepositoryCoordinatorHandoff.Proposal handoff,
        ByteString previous, ByteString next, ByteString modes) {
    static DocumentSuccessorFingerprint of(RepositorySuccessorInstall.Plan plan) {
        return new DocumentSuccessorFingerprint(plan.handoff(),
                digest(DocumentPublicationPreparationCodec.encode(plan.previous())),
                digest(DocumentPublicationPreparationCodec.encode(plan.next())),
                digest(ByteString.copyFromUtf8(RepositorySuccessorInstall.encodeModes(plan))));
    }

    private static ByteString digest(ByteString bytes) {
        return ByteString.copyFrom(DocumentPublicationPreparationJournal.digest(bytes));
    }

    @Override public String toString() { return "SuccessorFingerprint[private]"; }
}
