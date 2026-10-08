package ai.protomolt.proto.repo.container.ledger;

import java.util.List;

/** Internal operation-bound SQL authority; provider workers do not acquire authority themselves. */
interface DocumentUploadAuthority {
    DocumentOperationUploadAdmission.Admission admit();
    void renewOwnerAndSelections(List<DocumentSelectedAttemptLedger.Selected> selections);
    List<DocumentPartAttemptLedger.Attempt> renewSelections(List<DocumentSelectedAttemptLedger.Selected> selections);
    void verify(DocumentSelectedAttemptLedger.Selected selection, List<DocumentSelectedAttemptLedger.Observation> observations);
    void recheckPreparation(List<DocumentSelectedAttemptLedger.Selected> selections, Runnable active);
    /** Called after every provider, heartbeat and flusher worker exits, before successful delivery. */
    void afterDrain();
}
