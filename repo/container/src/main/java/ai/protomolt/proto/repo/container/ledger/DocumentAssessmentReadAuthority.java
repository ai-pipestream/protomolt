package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import ai.protomolt.proto.repo.v1.DocumentPublicationRejection;
import java.util.Objects;

/** Separate lifetime authorities; a terminal receipt never supplies a write fence. */
sealed interface DocumentAssessmentReadAuthority {
    void check(Tx tx, RepositoryCaller caller, DocumentPublicationCommand command,
            DocumentAssessmentReadProtection<DocumentAssessmentReadPlan> captured, RepositoryReadControl control);

    record LiveOwner(RepositoryOperationLedger.Owner owner) implements DocumentAssessmentReadAuthority {
        public LiveOwner { Objects.requireNonNull(owner); }
        @Override public void check(Tx tx, RepositoryCaller caller, DocumentPublicationCommand command,
                DocumentAssessmentReadProtection<DocumentAssessmentReadPlan> captured, RepositoryReadControl control) {
            DocumentAssessmentDeliveryAuthorization.check(tx, caller, owner, command, captured, control);
        }
    }

    record Rejected(DocumentPublicationRejection receipt) implements DocumentAssessmentReadAuthority {
        public Rejected { Objects.requireNonNull(receipt); }
        @Override public void check(Tx tx, RepositoryCaller caller, DocumentPublicationCommand command,
                DocumentAssessmentReadProtection<DocumentAssessmentReadPlan> captured, RepositoryReadControl control) {
            DocumentRejectedAssessmentReads.authorizeDelivery(tx, caller, command, receipt, captured, control);
        }
    }
}
