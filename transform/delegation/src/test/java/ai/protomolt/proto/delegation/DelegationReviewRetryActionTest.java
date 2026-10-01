package ai.protomolt.proto.delegation;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.delegation.v1.RetryCandidateReviewRequest;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

class DelegationReviewRetryActionTest {
    @Test
    void authorCannotRetryAndCoordinatorStillMustSatisfyTheContract() {
        try (var coordinator = new InProcessDelegationCoordinator();
                var bridge = new DelegationBridge(coordinator)) {
            var catalog = DelegationActions.register(ActionCatalog.defaults(ActionContext.create()), bridge);
            var author = Caller.scoped("author", Set.of(Scopes.WORKFLOW_AUTHOR));
            var operator = Caller.scoped("coordinator", Set.of(Scopes.WORKER_COORDINATE));
            var malformed = RetryCandidateReviewRequest.getDefaultInstance();
            ActionException denied = catchThrowableOfType(ActionException.class,
                    () -> catalog.execute("delegation-review-retry", malformed, author));
            assertThat(denied.code()).isEqualTo("permission-denied");
            ActionException invalid = catchThrowableOfType(ActionException.class,
                    () -> catalog.execute("delegation-review-retry", malformed, operator));
            assertThat(invalid.code()).isEqualTo("invalid-input");
            assertThat(coordinator.transcript().getEntriesCount()).isZero();
        }
    }
}
