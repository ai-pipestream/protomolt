package ai.protomolt.proto.delegation;

import ai.protomolt.proto.delegation.v1.AcceptanceCheck;
import ai.protomolt.proto.delegation.v1.CheckEvidence;
import ai.protomolt.proto.delegation.v1.CheckVerdict;
import ai.protomolt.proto.delegation.v1.CommitReference;
import ai.protomolt.proto.delegation.v1.CompletionCandidate;
import ai.protomolt.proto.delegation.v1.TaskSpec;
import com.google.protobuf.Timestamp;

import java.util.List;

/** Custom delegation fixtures for a gRPC Java implementation job. */
final class GrpcJavaTaskFixtures {

    static final String SOURCE_CHECK = "source-claims";
    static final String BUILD_CHECK = "gradle-test";

    private GrpcJavaTaskFixtures() {
    }

    static GrpcJavaImplementationJob job() {
        TaskSpec spec = TaskSpec.newBuilder()
                .setObjective("Implement a gRPC Java coordinator with blocking event wait")
                .addAllowedScope("src/main/java/**")
                .addAllowedScope("src/test/java/**")
                .addConstraints("use virtual threads for worker execution")
                .addRequiredChecks(check(SOURCE_CHECK,
                        "reported methods and tests exist in the resulting commit"))
                .addRequiredChecks(check(BUILD_CHECK, "the module tests pass"))
                .build();
        return new GrpcJavaImplementationJob(spec,
                List.of(new JavaMethodClaim(
                        "src/main/java/example/Coordinator.java", "waitForEvent")),
                List.of(new JavaTestClaim(
                        "src/test/java/example/CoordinatorTest.java",
                        "revisesFailedCandidate")));
    }

    static CompletionCandidate result(int attempt, int revision, String commit) {
        return CompletionCandidate.newBuilder()
                .setAttempt(attempt)
                .setRevision(revision)
                .setSummary("implemented coordinator and deterministic revision test")
                .addEvidence(passed(SOURCE_CHECK))
                .addEvidence(passed(BUILD_CHECK))
                .addCommits(CommitReference.newBuilder()
                        .setRepository("fixture-repository")
                        .setCommit(commit)
                        .setSubject("implement coordinator"))
                .build();
    }

    private static AcceptanceCheck check(String name, String description) {
        return AcceptanceCheck.newBuilder()
                .setName(name)
                .setDescription(description)
                .build();
    }

    private static CheckEvidence passed(String name) {
        return CheckEvidence.newBuilder()
                .setCheckName(name)
                .setVerdict(CheckVerdict.CHECK_VERDICT_PASSED)
                .setRanAt(Timestamp.newBuilder().setSeconds(1_800_000_000L))
                .setDetail(name + " passed")
                .build();
    }

    record GrpcJavaImplementationJob(TaskSpec spec,
                                     List<JavaMethodClaim> methods,
                                     List<JavaTestClaim> tests) {
    }

    record JavaMethodClaim(String path, String method) {
    }

    record JavaTestClaim(String path, String method) {
    }
}
