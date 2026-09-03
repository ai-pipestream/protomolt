package ai.protomolt.proto.delegation;

import ai.protomolt.proto.delegation.CandidateReviewer.ReviewDecision;
import ai.protomolt.proto.delegation.GrpcJavaTaskFixtures.GrpcJavaImplementationJob;
import ai.protomolt.proto.delegation.GrpcJavaTaskFixtures.JavaMethodClaim;
import ai.protomolt.proto.delegation.GrpcJavaTaskFixtures.JavaTestClaim;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/** Reviews source claims against the exact Git commit reported by a candidate. */
final class GitCommitClaimReviewer implements CandidateReviewer {

    private final Path repository;
    private final GrpcJavaImplementationJob job;

    GitCommitClaimReviewer(Path repository, GrpcJavaImplementationJob job) {
        this.repository = repository;
        this.job = job;
    }

    @Override
    public ReviewDecision review(ReviewContext context) throws Exception {
        String commit = context.candidate().getCommits(0).getCommit();
        List<String> missing = new ArrayList<>();
        for (JavaMethodClaim claim : job.methods()) {
            String source = read(commit, claim.path());
            if (!Pattern.compile("\\b" + Pattern.quote(claim.method())
                            + "\\s*\\(").matcher(source).find()) {
                missing.add(claim.path() + "#" + claim.method());
            }
        }
        for (JavaTestClaim claim : job.tests()) {
            String source = read(commit, claim.path());
            Pattern test = Pattern.compile("@Test[\\s\\S]{0,512}\\b"
                    + Pattern.quote(claim.method()) + "\\s*\\(");
            if (!test.matcher(source).find()) {
                missing.add(claim.path() + "#" + claim.method());
            }
        }
        if (!missing.isEmpty()) {
            return ReviewDecision.revise(
                    "resulting commit is missing reported source claims: " + missing,
                    List.of(GrpcJavaTaskFixtures.SOURCE_CHECK));
        }
        return ReviewDecision.accept("reported methods and tests exist in commit "
                + commit.substring(0, 12));
    }

    private String read(String commit, String path) throws IOException, InterruptedException {
        Process process = new ProcessBuilder("git", "show", commit + ":" + path)
                .directory(repository.toFile())
                .redirectErrorStream(true)
                .start();
        String output = new String(process.getInputStream().readAllBytes(),
                StandardCharsets.UTF_8);
        int status = process.waitFor();
        return status == 0 ? output : "";
    }
}
