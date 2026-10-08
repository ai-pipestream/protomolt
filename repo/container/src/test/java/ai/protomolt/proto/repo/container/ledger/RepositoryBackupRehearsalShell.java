package ai.protomolt.proto.repo.container.ledger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Runs external commands for the rehearsal, records every invocation with its exit code in a
 * command log with secrets redacted, and never swallows a failure. Adapted from the draft
 * harness on GitHub PR #413 (commit 4862f35f9), which did not land.
 */
final class RepositoryBackupRehearsalShell {
    record Result(List<String> command, int exitCode, String stdout, String stderr) {
        boolean ok() { return exitCode == 0; }
        Result require(String what) {
            if (exitCode != 0) throw new IllegalStateException(what + " failed with exit " + exitCode + ": " + String.join(" ", command)
                    + "\nstdout: " + stdout + "\nstderr: " + stderr);
            return this;
        }
    }

    private final Path log;
    private final List<String> secrets;

    RepositoryBackupRehearsalShell(Path log, List<String> secrets) {
        this.log = log;
        this.secrets = List.copyOf(secrets);
    }

    Result run(List<String> command) { return run(command, Map.of(), 600); }

    Result run(List<String> command, Map<String, String> environment, long timeoutSeconds) {
        var builder = new ProcessBuilder(command);
        builder.environment().putAll(environment);
        try {
            var process = builder.start();
            var out = new java.util.concurrent.CompletableFuture<byte[]>();
            var err = new java.util.concurrent.CompletableFuture<byte[]>();
            Thread.ofVirtual().start(() -> { try { out.complete(process.getInputStream().readAllBytes()); } catch (IOException e) { out.completeExceptionally(e); } });
            Thread.ofVirtual().start(() -> { try { err.complete(process.getErrorStream().readAllBytes()); } catch (IOException e) { err.completeExceptionally(e); } });
            process.getOutputStream().close();
            if (!process.waitFor(timeoutSeconds, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                throw new IllegalStateException("Command timed out after " + timeoutSeconds + "s: " + redact(String.join(" ", command)));
            }
            var result = new Result(command, process.exitValue(), new String(out.get(), StandardCharsets.UTF_8), new String(err.get(), StandardCharsets.UTF_8));
            record(result);
            return result;
        } catch (IOException | InterruptedException | java.util.concurrent.ExecutionException failure) {
            throw new IllegalStateException("Command could not run: " + redact(String.join(" ", command)), failure);
        }
    }

    /** A note in the command log that is not a command (host exits, injected steps). */
    void note(String text) {
        append(Instant.now() + " note: " + redact(text) + "\n");
    }

    private void record(Result result) {
        var line = new StringBuilder();
        line.append(Instant.now()).append(" exit=").append(result.exitCode()).append(" $ ")
                .append(redact(String.join(" ", result.command()))).append('\n');
        if (!result.stdout().isBlank()) line.append("  stdout: ").append(redact(trim(result.stdout()))).append('\n');
        if (!result.stderr().isBlank()) line.append("  stderr: ").append(redact(trim(result.stderr()))).append('\n');
        append(line.toString());
    }

    private void append(String text) {
        try {
            Files.writeString(log, text, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException failure) { throw new IllegalStateException("Cannot append command log " + log, failure); }
    }

    private static String trim(String text) {
        var value = text.strip().replace("\n", "\n          ");
        return value.length() > 4000 ? value.substring(0, 4000) + "...[truncated]" : value;
    }

    String redact(String text) {
        for (String secret : secrets) if (!secret.isEmpty()) text = text.replace(secret, "<redacted>");
        return text;
    }
}
