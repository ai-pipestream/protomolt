package ai.protomolt.proto.repo.recovery;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/** Asserted observations written as a JUnit-style XML report; a check either passed or the run failed. */
final class Checks {
    record Check(String name, boolean passed, String detail, Instant at) {}

    private final String suite;
    private final List<Check> checks = new ArrayList<>();
    private final Path markers;

    Checks(String suite, Path markers) { this.suite = suite; this.markers = markers; }

    void pass(String name, String detail) { record(new Check(name, true, detail, Instant.now())); }

    void require(boolean condition, String name, String detail) {
        record(new Check(name, condition, detail, Instant.now()));
        if (!condition) throw new RehearsalFailure("Check failed: " + name + " (" + detail + ")");
    }

    private void record(Check check) {
        checks.add(check);
        var line = (check.passed() ? "CHECK_OK " : "CHECK_FAILED ") + check.name() + " :: " + check.detail() + "\n";
        System.out.print(line);
        try { Files.writeString(markers, line, StandardCharsets.UTF_8, java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND); }
        catch (IOException failure) { throw new RehearsalFailure("Cannot append markers " + markers, failure); }
    }

    List<Check> checks() { return List.copyOf(checks); }

    void writeXml(Path file) {
        var xml = new StringBuilder("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        long failures = checks.stream().filter(check -> !check.passed()).count();
        xml.append("<testsuite name=\"").append(escape(suite)).append("\" tests=\"").append(checks.size())
                .append("\" failures=\"").append(failures).append("\" errors=\"0\" skipped=\"0\">\n");
        for (var check : checks) {
            xml.append("  <testcase classname=\"").append(escape(suite)).append("\" name=\"").append(escape(check.name()))
                    .append("\" timestamp=\"").append(check.at()).append("\">");
            if (!check.passed()) xml.append("<failure message=\"").append(escape(check.detail())).append("\"/>");
            xml.append("<system-out>").append(escape(check.detail())).append("</system-out></testcase>\n");
        }
        xml.append("</testsuite>\n");
        try { Files.writeString(file, xml, StandardCharsets.UTF_8); }
        catch (IOException failure) { throw new RehearsalFailure("Cannot write " + file, failure); }
    }

    private static String escape(String text) {
        return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;");
    }
}
