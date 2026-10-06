import java.nio.file.*;
import java.util.*;

/** Recompute measured distributions from extracted raw benchmark directories. */
class Summarize {
    record Key(String mode, int replicas, int pool) {}
    static class Samples {
        long operations, nanos;
        int windows;
        final Map<String, List<Long>> latency = new TreeMap<>();
        final Map<String, long[]> metrics = new TreeMap<>();
    }
    public static void main(String[] args) throws Exception {
        Locale.setDefault(Locale.ROOT);
        var groups = new LinkedHashMap<Key, Samples>();
        List<String> baselineEnvironment = null;
        for (String argument : args) {
            Path root = Path.of(argument);
            var environment = Files.readAllLines(root.resolve("environment.txt"));
            var comparable = environment.stream().filter(s -> !s.startsWith("journaled=") && !s.startsWith("loadavg=")).toList();
            if (baselineEnvironment == null) baselineEnvironment = comparable;
            else if (!baselineEnvironment.equals(comparable)) throw new IllegalArgumentException("Mismatched benchmark settings: " + root);
            String mode = environment.stream()
                    .filter(s -> s.startsWith("journaled=")).findFirst().orElseThrow().substring(10);
            if (!Set.of("true", "false").contains(mode)) throw new IllegalArgumentException(mode);
            var windows = Files.readAllLines(root.resolve("windows.csv"));
            if (windows.size() != 13) throw new IllegalArgumentException("Expected twelve windows: " + root);
            for (String line : windows.subList(1, windows.size())) {
                String[] row = line.split(",");
                int replicas = Integer.parseInt(row[1]);
                var samples = groups.computeIfAbsent(new Key(mode, replicas, Integer.parseInt(row[2])), k -> new Samples());
                samples.windows++;
                samples.operations += Long.parseLong(row[4]);
                samples.nanos += Long.parseLong(row[5]);
                long count = 0;
                for (int worker = 0; worker < replicas; worker++) {
                    String prefix = row[0] + "-" + worker;
                    for (String operation : Files.readAllLines(root.resolve(prefix + "-operations.csv"))) {
                        String[] cells = operation.split(",");
                        if (!cells[0].equals("measure")) continue;
                        samples.latency.computeIfAbsent(cells[3], k -> new ArrayList<>()).add(Long.parseLong(cells[5]));
                        count++;
                    }
                    for (String metric : Files.readAllLines(root.resolve(prefix + "-measure-metrics.csv"))) {
                        String[] cells = metric.split(",");
                        if (cells[0].equals("metric")) continue;
                        var values = samples.metrics.computeIfAbsent(cells[0], k -> new long[3]);
                        for (int i = 0; i < 3; i++) values[i] += Long.parseLong(cells[i + 1]);
                    }
                }
                if (count != Long.parseLong(row[4])) throw new IllegalStateException("Operation count mismatch: " + row[0]);
            }
        }
        System.out.println("journaled,replicas,pool_per_replica,windows,operations,ops_per_second,kind,samples,mean_ms,p50_ms,p95_ms");
        groups.forEach((key, samples) -> samples.latency.forEach((kind, values) -> {
            Collections.sort(values);
            double mean = values.stream().mapToLong(Long::longValue).average().orElseThrow() / 1e6;
            System.out.printf("%s,%d,%d,%d,%d,%.2f,%s,%d,%.2f,%.2f,%.2f%n", key.mode(), key.replicas(), key.pool(),
                    samples.windows, samples.operations, samples.operations * 1e9 / samples.nanos, kind, values.size(), mean,
                    values.get((int)Math.ceil(values.size() * .5) - 1) / 1e6,
                    values.get((int)Math.ceil(values.size() * .95) - 1) / 1e6);
        }));
        System.err.println("journaled,replicas,pool_per_replica,metric,count,nanos,failures");
        groups.forEach((key, samples) -> samples.metrics.forEach((metric, values) ->
                System.err.printf("%s,%d,%d,%s,%d,%d,%d%n", key.mode(), key.replicas(), key.pool(), metric, values[0], values[1], values[2])));
    }
}
