package ai.protomolt.proto.repo.container.ledger;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Run-scoped disposable stores through the Docker CLI: pinned PostgreSQL and RustFS on named
 * volumes, logical dump and restore inside the PostgreSQL image, and offline volume archives.
 * Named volumes are required because the provider snapshot is the stopped volume itself, not an
 * object copy. Every resource carries the run id and is removed by {@link #cleanup()}; nothing
 * here touches resources it did not create. Adapted from GitHub PR #413 (4862f35f9).
 */
final class RepositoryBackupRehearsalStores {
    static final String POSTGRES_IMAGE = "postgres:18-alpine";
    static final String POSTGRES_DATA = "/var/lib/postgresql";
    static final String POSTGRES_PGDATA = "/var/lib/postgresql/18/docker";
    static final String RUSTFS_IMAGE = "rustfs/rustfs:1.0.0-beta.11-preview.1";

    record Postgres(String container, String volume, int hostPort, String database, String user, String password) {
        String jdbcUrl() { return "jdbc:postgresql://127.0.0.1:" + hostPort + "/" + database; }
    }
    record Provider(String container, String volume, int hostPort, String accessKey, String secretKey) {
        String endpoint() { return "http://127.0.0.1:" + hostPort; }
    }

    private final RepositoryBackupRehearsalShell shell;
    private final String run;
    private final String network;
    private final String uid;
    private final List<String> created = new ArrayList<>();

    RepositoryBackupRehearsalStores(RepositoryBackupRehearsalShell shell, String run) {
        this.shell = shell;
        this.run = run;
        this.network = run + "-net";
        this.uid = shell.run(List.of("id", "-u")).require("id").stdout().strip() + ":" + shell.run(List.of("id", "-g")).require("id").stdout().strip();
    }

    /** Docker must be reachable and both pinned images present; an absent image is pulled by its exact tag. */
    void requireDocker() {
        var version = shell.run(List.of("docker", "version", "--format", "{{.Server.Version}}"));
        if (!version.ok()) throw new IllegalStateException("Docker is required and must be reachable: " + version.stderr());
        for (String image : List.of(POSTGRES_IMAGE, RUSTFS_IMAGE)) {
            if (!shell.run(List.of("docker", "image", "inspect", image, "--format", "{{.Id}}")).ok())
                shell.run(List.of("docker", "pull", image), Map.of(), 900).require("docker pull " + image);
        }
    }

    String imageId(String image) {
        return shell.run(List.of("docker", "image", "inspect", image, "--format", "{{.Id}}")).require("image inspect").stdout().strip();
    }

    void createNetwork() {
        shell.run(List.of("docker", "network", "create", network)).require("network create");
        created.add("network:" + network);
    }

    String createVolume(String suffix) {
        String name = run + "-" + suffix;
        shell.run(List.of("docker", "volume", "create", name)).require("volume create");
        created.add("volume:" + name);
        return name;
    }

    boolean volumeEmpty(String volume) {
        var result = shell.run(List.of("docker", "run", "--rm", "-v", volume + ":/data:ro", POSTGRES_IMAGE, "sh", "-c", "find /data -mindepth 1 | head -1"));
        result.require("volume inspect");
        return result.stdout().isBlank();
    }

    Postgres startPostgres(String suffix, String volume, String user, String password, String database) {
        String name = run + "-" + suffix;
        shell.run(List.of("docker", "run", "-d", "--name", name, "--network", network, "-p", "127.0.0.1:0:5432",
                "-v", volume + ":" + POSTGRES_DATA, "-e", "POSTGRES_USER=" + user, "-e", "POSTGRES_PASSWORD=" + password,
                "-e", "POSTGRES_DB=" + database, POSTGRES_IMAGE, "-c", "fsync=on"), Map.of(), 120).require("start postgres");
        created.add("container:" + name);
        int port = mappedPort(name, "5432/tcp");
        var postgres = new Postgres(name, volume, port, database, user, password);
        awaitPostgres(postgres);
        return postgres;
    }

    void awaitPostgres(Postgres postgres) {
        long deadline = System.nanoTime() + Duration.ofSeconds(120).toNanos();
        while (true) {
            var ready = shell.run(List.of("docker", "exec", postgres.container(), "pg_isready", "-U", postgres.user(), "-d", postgres.database()));
            if (ready.ok()) {
                // pg_isready can pass during the entrypoint's temporary server; require a real connection.
                try (var catalog = new RepositoryBackupRehearsalCatalog(postgres.jdbcUrl(), postgres.user(), postgres.password())) { catalog.currentXid(); return; }
                catch (IllegalStateException notYet) { /* retry */ }
            }
            if (System.nanoTime() > deadline) throw new IllegalStateException("PostgreSQL " + postgres.container() + " did not become ready");
            sleep(500);
        }
    }

    Provider startRustFs(String suffix, String volume, int hostPort, String accessKey, String secretKey) {
        String name = run + "-" + suffix;
        if (!portFree(hostPort)) throw new IllegalStateException("Host port " + hostPort + " is not free for the provider endpoint");
        shell.run(List.of("docker", "run", "-d", "--name", name, "--network", network, "-p", "127.0.0.1:" + hostPort + ":9000",
                "-v", volume + ":/data", "-e", "RUSTFS_VOLUMES=/data", "-e", "RUSTFS_ADDRESS=:9000", "-e", "RUSTFS_CONSOLE_ENABLE=false",
                "-e", "RUSTFS_ACCESS_KEY=" + accessKey, "-e", "RUSTFS_SECRET_KEY=" + secretKey, RUSTFS_IMAGE, "/data"), Map.of(), 120)
                .require("start rustfs");
        created.add("container:" + name);
        var provider = new Provider(name, volume, hostPort, accessKey, secretKey);
        var client = HttpClient.newHttpClient();
        long deadline = System.nanoTime() + Duration.ofSeconds(120).toNanos();
        while (true) {
            try {
                var response = client.send(HttpRequest.newBuilder(URI.create(provider.endpoint() + "/health")).timeout(Duration.ofSeconds(2)).build(),
                        HttpResponse.BodyHandlers.discarding());
                if (response.statusCode() == 200) return provider;
            } catch (IOException | InterruptedException notYet) { /* retry */ }
            if (System.nanoTime() > deadline) throw new IllegalStateException("RustFS " + name + " did not become healthy");
            sleep(500);
        }
    }

    int mappedPort(String container, String port) {
        var result = shell.run(List.of("docker", "port", container, port)).require("docker port");
        var line = result.stdout().lines().filter(value -> value.contains("127.0.0.1")).findFirst().orElse(result.stdout().lines().findFirst().orElse(""));
        return Integer.parseInt(line.substring(line.lastIndexOf(':') + 1).strip());
    }

    static int reservePort() {
        try (var socket = new ServerSocket(0)) { return socket.getLocalPort(); }
        catch (IOException failure) { throw new IllegalStateException("Cannot reserve a port", failure); }
    }

    static boolean portFree(int port) {
        try (var socket = new ServerSocket(port, 1, java.net.InetAddress.getLoopbackAddress())) { return true; }
        catch (IOException inUse) { return false; }
    }

    /** Clean stop; the inspect state proves the process ended before anything reads its storage. */
    Map<String, String> stop(String container) {
        shell.run(List.of("docker", "stop", "-t", "60", container), Map.of(), 120).require("docker stop " + container);
        var state = shell.run(List.of("docker", "inspect", container, "--format", "{{.State.Status}} {{.State.ExitCode}} {{.State.FinishedAt}}")).require("inspect");
        var parts = state.stdout().strip().split(" ");
        if (!parts[0].equals("exited")) throw new IllegalStateException(container + " is not exited: " + state.stdout());
        return Map.of("status", parts[0], "exitCode", parts[1], "finishedAt", parts[2]);
    }

    void start(String container) {
        shell.run(List.of("docker", "start", container)).require("docker start " + container);
    }

    void remove(String container) {
        shell.run(List.of("docker", "rm", "-f", container)).require("docker rm " + container);
        if (shell.run(List.of("docker", "inspect", container)).ok()) throw new IllegalStateException(container + " still exists after removal");
        created.remove("container:" + container);
    }

    boolean exists(String container) { return shell.run(List.of("docker", "inspect", container, "--format", "{{.Id}}")).ok(); }

    /** Logical dump from a sibling container of the same pinned image; exit code and stderr are recorded. */
    RepositoryBackupRehearsalShell.Result pgDump(Postgres source, Path target) {
        return shell.run(List.of("docker", "run", "--rm", "--network", network, "--user", uid, "-e", "PGPASSWORD", "-v", target.getParent() + ":/out",
                POSTGRES_IMAGE, "pg_dump", "-h", source.container(), "-U", source.user(), "-d", source.database(), "--format=custom",
                "--no-owner", "--no-privileges", "-f", "/out/" + target.getFileName()), Map.of("PGPASSWORD", source.password()), 600);
    }

    RepositoryBackupRehearsalShell.Result pgRestore(Postgres target, Path dump) {
        return shell.run(List.of("docker", "run", "--rm", "--network", network, "--user", uid, "-e", "PGPASSWORD", "-v", dump.getParent() + ":/in:ro",
                POSTGRES_IMAGE, "pg_restore", "-h", target.container(), "-U", target.user(), "-d", target.database(), "--exit-on-error",
                "--no-owner", "--no-privileges", "/in/" + dump.getFileName()), Map.of("PGPASSWORD", target.password()), 600);
    }

    /** Archive a stopped volume byte for byte, preserving ownership; the file lands owned by the operator. */
    RepositoryBackupRehearsalShell.Result archiveVolume(String volume, Path target) {
        return shell.run(List.of("docker", "run", "--rm", "-v", volume + ":/data:ro", "-v", target.getParent() + ":/out", POSTGRES_IMAGE, "sh", "-c",
                "set -e; cd /data && tar --numeric-owner -cf /out/" + target.getFileName() + " . && chown " + uid + " /out/" + target.getFileName()), Map.of(), 600);
    }

    RepositoryBackupRehearsalShell.Result extractVolume(Path archive, String volume) {
        return shell.run(List.of("docker", "run", "--rm", "-v", volume + ":/data", "-v", archive.getParent() + ":/in:ro", POSTGRES_IMAGE, "sh", "-c",
                "set -e; cd /data && tar --numeric-owner -xf /in/" + archive.getFileName()), Map.of(), 600);
    }

    /** Content digest of a volume as evidence that a refused operation left it untouched. */
    String volumeDigest(String volume) {
        return shell.run(List.of("docker", "run", "--rm", "-v", volume + ":/data:ro", POSTGRES_IMAGE, "sh", "-c",
                "cd /data && find . -type f | LC_ALL=C sort | xargs -r sha256sum | sha256sum | cut -d' ' -f1"), Map.of(), 600).require("volume digest").stdout().strip();
    }

    void writeLogs(String container, Path file) {
        var logs = shell.run(List.of("docker", "logs", container));
        try { Files.writeString(file, shell.redact(logs.stdout() + logs.stderr())); } catch (IOException failure) { throw new IllegalStateException("Cannot write logs", failure); }
    }

    void cleanup() {
        for (var resource : new ArrayList<>(created).reversed()) {
            int colon = resource.indexOf(':');
            String kind = resource.substring(0, colon), name = resource.substring(colon + 1);
            switch (kind) {
                case "container" -> shell.run(List.of("docker", "rm", "-f", name));
                case "volume" -> shell.run(List.of("docker", "volume", "rm", "-f", name));
                case "network" -> shell.run(List.of("docker", "network", "rm", name));
                default -> throw new IllegalStateException(resource);
            }
        }
        created.clear();
    }

    List<String> created() { return List.copyOf(created); }

    static void sleep(long millis) {
        try { Thread.sleep(millis); } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IllegalStateException("Interrupted"); }
    }
}
