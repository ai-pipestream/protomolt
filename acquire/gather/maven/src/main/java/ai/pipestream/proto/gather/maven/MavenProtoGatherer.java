package ai.pipestream.proto.gather.maven;

import ai.pipestream.proto.gather.GatherException;
import ai.pipestream.proto.gather.JarProtoExtraction;
import ai.pipestream.proto.gather.ProtoGatherer;
import ai.pipestream.proto.sources.ProtoSourceSet;

import org.apache.maven.repository.internal.MavenRepositorySystemUtils;
import org.eclipse.aether.DefaultRepositorySystemSession;
import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.artifact.Artifact;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.eclipse.aether.collection.CollectRequest;
import org.eclipse.aether.graph.Dependency;
import org.eclipse.aether.repository.LocalRepository;
import org.eclipse.aether.repository.RemoteRepository;
import org.eclipse.aether.resolution.ArtifactRequest;
import org.eclipse.aether.resolution.ArtifactResult;
import org.eclipse.aether.resolution.DependencyRequest;
import org.eclipse.aether.supplier.RepositorySystemSupplier;
import org.eclipse.aether.util.artifact.JavaScopes;
import org.eclipse.aether.util.filter.DependencyFilterUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Gathers {@code .proto} entries from Maven artifacts resolved by coordinate.
 *
 * <p>Artifacts are resolved with the standalone Maven Resolver (no Maven installation needed)
 * against the configured remote repositories (default: Maven Central) through a local
 * repository cache (default: {@code ${user.home}/.m2/repository}). With
 * {@link Builder#transitive(boolean) transitive(true)} the runtime dependency graph of each
 * coordinate is resolved and every resolved jar is scanned; otherwise only the named artifacts
 * are.</p>
 *
 * <p>Jar entries are extracted exactly like {@link ai.pipestream.proto.gather.JarProtoGatherer}
 * does it (shared {@link JarProtoExtraction}): in-jar paths become import paths and
 * {@code google/protobuf/**} entries are skipped by default because the compiler supplies the
 * well-known types. Each file's origin is {@code maven:<coordinate>}.</p>
 */
public final class MavenProtoGatherer implements ProtoGatherer {

    private static final String MAVEN_CENTRAL = "https://repo.maven.apache.org/maven2/";

    private final List<String> coordinates;
    private final List<String> repositories;
    private final Path localRepository;
    private final boolean transitive;
    private final JarProtoExtraction.Options extractionOptions;

    private MavenProtoGatherer(Builder builder) {
        this.coordinates = List.copyOf(builder.coordinates);
        this.repositories = builder.repositories.isEmpty()
                ? List.of(MAVEN_CENTRAL)
                : List.copyOf(builder.repositories);
        this.localRepository = (builder.localRepository != null
                ? builder.localRepository
                : Path.of(System.getProperty("user.home"), ".m2", "repository"))
                .toAbsolutePath().normalize();
        this.transitive = builder.transitive;
        this.extractionOptions = new JarProtoExtraction.Options(
                builder.includeGoogleWellKnownTypes, List.of(), List.of());
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public ProtoSourceSet gather() throws GatherException {
        Map<String, Path> jarsByCoordinate = resolve();
        ProtoSourceSet.Builder builder = ProtoSourceSet.builder();
        for (Map.Entry<String, Path> entry : jarsByCoordinate.entrySet()) {
            try {
                JarProtoExtraction.extract(entry.getValue(), "maven:" + entry.getKey(), extractionOptions)
                        .forEach(builder::add);
            } catch (IOException e) {
                throw new GatherException("Failed reading resolved artifact " + entry.getKey()
                        + " (" + entry.getValue() + ")", e);
            } catch (IllegalStateException e) {
                throw new GatherException("Conflicting proto sources: " + e.getMessage(), e);
            }
        }
        return builder.build();
    }

    /** Resolved jar per coordinate, insertion-ordered and de-duplicated. */
    private Map<String, Path> resolve() throws GatherException {
        RepositorySystem system = new RepositorySystemSupplier().get();
        try {
            DefaultRepositorySystemSession session = MavenRepositorySystemUtils.newSession();
            session.setLocalRepositoryManager(system.newLocalRepositoryManager(
                    session, new LocalRepository(localRepository.toFile())));
            List<RemoteRepository> remotes = remoteRepositories();
            Map<String, Path> resolved = new LinkedHashMap<>();
            for (String coordinate : coordinates) {
                Artifact artifact = parse(coordinate);
                if (transitive) {
                    resolveTransitively(system, session, remotes, artifact, coordinate, resolved);
                } else {
                    resolveSingle(system, session, remotes, artifact, coordinate, resolved);
                }
            }
            return resolved;
        } finally {
            system.shutdown();
        }
    }

    private static void resolveSingle(RepositorySystem system, DefaultRepositorySystemSession session,
                                      List<RemoteRepository> remotes, Artifact artifact,
                                      String coordinate, Map<String, Path> resolved) throws GatherException {
        try {
            ArtifactResult result = system.resolveArtifact(session,
                    new ArtifactRequest(artifact, remotes, null));
            resolved.putIfAbsent(coordinate, result.getArtifact().getFile().toPath());
        } catch (Exception e) {
            throw new GatherException("Failed resolving Maven artifact " + coordinate, e);
        }
    }

    private static void resolveTransitively(RepositorySystem system, DefaultRepositorySystemSession session,
                                            List<RemoteRepository> remotes, Artifact artifact,
                                            String coordinate, Map<String, Path> resolved) throws GatherException {
        try {
            CollectRequest collect = new CollectRequest(
                    new Dependency(artifact, JavaScopes.RUNTIME), remotes);
            DependencyRequest request = new DependencyRequest(collect,
                    DependencyFilterUtils.classpathFilter(JavaScopes.RUNTIME));
            for (ArtifactResult result : system.resolveDependencies(session, request).getArtifactResults()) {
                Artifact a = result.getArtifact();
                resolved.putIfAbsent(coordinateOf(a), a.getFile().toPath());
            }
        } catch (Exception e) {
            throw new GatherException("Failed resolving Maven dependency graph of " + coordinate, e);
        }
    }

    private static String coordinateOf(Artifact artifact) {
        String base = artifact.getGroupId() + ":" + artifact.getArtifactId() + ":" + artifact.getVersion();
        return artifact.getClassifier().isEmpty() ? base : base + ":" + artifact.getClassifier();
    }

    private List<RemoteRepository> remoteRepositories() {
        List<RemoteRepository> remotes = new ArrayList<>(repositories.size());
        for (int i = 0; i < repositories.size(); i++) {
            remotes.add(new RemoteRepository.Builder("repo" + i, "default", repositories.get(i)).build());
        }
        return remotes;
    }

    /** Parses {@code group:artifact:version} or {@code group:artifact:version:classifier}. */
    private static Artifact parse(String coordinate) throws GatherException {
        String[] parts = coordinate.split(":");
        if ((parts.length != 3 && parts.length != 4)
                || List.of(parts).stream().anyMatch(String::isBlank)) {
            throw new GatherException("Invalid Maven coordinate '" + coordinate
                    + "'; expected group:artifact:version[:classifier]");
        }
        String classifier = parts.length == 4 ? parts[3] : "";
        return new DefaultArtifact(parts[0], parts[1], classifier, "jar", parts[2]);
    }

    @Override
    public String origin() {
        return "maven:" + String.join(",", coordinates);
    }

    /** Builder for {@link MavenProtoGatherer}; at least one coordinate is required. */
    public static final class Builder {

        private final List<String> coordinates = new ArrayList<>();
        private final List<String> repositories = new ArrayList<>();
        private Path localRepository;
        private boolean transitive = false;
        private boolean includeGoogleWellKnownTypes = false;

        private Builder() {
        }

        /** Adds a {@code group:artifact:version[:classifier]} coordinate. */
        public Builder coordinate(String coordinate) {
            coordinates.add(Objects.requireNonNull(coordinate, "coordinate"));
            return this;
        }

        /** Adds {@code group:artifact:version[:classifier]} coordinates. */
        public Builder coordinates(List<String> coordinates) {
            Objects.requireNonNull(coordinates, "coordinates").forEach(this::coordinate);
            return this;
        }

        /** Remote repository URLs to resolve from; default is Maven Central. */
        public Builder repositories(List<String> urls) {
            repositories.addAll(Objects.requireNonNull(urls, "urls"));
            return this;
        }

        /** Local repository cache; default {@code ${user.home}/.m2/repository}. */
        public Builder localRepository(Path localRepository) {
            this.localRepository = Objects.requireNonNull(localRepository, "localRepository");
            return this;
        }

        /** When {@code true}, resolves the runtime dependency graph and scans every jar. */
        public Builder transitive(boolean transitive) {
            this.transitive = transitive;
            return this;
        }

        /** Whether to extract {@code google/protobuf/**} entries; see {@link JarProtoExtraction}. */
        public Builder includeGoogleWellKnownTypes(boolean include) {
            this.includeGoogleWellKnownTypes = include;
            return this;
        }

        public MavenProtoGatherer build() {
            if (coordinates.isEmpty()) {
                throw new IllegalStateException("At least one coordinate is required");
            }
            return new MavenProtoGatherer(this);
        }
    }
}
