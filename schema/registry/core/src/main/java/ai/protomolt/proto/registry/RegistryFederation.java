package ai.protomolt.proto.registry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.transport.RefSpec;
import org.eclipse.jgit.transport.URIish;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.eclipse.jgit.treewalk.filter.PathFilterGroup;

import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Registry federation: another mesh's git-backed registry, added as an ordinary git remote of
 * this registry's repository, synced by fetch plus a compatibility-gated import. Nothing is ever
 * pushed; federation is strictly pull.
 *
 * <p><b>Subjects carry their origin.</b> A remote subject {@code s} imports as
 * {@code <remote>:<s>} — the local remote name is the origin prefix, so a subject federated
 * through two meshes reads as a provenance chain ({@code b:a:s}). References between remote
 * subjects are rewritten to the namespaced names, so imported schemas resolve and compile
 * locally without touching the schema text (import paths in the text are reference
 * <em>names</em>, which do not change).</p>
 *
 * <p><b>Imports run the full local registration pipeline.</b> Each remote version is registered
 * through {@link GitSchemaRegistryStore#register}, so it is assigned a local global id, compiled
 * against its resolved references, and checked by a {@link CompatibilityWriteGate} under the
 * target subject's effective local mode — the sync path is always gated, even when the store's
 * own write gate is absent. A version that fails the gate is reported with its violations and
 * stops that subject's import (later versions build on it); other subjects continue.</p>
 *
 * <p><b>Descriptor artifacts federate too.</b> {@code descriptors/sha256/*.pb} blobs are
 * content-addressed, so missing ones import verbatim and existing ones are left alone.</p>
 *
 * <p><b>What never syncs:</b> the remote's {@code registry.json} (global ids and the global
 * compatibility mode are local), per-subject compatibility config (modes are local policy), and
 * workflows (deployment-specific). Remotes live in the repository's git config — node-local
 * deployment facts, not registry content — so adding one is not a commit.</p>
 *
 * <p>Sync is idempotent: re-running reports already-present versions and imports nothing new. A
 * remote whose history diverged from what was already imported (same version, different
 * content) is refused at the divergence point and reported — registry histories are append-only,
 * and federation never rewrites what it already imported.</p>
 */
public final class RegistryFederation {

    /** Separator between the origin (remote name) and the remote subject name. */
    public static final String ORIGIN_SEPARATOR = ":";

    private static final Pattern REMOTE_NAME = Pattern.compile("[a-z][a-z0-9-]{0,63}");
    private static final Pattern VERSION_PROTO = Pattern.compile("v(\\d+)\\.proto");
    private static final Pattern VERSION_META = Pattern.compile("v(\\d+)\\.json");
    private static final String SUBJECTS_PREFIX = "subjects/";
    private static final String DESCRIPTORS_PREFIX = "descriptors/sha256/";
    private static final int MAX_BLOB_BYTES = 32 * 1024 * 1024;

    private final GitSchemaRegistryStore store;
    private final SchemaRegistryStore.WriteGate syncGate = new CompatibilityWriteGate();
    private final ObjectMapper json = new ObjectMapper();

    private RegistryFederation(GitSchemaRegistryStore store) {
        this.store = store;
    }

    /** Federation over the given store's repository. */
    public static RegistryFederation over(GitSchemaRegistryStore store) {
        return new RegistryFederation(Objects.requireNonNull(store, "store"));
    }

    /** A configured remote: its local name (the origin prefix) and its git URL. */
    public record RemoteInfo(String name, String url) {
    }

    /** One subject's sync outcome; {@code rejections} carry gate violations or divergence. */
    public record SubjectSync(String remoteSubject, String localSubject,
                              int imported, int alreadyPresent, List<String> rejections) {
    }

    /** The full outcome of one sync pass. */
    public record SyncReport(String remote, List<SubjectSync> subjects,
                             int descriptorsImported, List<String> errors) {
    }

    // ---------------------------------------------------------------- remotes

    /** Every configured remote, sorted by name. */
    public List<RemoteInfo> remotes() {
        try {
            return store.git().remoteList().call().stream()
                    .map(remote -> new RemoteInfo(remote.getName(),
                            remote.getURIs().isEmpty() ? "" : remote.getURIs().getFirst().toString()))
                    .sorted(Comparator.comparing(RemoteInfo::name))
                    .toList();
        } catch (GitAPIException e) {
            throw new RegistryStoreException("Failed listing registry remotes", e);
        }
    }

    /**
     * Adds a remote registry. The name becomes the origin prefix of every subject imported from
     * it, so it is restricted to {@code [a-z][a-z0-9-]*} and must be new.
     */
    public RemoteInfo addRemote(String name, String url) {
        requireRemoteName(name);
        if (url == null || url.isBlank()) {
            throw new IllegalArgumentException("url is required");
        }
        if (remotes().stream().anyMatch(remote -> remote.name().equals(name))) {
            throw new IllegalArgumentException("remote '" + name + "' already exists");
        }
        URIish uri;
        try {
            uri = new URIish(url);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("url is not a valid git URL: " + url, e);
        }
        try {
            store.git().remoteAdd().setName(name).setUri(uri).call();
        } catch (GitAPIException e) {
            throw new RegistryStoreException("Failed adding registry remote '" + name + "'", e);
        }
        return new RemoteInfo(name, url);
    }

    /** Removes a remote. Subjects already imported from it stay — imports are local history. */
    public void removeRemote(String name) {
        requireRemoteName(name);
        if (remotes().stream().noneMatch(remote -> remote.name().equals(name))) {
            throw new IllegalArgumentException("unknown remote '" + name + "'");
        }
        try {
            store.git().remoteRemove().setRemoteName(name).call();
        } catch (GitAPIException e) {
            throw new RegistryStoreException("Failed removing registry remote '" + name + "'", e);
        }
    }

    // ---------------------------------------------------------------- sync

    /** Fetches the remote and imports its subjects and descriptor artifacts. */
    public SyncReport sync(String remoteName) {
        requireRemoteName(remoteName);
        if (remotes().stream().noneMatch(remote -> remote.name().equals(remoteName))) {
            throw new IllegalArgumentException(
                    "unknown remote '" + remoteName + "'; add it with registry-remotes first");
        }
        fetch(remoteName);
        RemoteTree tree = readRemoteTree(remoteName, resolveTip(remoteName));

        List<String> errors = new ArrayList<>(tree.errors());
        int descriptorsImported = importDescriptors(tree.descriptors(), errors);
        List<SubjectSync> subjects = importSubjects(remoteName, tree.subjects());
        return new SyncReport(remoteName, subjects, descriptorsImported, List.copyOf(errors));
    }

    private void fetch(String remoteName) {
        try {
            store.git().fetch()
                    .setRemote(remoteName)
                    .setRefSpecs(new RefSpec(
                            "+refs/heads/*:refs/remotes/" + remoteName + "/*"))
                    .setRemoveDeletedRefs(true)
                    .call();
        } catch (GitAPIException e) {
            throw new RegistryStoreException(
                    "Failed fetching registry remote '" + remoteName + "'", e);
        }
    }

    private ObjectId resolveTip(String remoteName) {
        Repository repository = store.git().getRepository();
        try {
            for (String branch : List.of("main", "master")) {
                ObjectId tip = repository.resolve(
                        "refs/remotes/" + remoteName + "/" + branch + "^{commit}");
                if (tip != null) {
                    return tip;
                }
            }
        } catch (Exception e) {
            throw new RegistryStoreException(
                    "Failed resolving remote '" + remoteName + "' after fetch", e);
        }
        throw new RegistryStoreException(
                "remote '" + remoteName + "' has no main or master branch to sync from");
    }

    private record RemoteVersion(int version, String schemaText, List<SchemaReference> references) {
    }

    private record RemoteTree(Map<String, List<RemoteVersion>> subjects,
                              Map<String, byte[]> descriptors,
                              List<String> errors) {
    }

    /** Reads subjects and descriptors straight from the fetched git objects — no merge. */
    private RemoteTree readRemoteTree(String remoteName, ObjectId tip) {
        Map<String, TreeMap<Integer, String>> texts = new LinkedHashMap<>();
        Map<String, TreeMap<Integer, List<SchemaReference>>> references = new LinkedHashMap<>();
        Map<String, byte[]> descriptors = new LinkedHashMap<>();
        List<String> errors = new ArrayList<>();

        Repository repository = store.git().getRepository();
        try (RevWalk revWalk = new RevWalk(repository);
                TreeWalk walk = new TreeWalk(repository)) {
            walk.addTree(revWalk.parseCommit(tip).getTree());
            walk.setRecursive(true);
            walk.setFilter(PathFilterGroup.createFromStrings("subjects", "descriptors/sha256"));
            while (walk.next()) {
                String path = walk.getPathString();
                byte[] bytes;
                try {
                    bytes = repository.open(walk.getObjectId(0)).getBytes(MAX_BLOB_BYTES);
                } catch (Exception e) {
                    errors.add(path + ": unreadable in remote '" + remoteName + "': "
                            + e.getMessage());
                    continue;
                }
                if (path.startsWith(DESCRIPTORS_PREFIX) && path.endsWith(".pb")) {
                    String fingerprint = path.substring(DESCRIPTORS_PREFIX.length(),
                            path.length() - ".pb".length());
                    descriptors.put(fingerprint, bytes);
                    continue;
                }
                if (!path.startsWith(SUBJECTS_PREFIX)) {
                    continue;
                }
                String[] parts = path.split("/");
                if (parts.length != 3) {
                    continue; // the layout is subjects/<encoded>/<file>; anything else is noise
                }
                String subject = URLDecoder.decode(parts[1], StandardCharsets.UTF_8);
                String file = parts[2];
                Matcher proto = VERSION_PROTO.matcher(file);
                if (proto.matches()) {
                    texts.computeIfAbsent(subject, key -> new TreeMap<>())
                            .put(Integer.parseInt(proto.group(1)),
                                    new String(bytes, StandardCharsets.UTF_8));
                    continue;
                }
                Matcher meta = VERSION_META.matcher(file);
                if (meta.matches()) {
                    try {
                        references.computeIfAbsent(subject, key -> new TreeMap<>())
                                .put(Integer.parseInt(meta.group(1)), parseReferences(bytes));
                    } catch (Exception e) {
                        errors.add(path + ": unparseable metadata in remote '" + remoteName
                                + "': " + e.getMessage());
                    }
                }
                // config.json is local policy on the remote; modes never federate.
            }
        } catch (Exception e) {
            throw new RegistryStoreException(
                    "Failed reading the fetched tree of remote '" + remoteName + "'", e);
        }

        Map<String, List<RemoteVersion>> subjects = new TreeMap<>();
        for (Map.Entry<String, TreeMap<Integer, String>> entry : texts.entrySet()) {
            String subject = entry.getKey();
            List<RemoteVersion> versions = new ArrayList<>();
            for (Map.Entry<Integer, String> version : entry.getValue().entrySet()) {
                List<SchemaReference> refs = references
                        .getOrDefault(subject, new TreeMap<>())
                        .get(version.getKey());
                if (refs == null) {
                    errors.add("subject '" + subject + "' v" + version.getKey()
                            + " has schema text but no metadata; skipping the subject");
                    versions = null;
                    break;
                }
                versions.add(new RemoteVersion(version.getKey(), version.getValue(), refs));
            }
            if (versions != null) {
                subjects.put(subject, List.copyOf(versions));
            }
        }
        return new RemoteTree(subjects, descriptors, errors);
    }

    private List<SchemaReference> parseReferences(byte[] metadataJson) throws Exception {
        JsonNode meta = json.readTree(new String(metadataJson, StandardCharsets.UTF_8));
        List<SchemaReference> references = new ArrayList<>();
        for (JsonNode reference : meta.path("references")) {
            references.add(new SchemaReference(
                    reference.path("name").asText(),
                    reference.path("subject").asText(),
                    reference.path("version").asInt()));
        }
        return List.copyOf(references);
    }

    private int importDescriptors(Map<String, byte[]> descriptors, List<String> errors) {
        int imported = 0;
        for (Map.Entry<String, byte[]> entry : descriptors.entrySet()) {
            try {
                if (store.descriptorSet(entry.getKey()).isPresent()) {
                    continue;
                }
                store.putDescriptorSet(entry.getKey(), ByteString.copyFrom(entry.getValue()));
                imported++;
            } catch (IllegalArgumentException | RegistryStoreException e) {
                errors.add("descriptor " + entry.getKey() + ": " + e.getMessage());
            }
        }
        return imported;
    }

    /** Per-subject import state, advanced across fixpoint passes as references resolve. */
    private static final class SubjectImport {
        final String remoteSubject;
        final String localSubject;
        final List<RemoteVersion> versions;
        final List<String> rejections = new ArrayList<>();
        int next; // index into versions
        int imported;
        int alreadyPresent;
        boolean done;

        SubjectImport(String remoteName, String remoteSubject, List<RemoteVersion> versions) {
            this.remoteSubject = remoteSubject;
            this.localSubject = remoteName + ORIGIN_SEPARATOR + remoteSubject;
            this.versions = versions;
        }

        SubjectSync report() {
            return new SubjectSync(remoteSubject, localSubject, imported, alreadyPresent,
                    List.copyOf(rejections));
        }
    }

    /**
     * Imports every subject, iterating to a fixpoint so cross-subject references resolve in
     * dependency order without an explicit topological sort: a version whose rewritten
     * references are not registered yet defers its subject to the next pass.
     */
    private List<SubjectSync> importSubjects(String remoteName,
                                             Map<String, List<RemoteVersion>> remoteSubjects) {
        List<SubjectImport> imports = remoteSubjects.entrySet().stream()
                .map(entry -> new SubjectImport(remoteName, entry.getKey(), entry.getValue()))
                .toList();

        boolean progressed = true;
        while (progressed) {
            progressed = false;
            for (SubjectImport subject : imports) {
                if (!subject.done) {
                    progressed |= advance(remoteName, subject);
                }
            }
        }
        for (SubjectImport subject : imports) {
            if (!subject.done) {
                RemoteVersion stuck = subject.versions.get(subject.next);
                subject.rejections.add("v" + stuck.version()
                        + ": references unresolved after import: " + stuck.references().stream()
                                .map(ref -> remoteName + ORIGIN_SEPARATOR + ref.subject()
                                        + " v" + ref.version())
                                .reduce((a, b) -> a + ", " + b).orElse("(none)"));
            }
        }
        return imports.stream().map(SubjectImport::report).toList();
    }

    /** One pass over one subject; returns whether any version advanced. */
    private boolean advance(String remoteName, SubjectImport subject) {
        boolean progressed = false;
        while (subject.next < subject.versions.size()) {
            RemoteVersion candidate = subject.versions.get(subject.next);
            List<SchemaReference> rewritten = candidate.references().stream()
                    .map(ref -> new SchemaReference(ref.name(),
                            remoteName + ORIGIN_SEPARATOR + ref.subject(), ref.version()))
                    .toList();

            List<StoredSchema> history = RegistrationSupport.history(store, subject.localSubject);
            if (subject.next < history.size()) {
                StoredSchema existing = history.get(subject.next);
                if (SchemaContents.sameContent(existing, candidate.schemaText(), rewritten)) {
                    subject.alreadyPresent++;
                    subject.next++;
                    progressed = true;
                    continue;
                }
                subject.rejections.add("v" + candidate.version()
                        + ": remote history diverged from the local import; refusing to rewrite");
                subject.done = true;
                return progressed;
            }

            for (SchemaReference reference : rewritten) {
                if (store.version(reference.subject(), reference.version()).isEmpty()) {
                    return progressed; // defer: the referenced import has not landed yet
                }
            }
            try {
                RegistrationSupport.enforceWriteGate(syncGate, store, subject.localSubject,
                        history, candidate.schemaText(), rewritten);
                store.register(subject.localSubject, candidate.schemaText(), rewritten);
                subject.imported++;
                subject.next++;
                progressed = true;
            } catch (IncompatibleRegistrationException e) {
                subject.rejections.add("v" + candidate.version() + ": incompatible with the"
                        + " local import under the effective mode: "
                        + String.join("; ", e.violations()));
                subject.done = true;
                return progressed;
            } catch (RegistryStoreException e) {
                subject.rejections.add("v" + candidate.version() + ": " + e.getMessage());
                subject.done = true;
                return progressed;
            }
        }
        subject.done = true;
        return progressed;
    }

    private static void requireRemoteName(String name) {
        if (name == null || !REMOTE_NAME.matcher(name).matches()) {
            throw new IllegalArgumentException(
                    "remote names use [a-z][a-z0-9-]* (max 64 chars); got '" + name + "'");
        }
    }
}
