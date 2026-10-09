package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.descriptors.ClosedDescriptorSet;
import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.v1.*;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.protovalidate.ProtovalidateRuleSource;
import ai.protomolt.proto.validate.source.ProtomoltRuleSource;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.*;
import java.util.concurrent.CancellationException;

/**
 * Pure candidate verification under a host-selected policy and retained definitions.
 * This does not authorize a repository operation or prove provider durability. The
 * committing host must bind the member to its canonical command, recheck policy and
 * authorization, and match every selected physical object before publishing.
 */
public final class DocumentSchemaAdmission {
    /** Fixed rule dialects and structural ceilings, independent of ServiceLoader state. */
    public static final String PROFILE = "protomolt-retained-schema-admission/v1";
    static final ProtoValidator VALIDATOR = ProtoValidator.create(
            List.of(new ProtomoltRuleSource(), new ProtovalidateRuleSource()),
            ai.protomolt.proto.validate.spi.TaxonomyCatalog.empty(),
            ai.protomolt.proto.validate.spi.PostalCodeCatalog.empty());
    /** Fixed configuration of this loaded admission implementation, not caller-supplied provenance. */
    public record RuntimeProfile(String validationProfile, String catalogConfiguration) {}
    /** Method invocation reads the loaded implementation; public string constants may be inlined by consumers. */
    public static RuntimeProfile runtimeProfile() {
        return new RuntimeProfile(PROFILE, "empty-taxonomy-and-postal/v1");
    }
    private static final int MIB = 1024 * 1024;
    private DocumentSchemaAdmission() {}

    /** Retained schema bytes or metadata are absent, corrupt or inconsistent with their recorded identity. */
    public static class DataLoss extends IllegalStateException {
        DataLoss(String message) { super(message); }
        DataLoss(String message, Throwable cause) { super(message, cause); }
    }

    /**
     * Member-wide serialized byte/count budgets, not a heap reservation. V1 additionally
     * limits descriptor artifacts to 16 MiB/256 files/4096 edges/depth 64, structural
     * depth to 64, wire values to 1 million per fragment/root, and schema traversal to
     * 4096 types/65536 fields per root. Raw fragments and decoded payloads each
     * have a 256 MiB ceiling. Hosts reserve memory for decoded forms and I/O.
     */
    public record Limits(int maxFragments, long maxFragmentBytes, int maxRoots, long maxEvidenceBytes,
                         int maxBindings, long maxRetainedBytes, int maxDecodedBytes) {
        public Limits {
            if (maxFragments < 1 || maxFragments > 10000 || maxFragmentBytes < 1 || maxFragmentBytes > 256L * MIB
                    || maxRoots < 1 || maxRoots > 1024 || maxEvidenceBytes < 1 || maxEvidenceBytes > 16L * MIB
                    || maxBindings < 1 || maxBindings > 64 || maxRetainedBytes < 1 || maxRetainedBytes > 64L * MIB
                    || maxDecodedBytes < 1 || maxDecodedBytes > 256 * MIB)
                throw new IllegalArgumentException("invalid member admission limits");
        }
    }

    /** Authoritative absence is empty; I/O and access failures propagate. Bound allocation before returning bytes. */
    @FunctionalInterface public interface Reader { Optional<ByteString> read(String sha256); }

    /** Exact persisted association; the host authenticates its account/revision context. */
    public record Reference(String typeUrl, String descriptorSha256, String metadataCodec, int metadataVersion,
                            String metadataSha256, Optional<String> sourceSha256) {
        public Reference {
            Objects.requireNonNull(typeUrl); Objects.requireNonNull(descriptorSha256);
            Objects.requireNonNull(metadataCodec); Objects.requireNonNull(metadataSha256); Objects.requireNonNull(sourceSha256);
        }
        DocumentRetainedSchemaAssets.Reference internal() {
            return new DocumentRetainedSchemaAssets.Reference(typeUrl, descriptorSha256, metadataCodec,
                    metadataVersion, metadataSha256, sourceSha256);
        }
        /** Wire identity only; it does not verify asset bytes or transfer retention ownership. */
        public RepositorySchemaAssetReference toProto() {
            var value = RepositorySchemaAssetReference.newBuilder().setTypeUrl(typeUrl)
                    .setDescriptorSha256(descriptorSha256).setMetadataCodec(metadataCodec)
                    .setMetadataVersion(metadataVersion).setMetadataSha256(metadataSha256);
            sourceSha256.ifPresent(value::setSourceSha256);
            return value.build();
        }
        /** Validate the bounded association shape; the reader must still verify all referenced assets. */
        public static Reference fromProto(RepositorySchemaAssetReference value, Runnable control) {
            DocumentSchemaEvidenceCodec.measureAndValidate(Objects.requireNonNull(value), control);
            return new Reference(value.getTypeUrl(), value.getDescriptorSha256(), value.getMetadataCodec(),
                    value.getMetadataVersion(), value.getMetadataSha256(),
                    value.hasSourceSha256() ? Optional.of(value.getSourceSha256()) : Optional.empty());
        }
    }

    /** Original canonical storage bytes, not an already parsed approximation. */
    public record EncodedEvidence(String codec, int version, ByteString bytes, String sha256) {
        public EncodedEvidence { Objects.requireNonNull(codec); Objects.requireNonNull(bytes); Objects.requireNonNull(sha256); }
    }

    /**
     * Ordinals index the complete member, including EMPTY slots. The host must keep
     * supplied maps/lists stable and bounded during check; snapshots are taken after
     * count checks. Command and policy digests are identity bindings, not grants.
     * References list payload definitions; container is separately required for the
     * Document layout. Every supplied association must be used by the candidate.
     */
    public record Request(ByteString commandSha256, String policySha256, boolean requireStructuredRoot,
                          DocumentPublicationMember member, Map<Integer, ByteString> fragments,
                          Map<Integer, List<EncodedEvidence>> evidence, Reference container,
                          List<Reference> references) {
        public Request {
            Objects.requireNonNull(commandSha256); Objects.requireNonNull(policySha256); Objects.requireNonNull(member);
            Objects.requireNonNull(fragments); Objects.requireNonNull(evidence); Objects.requireNonNull(container);
            Objects.requireNonNull(references);
        }
    }

    /** One verified bundle bound to its complete member ordinal and exact raw fragment. */
    public record RootEvidence(int ordinal, DocumentSchemaRootLocator locator, String locatorSha256,
                               EncodedEvidence encoded) {}

    /**
     * Decode canonical evidence and recover its locator identity. The supplied ordinal
     * is not authenticated here; the repository must bind it and the locator to the
     * retained slot. This performs no candidate validation or registry lookup.
     * Encoded bytes and parsed heap remain caller-owned; scratch is reserved and closed.
     */
    public static RootEvidence decodeRootEvidence(int ordinal, EncodedEvidence encoded,
            DocumentAdmissionReservations reservations, Runnable control) throws InvalidProtocolBufferException {
        if (ordinal < 0 || ordinal >= 10000) throw new IllegalArgumentException("Evidence ordinal outside member bound");
        Objects.requireNonNull(encoded); Objects.requireNonNull(reservations); Objects.requireNonNull(control);
        var evidence = DocumentRootSchemaEvidenceCodec.decode(encoded.codec(), encoded.version(), encoded.bytes(),
                encoded.sha256(), reservations, control);
        try (var locator = DocumentSchemaEvidenceCodec.encodeOwned(evidence.getRoot(), reservations, control)) {
            return new RootEvidence(ordinal, evidence.getRoot(), locator.value().sha256(), encoded);
        }
    }

    /** Complete definition from the host's authorized registry/compiler, never executable code. */
    public record Definition(RepositorySchemaAsset metadata, ByteString descriptors, Optional<ByteString> source) {
        public Definition { Objects.requireNonNull(metadata); Objects.requireNonNull(descriptors); Objects.requireNonNull(source); }
    }

    /** Host context is captured by the resolver; prefix selects the next Any before its boundary. */
    public record Selection(int ordinal, DocumentSchemaRootLocator root, String typeUrl,
                            List<RepositorySchemaOccurrenceStep> prefix, String valueSha256, long valueSizeBytes) {
        public Selection { Objects.requireNonNull(root); Objects.requireNonNull(typeUrl);
            prefix = List.copyOf(prefix); Objects.requireNonNull(valueSha256); }
    }

    /** Select an authorized immutable definition for this occurrence; failures propagate without fallback. */
    @FunctionalInterface public interface Resolver { Definition select(Selection occurrence); }

    /**
     * Owned resolver scope. Keep it open through admission's budgeted copies; the
     * returned definitions are borrowed. Close releases owned resources, is idempotent
     * and must not throw. Hosts may impose thread affinity on a scope.
     */
    public interface Resolution extends Resolver, AutoCloseable {
        @Override void close();
    }

    /** Host bounds allocations and keeps collections stable throughout preparation, as for Request. */
    public record Preparation(ByteString commandSha256, String policySha256, boolean requireStructuredRoot,
                              DocumentPublicationMember member, Map<Integer, ByteString> fragments, Definition container) {
        public Preparation { Objects.requireNonNull(commandSha256); Objects.requireNonNull(policySha256);
            Objects.requireNonNull(member); Objects.requireNonNull(fragments); Objects.requireNonNull(container); }
    }

    /** Generate evidence from checked payloads, then independently replay it before delivering a proof. */
    public static Proof prepareAndCheck(Preparation request, Resolver resolver, Limits limits, Runnable control)
            throws InvalidProtocolBufferException {
        return DocumentSchemaPreparation.check(request, resolver, limits, control);
    }

    /**
     * Own generated evidence and private copies of the selected descriptor/source assets.
     * Reserve actual serialized bytes before allocation, including temporary canonical
     * comparisons. The host must separately own fragment inputs through the returned
     * proof's lifetime, bound resolver input allocations, and budget parsed JVM objects.
     * Resolver bytes must remain stable while preparation copies them. Failure releases
     * every acquired lease and propagates without an unbudgeted retry.
     */
    public static PreparedProof prepareAndCheck(Preparation request, Resolver resolver, Limits limits,
            DocumentAdmissionReservations reservations, Runnable control) throws InvalidProtocolBufferException {
        var resources = new DocumentAdmissionResources(reservations);
        boolean transferred = false;
        try (var temporary = new DocumentAdmissionResources(reservations)) {
            var proof = DocumentSchemaPreparation.check(request, resolver, limits, resources, temporary, control);
            var result = new PreparedProof(proof, resources);
            transferred = true;
            return result;
        } catch (DocumentAdmissionResources.ReservationFailure failure) {
            throw failure.original;
        } finally {
            if (!transferred) resources.close();
        }
    }

    /**
     * Independently check an accepted assessment using only its frozen artifacts and
     * recorded evaluation time. The result owns private schema/evidence copies; the
     * host still owns fragment bytes through the returned proof's lifetime. Keep the
     * view open and inputs stable during this call. This does not authorize a policy
     * or publication. Capacity and integrity failures propagate without fallback.
     */
    public static PreparedProof checkAccepted(DocumentSchemaAssessment.View assessment, Limits limits,
            DocumentAdmissionReservations reservations, Runnable control) throws InvalidProtocolBufferException {
        Objects.requireNonNull(assessment); Objects.requireNonNull(limits); active(control);
        var request = DocumentSchemaAssessmentReplay.Request.from(assessment);
        if (request.expectedFailure().isPresent())
            throw new IllegalArgumentException("Invalid assessment cannot produce an admission proof");
        var artifacts = assessment.artifacts();
        var resources = new DocumentAdmissionResources(reservations);
        boolean transferred = false;
        try {
            var copied = new HashMap<String, ByteString>();
            var proof = check(request.candidate(), hash -> {
                active(control);
                var bytes = artifacts.get(hash);
                if (bytes == null) return Optional.empty();
                return Optional.of(copied.computeIfAbsent(hash, ignored -> resources.copy(bytes, () -> active(control))));
            }, limits, resources, request.evaluatedAt(), control);
            if (!proof.artifacts().keySet().equals(artifacts.keySet()))
                throw new IllegalArgumentException("Assessment artifacts differ from complete proof union");
            active(control);
            var result = new PreparedProof(proof, resources);
            transferred = true;
            return result;
        } catch (DocumentAdmissionResources.ReservationFailure failure) {
            throw failure.original;
        } finally {
            if (!transferred) resources.close();
        }
    }

    /** Owns serialized schema/evidence bytes; borrowed proof references must not outlive this owner. */
    public static final class PreparedProof implements AutoCloseable {
        private Proof proof;
        private final DocumentAdmissionResources resources;
        private PreparedProof(Proof proof, DocumentAdmissionResources resources) {
            this.proof = proof; this.resources = resources;
        }
        /** Borrow while open. Callers coordinate consumers before closing this owner. */
        public synchronized Proof proof() {
            if (proof == null) throw new IllegalStateException("Prepared proof is closed");
            return proof;
        }
        @Override public synchronized void close() {
            if (proof == null) return;
            proof = null;
            resources.close();
        }
    }

    /** Constructed only after complete checking. Immutable content proof, not an authorization receipt. */
    public static final class Proof {
        private final ByteString commandSha256;
        private final String policySha256;
        private final boolean requireStructuredRoot;
        private final Limits limits;
        private final DocumentPublicationMember member;
        private final Document document;
        private final Map<Integer, ByteString> fragments;
        private final List<RootEvidence> roots;
        private final Reference containerReference;
        private final List<Reference> references;
        private final Map<String, ByteString> artifacts;
        private Proof(Request request, Limits limits, Document document, Map<Integer, ByteString> fragments,
                      List<RootEvidence> roots, List<Reference> references, Map<String, ByteString> artifacts) {
            this.commandSha256 = request.commandSha256(); this.policySha256 = request.policySha256();
            this.requireStructuredRoot = request.requireStructuredRoot();
            this.containerReference = request.container();
            this.limits = limits;
            this.member = request.member(); this.document = document; this.fragments = Map.copyOf(fragments);
            this.roots = List.copyOf(roots); this.references = List.copyOf(references); this.artifacts = Map.copyOf(artifacts);
        }
        public ByteString commandSha256() { return commandSha256; }
        public String policySha256() { return policySha256; }
        public boolean requireStructuredRoot() { return requireStructuredRoot; }
        public Limits limits() { return limits; }
        public String validationProfile() { return PROFILE; }
        public DocumentPublicationMember member() { return member; }
        public Document document() { return document; }
        public Map<Integer, ByteString> fragments() { return fragments; }
        public List<RootEvidence> roots() { return roots; }
        /** Exact containing Document association, distinct from aliases used by payloads. */
        public Reference containerReference() { return containerReference; }
        /** Includes the containing Document definition and all used payload associations. */
        public List<Reference> references() { return references; }
        /** Exact normalized descriptor, metadata and optional source bytes to retain atomically. */
        public Map<String, ByteString> artifacts() { return artifacts; }
    }

    public static Proof check(Request request, Reader reader, Limits limits, Runnable control)
            throws InvalidProtocolBufferException {
        return check(request, reader, limits, null, control);
    }

    /** Resources already own reader assets; this check additionally retains final encoded evidence. */
    static Proof check(Request request, Reader reader, Limits limits, DocumentAdmissionResources resources,
            Runnable control) throws InvalidProtocolBufferException {
        return check(request, reader, limits, resources, java.time.Instant.now(), control);
    }

    /** Internal proof checking at the exact assessment time, with the same ownership requirements. */
    static Proof check(Request request, Reader reader, Limits limits, DocumentAdmissionResources resources,
            java.time.Instant evaluatedAt, Runnable control) throws InvalidProtocolBufferException {
        Objects.requireNonNull(evaluatedAt);
        Objects.requireNonNull(request); Objects.requireNonNull(reader); Objects.requireNonNull(limits);
        Objects.requireNonNull(control); active(control);
        if (request.commandSha256().size() != 32 || !request.policySha256().matches("[0-9a-f]{64}"))
            throw new IllegalArgumentException("exact command and policy digests required");
        var member = request.member();
        if (member.getPartsCount() > limits.maxFragments() || request.fragments().size() > limits.maxFragments()
                || request.evidence().size() > limits.maxFragments() || request.references().size() >= limits.maxBindings())
            throw new IllegalArgumentException("member admission count exceeds limit");
        var fragments = Map.copyOf(request.fragments());
        var assembly = checkMember(member, fragments, request.requireStructuredRoot(), limits, evaluatedAt, control);
        var bundles = decodeEvidence(request.evidence(), limits, resources, control);
        if (bundles.keySet().stream().anyMatch(ordinal -> ordinal >= member.getPartsCount()))
            throw new IllegalArgumentException("evidence ordinal is outside the member");
        var artifacts = new HashMap<String, ByteString>();
        var retained = new DocumentRetainedSchemaAssets(hash -> {
            var result = Objects.requireNonNull(reader.read(hash));
            result.ifPresent(bytes -> artifacts.put(hash, bytes));
            return result;
        }, new DocumentRetainedSchemaAssets.Limits(limits.maxBindings(), limits.maxRetainedBytes(),
                new ClosedDescriptorSet.Limits(16 * MIB, 256, 4096, 64)), resources);
        var references = new ArrayList<Reference>();
        references.add(request.container()); references.addAll(List.copyOf(request.references()));
        var byKey = new HashMap<DocumentPayloadCheck.SchemaKey, Reference>();
        var metadata = new HashMap<DocumentPayloadCheck.SchemaKey, RepositorySchemaAsset>();
        var expectedArtifacts = new HashSet<String>();
        for (var reference : references) {
            active(control);
            var key = key(reference);
            if (byKey.putIfAbsent(key, reference) != null) throw new IllegalArgumentException("duplicate schema association");
            expectedArtifacts.add(reference.descriptorSha256()); expectedArtifacts.add(reference.metadataSha256());
            reference.sourceSha256().ifPresent(expectedArtifacts::add);
        }
        if (expectedArtifacts.size() > 64) throw new IllegalArgumentException("retained artifact count exceeds limit");
        for (var reference : references) {
            active(control);
            // Verify claimed source retention before the internal descriptor-only replay.
            metadata.put(key(reference), retained.resolve(reference.internal(), () -> active(control)).metadata());
        }
        var container = retained.resolve(request.container().internal(), () -> active(control)).schema();
        var used = new HashSet<DocumentPayloadCheck.SchemaKey>(); used.add(key(request.container()));
        var roots = new ArrayList<RootEvidence>();
        long decodedRemaining = limits.maxDecodedBytes();
        for (int ordinal = 0; ordinal < member.getPartsCount(); ordinal++) {
            active(control);
            var part = member.getParts(ordinal);
            var evidence = bundles.getOrDefault(ordinal, List.of());
            if (part.hasEmpty()) {
                if (!evidence.isEmpty()) throw new IllegalArgumentException("evidence attached to empty slot");
                continue;
            }
            if (part.getSlot().getPart() != DocumentPart.DOCUMENT_PART_CORE
                    && part.getSlot().getPart() != DocumentPart.DOCUMENT_PART_PARSED) {
                if (!evidence.isEmpty()) throw new IllegalArgumentException("evidence attached to unsupported root slot");
                continue; // Assembly confinement excludes Any-bearing fields from these slots.
            }
            if (decodedRemaining < 1 && !evidence.isEmpty())
                throw new IllegalArgumentException("member decoded payload budget exhausted");
            var replayLimits = new DocumentFragmentSchemaReplay.Limits(
                    new DocumentAnyRootInventory.Limits(Integer.MAX_VALUE, 1_000_000, 64, limits.maxRoots(), 10000),
                    new DocumentPayloadCheck.Limits((int) Math.max(1, decodedRemaining), 1_000_000, 64, 4096, 65536),
                    DocumentSchemaOccurrences.Limits.DEFAULT, new DocumentSchemaReplay.Limits(4096, 4L * MIB, 65536),
                    limits.maxEvidenceBytes());
            var checked = DocumentFragmentSchemaReplay.check(container, part.getSlot(), fragments.get(ordinal),
                            member.getDestination().getAddress().getDocId(), evidence, metadata, retained, VALIDATOR,
                            replayLimits, resources, evaluatedAt, () -> active(control));
            for (int i = 0; i < checked.size(); i++) {
                var root = checked.get(i);
                decodedRemaining -= root.checked().payload().decodedBytes();
                used.addAll(root.checked().assets().keySet());
                if (part.getSlot().getPart() == DocumentPart.DOCUMENT_PART_CORE && member.hasStructuredSchema()
                        && !root.checked().payload().schema().condition().equals(member.getStructuredSchema()))
                    throw new IllegalArgumentException("structured root differs from required schema");
                var bundle = evidence.stream().filter(value -> value.getRoot().equals(root.root())).findFirst().orElseThrow();
                if (resources == null) {
                    var encoded = DocumentRootSchemaEvidenceCodec.encode(bundle, () -> active(control));
                    roots.add(new RootEvidence(ordinal, root.root(),
                            DocumentSchemaRootCodec.encode(root.root(), () -> active(control)).sha256(),
                            new EncodedEvidence(DocumentRootSchemaEvidenceCodec.CODEC, 1, encoded.bytes(), encoded.sha256())));
                } else {
                    var encoded = resources.retain(DocumentRootSchemaEvidenceCodec.encodeOwned(bundle,
                            limits.maxEvidenceBytes(), resources, () -> active(control)));
                    try (var locator = DocumentSchemaEvidenceCodec.encodeOwned(root.root(), resources, () -> active(control))) {
                        roots.add(new RootEvidence(ordinal, root.root(), locator.value().sha256(),
                                new EncodedEvidence(DocumentRootSchemaEvidenceCodec.CODEC, 1, encoded.bytes(), encoded.sha256())));
                    }
                }
            }
        }
        if (!used.equals(byKey.keySet())) throw new IllegalArgumentException("schema associations differ from complete used set");
        if (!artifacts.keySet().equals(expectedArtifacts))
            throw new IllegalArgumentException("retained artifacts differ from complete bounded union");
        active(control);
        return new Proof(request, limits, assembly.document(), fragments, roots, references, artifacts);
    }

    static Map<Integer, List<DocumentRootSchemaEvidence>> decodeEvidence(
            Map<Integer, List<EncodedEvidence>> input, Limits limits, DocumentAdmissionReservations reservations,
            Runnable control) throws InvalidProtocolBufferException {
        var result = new HashMap<Integer, List<DocumentRootSchemaEvidence>>();
        long bytes = 0; int roots = 0;
        for (var entry : input.entrySet()) {
            active(control);
            if (entry.getKey() < 0 || entry.getKey() >= limits.maxFragments())
                throw new IllegalArgumentException("invalid evidence ordinal");
            if (entry.getValue().size() > limits.maxRoots() - roots)
                throw new IllegalArgumentException("member evidence root limit exceeded");
            if (entry.getValue().isEmpty()) throw new IllegalArgumentException("empty evidence entry is not a root binding");
            var decoded = new ArrayList<DocumentRootSchemaEvidence>();
            for (var encoded : entry.getValue()) {
                active(control);
                if (encoded.bytes().size() > limits.maxEvidenceBytes() - bytes)
                    throw new IllegalArgumentException("member evidence byte limit exceeded");
                bytes += encoded.bytes().size(); roots++;
                decoded.add(reservations == null
                        ? DocumentRootSchemaEvidenceCodec.decode(encoded.codec(), encoded.version(), encoded.bytes(),
                                encoded.sha256(), () -> active(control))
                        : DocumentRootSchemaEvidenceCodec.decode(encoded.codec(), encoded.version(), encoded.bytes(),
                                encoded.sha256(), reservations, () -> active(control)));
            }
            result.put(entry.getKey(), List.copyOf(decoded));
        }
        return Map.copyOf(result);
    }

    static DocumentRevisionAssembly.Result checkMember(DocumentPublicationMember member, Map<Integer, ByteString> fragments,
            boolean requireStructuredRoot, Limits limits, Runnable control) throws InvalidProtocolBufferException {
        return checkMember(member, fragments, requireStructuredRoot, limits, java.time.Instant.now(), control);
    }

    static DocumentRevisionAssembly.Result checkMember(DocumentPublicationMember member, Map<Integer, ByteString> fragments,
            boolean requireStructuredRoot, Limits limits, java.time.Instant evaluatedAt, Runnable control)
            throws InvalidProtocolBufferException {
        if (member.getPartsCount() > limits.maxFragments() || fragments.size() > limits.maxFragments())
            throw new IllegalArgumentException("member admission count exceeds limit");
        if (member.getSerializedSize() > MIB) throw new IllegalArgumentException("publication member exceeds byte limit");
        VALIDATOR.validate(member, evaluatedAt).throwIfInvalid();
        var assembly = assemble(member, fragments, limits, control);
        if (!assembly.document().hasOwnership() || !assembly.document().getOwnership().equals(member.getOwnership()))
            throw new IllegalArgumentException("decoded ownership differs from publication member");
        requireKnownDocument(assembly.document(), "", 0, new int[1], control);
        VALIDATOR.validate(assembly.document(), evaluatedAt).throwIfInvalid();
        if ((requireStructuredRoot || member.hasStructuredSchema()) && !assembly.document().hasStructuredData())
            throw new IllegalArgumentException("required structured root is absent");
        return assembly;
    }

    private static DocumentRevisionAssembly.Result assemble(DocumentPublicationMember member, Map<Integer, ByteString> bytes,
            Limits limits, Runnable control) throws InvalidProtocolBufferException {
        var expected = new HashSet<Integer>(); var fragments = new ArrayList<DocumentRevisionAssembly.Fragment>();
        long total = 0;
        for (int ordinal = 0; ordinal < member.getPartsCount(); ordinal++) {
            active(control);
            var part = member.getParts(ordinal);
            if (part.hasEmpty()) continue;
            expected.add(ordinal);
            var value = bytes.get(ordinal);
            if (value == null || value.size() > limits.maxFragmentBytes() - total)
                throw new IllegalArgumentException("member fragments missing or exceed byte limit");
            total += value.size();
            long size;
            String hash;
            switch (part.getContentCase()) {
                case UPLOAD -> { size = part.getUpload().getSizeBytes(); hash = part.getUpload().getSha256(); }
                case REUSE -> { size = part.getReuse().getObject().getSizeBytes(); hash = part.getReuse().getObject().getSha256(); }
                case HISTORICAL_REUSE -> {
                    size = part.getHistoricalReuse().getObject().getSizeBytes();
                    hash = part.getHistoricalReuse().getObject().getSha256();
                }
                default -> throw new IllegalArgumentException("Unsupported publication content declaration");
            }
            if (value.size() != size || !DocumentSchemaOccurrences.sha256(value, () -> active(control)).equals(hash))
                throw new IllegalArgumentException("fragment differs from publication declaration");
            fragments.add(new DocumentRevisionAssembly.Fragment(part.getSlot().getPart(), part.getSlot().getSubKey(), value));
        }
        if (!bytes.keySet().equals(expected)) throw new IllegalArgumentException("fragment ordinals differ from complete member");
        return DocumentRevisionAssembly.assemble(fragments, member.getDestination().getAddress().getDocId(),
                new DocumentRevisionAssembly.Limits(limits.maxFragmentBytes(), limits.maxFragments(), 64, 1_000_000, 1_000_000), control);
    }

    private static DocumentPayloadCheck.SchemaKey key(Reference reference) {
        return new DocumentPayloadCheck.SchemaKey(reference.typeUrl(), reference.descriptorSha256());
    }

    /** Fail on newly introduced or unknown Any locations instead of silently omitting them. */
    private static void requireKnownDocument(com.google.protobuf.Message message, String path, int depth,
                                             int[] visited, Runnable control) {
        active(control);
        if (depth > 64 || ++visited[0] > 1_000_000)
            throw new IllegalArgumentException("document root discovery exceeds structural limits");
        if (!message.getUnknownFields().asMap().isEmpty())
            throw new IllegalArgumentException("unknown document fields cannot receive typed admission");
        if (message.getDescriptorForType().getFullName().equals("google.protobuf.Any")) {
            if (!path.equals("structured_data") && !path.equals("parser_results.value.document.shape"))
                throw new IllegalArgumentException("unsupported document Any location: " + path);
            return;
        }
        for (var entry : message.getAllFields().entrySet()) {
            var field = entry.getKey();
            if (field.getJavaType() != com.google.protobuf.Descriptors.FieldDescriptor.JavaType.MESSAGE) continue;
            String childPath = path.isEmpty() ? field.getName() : path + "." + field.getName();
            if (field.isRepeated()) {
                for (var child : (List<?>) entry.getValue())
                    requireKnownDocument((com.google.protobuf.Message) child, childPath, depth + 1, visited, control);
            } else requireKnownDocument((com.google.protobuf.Message) entry.getValue(), childPath, depth + 1, visited, control);
        }
    }
    private static void active(Runnable control) {
        if (Thread.currentThread().isInterrupted()) throw new CancellationException("member schema admission interrupted");
        control.run();
    }
}
