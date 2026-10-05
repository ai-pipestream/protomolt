package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.repo.admission.*;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.*;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.v1.*;
import ai.protomolt.proto.validate.*;
import com.google.protobuf.*;
import java.time.Instant;
import java.util.*;

/** Real typed/opaque assessment and ownership checks in the observed standard JVM. */
public final class ObservedAssessmentProbe {
    private static final Instant AT = Instant.parse("2000-01-01T00:00:00.123456789Z");
    private record Member(DocumentPublicationMember member, Map<Integer, ByteString> fragments) {}
    public static void run(DocumentAssessmentRuntimeObserver.Observation observation) throws Exception {
        var a = member("a"); var b = member("b");
        var command = new DocumentPublicationCommand(DocumentPublicationIntent.newBuilder().setEncodingVersion(1)
                .setOperationId(UUID.randomUUID().toString()).setAccountId("account").addMembers(a.member()).addMembers(b.member()).build());
        var policy = DocumentAdmissionPolicy.of(DocumentSchemaPolicy.newBuilder().setEncodingVersion(1).setAccountId("account")
                .setValidationProfile(DocumentSchemaAdmission.PROFILE).setMode(DocumentSchemaPolicyMode.DOCUMENT_SCHEMA_POLICY_MODE_OPAQUE_ALLOWED)
                .setAnyResolvedSchema(true).setLimits(DocumentSchemaPolicyLimits.newBuilder().setMaxFragments(20).setMaxFragmentBytes(4_000_000)
                        .setMaxRoots(100).setMaxEvidenceBytes(4_000_000).setMaxBindings(20).setMaxRetainedBytes(16_000_000)
                        .setMaxDecodedBytes(1_000_000)).build(), () -> {});
        var budget = new PayloadBudget(32_000_000);
        var schema = invalidSchema();
        var calls = new java.util.concurrent.atomic.AtomicInteger();
        var assessment = DocumentPublicationAssessment.prepare(command, new DocumentSchemaPolicies.Selection("account", 1, policy),
                Map.of("a", DocumentPublicationCandidate.Mode.TYPED, "b", DocumentPublicationCandidate.Mode.OPAQUE),
                Map.of("a", a.fragments(), "b", b.fragments()), Optional.of(asset(Document.getDescriptor())),
                (member, occurrence) -> { calls.incrementAndGet(); return schema; }, budget,
                new DocumentRevisionAssembly.Limits(4_000_000, 32, 64, 10000, 1_000_000), AT, () -> {});
        var owner = new RepositoryOperationLedger.Owner(new RepositoryOperationLedger.Key("account", "principal", command.operationId()),
                1, UUID.randomUUID(), AT);
        try (assessment) {
            long owned = budget.reservedBytes();
            String classpath = System.getProperty("java.class.path");
            var changedAfterEncoding = new java.util.concurrent.atomic.AtomicBoolean();
            try {
                try (var unexpected = assessment.encodeManifest(owner, observation, () -> {
                    boolean observing = StackWalker.getInstance().walk(frames -> frames.anyMatch(frame ->
                            frame.getClassName().equals(DocumentAssessmentRuntimeObserver.Observation.class.getName())
                                    && frame.getMethodName().equals("identity")));
                    if (observing && budget.reservedBytes() > owned && changedAfterEncoding.compareAndSet(false, true)) {
                        System.setProperty("java.class.path", classpath + java.io.File.pathSeparator + "changed.jar");
                    }
                })) { throw new AssertionError("Changed runtime accepted"); }
            } catch (IllegalStateException expected) {
                require(expected.getMessage().contains("differs from its observation"), "specific runtime context failure");
            } finally { System.setProperty("java.class.path", classpath); }
            require(changedAfterEncoding.get() && budget.reservedBytes() == owned, "post-encode failure releases output");
            var stop = new java.util.concurrent.CancellationException("stop");
            try (var unexpected = assessment.encodeManifest(owner, observation, () -> {
                if (budget.reservedBytes() > owned) throw stop;
            })) { throw new AssertionError("Cancellation accepted"); }
            catch (java.util.concurrent.CancellationException expected) { require(expected == stop, "original cancellation"); }
            require(budget.reservedBytes() == owned, "cancellation releases scratch");
            var escaped = new java.util.concurrent.atomic.AtomicReference<DocumentAssessmentEvidence>();
            String result = assessment.withRetentionEvidence(owner, observation, () -> {}, evidence -> {
                escaped.set(evidence);
                require(evidence.command(() -> {}).equals(command), "retention command identity");
                require(evidence.policy(() -> {}).equals(assessment.policy()), "retention policy identity");
                require(evidence.artifacts(() -> {}).equals(assessment.artifacts()), "complete normalized schema set");
                var roots = evidence.roots(() -> {});
                require(roots.size() == assessment.typed().get("a").roots().size(), "complete independent root set");
                for (var root : roots) {
                    require(root.member().equals("a"), "opaque member contributes no typed roots");
                    var source = assessment.typed().get("a").roots().stream()
                            .filter(value -> value.ordinal() == root.ordinal() && value.locatorSha256().equals(root.locatorSha256()))
                            .findFirst().orElseThrow();
                    require(source.encoded().bytes().equals(root.bytes()) && source.encoded().sha256().equals(root.sha256()), "exact encoded root ownership");
                    var upload = a.member().getParts(root.ordinal()).getUpload();
                    require(root.fragmentSha256().equals(upload.getSha256()) && root.fragmentSize() == upload.getSizeBytes(), "full ordinal fragment binding");
                }
                try { assessment.close(); throw new AssertionError("Retention callback allowed parent close"); }
                catch (IllegalStateException expected) { require(expected.getMessage().contains("verification is active"), "retention busy guard"); }
                require(budget.reservedBytes() == owned + evidence.manifestBytes(() -> {}).size(), "retention borrows without payload copies");
                return evidence.manifestSha256(() -> {});
            });
            require(result.matches("[0-9a-f]{64}") && budget.reservedBytes() == owned, "scope success releases encoding");
            try { escaped.get().artifacts(() -> {}); throw new AssertionError("Escaped retention scope remained live"); }
            catch (IllegalStateException expected) { require(expected.getMessage().contains("scope is closed"), "scope invalidated"); }
            try {
                assessment.withRetentionEvidence(owner, observation, () -> {}, evidence -> { throw stop; });
                throw new AssertionError("Retention callback cancellation accepted");
            } catch (java.util.concurrent.CancellationException expected) { require(expected == stop, "retention preserves callback failure"); }
            require(budget.reservedBytes() == owned, "failed callback releases encoding");
            try {
                assessment.withRetentionEvidence(owner, observation, () -> {}, evidence -> evidence.manifestBytes(() -> {
                    boolean exposingBytes = StackWalker.getInstance().walk(frames -> frames.anyMatch(frame ->
                            frame.getClassName().equals(DocumentPublicationAssessment.ObservedManifest.class.getName())
                                    && frame.getMethodName().equals("bytes")));
                    if (exposingBytes) evidence.close();
                }));
                throw new AssertionError("Reentrant scope close exposed manifest bytes");
            } catch (IllegalStateException expected) { require(expected.getMessage().contains("scope is closed"), "closed getter guard"); }
            require(budget.reservedBytes() == owned, "closed getter releases encoding");
            try {
                assessment.withRetentionEvidence(owner, observation, () -> {}, evidence -> {
                    System.setProperty("java.class.path", classpath + java.io.File.pathSeparator + "changed.jar");
                    return "must not return";
                });
                throw new AssertionError("Post-callback runtime drift accepted");
            } catch (IllegalStateException expected) { require(expected.getMessage().contains("differs from its observation"), "post-callback context guard"); }
            finally { System.setProperty("java.class.path", classpath); }
            require(budget.reservedBytes() == owned, "post-callback failure releases encoding");
            var checkedBusy = new java.util.concurrent.atomic.AtomicBoolean();
            try (var encoded = assessment.encodeManifest(owner, observation, () -> {
                if (checkedBusy.compareAndSet(false, true)) {
                    try { assessment.close(); throw new AssertionError("Active encoding allowed parent close"); }
                    catch (IllegalStateException expected) { require(expected.getMessage().contains("verification is active"), "busy close guard"); }
                    try (var nested = assessment.encodeManifest(owner, observation, () -> {})) { throw new AssertionError("Reentrant encoding accepted"); }
                    catch (IllegalStateException expected) { require(expected.getMessage().contains("verification is active"), "busy encode guard"); }
                    catch (InvalidProtocolBufferException unexpected) { throw new AssertionError(unexpected); }
                }
            })) {
                var manifest = DocumentAssessmentManifestCodec.decode(DocumentAssessmentManifestCodec.CODEC, 1,
                        encoded.bytes(() -> {}), encoded.sha256(() -> {}), bytes -> {
                            var lease = budget.reserve(bytes); return lease::close;
                        }, () -> {});
                require(manifest.getRuntime().equals(observation.identity(() -> {})), "observed runtime bound");
                require(manifest.getFirstFailure().getRuleId().equals("reject-fixture") && manifest.getFirstFailure().getMemberId().equals("a"), "typed rejection bound");
                var typed = assessment.typed().get("a");
                var root = typed.roots().getFirst();
                require(manifest.getFirstFailure().getRoot().getSha256().equals(root.encoded().sha256())
                        && manifest.getFirstFailure().getRoot().getOrdinal() == root.ordinal(), "exact failure root bound");
                require(manifest.getFirstFailure().getOccurrence().getStepsList().equals(typed.failure().orElseThrow().occurrence()), "exact failure occurrence bound");
                require(manifest.getMembers(0).hasTyped() && manifest.getMembers(1).getOpaque(), "explicit modes retained");
                require(manifest.getEvaluatedAt().getNanos() == AT.getNano(), "exact evaluation instant");
                require(calls.get() == 1, "replay uses retained schema instead of registry");
                var wrongOwner = new RepositoryOperationLedger.Owner(owner.key(), owner.generation() + 1, owner.token(), owner.leaseUntil());
                try (var unexpected = new DocumentAssessmentEvidence(assessment, wrongOwner, encoded, bytes -> {
                    var lease = budget.reserve(bytes); return lease::close;
                }, () -> {})) { throw new AssertionError("Mismatched retention generation accepted"); }
                catch (IllegalArgumentException expected) { require(expected.getMessage().contains("retention identity differs"), "retention owner binding"); }
                assessment.close();
                require(budget.reservedBytes() == encoded.bytes(() -> {}).size(), "independent output lifetime");
                var thread = Thread.currentThread(); var previous = thread.getContextClassLoader();
                try {
                    thread.setContextClassLoader(ClassLoader.getPlatformClassLoader());
                    try { encoded.bytes(() -> {}); throw new AssertionError("Stale observed bytes exposed"); }
                    catch (IllegalStateException expected) { }
                    encoded.close();
                } finally { thread.setContextClassLoader(previous); }
                try { encoded.check(() -> {}); throw new AssertionError("Closed owner accepted"); }
                catch (IllegalStateException expected) { require(expected.getMessage().contains("closed"), "closed output"); }
            }
        }
        require(budget.reservedBytes() == 0, "all reservations released");
    }
    private static Member member(String id) throws Exception {
        var ownership = OwnershipContext.newBuilder().setAccountId("account").setDatasourceId("source").setSecurity(DocumentSecurity.getDefaultInstance()).build();
        var document = Document.newBuilder().setDocId("doc-" + id).setOwnership(ownership).setStructuredData(Any.pack(StringValue.of("payload"), "type.test")).build();
        var member = DocumentPublicationMember.newBuilder().setMemberId(id).setDriveId(UUID.randomUUID().toString()).setOwnership(ownership)
                .setRowKind(DocumentPublicationRowKind.DOCUMENT_PUBLICATION_ROW_KIND_PIPELINE)
                .setDestination(DocumentRevisionCondition.newBuilder().setIfAbsent(true).setAddress(NodeAddress.newBuilder()
                        .setAccountId("account").setDocId("doc-" + id).setGraphId("graph").setGraphAddressId("node")));
        var fragments = new HashMap<Integer, ByteString>();
        for (var part : DocumentPartCodec.split(document, PartLayouts.document())) {
            var bytes = ByteString.copyFrom(part.bytes()); fragments.put(fragments.size(), bytes);
            member.addParts(DocumentPublicationPart.newBuilder().setSlot(DocumentPublicationSlot.newBuilder().setPart(part.part()).setSubKey(part.subKey()))
                    .setUpload(PublicationUpload.newBuilder().setSizeBytes(bytes.size()).setSha256(sha(bytes)).setContentType("application/protobuf")));
        }
        return new Member(member.build(), Map.copyOf(fragments));
    }
    private static DocumentSchemaAdmission.Definition invalidSchema() throws Exception {
        var proto = StringValue.getDescriptor().getFile().toProto().toBuilder().addDependency(ValidateProto.getDescriptor().getName());
        for (var type : proto.getMessageTypeBuilderList()) if (type.getName().equals("StringValue"))
            type.setOptions(type.getOptions().toBuilder().setExtension(ValidateProto.message,
                    MessageRules.newBuilder().addCel(CelRule.newBuilder().setId("reject-fixture").setExpression("false")).build()));
        return asset(com.google.protobuf.Descriptors.FileDescriptor.buildFrom(proto.build(),
                new com.google.protobuf.Descriptors.FileDescriptor[]{ValidateProto.getDescriptor()}).findMessageTypeByName("StringValue"));
    }
    private static DocumentSchemaAdmission.Definition asset(com.google.protobuf.Descriptors.Descriptor type) throws Exception {
        var closure = DescriptorFingerprints.closure(type); var bytes = closure.toByteString();
        var metadata = RepositorySchemaAsset.newBuilder().setSchema(PublicationSchemaCondition.newBuilder().setTypeName(type.getFullName())
                        .setDescriptorFingerprint(DescriptorFingerprints.fingerprint(closure))).setArtifactSha256(sha(bytes))
                .setTypeUrl("type.test/" + type.getFullName()).setCompilation(SchemaCompilationProvenance.newBuilder()
                        .setOrigin(SchemaCompilationOrigin.SCHEMA_COMPILATION_ORIGIN_IMPORTED_DESCRIPTOR).setEvidence(SchemaCompilerEvidence.SCHEMA_COMPILER_EVIDENCE_UNKNOWN)
                        .setUnknownCompilerReason("synthetic descriptor fixture; compiler unknown")
                        .setAdmissionRuntime(SchemaToolIdentity.newBuilder().setName("fixture-descriptor-origin").setVersion("test"))).build();
        return new DocumentSchemaAdmission.Definition(metadata, bytes, Optional.empty());
    }
    private static String sha(ByteString bytes) throws Exception {
        return HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray()));
    }
    private static void require(boolean condition, String label) { if (!condition) throw new AssertionError(label); }
}
