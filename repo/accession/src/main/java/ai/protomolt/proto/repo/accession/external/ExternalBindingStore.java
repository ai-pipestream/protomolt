package ai.protomolt.proto.repo.accession.external;

import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.grpc.profile.ServiceProfileRepository;
import ai.protomolt.proto.grpc.profile.ServiceProfileValidation;
import ai.protomolt.proto.grpc.profile.v1.ServiceEndpoint;
import ai.protomolt.proto.grpc.profile.v1.ServiceProfile;
import ai.protomolt.proto.grpc.profile.v1.Transport;
import ai.protomolt.proto.grpc.workspace.ProfileCredentialResolver;
import ai.protomolt.proto.registry.ConfigDocumentStore;
import ai.protomolt.proto.repo.external.v1.ExternalBindingLifecycle;
import ai.protomolt.proto.repo.external.v1.ExternalEndpointSummary;
import ai.protomolt.proto.repo.external.v1.ExternalIdentityProof;
import ai.protomolt.proto.repo.external.v1.ExternalRetentionPolicy;
import ai.protomolt.proto.repo.external.v1.ExternalSystemBinding;
import ai.protomolt.proto.repo.v1.Access;
import ai.protomolt.proto.repo.v1.AccessRule;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.DocumentSecurity;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.OwnershipContext;
import ai.protomolt.proto.repo.v1.Principal;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.repo.v2.DocumentPart;
import ai.protomolt.proto.repo.v2.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v2.DocumentSummary;
import ai.protomolt.proto.repo.v2.DocumentTarget;
import ai.protomolt.proto.repo.v2.ExpectedRevision;
import ai.protomolt.proto.repo.v2.GetDocumentRequest;
import ai.protomolt.proto.repo.v2.GetDocumentResponse;
import ai.protomolt.proto.repo.v2.IntakeOrigin;
import ai.protomolt.proto.repo.v2.ListDocumentsRequest;
import ai.protomolt.proto.repo.v2.ListDocumentsResponse;
import ai.protomolt.proto.repo.v2.PartSelection;
import ai.protomolt.proto.repo.v2.PartSelector;
import ai.protomolt.proto.repo.v2.PartWrite;
import ai.protomolt.proto.repo.v2.RevisionSelector;
import ai.protomolt.proto.repo.v2.SaveDocumentRequest;
import ai.protomolt.proto.repo.v2.SaveOwner;
import ai.protomolt.proto.repo.v2.ValidationMode;
import ai.protomolt.proto.repo.v2.WriteAction;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.ValidationResult;
import ai.protomolt.proto.validate.protovalidate.ProtovalidateRuleSource;
import ai.protomolt.proto.validate.source.ProtomoltRuleSource;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import io.grpc.Channel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Account-scoped versioned store for {@link ExternalSystemBinding} configurations,
 * backed by the selected-part v2 {@code DocumentService} ({@code external-bindings:<account_id>})
 * and optionally mirrored through the registry config lane ({@link ConfigDocumentStore}).
 *
 * <p>Validates referenced {@link ServiceProfile} and {@link ServiceEndpoint} definitions
 * via {@link ServiceProfileValidation} and verifies live credential resolution through
 * {@link ProfileCredentialResolver} without persisting secret values, bearer URLs, or key material.
 */
public final class ExternalBindingStore {

    private static final ProtoValidator VALIDATOR = ProtoValidator.create(
            List.of(new ProtomoltRuleSource(), new ProtovalidateRuleSource()));

    private static final PartSelector CORE_SELECTOR =
            PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE).build();

    private static final PartSelection CORE_SELECTION =
            PartSelection.newBuilder().addSelectors(CORE_SELECTOR).build();

    private static final PartWrite CORE_PUT = PartWrite.newBuilder()
            .setSelector(CORE_SELECTOR)
            .setAction(WriteAction.WRITE_ACTION_PUT)
            .build();

    private final DocumentServiceGrpc.DocumentServiceBlockingStub documents;
    private final ServiceProfileRepository profiles;
    private final ProfileCredentialResolver credentials;
    private final ExternalBindingConfigLane configLane;
    private final String drive;
    private final SaveOwner saveOwner;
    private final DocumentSecurity bindingSecurity;
    private final Clock clock;

    /**
     * Constructs an {@link ExternalBindingStore} with default intake datasource ownership.
     *
     * @param documents gRPC channel to {@code DocumentService}
     * @param profiles durable service profile repository
     * @param credentials host-owned outbound credential resolver
     * @param configStore optional config-lane store (may be {@code null})
     * @param drive named repository drive used for binding saves
     * @param bindingWriters the only identities granted WRITE on binding documents
     * @param clock clock stamping binding revision creation and retirement instants
     */
    public ExternalBindingStore(
            Channel documents,
            ServiceProfileRepository profiles,
            ProfileCredentialResolver credentials,
            ConfigDocumentStore configStore,
            String drive,
            Set<Principal> bindingWriters,
            Clock clock) {
        this(
                documents,
                profiles,
                credentials,
                configStore,
                drive,
                SaveOwner.newBuilder()
                        .setIntake(
                                IntakeOrigin.newBuilder()
                                        .setDatasourceId(
                                                ExternalPublicationCoordinates
                                                        .DEFAULT_BINDINGS_DATASOURCE_ID))
                        .build(),
                bindingWriters,
                clock);
    }

    /**
     * Constructs an {@link ExternalBindingStore} with an explicit {@link SaveOwner}.
     *
     * @param documents gRPC channel to {@code DocumentService}
     * @param profiles durable service profile repository
     * @param credentials host-owned outbound credential resolver
     * @param configStore optional config-lane store (may be {@code null})
     * @param drive named repository drive used for binding saves
     * @param saveOwner explicit repository save owner
     * @param bindingWriters the only identities granted WRITE on binding documents; account
     *        members read them. A binding redirects publication routing, so write access is
     *        never granted to every account member.
     * @param clock clock stamping binding revision creation and retirement instants
     */
    public ExternalBindingStore(
            Channel documents,
            ServiceProfileRepository profiles,
            ProfileCredentialResolver credentials,
            ConfigDocumentStore configStore,
            String drive,
            SaveOwner saveOwner,
            Set<Principal> bindingWriters,
            Clock clock) {
        this.documents = DocumentServiceGrpc.newBlockingStub(
                Objects.requireNonNull(documents, "documents"));
        this.profiles = Objects.requireNonNull(profiles, "profiles");
        this.credentials = Objects.requireNonNull(credentials, "credentials");
        this.configLane = new ExternalBindingConfigLane(configStore);
        ExternalPublicationCoordinates.requireNonBlankCoordinate(drive, "drive");
        this.drive = drive;
        this.saveOwner = Objects.requireNonNull(saveOwner, "saveOwner");
        this.bindingSecurity = bindingSecurity(bindingWriters);
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /**
     * The ACL written on every binding revision: READ for account members, WRITE only for
     * the named writers. A {@code public} writer would hand routing control to every account
     * member and is refused, as is an empty writer set.
     */
    static DocumentSecurity bindingSecurity(Set<Principal> bindingWriters) {
        Objects.requireNonNull(bindingWriters, "bindingWriters");
        if (bindingWriters.isEmpty()) {
            throw new IllegalArgumentException("bindingWriters must name at least one identity");
        }
        DocumentSecurity.Builder security = DocumentSecurity.newBuilder()
                .addPermissions(AccessRule.newBuilder()
                        .setIdentityType("public")
                        .setIdentity("public")
                        .setAccess(Access.ACCESS_READ));
        List<Principal> ordered = new ArrayList<>(bindingWriters);
        ordered.sort((a, b) -> {
            int byType = a.getIdentityType().compareTo(b.getIdentityType());
            return byType != 0 ? byType : a.getIdentity().compareTo(b.getIdentity());
        });
        for (Principal writer : ordered) {
            if (writer.getIdentityType().isBlank() || writer.getIdentity().isBlank()) {
                throw new IllegalArgumentException("binding writer identity must not be blank");
            }
            if (writer.getIdentityType().equalsIgnoreCase("public")) {
                throw new IllegalArgumentException(
                        "binding writers cannot be public: every account member would control routing");
            }
            security.addPermissions(AccessRule.newBuilder()
                    .setIdentityType(writer.getIdentityType())
                    .setIdentity(writer.getIdentity())
                    .setAccess(Access.ACCESS_WRITE));
        }
        return security.build();
    }

    /**
     * Specification for creating or rotating an active {@link ExternalSystemBinding}.
     *
     * @param accountId owning repository account ID
     * @param bindingId stable path-safe binding ID
     * @param provider external provider family ID
     * @param serviceProfileId referenced {@code ServiceProfile} name
     * @param serviceEndpointId referenced {@code ServiceEndpoint} name inside the profile
     * @param dataset external dataset/collection/index family
     * @param namespace external workspace/tenant namespace
     * @param contractVersion wire contract version identifier
     * @param retentionPolicy retention behavior upon source purge
     * @param requiredIdentityProof proof every publish receipt through the binding must carry
     * @param labels optional non-secret operator labels
     * @param operationId repository save operation ID
     */
    public record BindingSpec(
            String accountId,
            String bindingId,
            String provider,
            String serviceProfileId,
            String serviceEndpointId,
            String dataset,
            String namespace,
            String contractVersion,
            ExternalRetentionPolicy retentionPolicy,
            ExternalIdentityProof requiredIdentityProof,
            Map<String, String> labels,
            String operationId) {

        /** Validates required binding specification inputs. */
        public BindingSpec {
            Objects.requireNonNull(accountId, "accountId");
            Objects.requireNonNull(bindingId, "bindingId");
            Objects.requireNonNull(provider, "provider");
            Objects.requireNonNull(serviceProfileId, "serviceProfileId");
            Objects.requireNonNull(serviceEndpointId, "serviceEndpointId");
            Objects.requireNonNull(dataset, "dataset");
            Objects.requireNonNull(namespace, "namespace");
            Objects.requireNonNull(contractVersion, "contractVersion");
            Objects.requireNonNull(retentionPolicy, "retentionPolicy");
            Objects.requireNonNull(requiredIdentityProof, "requiredIdentityProof");
            labels = labels == null ? Map.of() : Map.copyOf(labels);
            Objects.requireNonNull(operationId, "operationId");
        }
    }

    /**
     * Creates revision 1 of a binding or rotates an existing {@code ACTIVE} binding to
     * {@code current_revision + 1}, preserving every prior revision immutably in
     * {@code external-bindings:<accountId>} and mirroring the validated envelope to
     * {@link ConfigDocumentStore} when configured.
     *
     * @param spec binding specification
     * @return the persisted {@link ExternalSystemBinding} revision
     */
    public ExternalSystemBinding saveBinding(BindingSpec spec) {
        Objects.requireNonNull(spec, "spec");
        ExternalPublicationCoordinates.requireBindingId(spec.bindingId());
        ExternalPublicationCoordinates.requireNonBlankCoordinate(spec.accountId(), "accountId");
        ExternalPublicationCoordinates.requireNonBlankCoordinate(
                spec.operationId(), "operationId");
        ExternalPublicationCoordinates.requireNonSecretText(spec.dataset(), "dataset", 256);
        ExternalPublicationCoordinates.requireNonSecretText(spec.namespace(), "namespace", 256);
        ExternalPublicationCoordinates.requireNonSecretText(
                spec.contractVersion(), "contractVersion", 128);
        ExternalPublicationCoordinates.validateNonSecretLabels(spec.labels());

        ResolvedEndpoint resolvedEndpoint = validateAndProbeProfile(
                spec.serviceProfileId(), spec.serviceEndpointId());

        NodeAddress address = ExternalPublicationCoordinates.bindingAddress(
                spec.accountId(), spec.bindingId());
        Optional<LoadedBinding> existing = loadOptionalCurrent(address);
        long nextRevision;
        ExpectedRevision expected;
        if (existing.isEmpty()) {
            nextRevision = 1L;
            expected = ExpectedRevision.newBuilder().setNone(true).build();
        } else {
            ExternalSystemBinding current = existing.get().binding();
            if (current.getLifecycle() == ExternalBindingLifecycle.EXTERNAL_BINDING_LIFECYCLE_RETIRED) {
                throw Status.FAILED_PRECONDITION
                        .withDescription(
                                "Retired binding '" + spec.bindingId()
                                        + "' cannot be reactivated; create a new binding_id")
                        .asRuntimeException();
            }
            if (!current.getProvider().equals(spec.provider())) {
                throw Status.FAILED_PRECONDITION
                        .withDescription(
                                "Binding '" + spec.bindingId()
                                        + "' cannot change provider from '" + current.getProvider()
                                        + "' to '" + spec.provider() + "'")
                        .asRuntimeException();
            }
            nextRevision = current.getBindingRevision() + 1L;
            expected = ExpectedRevision.newBuilder()
                    .setRevision(existing.get().documentRevision())
                    .build();
        }

        ExternalEndpointSummary summary = ExternalEndpointSummary.newBuilder()
                .setHost(resolvedEndpoint.endpoint().getHost())
                .setPort(resolvedEndpoint.endpoint().getPort())
                .setTlsEnabled(
                        resolvedEndpoint.endpoint().getTransport() == Transport.TRANSPORT_TLS)
                .build();

        Timestamp createdAt = timestamp(clock.instant());
        ExternalSystemBinding.Builder builder = ExternalSystemBinding.newBuilder()
                .setBindingId(spec.bindingId())
                .setAccountId(spec.accountId())
                .setBindingRevision(nextRevision)
                .setProvider(spec.provider())
                .setServiceProfileId(spec.serviceProfileId())
                .setServiceEndpointId(spec.serviceEndpointId())
                .setDataset(spec.dataset())
                .setNamespace(spec.namespace())
                .setContractVersion(spec.contractVersion())
                .setEndpointSummary(summary)
                .setLifecycle(ExternalBindingLifecycle.EXTERNAL_BINDING_LIFECYCLE_ACTIVE)
                .setRetentionPolicy(spec.retentionPolicy())
                .setRequiredIdentityProof(spec.requiredIdentityProof())
                .putAllLabels(spec.labels())
                .setCreatedAt(createdAt);

        ExternalSystemBinding candidate = builder.build();
        validateBindingContract(candidate);
        configLane.gate(candidate);
        persistBindingDocument(address, expected, candidate, spec.operationId());
        configLane.mirror(candidate);
        return candidate;
    }

    /**
     * Retires an active binding by writing a new immutable revision with
     * {@link ExternalBindingLifecycle#EXTERNAL_BINDING_LIFECYCLE_RETIRED}.
     * Historical binding revisions remain readable by exact {@code binding_revision}.
     *
     * @param accountId owning account identifier
     * @param bindingId binding identifier to retire
     * @param retiredReason non-blank operator reason for retirement
     * @param operationId repository save operation ID
     * @return the newly persisted retired {@link ExternalSystemBinding} revision
     */
    public ExternalSystemBinding retireBinding(
            String accountId, String bindingId, String retiredReason, String operationId) {
        ExternalPublicationCoordinates.requireNonBlankCoordinate(accountId, "accountId");
        ExternalPublicationCoordinates.requireBindingId(bindingId);
        ExternalPublicationCoordinates.requireNonBlankCoordinate(retiredReason, "retiredReason");
        ExternalPublicationCoordinates.requireNonSecretText(retiredReason, "retiredReason", 512);
        ExternalPublicationCoordinates.requireNonBlankCoordinate(operationId, "operationId");

        NodeAddress address = ExternalPublicationCoordinates.bindingAddress(accountId, bindingId);
        LoadedBinding existing = loadOptionalCurrent(address)
                .orElseThrow(() -> Status.NOT_FOUND
                        .withDescription("External binding not found: " + bindingId)
                        .asRuntimeException());
        ExternalSystemBinding current = existing.binding();
        if (current.getLifecycle() == ExternalBindingLifecycle.EXTERNAL_BINDING_LIFECYCLE_RETIRED) {
            return current;
        }

        Timestamp now = timestamp(clock.instant());
        ExternalSystemBinding retired = current.toBuilder()
                .setBindingRevision(current.getBindingRevision() + 1L)
                .setLifecycle(ExternalBindingLifecycle.EXTERNAL_BINDING_LIFECYCLE_RETIRED)
                .setRetiredReason(retiredReason)
                .setRetiredAt(now)
                .setCreatedAt(now)
                .build();

        validateBindingContract(retired);
        configLane.gate(retired);
        ExpectedRevision expected = ExpectedRevision.newBuilder()
                .setRevision(existing.documentRevision())
                .build();
        persistBindingDocument(address, expected, retired, operationId);
        configLane.mirror(retired);
        return retired;
    }

    /**
     * Loads the latest revision of {@code bindingId} in {@code accountId}, enforcing
     * caller repository authorization on {@code external-bindings:<accountId>}.
     *
     * @param accountId owning account ID
     * @param bindingId binding ID
     * @return the current {@link ExternalSystemBinding}, or empty if absent
     */
    public Optional<ExternalSystemBinding> getCurrentBinding(String accountId, String bindingId) {
        NodeAddress address = ExternalPublicationCoordinates.bindingAddress(accountId, bindingId);
        return loadOptionalCurrent(address).map(LoadedBinding::binding);
    }

    /**
     * Loads an exact immutable historical {@code bindingRevision} for {@code bindingId}.
     *
     * @param accountId owning account ID
     * @param bindingId binding ID
     * @param bindingRevision 1-based binding revision number
     * @return the historical {@link ExternalSystemBinding}, or empty if absent
     */
    public Optional<ExternalSystemBinding> getBindingRevision(
            String accountId, String bindingId, long bindingRevision) {
        if (bindingRevision <= 0L) {
            throw new IllegalArgumentException("bindingRevision must be > 0");
        }
        NodeAddress address = ExternalPublicationCoordinates.bindingAddress(accountId, bindingId);
        try {
            GetDocumentResponse response = documents.getDocument(
                    GetDocumentRequest.newBuilder()
                            .setAddress(address)
                            .setRevision(RevisionSelector.newBuilder()
                                    .setRevision(bindingRevision))
                            .setSelection(CORE_SELECTION)
                            .build());
            ExternalSystemBinding unpacked = unpackBinding(response.getDocument());
            if (unpacked.getBindingRevision() != bindingRevision) {
                throw Status.DATA_LOSS
                        .withDescription("Stored binding revision number mismatch")
                        .asRuntimeException();
            }
            return Optional.of(unpacked);
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
                return Optional.empty();
            }
            throw e;
        }
    }

    /**
     * Lists the current revisions of all external-system bindings in {@code accountId}.
     *
     * @param accountId owning account ID
     * @return list of current {@link ExternalSystemBinding} definitions
     */
    public List<ExternalSystemBinding> listBindings(String accountId) {
        ExternalPublicationCoordinates.requireNonBlankCoordinate(accountId, "accountId");
        String graphId = ExternalPublicationCoordinates.BINDINGS_GRAPH_PREFIX + accountId;
        List<ExternalSystemBinding> result = new ArrayList<>();
        String pageToken = "";
        do {
            ListDocumentsResponse page = documents.listDocuments(
                    ListDocumentsRequest.newBuilder()
                            .setAccountId(accountId)
                            .setGraphId(graphId)
                            .setGraphAddressId(
                                    ExternalPublicationCoordinates.BINDINGS_GRAPH_ADDRESS_ID)
                            .setPageSize(100)
                            .setPageToken(pageToken)
                            .build());
            for (DocumentSummary summary : page.getDocumentsList()) {
                loadOptionalCurrent(summary.getAddress())
                        .map(LoadedBinding::binding)
                        .ifPresent(result::add);
            }
            pageToken = page.getNextPageToken();
        } while (!pageToken.isEmpty());
        return List.copyOf(result);
    }

    /**
     * Resolves live outbound profile, endpoint, and ephemeral credentials for {@code binding}
     * without persisting any secret material.
     *
     * @param binding binding configuration revision
     * @return live {@link ExternalProviderGateway.OutboundAuthContext}
     */
    public ExternalProviderGateway.OutboundAuthContext resolveOutboundAuth(
            ExternalSystemBinding binding) {
        Objects.requireNonNull(binding, "binding");
        ResolvedEndpoint resolved = validateAndProbeProfile(
                binding.getServiceProfileId(), binding.getServiceEndpointId());
        return new ExternalProviderGateway.OutboundAuthContext(
                binding,
                resolved.profile(),
                resolved.endpoint(),
                resolved.headers(),
                resolved.credentialBindingSha256());
    }

    private record LoadedBinding(ExternalSystemBinding binding, long documentRevision) {
    }

    private record ResolvedEndpoint(
            ServiceProfile profile,
            ServiceEndpoint endpoint,
            Metadata headers,
            String credentialBindingSha256) {
    }

    private ResolvedEndpoint validateAndProbeProfile(
            String serviceProfileId, String serviceEndpointId) {
        ServiceProfile profile;
        try {
            profile = profiles.find(serviceProfileId)
                    .orElseThrow(() -> Status.FAILED_PRECONDITION
                            .withDescription(
                                    "Referenced ServiceProfile not found: " + serviceProfileId)
                            .asRuntimeException());
        } catch (IOException ioError) {
            throw Status.UNAVAILABLE
                    .withDescription("ServiceProfileRepository unavailable")
                    .withCause(ioError)
                    .asRuntimeException();
        }
        try {
            ServiceProfileValidation.validateConnectionProfile(profile);
        } catch (IllegalArgumentException invalid) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("Invalid ServiceProfile '" + serviceProfileId
                            + "': " + invalid.getMessage())
                    .withCause(invalid)
                    .asRuntimeException();
        }

        ServiceEndpoint selected = null;
        for (ServiceEndpoint candidate : profile.getEndpointsList()) {
            if (candidate.getName().equals(serviceEndpointId)) {
                selected = candidate;
                break;
            }
        }
        if (selected == null) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("ServiceEndpoint '" + serviceEndpointId
                            + "' not found in profile '" + serviceProfileId + "'")
                    .asRuntimeException();
        }

        Metadata resolvedHeaders = new Metadata();
        try {
            if (!selected.getCredentialRef().isEmpty()) {
                Metadata headers = credentials.headers(profile, selected);
                if (headers == null) {
                    throw Status.FAILED_PRECONDITION
                            .withDescription("ProfileCredentialResolver returned null headers")
                            .asRuntimeException();
                }
                resolvedHeaders = headers;
            }
            if (!selected.getTrustRef().isEmpty()
                    || !selected.getClientCertificateRef().isEmpty()) {
                credentials.channelCredentials(profile, selected);
            }
        } catch (ActionException credFailure) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("Failed to resolve credentials for profile '"
                            + serviceProfileId + "': " + credFailure.getMessage())
                    .withCause(credFailure)
                    .asRuntimeException();
        }

        String nonSecretRefMaterial = "v1\0"
                + profile.getName() + "\0"
                + selected.getName() + "\0"
                + selected.getCredentialRef() + "\0"
                + selected.getTrustRef() + "\0"
                + selected.getClientCertificateRef();
        String credentialBindingSha256 = ExternalOperationKeys.sha256Hex(
                nonSecretRefMaterial.getBytes(StandardCharsets.UTF_8));
        return new ResolvedEndpoint(profile, selected, resolvedHeaders, credentialBindingSha256);
    }

    /**
     * Writes the config-lane mirror of an already committed binding revision, read back from
     * the canonical {@code DocumentService} revision. Repeating it writes the same document.
     *
     * @param accountId owning account ID
     * @param bindingId binding ID
     * @param bindingRevision committed binding revision to mirror
     * @return the mirrored canonical revision
     * @throws StatusRuntimeException {@code NOT_FOUND} when the revision does not exist,
     *         {@code FAILED_PRECONDITION} when no config lane is configured
     */
    public ExternalSystemBinding mirrorRevision(
            String accountId, String bindingId, long bindingRevision) {
        if (!configLane.configured()) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("No config-lane store is configured")
                    .asRuntimeException();
        }
        ExternalSystemBinding committed = getBindingRevision(accountId, bindingId, bindingRevision)
                .orElseThrow(() -> Status.NOT_FOUND
                        .withDescription("ExternalSystemBinding '" + bindingId + "' revision "
                                + bindingRevision + " not found")
                        .asRuntimeException());
        configLane.mirror(committed);
        return committed;
    }

    private Optional<LoadedBinding> loadOptionalCurrent(NodeAddress address) {
        try {
            GetDocumentResponse response = documents.getDocument(
                    GetDocumentRequest.newBuilder()
                            .setAddress(address)
                            .setSelection(CORE_SELECTION)
                            .build());
            return Optional.of(new LoadedBinding(
                    unpackBinding(response.getDocument()),
                    response.getManifest().getRevision()));
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
                return Optional.empty();
            }
            throw e;
        }
    }

    private void persistBindingDocument(
            NodeAddress address,
            ExpectedRevision expected,
            ExternalSystemBinding binding,
            String operationId) {
        String datasourceId = saveOwner.hasIntake()
                ? saveOwner.getIntake().getDatasourceId()
                : ExternalPublicationCoordinates.DEFAULT_BINDINGS_DATASOURCE_ID;
        Document doc = Document.newBuilder()
                .setDocId(address.getDocId())
                .setOwnership(OwnershipContext.newBuilder()
                        .setAccountId(binding.getAccountId())
                        .setDatasourceId(datasourceId)
                        .setSecurity(bindingSecurity))
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setTitle(binding.getProvider() + ":" + binding.getBindingId()
                                + "@v" + binding.getBindingRevision())
                        .setDocumentType("external-system-binding")
                        .setCreationDate(binding.getCreatedAt()))
                .setStructuredData(Any.pack(binding))
                .build();

        SaveDocumentRequest request = SaveDocumentRequest.newBuilder()
                .setOperationId(operationId)
                .setTarget(DocumentTarget.newBuilder().setAddress(address))
                .setExpected(expected)
                .setDocument(doc)
                .addWrites(CORE_PUT)
                .setValidationMode(ValidationMode.VALIDATION_MODE_TYPED)
                .setDrive(drive)
                .setOwner(saveOwner)
                .build();
        documents.saveDocument(request);
    }

    private static ExternalSystemBinding unpackBinding(Document document) {
        if (!document.hasStructuredData()) {
            throw Status.DATA_LOSS
                    .withDescription("Stored binding document is missing structured_data")
                    .asRuntimeException();
        }
        try {
            ExternalSystemBinding unpacked = document.getStructuredData()
                    .unpack(ExternalSystemBinding.class);
            validateBindingContract(unpacked);
            return unpacked;
        } catch (InvalidProtocolBufferException corrupt) {
            throw Status.DATA_LOSS
                    .withDescription("Stored binding payload failed to unpack")
                    .withCause(corrupt)
                    .asRuntimeException();
        }
    }

    private static void validateBindingContract(ExternalSystemBinding binding) {
        ValidationResult result = VALIDATOR.validate(binding);
        if (!result.valid()) {
            throw Status.INVALID_ARGUMENT
                    .withDescription("Invalid ExternalSystemBinding: " + result.violations())
                    .asRuntimeException();
        }
        ExternalPublicationCoordinates.validateNonSecretLabels(binding.getLabelsMap());
        ExternalPublicationCoordinates.requireNonSecretText(binding.getDataset(), "dataset", 256);
        ExternalPublicationCoordinates.requireNonSecretText(
                binding.getNamespace(), "namespace", 256);
    }

    private static Timestamp timestamp(Instant instant) {
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }
}
