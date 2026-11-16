package ai.protomolt.proto.serve;

import ai.protomolt.proto.authz.jdbc.CallerStoreConfig;
import ai.protomolt.proto.grpc.policy.OutboundChannelPolicy;

import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Package-private option builders and primitive parsers for {@link ProtoMoltServeArgs}.
 */
final class ProtoMoltServeOptionBuilders {

    private ProtoMoltServeOptionBuilders() {
    }

    static ProtoMoltServe.Options buildOptions(ProtoMoltServeArgs.ParsedState s) {
        ProtoMoltServe.JobsOptions jobs = buildJobs(s);
        ProtoMoltServe.DelegationOptions delegation = buildDelegation(s);
        ProtoMoltServe.TaskConsoleOptions taskConsole =
                s.taskConsoleToken != null && !s.taskConsoleToken.isBlank()
                        ? new ProtoMoltServe.TaskConsoleOptions(
                                s.taskConsoleToken, Duration.ofSeconds(s.taskConsoleSessionSeconds))
                        : null;
        ProtoMoltServe.ConfigLaneOptions configLane = buildConfigLane(s);
        ProtoMoltServe.IdentityStoreOptions identityStores = buildIdentityStores(s);
        ProtoMoltServe.MeshClusterOptions meshCluster = buildMeshCluster(s);
        OutboundChannelPolicy outboundPolicy = buildOutboundPolicy(s);
        ProtoMoltServe.WorkflowAuthoringOptions workflowAuthoring =
                (s.authoringPolicySha256 != null && !s.authoringPolicySha256.isBlank())
                        || s.authoringAuthorizationDir != null
                        ? new ProtoMoltServe.WorkflowAuthoringOptions(
                                s.authoringPolicySha256, s.authoringAuthorizationDir)
                        : null;
        ProtoMoltServe.WorkflowPreparationOptions workflowPreparation =
                s.preparationIntentDir != null || s.preparationTemplateProvider != null
                        ? new ProtoMoltServe.WorkflowPreparationOptions(
                                s.preparationIntentDir, s.preparationTemplateProvider)
                        : null;
        return new ProtoMoltServe.Options(s.host, s.grpcPort, s.httpPort, s.registryGit,
                s.registryPort, s.apiToken, s.demo, s.gatherCache, jobs,
                List.copyOf(s.inferenceModels), s.serviceWorkspace, outboundPolicy,
                s.workflowWorkspace, delegation, taskConsole, meshCluster, s.accessPolicy,
                identityStores, configLane, workflowAuthoring, workflowPreparation,
                s.profileTlsConfig);
    }

    static boolean isLowercaseSha256Hex(String value) {
        if (value.length() != 64) {
            return false;
        }
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (!(c >= '0' && c <= '9' || c >= 'a' && c <= 'f')) {
                return false;
            }
        }
        return true;
    }

    static boolean isPathSafeName(String value) {
        int length = value.length();
        if (length < 1 || length > 128 || !isAsciiAlphanumeric(value.charAt(0))) {
            return false;
        }
        for (int i = 1; i < length; i++) {
            char c = value.charAt(i);
            if (!isAsciiAlphanumeric(c) && c != '.' && c != '_' && c != '-') {
                return false;
            }
        }
        return true;
    }

    static OutboundChannelPolicy validateOptions(
            String apiToken, Path registryGit, OutboundChannelPolicy outboundPolicy,
            Path workflowWorkspace, ProtoMoltServe.JobsOptions jobs,
            ProtoMoltServe.DelegationOptions delegation, Path accessPolicy,
            ProtoMoltServe.IdentityStoreOptions identityStores,
            ProtoMoltServe.WorkflowAuthoringOptions workflowAuthoring,
            ProtoMoltServe.WorkflowPreparationOptions workflowPreparation) {
        if (apiToken != null && apiToken.isBlank()) {
            throw new IllegalArgumentException(
                    "API token must be nonblank when supplied; use null for an open node");
        }
        if (accessPolicy != null && apiToken == null) {
            throw new IllegalArgumentException("an access policy requires the operator "
                    + "api token; set --api-token alongside --access-policy");
        }
        if (identityStores != null && apiToken == null) {
            throw new IllegalArgumentException("external caller stores require the "
                    + "operator api token; set --api-token (or PROTOMOLT_API_TOKEN) alongside them");
        }
        if (workflowAuthoring != null && (apiToken == null || apiToken.isBlank()
                || registryGit == null || workflowWorkspace == null
                || delegation == null || jobs == null)) {
            throw new IllegalArgumentException("workflow authoring requires an API token, "
                    + "explicit persistent registry and workflow workspaces, durable "
                    + "delegation repository, and jobs database");
        }
        if (workflowPreparation != null && (workflowAuthoring == null || delegation == null
                || delegation.repoEndpoint() == null || delegation.repoEndpoint().isBlank())) {
            throw new IllegalArgumentException("workflow preparation requires workflow authoring "
                    + "and a durable delegation repository");
        }
        return outboundPolicy == null ? OutboundChannelPolicy.defaults() : outboundPolicy;
    }

    static void validateDelegationOptions(String repoEndpoint, String drive, String objectKey,
                                          String keyReference) {
        if (repoEndpoint == null || repoEndpoint.isBlank()) {
            throw new IllegalArgumentException("the delegation repo endpoint must not be blank");
        }
        if (drive == null || drive.isBlank()) {
            throw new IllegalArgumentException("the delegation repo drive must not be blank");
        }
        if (objectKey == null || objectKey.isBlank()) {
            throw new IllegalArgumentException(
                    "the delegation transcript object key must not be blank");
        }
        if (keyReference == null || keyReference.isBlank()) {
            throw new IllegalArgumentException(
                    "the delegation state key reference must not be blank");
        }
    }

    static void validateIdentityStores(URI oidcIntrospection, String oidcClientId,
                                       String oidcClientSecret, CallerStoreConfig callerStore) {
        if (oidcIntrospection == null && callerStore == null) {
            throw new IllegalArgumentException(
                    "identity stores require an OIDC endpoint or a JDBC caller store");
        }
        if (oidcIntrospection != null && (oidcClientId == null || oidcClientId.isBlank()
                || oidcClientSecret == null || oidcClientSecret.isBlank())) {
            throw new IllegalArgumentException("the OIDC introspection endpoint "
                    + "requires PROTOMOLT_AUTHZ_OIDC_CLIENT_ID and "
                    + "PROTOMOLT_AUTHZ_OIDC_CLIENT_SECRET");
        }
    }

    static void validateMeshCluster(String clusterId, String displayName, String trustDomain,
                                    Instant createdAt) {
        if (clusterId == null || !isPathSafeName(clusterId)) {
            throw new IllegalArgumentException("mesh cluster id must be path-safe");
        }
        if (displayName == null || displayName.length() > 256) {
            throw new IllegalArgumentException("mesh cluster name is invalid");
        }
        if (trustDomain == null || !(trustDomain.isEmpty() || isPathSafeName(trustDomain))) {
            throw new IllegalArgumentException("mesh trust domain is invalid");
        }
        java.util.Objects.requireNonNull(createdAt, "createdAt");
    }

    static Path envPath(String name, boolean trim) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            return null;
        }
        return Path.of(trim ? value.trim() : value);
    }

    static int envInt(String name, int fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : parseInt(value, name);
    }

    static long envLong(String name, long fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : parseLong(value, name);
    }

    static boolean envBoolean(String name, boolean fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : parseBoolean(value, name);
    }

    static int parseInt(String value, String name) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(name + " must be an integer: " + value, e);
        }
    }

    static long parseLong(String value, String name) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(name + " must be an integer: " + value, e);
        }
    }

    static boolean parseBoolean(String value, String name) {
        if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
            return Boolean.parseBoolean(value);
        }
        throw new IllegalArgumentException(name + " must be true or false: " + value);
    }

    private static boolean isAsciiAlphanumeric(char c) {
        return c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9';
    }

    private static ProtoMoltServe.JobsOptions buildJobs(ProtoMoltServeArgs.ParsedState s) {
        if (s.jobsJdbc != null && !s.jobsJdbc.isBlank()) {
            if (s.jobsUser == null || s.jobsUser.isBlank()) {
                System.err.println("--jobs-jdbc requires --jobs-user (or PROTOMOLT_JOBS_USER)");
                System.exit(2);
            }
            return new ProtoMoltServe.JobsOptions(s.jobsJdbc, s.jobsUser, s.jobsPassword,
                    s.jobsKafka, s.jobsRequestTopic, s.jobsWorkers, s.jobsTargetConcurrency);
        }
        if ((s.jobsKafka != null && !s.jobsKafka.isBlank())
                || (s.jobsRequestTopic != null && !s.jobsRequestTopic.isBlank())) {
            System.err.println("--jobs-kafka/--jobs-request-topic require --jobs-jdbc: "
                    + "the job row is the truth; the broker is propagation");
            System.exit(2);
        }
        return null;
    }

    private static ProtoMoltServe.DelegationOptions buildDelegation(
            ProtoMoltServeArgs.ParsedState s) {
        if (s.delegationRepoEndpoint != null && !s.delegationRepoEndpoint.isBlank()) {
            return new ProtoMoltServe.DelegationOptions(s.delegationRepoEndpoint,
                    s.delegationRepoTls,
                    nonBlankOr(s.delegationRepoDrive, ProtoMoltServe.DelegationOptions.DEFAULT_DRIVE),
                    nonBlankOr(s.delegationTranscriptObject,
                            ProtoMoltServe.DelegationOptions.DEFAULT_OBJECT_KEY),
                    nonBlankOr(s.delegationStateKeyRef,
                            ProtoMoltServe.DelegationOptions.DEFAULT_KEY_REFERENCE));
        }
        if (s.delegationRepoTlsSet
                || (s.delegationRepoDrive != null && !s.delegationRepoDrive.isBlank())
                || (s.delegationTranscriptObject != null && !s.delegationTranscriptObject.isBlank())
                || (s.delegationStateKeyRef != null && !s.delegationStateKeyRef.isBlank())) {
            System.err.println("--delegation-repo-drive/--delegation-transcript-object/"
                    + "--delegation-state-key-ref require --delegation-repo-endpoint: "
                    + "without a repository service the transcript stays in memory");
            System.exit(2);
        }
        return null;
    }

    private static ProtoMoltServe.ConfigLaneOptions buildConfigLane(
            ProtoMoltServeArgs.ParsedState s) {
        if (s.configUrl != null && !s.configUrl.isBlank()) {
            return new ProtoMoltServe.ConfigLaneOptions(s.configUrl.trim(), s.configRefreshSeconds);
        }
        if (s.configRefreshSeconds > 0) {
            throw new IllegalArgumentException(
                    "PROTOMOLT_CONFIG_REFRESH_SECONDS needs PROTOMOLT_CONFIG_URL: a"
                            + " refresh interval with nowhere to pull from reads nothing");
        }
        return null;
    }

    private static ProtoMoltServe.IdentityStoreOptions buildIdentityStores(
            ProtoMoltServeArgs.ParsedState s) {
        boolean oidcConfigured = s.oidcIntrospection != null && !s.oidcIntrospection.isBlank();
        boolean authzKeysConfigured = s.authzKeysJdbc != null && !s.authzKeysJdbc.isBlank();
        if (!oidcConfigured && !authzKeysConfigured) {
            return null;
        }
        return new ProtoMoltServe.IdentityStoreOptions(
                oidcConfigured ? URI.create(s.oidcIntrospection.trim()) : null,
                s.oidcClientId, s.oidcClientSecret,
                authzKeysConfigured ? CallerStoreConfig.fromEnvironmentMap(System.getenv()) : null);
    }

    private static ProtoMoltServe.MeshClusterOptions buildMeshCluster(
            ProtoMoltServeArgs.ParsedState s) {
        if (s.meshClusterId != null && !s.meshClusterId.isBlank()) {
            if (s.meshCreatedAt == null || s.meshCreatedAt.isBlank()) {
                throw new IllegalArgumentException(
                        "--mesh-cluster-id requires --mesh-created-at so the durable "
                                + "cluster fingerprint stays stable across restarts");
            }
            try {
                return new ProtoMoltServe.MeshClusterOptions(s.meshClusterId,
                        nonBlankOr(s.meshClusterName, s.meshClusterId),
                        s.meshTrustDomain == null ? "" : s.meshTrustDomain,
                        Instant.parse(s.meshCreatedAt));
            } catch (DateTimeParseException e) {
                throw new IllegalArgumentException("mesh created-at must be an ISO-8601 instant", e);
            }
        }
        if ((s.meshClusterName != null && !s.meshClusterName.isBlank())
                || (s.meshTrustDomain != null && !s.meshTrustDomain.isBlank())
                || (s.meshCreatedAt != null && !s.meshCreatedAt.isBlank())) {
            throw new IllegalArgumentException(
                    "mesh cluster name, trust domain, and created-at require a mesh cluster id");
        }
        return null;
    }

    private static OutboundChannelPolicy buildOutboundPolicy(ProtoMoltServeArgs.ParsedState s) {
        if (s.maxDeadlineMs <= 0) {
            throw new IllegalArgumentException("grpc max deadline must be positive");
        }
        OutboundChannelPolicy.Builder builder = OutboundChannelPolicy.builder()
                .allowPlaintext(s.allowPlaintext)
                .allowTls(s.allowTls)
                .maxDeadline(Duration.ofMillis(s.maxDeadlineMs))
                .maxActiveChannels(s.maxActiveChannels)
                .maxInboundMessageBytes(s.maxInboundMessageBytes)
                .maxOutboundMessageBytes(s.maxOutboundMessageBytes);
        if (s.allowedSchemes != null) {
            builder.allowedSchemes(parseCsv(s.allowedSchemes, "grpc allowed schemes"));
        }
        if (s.allowedHosts != null) {
            builder.allowedHosts(parseCsv(s.allowedHosts, "grpc allowed hosts"));
        }
        if (s.allowedPorts != null) {
            builder.allowedPorts(parsePorts(s.allowedPorts));
        }
        return builder.build();
    }

    private static String nonBlankOr(String value, String fallback) {
        return value == null || value.isBlank() ? fallback : value;
    }

    private static Set<String> parseCsv(String value, String name) {
        Set<String> values = new LinkedHashSet<>();
        if (value != null && !value.isBlank()) {
            for (String item : commaSeparated(value)) {
                if (item.isBlank()) {
                    throw new IllegalArgumentException(name + " contains an empty value");
                }
                values.add(item.trim());
            }
        }
        return Set.copyOf(values);
    }

    private static Set<Integer> parsePorts(String value) {
        Set<Integer> ports = new LinkedHashSet<>();
        for (String item : commaSeparated(value)) {
            if (item.isBlank()) {
                throw new IllegalArgumentException("grpc allowed ports contains an empty value");
            }
            ports.add(parseInt(item.trim(), "grpc allowed port"));
        }
        return Set.copyOf(ports);
    }

    private static List<String> commaSeparated(String value) {
        List<String> items = new ArrayList<>();
        int start = 0;
        while (true) {
            int comma = value.indexOf(',', start);
            items.add(comma < 0 ? value.substring(start) : value.substring(start, comma));
            if (comma < 0) {
                return items;
            }
            start = comma + 1;
        }
    }
}
