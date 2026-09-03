package ai.protomolt.proto.metric.service;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.authz.AccessPolicy;
import ai.protomolt.proto.authz.MetricAccess;
import ai.protomolt.proto.authz.MetricMemberDeny;
import ai.protomolt.proto.authz.MetricRowFilter;
import ai.protomolt.proto.authz.Principal;
import ai.protomolt.proto.metric.DescribeMappingResponse;
import ai.protomolt.proto.metric.MappingMember;
import ai.protomolt.proto.metric.MemberRef;
import ai.protomolt.proto.metric.MetricFilter;
import ai.protomolt.proto.metric.QueryMetricsRequest;
import ai.protomolt.proto.metric.spi.MetricMapping;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * The compile-time rewrite behind {@code Principal.metric_access}: what a restricted
 * principal may see and which rows its queries reduce over, applied before the query
 * compiles. Denied members disappear from a description and refuse a query by name;
 * row filters are ANDed into the request as ordinary equality filters, so the executor
 * runs the narrowed reduction and the response's evidence says so. The operator and any
 * principal without rules pass untouched, and a policy swapped on the config lane
 * re-scopes the rewrite on its next read.
 *
 * <p>Fail-closed edges: a restricted principal is refused subjects this mount does not
 * serve statically (rollup tables reached through the subject resolver carry no
 * mapping-level rules to enforce) and refused the rollup rebuild verb (a rebuilt rollup
 * is an unrestricted reduction the principal could then read around its row filters).
 */
final class MetricAccessRewrites {

    private final Supplier<AccessPolicy> policy;
    private volatile AccessPolicy indexed;
    private volatile Map<String, MetricAccess> byPrincipal = Map.of();

    MetricAccessRewrites(Supplier<AccessPolicy> policy) {
        this.policy = policy;
    }

    /** The caller's metric access, or null when nothing rewrites. */
    MetricAccess accessFor(Caller caller) {
        if (policy == null || caller.unrestricted()) {
            return null;
        }
        AccessPolicy current = policy.get();
        if (current == null) {
            return null;
        }
        if (current != indexed) {
            Map<String, MetricAccess> fresh = new HashMap<>();
            for (Principal principal : current.getPrincipalsList()) {
                if (principal.hasMetricAccess()) {
                    fresh.put(principal.getName(), principal.getMetricAccess());
                }
            }
            byPrincipal = Map.copyOf(fresh);
            indexed = current;
        }
        return byPrincipal.get(caller.name());
    }

    /** A description with the caller's denied members removed. */
    static DescribeMappingResponse filter(DescribeMappingResponse response,
                                          MetricAccess access, String subject) {
        Set<String> denied = denied(access, subject);
        if (denied.isEmpty()) {
            return response;
        }
        DescribeMappingResponse.Builder kept = response.toBuilder().clearMembers();
        for (MappingMember member : response.getMembersList()) {
            if (!denied.contains(member.getName())) {
                kept.addMembers(member);
            }
        }
        return kept.build();
    }

    /**
     * The request with the caller's row filters ANDed in, refusing a denied member by
     * name first. The injected member must exist on the mapping: a policy filtering a
     * member the mapping does not declare is the operator's error, said loudly rather
     * than blamed on the caller.
     */
    static QueryMetricsRequest rewrite(QueryMetricsRequest request, MetricAccess access,
                                       Caller caller, MetricMapping mapping) {
        String subject = request.getMappingSubject();
        Set<String> denied = denied(access, subject);
        List<String> requested = new ArrayList<>(request.getMeasuresList());
        for (MemberRef dimension : request.getDimensionsList()) {
            requested.add(dimension.getName());
        }
        for (MetricFilter filter : request.getFiltersList()) {
            requested.add(filter.getMember());
        }
        for (String name : requested) {
            if (denied.contains(name)) {
                throw new StatusRuntimeException(Status.PERMISSION_DENIED.withDescription(
                        "caller '" + caller.name() + "' may not query member '" + name
                                + "' on '" + subject + "'"));
            }
        }
        QueryMetricsRequest.Builder narrowed = request.toBuilder();
        boolean injected = false;
        for (MetricRowFilter filter : access.getRowFiltersList()) {
            if (!subject.equals(filter.getMappingSubject())) {
                continue;
            }
            if (mapping.member(filter.getMember()).isEmpty()) {
                throw new StatusRuntimeException(Status.FAILED_PRECONDITION.withDescription(
                        "the access policy filters member '" + filter.getMember()
                                + "' for caller '" + caller.name() + "', which mapping '"
                                + subject + "' does not declare"));
            }
            narrowed.addFilters(MetricFilter.newBuilder()
                    .setMember(filter.getMember())
                    .addAllEquals(filter.getEqualsList()));
            injected = true;
        }
        return injected ? narrowed.build() : request;
    }

    private static Set<String> denied(MetricAccess access, String subject) {
        Set<String> denied = new LinkedHashSet<>();
        for (MetricMemberDeny deny : access.getDenyList()) {
            if (subject.equals(deny.getMappingSubject())) {
                denied.addAll(deny.getMembersList());
            }
        }
        return denied;
    }
}
