package ai.protomolt.proto.pipeline;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.grpc.workflow.v1.TypedEdge;
import ai.protomolt.proto.repo.spi.CallerIdentityExpansion;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v1.DocumentSecurity;
import ai.protomolt.proto.repo.v1.IdentityLink;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static ai.protomolt.proto.pipeline.AclMappingTestSupport.allows;
import static ai.protomolt.proto.pipeline.AclMappingTestSupport.caller;
import static ai.protomolt.proto.pipeline.AclMappingTestSupport.field;
import static ai.protomolt.proto.pipeline.AclMappingTestSupport.loadEdge;
import static ai.protomolt.proto.pipeline.AclMappingTestSupport.loadRegistry;
import static ai.protomolt.proto.pipeline.AclMappingTestSupport.mapIdentityLinks;
import static ai.protomolt.proto.pipeline.AclMappingTestSupport.mapSecurity;
import static ai.protomolt.proto.pipeline.AclMappingTestSupport.parseText;
import static ai.protomolt.proto.pipeline.AclMappingTestSupport.principal;
import static ai.protomolt.proto.pipeline.AclMappingTestSupport.readDelimited;
import static ai.protomolt.proto.pipeline.AclMappingTestSupport.writeAndVerifySearchFixture;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Recorded live-corpus verification for SharePoint role assignments, site collection admins,
 * Entra group deltas, Microsoft 365 group owners, and search-term projection.
 */
class SharePointRecordedSecurityTest {

    private static DescriptorRegistry registry;
    private static TypedEdge edge;
    private static TypedEdge links;
    private static TypedEdge entraLinks;
    private static TypedEdge groupLinks;
    private static TypedEdge adminLinks;
    private static TypedEdge ownerLinks;
    private static Descriptor entity;
    private static Descriptor siteAcl;
    private static Descriptor siteRoleAssignment;
    private static Descriptor graphUser;

    @BeforeAll
    static void load() throws Exception {
        registry = loadRegistry("acl/microsoft-v1.binpb");
        edge = loadEdge("acl/sharepoint-document-security.textproto");
        links = loadEdge("acl/sharepoint-identity-links.textproto");
        entraLinks = loadEdge("acl/entra-identity-links.textproto");
        groupLinks = loadEdge("acl/entra-group-links.textproto");
        adminLinks = loadEdge("acl/sharepoint-site-admin-links.textproto");
        ownerLinks = loadEdge("acl/entra-group-owner-links.textproto");
        entity = registry.findDescriptorByFullName("ai.pipestream.microsoft.v1.MicrosoftEntity");
        siteAcl = registry.findDescriptorByFullName("ai.pipestream.microsoft.v1.SiteAcl");
        siteRoleAssignment = registry.findDescriptorByFullName("ai.pipestream.microsoft.v1.SiteRoleAssignment");
        graphUser = registry.findDescriptorByFullName("ai.pipestream.microsoft.v1.GraphUser");
    }

    @Test
    void theRealEntraUserReadsThroughItsRealEntraGroups() throws Exception {
        Descriptor groupChange = registry.findDescriptorByFullName("ai.pipestream.microsoft.v1.GroupChange");
        List<DynamicMessage> changes = readDelimited(groupChange, "acl/entra-group-changes.binpb");
        List<IdentityLink> all = new ArrayList<>(entraLinksOfRecordedUsers());
        List<String> withMembers = new ArrayList<>();
        List<String> empty = new ArrayList<>();
        for (DynamicMessage change : changes) {
            List<IdentityLink> mapped = mapIdentityLinks(registry, groupLinks, Map.of("change", change), "entra-groups");
            all.addAll(mapped);
            String id = field((Message) field(change, "group"), "id").toString();
            (mapped.isEmpty() ? empty : withMembers).add(id);
        }
        assertThat(withMembers).isNotEmpty();
        assertThat(empty).as("a recorded group without members").isNotEmpty();

        RepositoryCaller user = new CallerIdentityExpansion(all).expand(
                caller("user-principal-name:" + entraUpns().getFirst()));
        assertThat(user.identities()).containsAll(withMembers.stream().map(id -> principal("entra-group:" + id))
                .toList());
        DynamicMessage siteAclRecord = recordedSiteAcl();
        for (String id : withMembers) {
            assertThat(allows(user, map(grant(siteAclRecord, id, "")))).as("granted to group %s", id).isTrue();
            assertThat(allows(user, map(grant(siteAclRecord, id, "_o"))))
                    .as("granted to the owners of group %s", id).isFalse();
        }
        assertThat(allows(user, map(grant(siteAclRecord, empty.getFirst(), ""))))
                .as("granted to a group the user is not in").isFalse();
    }

    @Test
    void theRecordedSiteAdminReadsEveryItemThroughTheAdminLinkAlone() throws Exception {
        Recorded recorded = recorded();
        List<IdentityLink> admin = recorded.links().stream()
                .filter(l -> l.getTo().getIdentityType().equals("sharepoint-site-admin")).toList();
        assertThat(admin).singleElement().satisfies(l ->
                assertThat(l.getFrom().getIdentity()).isEqualTo(entraUpns().getFirst()));
        RepositoryCaller caller = new CallerIdentityExpansion(admin).expand(
                caller("user-principal-name:" + entraUpns().getFirst()));
        for (var item : recorded.items().entrySet()) {
            assertThat(allows(caller, item.getValue())).as("the site admin on %s", item.getKey()).isTrue();
        }
    }

    @Test
    void theRealGroupOwnerReadsThroughTheOwnersClaim() throws Exception {
        Descriptor graphGroup = registry.findDescriptorByFullName("ai.pipestream.microsoft.v1.GraphGroup");
        Descriptor member = registry.findDescriptorByFullName("ai.pipestream.microsoft.v1.DirectoryMember");
        List<IdentityLink> all = new ArrayList<>(entraLinksOfRecordedUsers());
        String groupId;
        try (InputStream in = SharePointRecordedSecurityTest.class.getClassLoader()
                .getResourceAsStream("acl/entra-group-owners.binpb")) {
            DynamicMessage.Builder group = DynamicMessage.newBuilder(graphGroup);
            assertThat(group.mergeDelimitedFrom(in)).isTrue();
            DynamicMessage built = group.build();
            groupId = field(built, "id").toString();
            DynamicMessage.Builder owner = DynamicMessage.newBuilder(member);
            while (owner.mergeDelimitedFrom(in)) {
                Map<String, DynamicMessage> values = new LinkedHashMap<>();
                values.put("group", built);
                values.put("owner", owner.build());
                all.addAll(mapIdentityLinks(registry, ownerLinks, values, "entra-owners"));
                owner = DynamicMessage.newBuilder(member);
            }
        }
        assertThat(all).extracting(l -> l.getTo().getIdentityType()).contains("entra-group-owner");
        RepositoryCaller user = new CallerIdentityExpansion(all).expand(
                caller("user-principal-name:" + entraUpns().getFirst()));
        DynamicMessage siteAclRecord = recordedSiteAcl();
        assertThat(allows(user, map(grant(siteAclRecord, groupId, "_o")))).as("owners of %s", groupId).isTrue();
        assertThat(allows(user, map(grant(siteAclRecord, groupId, "")))).as("members of %s, not owner links", groupId)
                .isFalse();
        assertThat(allows(user, map(grant(siteAclRecord, "00000000-0000-4000-8000-00000000ffff", "_o"))))
                .as("owners of another group").isFalse();
    }

    @Test
    void theRealEntraUserReadsTheRealSharePointItemsThroughItsSiteGroup() throws Exception {
        Recorded recorded = recorded();
        assertThat(recorded.items()).hasSizeGreaterThanOrEqualTo(10);
        assertThat(recorded.items().values().stream().map(DocumentSecurity::getPermissionsCount))
                .allSatisfy(count -> assertThat(count).isPositive());
        assertThat(recorded.items().keySet().stream().filter(id -> recorded.owners().get(id).contains("/list_item/")))
                .as("items under their own role assignments").isNotEmpty();

        String upn = entraUpns().getFirst();
        Map<String, RepositoryCaller> callers = callers(recorded, upn);
        for (var item : recorded.items().entrySet()) {
            assertThat(allows(callers.get("user-principal-name:" + upn), item.getValue()))
                    .as("the Entra user on %s", item.getKey()).isTrue();
            assertThat(allows(callers.get("user-principal-name:" + upn + " without groups"), item.getValue()))
                    .as("the Entra user without its group on %s", item.getKey()).isFalse();
            assertThat(allows(callers.get(recorded.limitedGroup() + " member"), item.getValue()))
                    .as("Limited Access only on %s", item.getKey()).isFalse();
        }
    }

    @Test
    void writesTheSearchFixtureFromTheRecordedLiveRecords(@TempDir Path out) throws Exception {
        Recorded recorded = recorded();
        Map<String, RepositoryCaller> callers = callers(recorded, entraUpns().getFirst());
        Map<String, Map<String, Boolean>> decisions = writeAndVerifySearchFixture(
                recorded.items(),
                callers,
                out.resolve("sharepoint-items.ndjson"),
                out.resolve("sharepoint-callers.json"));
        assertThat(decisions.values()).anySatisfy(d -> assertThat(d).containsValue(true));
        assertThat(decisions.values()).anySatisfy(d -> assertThat(d).containsValue(false));
    }

    private record Recorded(
            Map<String, DocumentSecurity> items,
            Map<String, String> owners,
            List<IdentityLink> links,
            String limitedGroup) {
    }

    private static Recorded recorded() throws Exception {
        List<DynamicMessage> records = readDelimited(entity, "acl/sharepoint-sync-records.binpb");
        Map<String, DynamicMessage> acls = new LinkedHashMap<>();
        List<IdentityLink> mappedLinks = new ArrayList<>();
        Map<String, String> owners = new LinkedHashMap<>();
        String limited = null;
        for (DynamicMessage record : records) {
            String id = field(record, "entity_id").toString();
            FieldDescriptor arm = record.getOneofFieldDescriptor(entity.getOneofs().getFirst());
            Message value = (Message) record.getField(arm);
            switch (arm.getName()) {
                case "site_acl" -> {
                    acls.put(id, (DynamicMessage) value);
                    String candidate = findLimitedOnlySharePointGroup(value);
                    if (candidate != null) {
                        limited = candidate;
                    }
                }
                case "site_group" -> mappedLinks.addAll(
                        mapIdentityLinks(registry, links, Map.of("group", (DynamicMessage) value), "sp-identity"));
                case "site_admins" -> mappedLinks.addAll(
                        mapIdentityLinks(registry, adminLinks, Map.of("admins", (DynamicMessage) value), "sp-admins"));
                default -> {
                    FieldDescriptor owner = value.getDescriptorForType().findFieldByName("acl_owner_id");
                    if (owner != null && !value.getField(owner).toString().isEmpty()) {
                        owners.put(arm.getName() + ":" + id, value.getField(owner).toString());
                    }
                }
            }
        }
        Map<String, DocumentSecurity> items = new LinkedHashMap<>();
        for (var owner : owners.entrySet()) {
            DynamicMessage acl = acls.get(owner.getValue());
            assertThat(acl).as("role assignments named by %s", owner.getKey()).isNotNull();
            items.put(owner.getKey(), map(acl));
        }
        assertThat(limited).as("a group that holds only Limited Access").isNotNull();
        return new Recorded(items, owners, mappedLinks, limited);
    }

    private static String findLimitedOnlySharePointGroup(Message siteAclValue) {
        for (Object a : (List<?>) siteAclValue.getField(siteAcl.findFieldByName("assignments"))) {
            Message assignment = (Message) a;
            Message member = (Message) field(assignment, "member");
            List<?> roles = (List<?>) field(assignment, "roles");
            boolean limitedOnly = roles.stream().map(Message.class::cast)
                    .allMatch(r -> field(r, "name").toString().contains("Limited Access"));
            if (limitedOnly && field(member, "principal_type").toString().endsWith("SHAREPOINT_GROUP")) {
                return "sharepoint-group:" + siteOf(siteAclValue) + "/" + field(member, "id");
            }
        }
        return null;
    }

    private static Map<String, RepositoryCaller> callers(Recorded recorded, String upn) throws Exception {
        List<IdentityLink> all = new ArrayList<>(recorded.links());
        all.addAll(entraLinksOfRecordedUsers());
        CallerIdentityExpansion expansion = new CallerIdentityExpansion(all);
        Map<String, RepositoryCaller> callers = new LinkedHashMap<>();
        callers.put("user-principal-name:" + upn, expansion.expand(caller("user-principal-name:" + upn)));
        callers.put("user-principal-name:" + upn + " without groups",
                new CallerIdentityExpansion(entraLinksOfRecordedUsers()).expand(caller("user-principal-name:" + upn)));
        callers.put("email:join-proof@example.invalid", expansion.expand(caller("email:join-proof@example.invalid")));
        callers.put(recorded.limitedGroup() + " member", expansion.expand(caller(recorded.limitedGroup())));
        return callers;
    }

    private static DynamicMessage grant(DynamicMessage acl, String id, String suffix) throws Exception {
        DynamicMessage assignment = parseText(siteRoleAssignment, """
                member { id: 900 principal_type: SITE_PRINCIPAL_TYPE_SECURITY_GROUP
                  login_name: "c:0o.c|federateddirectoryclaimprovider|%s%s" directory_object_id: "%s"
                  directory_group_owners: %s }
                roles { name: "Read" base_permissions_low: 138612833 }
                """.formatted(id, suffix, id, !suffix.isEmpty()));
        return acl.toBuilder().addRepeatedField(siteAcl.findFieldByName("assignments"), assignment).build();
    }

    private static DynamicMessage recordedSiteAcl() throws IOException {
        for (DynamicMessage built : readDelimited(entity, "acl/sharepoint-sync-records.binpb")) {
            if (field(built, "entity_id").toString().startsWith("site_acl/site/")) {
                return (DynamicMessage) field(built, "site_acl");
            }
        }
        throw new IOException("no site ACL among the recorded records");
    }

    private static List<String> entraUpns() throws IOException {
        List<String> upns = new ArrayList<>();
        for (DynamicMessage user : readDelimited(graphUser, "acl/entra-users.binpb")) {
            upns.add(field(user, "user_principal_name").toString());
        }
        assertThat(upns).isNotEmpty();
        return upns;
    }

    private static List<IdentityLink> entraLinksOfRecordedUsers() throws Exception {
        List<IdentityLink> out = new ArrayList<>();
        for (DynamicMessage user : readDelimited(graphUser, "acl/entra-users.binpb")) {
            out.addAll(mapIdentityLinks(registry, entraLinks, Map.of("user", user), "entra-identity"));
        }
        return out;
    }

    private static String siteOf(Message acl) {
        Message securable = (Message) acl.getField(siteAcl.findFieldByName("securable"));
        FieldDescriptor arm = securable.getOneofFieldDescriptor(
                securable.getDescriptorForType().getOneofs().getFirst());
        Object value = securable.getField(arm);
        if (value instanceof Message ref) {
            return field(ref, "site_id").toString();
        }
        return value.toString();
    }

    private static DocumentSecurity map(DynamicMessage acl) throws Exception {
        return mapSecurity(registry, edge, Map.of("acl", acl), "sharepoint-acl");
    }
}
