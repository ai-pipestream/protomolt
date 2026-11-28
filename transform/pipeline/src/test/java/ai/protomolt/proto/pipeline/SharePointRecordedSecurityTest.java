package ai.protomolt.proto.pipeline;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.grpc.workflow.v1.TypedEdge;
import ai.protomolt.proto.repo.spi.CallerIdentityExpansion;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v1.DocumentSecurity;
import ai.protomolt.proto.repo.v1.IdentityLink;
import ai.protomolt.proto.repo.v1.Principal;
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
import static ai.protomolt.proto.pipeline.AclMappingTestSupport.isComplete;
import static ai.protomolt.proto.pipeline.AclMappingTestSupport.loadEdge;
import static ai.protomolt.proto.pipeline.AclMappingTestSupport.loadRegistry;
import static ai.protomolt.proto.pipeline.AclMappingTestSupport.mapIdentityLinks;
import static ai.protomolt.proto.pipeline.AclMappingTestSupport.mapSecurity;
import static ai.protomolt.proto.pipeline.AclMappingTestSupport.parseText;
import static ai.protomolt.proto.pipeline.AclMappingTestSupport.principal;
import static ai.protomolt.proto.pipeline.AclMappingTestSupport.readDelimited;
import static ai.protomolt.proto.pipeline.AclMappingTestSupport.reads;
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

        String userId = entraUserIds().getFirst();
        List<String> direct = all.stream()
                .filter(l -> l.getFrom().getIdentityType().equals("entra-user")
                        && l.getFrom().getIdentity().equals(userId)
                        && l.getTo().getIdentityType().equals("entra-group"))
                .map(l -> l.getTo().getIdentity()).toList();
        assertThat(direct).as("groups the recorded user is a direct member of").isNotEmpty();

        RepositoryCaller user = new CallerIdentityExpansion(all).expand(
                caller("user-principal-name:" + entraUpns().getFirst()));
        List<String> reached = user.identities().stream()
                .filter(p -> p.getIdentityType().equals("entra-group")).map(Principal::getIdentity).toList();
        assertThat(reached).containsAll(direct);
        assertThat(reached).as("only groups that have members").isSubsetOf(withMembers);
        assertThat(reached).doesNotContainAnyElementsOf(empty);
        DynamicMessage siteAclRecord = recordedSiteAcl();
        for (String id : direct) {
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
        assertThat(recorded.items().values()).as("rules beside the site admin rule").allSatisfy(security ->
                assertThat(security.getPermissionsList()).anySatisfy(rule ->
                        assertThat(rule.getIdentityType()).isNotEqualTo("sharepoint-site-admin")));
        assertThat(recorded.items().keySet().stream().filter(id -> recorded.owners().get(id).contains("/list_item/")))
                .as("items under their own role assignments").isNotEmpty();

        String upn = entraUpns().getFirst();
        Map<String, RepositoryCaller> callers = callers(recorded, upn);
        for (var item : recorded.items().entrySet()) {
            assertThat(allows(callers.get("user-principal-name:" + upn), item.getValue()))
                    .as("the Entra user on %s", item.getKey()).isTrue();
            assertThat(allows(callers.get("user-principal-name:" + upn + " without the site admin link"),
                    item.getValue())).as("the Entra user through its site group alone on %s", item.getKey()).isTrue();
            assertThat(allows(callers.get("user-principal-name:" + upn + " without groups"), item.getValue()))
                    .as("the Entra user without its group on %s", item.getKey()).isFalse();
            assertThat(allows(callers.get(recorded.limitedGroup() + " member"), item.getValue()))
                    .as("Limited Access only on %s", item.getKey()).isFalse();
        }
    }

    @Test
    void theRecordedSharedFileIsReadThroughItsLinkForNamedPeopleAndItsInvitedGroupOnly() throws Exception {
        List<DynamicMessage> records = readDelimited(entity, "acl/sharepoint-sync-records.binpb");
        Map<String, DynamicMessage> shared = new LinkedHashMap<>();
        List<String> owners = new ArrayList<>();
        Map<String, DynamicMessage> linkGroups = new LinkedHashMap<>();
        for (DynamicMessage record : records) {
            FieldDescriptor arm = record.getOneofFieldDescriptor(entity.getOneofs().getFirst());
            Message value = (Message) record.getField(arm);
            switch (arm.getName()) {
                case "site_acl" -> {
                    if (assignments(value).stream().anyMatch(a -> linkScope(member(a)) != null)) {
                        shared.put(field(record, "entity_id").toString(), (DynamicMessage) value);
                    }
                }
                case "site_group" -> {
                    Message group = (Message) field(value, "group");
                    if (linkScope(group) != null) {
                        linkGroups.put(field(group, "id").toString(), (DynamicMessage) value);
                    }
                }
                case "drive_item" -> owners.add(field(value, "acl_owner_id").toString());
                default -> {
                }
            }
        }
        assertThat(shared).as("role assignment sets that name a link group").hasSize(1);
        String aclId = shared.keySet().iterator().next();
        DynamicMessage acl = shared.get(aclId);
        assertThat(aclId).as("the shared file has role assignments of its own").contains("/list_item/");
        assertThat(owners).as("a file under those role assignments").contains(aclId);

        String site = siteOf(acl);
        Map<String, String> groupOfScope = new LinkedHashMap<>();
        String invited = null;
        for (Message assignment : assignments(acl)) {
            Message member = member(assignment);
            String scope = linkScope(member);
            if (scope != null) {
                assertThat(levels(assignment)).as("the level of the %s link group", scope).containsExactly("Read");
                groupOfScope.put(scope, field(member, "id").toString());
            } else if (!field(member, "directory_object_id").toString().isEmpty()) {
                invited = field(member, "directory_object_id").toString();
                assertThat(levels(assignment)).as("the level of the invited group").containsExactly("Contribute");
            }
        }
        assertThat(groupOfScope).containsOnlyKeys("organization", "users");
        assertThat(invited).as("the invited directory group").isNotNull();
        String namedPeople = "sharepoint-group:" + site + "/" + groupOfScope.get("users");
        String organization = "sharepoint-group:" + site + "/" + groupOfScope.get("organization");

        DocumentSecurity security = map(acl);
        assertThat(security.getPermissionsList()).extracting(r -> r.getIdentityType() + ":" + r.getIdentity())
                .contains(namedPeople, "entra-group:" + invited)
                .doesNotContain(organization);
        assertThat(reads(security, namedPeople)).as("the link for named people").isTrue();
        assertThat(reads(security, organization)).as("the organization link").isFalse();
        assertThat(reads(security, "entra-group:" + invited)).as("the invited group on the file").isTrue();

        String upn = entraUpns().getFirst();
        List<IdentityLink> throughTheLink = new ArrayList<>(entraLinksOfRecordedUsers());
        throughTheLink.addAll(mapIdentityLinks(registry, links,
                Map.of("group", linkGroups.get(groupOfScope.get("users"))), "sp-identity"));
        assertThat(throughTheLink).as("the user granted on the link is a member of its group")
                .anySatisfy(l -> assertThat(l.getTo()).isEqualTo(principal(namedPeople)));
        assertThat(allows(new CallerIdentityExpansion(throughTheLink).expand(caller("user-principal-name:" + upn)),
                security)).as("the granted user through the link's group alone").isTrue();

        List<IdentityLink> redeemed = new ArrayList<>(entraLinksOfRecordedUsers());
        redeemed.add(IdentityLink.newBuilder().setFrom(principal("user-principal-name:" + upn))
                .setTo(principal(organization)).build());
        assertThat(allows(new CallerIdentityExpansion(redeemed).expand(caller("user-principal-name:" + upn)),
                security)).as("a member of the organization link's group and nothing else").isFalse();

        DynamicMessage siteAclRecord = recordedSiteAcl();
        String invitedGroup = invited;
        assertThat(assignments(siteAclRecord)).as("the invited group on the site")
                .filteredOn(a -> field(member(a), "directory_object_id").toString().equals(invitedGroup))
                .singleElement()
                .satisfies(a -> assertThat(levels(a)).containsExactly("Limited Access"));
        assertThat(reads(map(siteAclRecord), "entra-group:" + invited))
                .as("Limited Access on the site lets the invited group read nothing there").isFalse();
    }

    @Test
    void writesTheSearchFixtureFromTheRecordedLiveRecords(@TempDir Path out) throws Exception {
        Recorded recorded = recorded();
        Map<String, RepositoryCaller> callers = callers(recorded, entraUpns().getFirst());
        Map<String, Map<String, Boolean>> decisions = writeAndVerifySearchFixture(
                recorded.items(),
                recorded.complete(),
                callers,
                out.resolve("sharepoint-items.ndjson"),
                out.resolve("sharepoint-callers.json"));
        assertThat(decisions.values()).anySatisfy(d -> assertThat(d).containsValue(true));
        assertThat(decisions.values()).anySatisfy(d -> assertThat(d).containsValue(false));
    }

    private record Recorded(
            Map<String, DocumentSecurity> items,
            Map<String, String> owners,
            Map<String, Boolean> complete,
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
        Map<String, Boolean> complete = new LinkedHashMap<>();
        for (var owner : owners.entrySet()) {
            DynamicMessage acl = acls.get(owner.getValue());
            assertThat(acl).as("role assignments named by %s", owner.getKey()).isNotNull();
            items.put(owner.getKey(), map(acl));
            complete.put(owner.getKey(), isComplete(acl, "completeness"));
        }
        assertThat(limited).as("a group that holds only Limited Access").isNotNull();
        return new Recorded(items, owners, complete, mappedLinks, limited);
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
        callers.put("user-principal-name:" + upn + " without the site admin link",
                new CallerIdentityExpansion(all.stream()
                        .filter(l -> !l.getTo().getIdentityType().equals("sharepoint-site-admin")).toList())
                        .expand(caller("user-principal-name:" + upn)));
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

    private static List<String> entraUserIds() throws IOException {
        List<String> ids = new ArrayList<>();
        for (DynamicMessage user : readDelimited(graphUser, "acl/entra-users.binpb")) {
            ids.add(field(user, "id").toString());
        }
        assertThat(ids).isNotEmpty();
        return ids;
    }

    private static List<IdentityLink> entraLinksOfRecordedUsers() throws Exception {
        List<IdentityLink> out = new ArrayList<>();
        for (DynamicMessage user : readDelimited(graphUser, "acl/entra-users.binpb")) {
            out.addAll(mapIdentityLinks(registry, entraLinks, Map.of("user", user), "entra-identity"));
        }
        return out;
    }

    private static List<Message> assignments(Message acl) {
        return ((List<?>) field(acl, "assignments")).stream().map(Message.class::cast).toList();
    }

    private static Message member(Message assignment) {
        return (Message) field(assignment, "member");
    }

    private static List<String> levels(Message assignment) {
        return ((List<?>) field(assignment, "roles")).stream()
                .map(r -> field((Message) r, "name").toString()).toList();
    }

    /** Scope of the sharing link a group or principal stands for; {@code null} when it stands for none. */
    private static String linkScope(Message groupOrPrincipal) {
        FieldDescriptor link = groupOrPrincipal.getDescriptorForType().findFieldByName("sharing_link");
        return groupOrPrincipal.hasField(link)
                ? field((Message) groupOrPrincipal.getField(link), "scope").toString() : null;
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
