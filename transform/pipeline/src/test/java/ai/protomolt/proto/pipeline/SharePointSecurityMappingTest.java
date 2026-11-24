package ai.protomolt.proto.pipeline;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.grpc.workflow.v1.TypedEdge;
import ai.protomolt.proto.repo.spi.CallerIdentityExpansion;
import ai.protomolt.proto.repo.v1.Access;
import ai.protomolt.proto.repo.v1.DocumentSecurity;
import ai.protomolt.proto.repo.v1.IdentityLink;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static ai.protomolt.proto.pipeline.AclMappingTestSupport.caller;
import static ai.protomolt.proto.pipeline.AclMappingTestSupport.loadEdge;
import static ai.protomolt.proto.pipeline.AclMappingTestSupport.loadRegistry;
import static ai.protomolt.proto.pipeline.AclMappingTestSupport.mapIdentityLinks;
import static ai.protomolt.proto.pipeline.AclMappingTestSupport.mapSecurity;
import static ai.protomolt.proto.pipeline.AclMappingTestSupport.parseText;
import static ai.protomolt.proto.pipeline.AclMappingTestSupport.principal;
import static ai.protomolt.proto.pipeline.AclMappingTestSupport.reads;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for SharePoint role-assignment, site-group, and Entra group-delta mapping configs.
 */
class SharePointSecurityMappingTest {

    private static DescriptorRegistry registry;
    private static TypedEdge edge;
    private static TypedEdge links;
    private static TypedEdge groupLinks;
    private static Descriptor siteAcl;
    private static Descriptor siteGroupMembers;
    private static Descriptor groupChange;

    @BeforeAll
    static void load() throws Exception {
        registry = loadRegistry("acl/microsoft-v1.binpb");
        edge = loadEdge("acl/sharepoint-document-security.textproto");
        links = loadEdge("acl/sharepoint-identity-links.textproto");
        groupLinks = loadEdge("acl/entra-group-links.textproto");
        siteAcl = registry.findDescriptorByFullName("ai.pipestream.microsoft.v1.SiteAcl");
        siteGroupMembers = registry.findDescriptorByFullName("ai.pipestream.microsoft.v1.SiteGroupMembers");
        groupChange = registry.findDescriptorByFullName("ai.pipestream.microsoft.v1.GroupChange");
    }

    /** Read, Edit, Limited Access, view-without-open and two levels on one assignment; users, a claim and a directory group. */
    private static final String ACL = """
            securable { list_item { site_id: "host,coll,web" list_id: "L" item_id: "7" } }
            assignments { member { id: 4 principal_type: SITE_PRINCIPAL_TYPE_SHAREPOINT_GROUP login_name: "Visitors" }
              roles { name: "Read" base_permissions_low: 138612833 } }
            assignments { member { id: 5 principal_type: SITE_PRINCIPAL_TYPE_SHAREPOINT_GROUP login_name: "Members" }
              roles { name: "Limited Access" base_permissions_low: 134287360 }
              roles { name: "Edit" base_permissions_low: 1011030767 } }
            assignments { member { id: 11 principal_type: SITE_PRINCIPAL_TYPE_SHAREPOINT_GROUP login_name: "Limited" }
              roles { name: "Limited Access" base_permissions_low: 134287360 } }
            assignments { member { id: 12 principal_type: SITE_PRINCIPAL_TYPE_SHAREPOINT_GROUP login_name: "Listers" }
              roles { name: "View list items only" base_permissions_low: 1 } }
            assignments { member { id: 20 principal_type: SITE_PRINCIPAL_TYPE_USER
                login_name: "i:0#.f|membership|ada@contoso.com" user_principal_name: "ada@contoso.com" email: "ada@example.com" }
              roles { name: "Read" base_permissions_low: 138612833 } }
            assignments { member { id: 21 principal_type: SITE_PRINCIPAL_TYPE_USER login_name: "i:0#.f|guest" email: "guest@example.org" }
              roles { name: "Read" base_permissions_low: 138612833 } }
            assignments { member { id: 22 principal_type: SITE_PRINCIPAL_TYPE_USER login_name: "app@sharepoint" }
              roles { name: "Full Control" base_permissions_low: 4294967295 } }
            assignments { member { id: 30 principal_type: SITE_PRINCIPAL_TYPE_SECURITY_GROUP
                login_name: "c:0o.c|federateddirectoryclaimprovider|g-1" directory_object_id: "g-1" }
              roles { name: "Read" base_permissions_low: 138612833 } }
            assignments { member { id: 31 principal_type: SITE_PRINCIPAL_TYPE_SECURITY_GROUP
                login_name: "c:0-.f|rolemanager|spo-grid-all-users/t" }
              roles { name: "Read" base_permissions_low: 138612833 } }
            assignments { member { id: 32 principal_type: SITE_PRINCIPAL_TYPE_SECURITY_GROUP
                login_name: "c:0o.c|federateddirectoryclaimprovider|g-2_o" directory_object_id: "g-2"
                directory_group_owners: true }
              roles { name: "Edit" base_permissions_low: 1011030767 } }
            completeness: ACL_COMPLETENESS_COMPLETE
            """;

    @Test
    void assignmentsThatViewItemsBecomeReadRulesAndLimitedAccessGrantsNothing() throws Exception {
        DocumentSecurity security = map(parseText(siteAcl, ACL));

        assertThat(security.getPermissionsList()).extracting(r -> r.getIdentityType() + ":" + r.getIdentity())
                .containsExactly("sharepoint-group:host,coll,web/4", "sharepoint-group:host,coll,web/5",
                        "user-principal-name:ada@contoso.com", "email:guest@example.org", "entra-group:g-1",
                        "sharepoint-claim:c:0-.f|rolemanager|spo-grid-all-users/t", "entra-group-owner:g-2",
                        "sharepoint-site-admin:host,coll,web");
        assertThat(security.getPermissionsList()).allSatisfy(r ->
                assertThat(r.getAccess()).isEqualTo(Access.ACCESS_READ));
        assertThat(security.getRequiredRuleSetsList()).isEmpty();
        assertThat(reads(security, "sharepoint-group:host,coll,web/11")).as("Limited Access only").isFalse();
        assertThat(reads(security, "sharepoint-group:host,coll,web/12")).as("lists items, cannot open them").isFalse();
        assertThat(reads(security, "public:public")).as("the Everyone-except-external claim grants no one").isFalse();
        assertThat(reads(security, "user-principal-name:ADA@contoso.com")).isTrue();
        assertThat(reads(security, "entra-group:g-2")).as("members of a group whose owners are granted").isFalse();
        assertThat(reads(security, "sharepoint-site-admin:host,coll,web")).isTrue();
        assertThat(reads(security, "sharepoint-site-admin:other,coll,web")).isFalse();
    }

    @Test
    void anIncompleteAclMapsToNoRuleAndDenies() throws Exception {
        for (String completeness : List.of("ACL_COMPLETENESS_UNAVAILABLE", "ACL_COMPLETENESS_UNSPECIFIED")) {
            DocumentSecurity security = map(parseText(siteAcl, ACL.replace("ACL_COMPLETENESS_COMPLETE", completeness)));
            assertThat(security.getPermissionsList()).as(completeness).isEmpty();
            assertThat(reads(security, "sharepoint-group:host,coll,web/4")).isFalse();
        }
    }

    @Test
    void groupMembersLinkToTheSiteGroup() throws Exception {
        String group = """
                site_id: "host,coll,web"
                group { id: 4 title: "Visitors" }
                members { id: 20 principal_type: SITE_PRINCIPAL_TYPE_USER user_principal_name: "ada@contoso.com" }
                members { id: 21 principal_type: SITE_PRINCIPAL_TYPE_USER email: "guest@example.org" }
                members { id: 22 principal_type: SITE_PRINCIPAL_TYPE_USER login_name: "app@sharepoint" }
                members { id: 30 principal_type: SITE_PRINCIPAL_TYPE_SECURITY_GROUP directory_object_id: "g-1" }
                members { id: 31 principal_type: SITE_PRINCIPAL_TYPE_SECURITY_GROUP login_name: "c:0(.s|true" }
                completeness: ACL_COMPLETENESS_COMPLETE
                """;
        assertThat(mapIdentityLinks(registry, links, Map.of("group", parseText(siteGroupMembers, group)), "sp-links"))
                .extracting(l -> l.getFrom().getIdentityType() + ":" + l.getFrom().getIdentity() + " -> "
                        + l.getTo().getIdentityType() + ":" + l.getTo().getIdentity())
                .containsExactly("user-principal-name:ada@contoso.com -> sharepoint-group:host,coll,web/4",
                        "email:guest@example.org -> sharepoint-group:host,coll,web/4",
                        "entra-group:g-1 -> sharepoint-group:host,coll,web/4");
        assertThat(mapIdentityLinks(registry, links, Map.of("group", parseText(siteGroupMembers,
                group.replace("ACL_COMPLETENESS_COMPLETE", "ACL_COMPLETENESS_UNAVAILABLE"))), "sp-links")).isEmpty();
    }

    @Test
    void groupMembersAndNestedGroupsLinkToTheEntraGroup() throws Exception {
        String change = """
                group { id: "g-parent" }
                member_changes { id: "u-1" type: "user" }
                member_changes { id: "g-child" type: "group" }
                member_changes { id: "u-2" type: "user" removed: true }
                member_changes { id: "d-1" type: "device" }
                """;
        List<IdentityLink> mapped = mapIdentityLinks(
                registry, groupLinks, Map.of("change", parseText(groupChange, change)), "entra-groups");
        assertThat(mapped)
                .extracting(l -> l.getFrom().getIdentityType() + ":" + l.getFrom().getIdentity() + " -> "
                        + l.getTo().getIdentityType() + ":" + l.getTo().getIdentity())
                .containsExactly("entra-user:u-1 -> entra-group:g-parent", "entra-group:g-child -> entra-group:g-parent");
        assertThat(mapIdentityLinks(
                registry, groupLinks, Map.of("change", parseText(groupChange, change + "removed: true")), "entra-groups"))
                .isEmpty();
        CallerIdentityExpansion expansion = new CallerIdentityExpansion(List.of(
                IdentityLink.newBuilder().setFrom(principal("entra-user:u-9")).setTo(principal("entra-group:g-child"))
                        .build(),
                mapped.get(1)));
        assertThat(expansion.expand(caller("entra-user:u-9")).identities()).as("nested membership")
                .contains(principal("entra-group:g-parent"));
    }

    private static DocumentSecurity map(DynamicMessage acl) throws Exception {
        return mapSecurity(registry, edge, Map.of("acl", acl), "sharepoint-acl");
    }
}
