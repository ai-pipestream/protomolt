package ai.protomolt.proto.pipeline;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.descriptors.GoogleDescriptorLoader;
import ai.protomolt.proto.grpc.workflow.v1.TypedEdge;
import ai.protomolt.proto.repo.spi.CallerIdentityExpansion;
import ai.protomolt.proto.repo.spi.DocumentAccessPolicy;
import ai.protomolt.proto.repo.spi.DocumentSecurityTerms;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v1.Access;
import ai.protomolt.proto.repo.v1.DocumentSecurity;
import ai.protomolt.proto.repo.v1.IdentityLink;
import ai.protomolt.proto.repo.v1.IdentityLinks;
import ai.protomolt.proto.repo.v1.Principal;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.TextFormat;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The SharePoint ACL mapping configs applied as pipeline edges: the role
 * assignments and site groups grpc-microsoft Sync emits with
 * {@code include_security}, mapped into DocumentSecurity and identity links
 * and evaluated by the repository policy.
 */
class SharePointSecurityMappingTest {

    private static final String ACCOUNT = "ACC";
    private static final String SITE = "host,coll,web";
    private static DescriptorRegistry registry;
    private static TypedEdge edge;
    private static TypedEdge links;
    private static TypedEdge entraLinks;
    private static Descriptor entity;
    private static Descriptor siteAcl;

    @BeforeAll
    static void load() throws Exception {
        registry = DescriptorRegistry.create(false);
        new GoogleDescriptorLoader("acl/microsoft-v1.binpb").loadDescriptors().forEach(registry::registerFile);
        registry.registerFile(DocumentSecurity.getDescriptor().getFile());
        registry.registerFile(IdentityLink.getDescriptor().getFile());
        edge = edge("acl/sharepoint-document-security.textproto");
        links = edge("acl/sharepoint-identity-links.textproto");
        entraLinks = edge("acl/entra-identity-links.textproto");
        entity = registry.findDescriptorByFullName("ai.pipestream.microsoft.v1.MicrosoftEntity");
        siteAcl = registry.findDescriptorByFullName("ai.pipestream.microsoft.v1.SiteAcl");
    }

    /** Read, Edit, Limited Access and two levels on one assignment; users, a claim and a directory group. */
    private static final String ACL = """
            securable { list_item { site_id: "host,coll,web" list_id: "L" item_id: "7" } }
            assignments { member { id: 4 principal_type: SITE_PRINCIPAL_TYPE_SHAREPOINT_GROUP login_name: "Visitors" }
              roles { name: "Read" base_permissions_low: 138612833 } }
            assignments { member { id: 5 principal_type: SITE_PRINCIPAL_TYPE_SHAREPOINT_GROUP login_name: "Members" }
              roles { name: "Limited Access" base_permissions_low: 134287360 }
              roles { name: "Edit" base_permissions_low: 1011030767 } }
            assignments { member { id: 11 principal_type: SITE_PRINCIPAL_TYPE_SHAREPOINT_GROUP login_name: "Limited" }
              roles { name: "Limited Access" base_permissions_low: 134287360 } }
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
            completeness: ACL_COMPLETENESS_COMPLETE
            """;

    @Test
    void assignmentsThatViewItemsBecomeReadRulesAndLimitedAccessGrantsNothing() throws Exception {
        DocumentSecurity security = map(acl(ACL));

        assertThat(security.getPermissionsList()).extracting(r -> r.getIdentityType() + ":" + r.getIdentity())
                .containsExactly("sharepoint-group:host,coll,web/4", "sharepoint-group:host,coll,web/5",
                        "user-principal-name:ada@contoso.com", "email:guest@example.org", "entra-group:g-1",
                        "sharepoint-claim:c:0-.f|rolemanager|spo-grid-all-users/t");
        assertThat(security.getPermissionsList()).allSatisfy(r ->
                assertThat(r.getAccess()).isEqualTo(Access.ACCESS_READ));
        assertThat(security.getRequiredRuleSetsList()).isEmpty();
        assertThat(reads(security, "sharepoint-group:host,coll,web/11")).as("Limited Access only").isFalse();
        assertThat(reads(security, "user-principal-name:ADA@contoso.com")).isTrue();
    }

    @Test
    void anIncompleteAclMapsToNoRuleAndDenies() throws Exception {
        for (String completeness : List.of("ACL_COMPLETENESS_UNAVAILABLE", "ACL_COMPLETENESS_UNSPECIFIED")) {
            DocumentSecurity security = map(acl(ACL.replace("ACL_COMPLETENESS_COMPLETE", completeness)));
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
        assertThat(groupLinks(message("ai.pipestream.microsoft.v1.SiteGroupMembers", group)))
                .extracting(l -> l.getFrom().getIdentityType() + ":" + l.getFrom().getIdentity() + " -> "
                        + l.getTo().getIdentityType() + ":" + l.getTo().getIdentity())
                .containsExactly("user-principal-name:ada@contoso.com -> sharepoint-group:host,coll,web/4",
                        "email:guest@example.org -> sharepoint-group:host,coll,web/4",
                        "entra-group:g-1 -> sharepoint-group:host,coll,web/4");
        assertThat(groupLinks(message("ai.pipestream.microsoft.v1.SiteGroupMembers",
                group.replace("ACL_COMPLETENESS_COMPLETE", "ACL_COMPLETENESS_UNAVAILABLE")))).isEmpty();
    }

    /**
     * End to end on recorded live data: {@code acl/sharepoint-sync-records.binpb} is what
     * grpc-microsoft Sync emitted with {@code include_security} in live run 45073949282
     * (commit 63b585e, recording {@code sync-security}), as delimited {@code MicrosoftEntity}
     * messages. Every drive, list and item maps through the role assignments its
     * {@code acl_owner_id} names. The tenant's one Entra user ({@code acl/entra-users.binpb})
     * signs in with its user principal name, is a member of the site's owners group, and reads
     * all of it; a member of a group that holds only Limited Access reads none of it.
     */
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

    /**
     * Writes the OpenSearch fixture from the recorded live records, the same shape as the
     * Confluence one. The terms must answer what the policy answers.
     */
    @Test
    void writesTheSearchFixtureFromTheRecordedLiveRecords() throws Exception {
        Recorded recorded = recorded();
        Map<String, RepositoryCaller> callers = callers(recorded, entraUpns().getFirst());
        StringBuilder items = new StringBuilder();
        Map<String, Map<String, Boolean>> decisions = new LinkedHashMap<>();
        callers.keySet().forEach(name -> decisions.put(name, new LinkedHashMap<>()));
        for (var item : recorded.items().entrySet()) {
            DocumentSecurityTerms terms = DocumentSecurityTerms.of(item.getValue(), List.of());
            items.append("{\"_id\":").append(json(item.getKey()))
                    .append(",\"acl_allow\":").append(json(terms.allow()))
                    .append(",\"acl_deny\":").append(json(terms.deny()))
                    .append(",\"acl_gates\":[],\"acl_complete\":true}\n");
            for (var caller : callers.entrySet()) {
                boolean allowed = allows(caller.getValue(), item.getValue());
                assertThat(terms.reads(DocumentSecurityTerms.callerTerms(caller.getValue().identities())))
                        .as("%s on %s", caller.getKey(), item.getKey()).isEqualTo(allowed);
                decisions.get(caller.getKey()).put(item.getKey(), allowed);
            }
        }
        assertThat(decisions.values()).anySatisfy(d -> assertThat(d).containsValue(true));
        assertThat(decisions.values()).anySatisfy(d -> assertThat(d).containsValue(false));

        StringBuilder callerJson = new StringBuilder("[");
        int n = 0;
        for (var caller : callers.entrySet()) {
            callerJson.append(n++ == 0 ? "\n" : ",\n").append("{\"caller\":").append(json(caller.getKey()))
                    .append(",\"terms\":").append(json(List.copyOf(
                            DocumentSecurityTerms.callerTerms(caller.getValue().identities()))))
                    .append(",\"reads\":{");
            int m = 0;
            for (var decision : decisions.get(caller.getKey()).entrySet()) {
                callerJson.append(m++ == 0 ? "" : ",").append(json(decision.getKey())).append(':')
                        .append(decision.getValue());
            }
            callerJson.append("}}");
        }
        callerJson.append("\n]\n");
        Path out = Path.of("build", "acl-opensearch");
        Files.createDirectories(out);
        Files.writeString(out.resolve("sharepoint-items.ndjson"), items);
        Files.writeString(out.resolve("sharepoint-callers.json"), callerJson);
    }

    /** Mapped recorded items by entity id, the owner each names, the group links, and a Limited Access group. */
    private record Recorded(Map<String, DocumentSecurity> items, Map<String, String> owners,
            List<IdentityLink> links, String limitedGroup) {
    }

    private static Recorded recorded() throws Exception {
        List<DynamicMessage> records = new ArrayList<>();
        try (InputStream in = SharePointSecurityMappingTest.class.getClassLoader()
                .getResourceAsStream("acl/sharepoint-sync-records.binpb")) {
            DynamicMessage.Builder record = DynamicMessage.newBuilder(entity);
            while (record.mergeDelimitedFrom(in)) {
                records.add(record.build());
                record = DynamicMessage.newBuilder(entity);
            }
        }
        Map<String, DynamicMessage> acls = new LinkedHashMap<>();
        List<IdentityLink> links = new ArrayList<>();
        Map<String, String> owners = new LinkedHashMap<>();
        String limited = null;
        for (DynamicMessage record : records) {
            String id = field(record, "entity_id").toString();
            FieldDescriptor arm = record.getOneofFieldDescriptor(entity.getOneofs().getFirst());
            Message value = (Message) record.getField(arm);
            switch (arm.getName()) {
                case "site_acl" -> {
                    acls.put(id, (DynamicMessage) value);
                    for (Object a : (List<?>) value.getField(siteAcl.findFieldByName("assignments"))) {
                        Message assignment = (Message) a;
                        Message member = (Message) assignment.getField(
                                assignment.getDescriptorForType().findFieldByName("member"));
                        List<?> roles = (List<?>) assignment.getField(
                                assignment.getDescriptorForType().findFieldByName("roles"));
                        boolean limitedOnly = roles.stream().map(Message.class::cast).allMatch(r ->
                                r.getField(r.getDescriptorForType().findFieldByName("name")).toString()
                                        .contains("Limited Access"));
                        if (limitedOnly && member.getField(member.getDescriptorForType()
                                .findFieldByName("principal_type")).toString().endsWith("SHAREPOINT_GROUP")) {
                            limited = "sharepoint-group:" + siteOf(value) + "/"
                                    + member.getField(member.getDescriptorForType().findFieldByName("id"));
                        }
                    }
                }
                case "site_group" -> links.addAll(groupLinks((DynamicMessage) value));
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
        return new Recorded(items, owners, links, limited);
    }

    private static Map<String, RepositoryCaller> callers(Recorded recorded, String upn) throws Exception {
        List<IdentityLink> all = new ArrayList<>(recorded.links());
        all.addAll(entraLinksOfRecordedUsers());
        CallerIdentityExpansion expansion = new CallerIdentityExpansion(all);
        Map<String, RepositoryCaller> callers = new LinkedHashMap<>();
        callers.put("user-principal-name:" + upn, expansion.expand(caller("user-principal-name:" + upn)));
        callers.put("user-principal-name:" + upn + " without groups",
                new CallerIdentityExpansion(entraLinksOfRecordedUsers())
                        .expand(caller("user-principal-name:" + upn)));
        callers.put("email:join-proof@example.invalid", expansion.expand(caller("email:join-proof@example.invalid")));
        callers.put(recorded.limitedGroup() + " member", expansion.expand(caller(recorded.limitedGroup())));
        return callers;
    }

    private static List<String> entraUpns() throws IOException {
        List<String> upns = new ArrayList<>();
        for (DynamicMessage user : entraUsers()) upns.add(field(user, "user_principal_name").toString());
        assertThat(upns).isNotEmpty();
        return upns;
    }

    private static List<IdentityLink> entraLinksOfRecordedUsers() throws Exception {
        List<IdentityLink> out = new ArrayList<>();
        for (DynamicMessage user : entraUsers()) {
            out.addAll(IdentityLinks.parseFrom(EdgeMapping.map(
                    registry.findDescriptorByFullName(entraLinks.getProduceType()), Map.of("user", user),
                    entraLinks.getRulesList(), entraLinks.getCelRulesList(), registry, "entra-identity")
                    .toByteString()).getLinksList());
        }
        return out;
    }

    private static List<DynamicMessage> entraUsers() throws IOException {
        Descriptor graphUser = registry.findDescriptorByFullName("ai.pipestream.microsoft.v1.GraphUser");
        List<DynamicMessage> users = new ArrayList<>();
        try (InputStream in = SharePointSecurityMappingTest.class.getClassLoader()
                .getResourceAsStream("acl/entra-users.binpb")) {
            DynamicMessage.Builder user = DynamicMessage.newBuilder(graphUser);
            while (user.mergeDelimitedFrom(in)) {
                users.add(user.build());
                user = DynamicMessage.newBuilder(graphUser);
            }
        }
        return users;
    }

    private static String siteOf(Message acl) {
        Message securable = (Message) acl.getField(siteAcl.findFieldByName("securable"));
        FieldDescriptor arm = securable.getOneofFieldDescriptor(
                securable.getDescriptorForType().getOneofs().getFirst());
        Object value = securable.getField(arm);
        if (value instanceof Message ref) {
            return ref.getField(ref.getDescriptorForType().findFieldByName("site_id")).toString();
        }
        return value.toString();
    }

    private static List<IdentityLink> groupLinks(DynamicMessage group) throws Exception {
        return IdentityLinks.parseFrom(EdgeMapping.map(registry.findDescriptorByFullName(links.getProduceType()),
                Map.of("group", group), links.getRulesList(), links.getCelRulesList(), registry,
                "sharepoint-identity").toByteString()).getLinksList();
    }

    private static DocumentSecurity map(DynamicMessage acl) throws Exception {
        return DocumentSecurity.parseFrom(EdgeMapping.map(registry.findDescriptorByFullName(edge.getProduceType()),
                Map.of("acl", acl), edge.getRulesList(), edge.getCelRulesList(), registry, "sharepoint-acl")
                .toByteString());
    }

    private static DynamicMessage acl(String text) throws TextFormat.ParseException {
        return message("ai.pipestream.microsoft.v1.SiteAcl", text);
    }

    private static DynamicMessage message(String type, String text) throws TextFormat.ParseException {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(registry.findDescriptorByFullName(type));
        TextFormat.merge(text, builder);
        return builder.build();
    }

    private static boolean allows(RepositoryCaller caller, DocumentSecurity security) {
        return DocumentAccessPolicy.allows(caller, ACCOUNT, security, List.of(), Access.ACCESS_READ);
    }

    private static boolean reads(DocumentSecurity security, String... identities) {
        return allows(caller(identities), security);
    }

    private static RepositoryCaller caller(String... identities) {
        Set<Principal> principals = java.util.Arrays.stream(identities).map(identity -> {
            int colon = identity.indexOf(':');
            return Principal.newBuilder().setIdentityType(identity.substring(0, colon))
                    .setIdentity(identity.substring(colon + 1)).build();
        }).collect(Collectors.toSet());
        return new RepositoryCaller("reader", false, Set.of(ACCOUNT), principals);
    }

    private static Object field(DynamicMessage message, String name) {
        return message.getField(message.getDescriptorForType().findFieldByName(name));
    }

    private static String json(String value) {
        StringBuilder out = new StringBuilder("\"");
        for (char c : value.toCharArray()) {
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                default -> {
                    if (c < 0x20) out.append(String.format("\\u%04x", (int) c));
                    else out.append(c);
                }
            }
        }
        return out.append('"').toString();
    }

    private static String json(List<String> values) {
        return values.stream().map(SharePointSecurityMappingTest::json).collect(Collectors.joining(",", "[", "]"));
    }

    private static TypedEdge edge(String name) throws IOException {
        TypedEdge.Builder builder = TypedEdge.newBuilder();
        try (InputStream in = SharePointSecurityMappingTest.class.getClassLoader().getResourceAsStream(name)) {
            if (in == null) throw new IOException("missing resource " + name);
            TextFormat.merge(new String(in.readAllBytes(), StandardCharsets.UTF_8), builder);
        }
        return builder.build();
    }
}
