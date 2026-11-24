package ai.protomolt.proto.pipeline;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.grpc.workflow.v1.TypedEdge;
import ai.protomolt.proto.repo.spi.CallerIdentityExpansion;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v1.AccessRule;
import ai.protomolt.proto.repo.v1.DocumentSecurity;
import ai.protomolt.proto.repo.v1.IdentityLink;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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
import static ai.protomolt.proto.pipeline.AclMappingTestSupport.mapSingleIdentityLink;
import static ai.protomolt.proto.pipeline.AclMappingTestSupport.principal;
import static ai.protomolt.proto.pipeline.AclMappingTestSupport.readDelimited;
import static ai.protomolt.proto.pipeline.AclMappingTestSupport.writeAndVerifySearchFixture;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Recorded live-corpus verification for Confluence ACL mapping, access-class expansion,
 * live {@code CheckContentPermission} agreement, and search-term projection.
 */
class ConfluenceRecordedSecurityTest {

    private static DescriptorRegistry registry;
    private static TypedEdge edge;
    private static TypedEdge links;
    private static TypedEdge entraLinks;
    private static TypedEdge classLinks;
    private static Descriptor entity;

    @BeforeAll
    static void load() throws Exception {
        registry = loadRegistry("acl/confluence-v1.binpb", "acl/microsoft-v1.binpb");
        edge = loadEdge("acl/confluence-document-security.textproto");
        links = loadEdge("acl/confluence-identity-links.textproto");
        entraLinks = loadEdge("acl/entra-identity-links.textproto");
        classLinks = loadEdge("acl/confluence-access-class-links.textproto");
        entity = registry.findDescriptorByFullName("ai.pipestream.confluence.v1.ConfluenceEntity");
    }

    @Test
    void theRecordedLiveSyncRecordsMapToTheSiteSecurity() throws Exception {
        List<DynamicMessage> records = readDelimited(entity, "acl/confluence-sync-records.binpb");
        DynamicMessage space = records.stream().filter(r -> field(r, "entity_id").toString().startsWith("space:"))
                .findFirst().orElseThrow();
        DynamicMessage child = records.stream().filter(ConfluenceRecordedSecurityTest::isRestrictedChild)
                .findFirst().orElseThrow();

        Map<String, DynamicMessage> values = new LinkedHashMap<>();
        values.put("space", space);
        values.put("item", child);
        DocumentSecurity security = mapSecurity(registry, edge, values, "confluence-acl");
        assertThat(security.getPermissionsList()).extracting(r -> r.getIdentityType() + ":" + r.getIdentity())
                .contains("atlassian-account:account-1", "atlassian-access-class:ALL_LICENSED_USERS");
        assertThat(security.getRequiredRuleSetsList()).singleElement().satisfies(set ->
                assertThat(set.getRulesList()).extracting(AccessRule::getIdentityType)
                        .contains("atlassian-account", "atlassian-group"));
        String group = security.getRequiredRuleSets(0).getRulesList().stream()
                .filter(r -> r.getIdentityType().equals("atlassian-group")).findFirst().orElseThrow().getIdentity();

        CallerIdentityExpansion expansion = new CallerIdentityExpansion(recordedLinks(records));
        RepositoryCaller caller = expansion.expand(caller("email:join-proof@example.invalid"));
        assertThat(caller.identities()).extracting(p -> p.getIdentityType() + ":" + p.getIdentity())
                .contains("atlassian-account:account-1", "atlassian-group:" + group);
        assertThat(allows(caller, security)).isTrue();

        RepositoryCaller outsider = expansion.expand(caller("atlassian-account:outsider",
                "atlassian-access-class:ALL_LICENSED_USERS"));
        assertThat(allows(outsider, security)).isFalse();

        String other = records.stream().filter(r -> field(r, "entity_id").toString()
                        .startsWith("group:" + group + "/member/"))
                .map(r -> field(r, "entity_id").toString().substring(("group:" + group + "/member/").length()))
                .filter(id -> !id.equals("account-1")).findFirst().orElseThrow();
        RepositoryCaller member = expansion.expand(caller("atlassian-account:" + other));
        assertThat(allows(member, security)).as("no access class among its identities").isFalse();
        RepositoryCaller licensed = expansion.expand(caller("atlassian-account:" + other,
                "atlassian-access-class:ALL_LICENSED_USERS"));
        assertThat(allows(licensed, security)).isTrue();
    }

    @Test
    void theRecordedEntraUserRefusesUnverifiedOtherMailsAndReadsThroughVerifiedJoin() throws Exception {
        Descriptor graphUser = registry.findDescriptorByFullName("ai.pipestream.microsoft.v1.GraphUser");
        List<IdentityLink> entra = new ArrayList<>();
        String upn = null;
        for (DynamicMessage record : readDelimited(graphUser, "acl/entra-users.binpb")) {
            upn = field(record, "user_principal_name").toString();
            entra.addAll(mapIdentityLinks(registry, entraLinks, Map.of("user", record), "entra-identity"));
        }
        assertThat(upn).isNotBlank();
        List<DynamicMessage> records = readDelimited(entity, "acl/confluence-sync-records.binpb");
        List<IdentityLink> confluence = recordedLinks(records);

        List<IdentityLink> defaultLinks = new ArrayList<>(entra);
        defaultLinks.addAll(confluence);
        RepositoryCaller defaultCaller = new CallerIdentityExpansion(defaultLinks)
                .expand(caller("user-principal-name:" + upn));
        assertThat(defaultCaller.identities()).extracting(p -> p.getIdentityType() + ":" + p.getIdentity())
                .as("entra-identity-links excludes unverified other_mails")
                .contains("email:email-1@example.invalid", "email:email-2@example.invalid")
                .doesNotContain("email:join-proof@example.invalid", "atlassian-account:account-1");

        List<IdentityLink> bridgedLinks = new ArrayList<>(defaultLinks);
        bridgedLinks.add(IdentityLink.newBuilder()
                .setFrom(principal("email:email-2@example.invalid"))
                .setTo(principal("email:join-proof@example.invalid"))
                .build());
        RepositoryCaller joined = new CallerIdentityExpansion(bridgedLinks)
                .expand(caller("user-principal-name:" + upn));
        assertThat(joined.identities()).extracting(p -> p.getIdentityType() + ":" + p.getIdentity())
                .contains("email:join-proof@example.invalid", "atlassian-account:account-1");

        DynamicMessage space = records.stream().filter(r -> field(r, "entity_id").toString().startsWith("space:"))
                .findFirst().orElseThrow();
        int pages = 0;
        int gated = 0;
        for (DynamicMessage item : records) {
            if (!item.hasField(entity.findFieldByName("access_chain"))) {
                continue;
            }
            Map<String, DynamicMessage> values = new LinkedHashMap<>();
            values.put("space", space);
            values.put("item", item);
            DocumentSecurity security = mapSecurity(registry, edge, values, "confluence-acl");
            String id = field(item, "entity_id").toString();
            pages++;
            gated += security.getRequiredRuleSetsCount() > 0 ? 1 : 0;
            assertThat(allows(joined, security)).as("verified bridge on %s", id).isTrue();
            assertThat(allows(defaultCaller, security)).as("unverified other_mails alone on %s", id).isFalse();
        }
        assertThat(pages).isPositive();
        assertThat(gated).as("pages under a view-restricted parent").isPositive();
    }

    @Test
    void writesTheSearchFixtureFromTheRecordedLiveRecords(@TempDir Path out) throws Exception {
        List<DynamicMessage> records = new ArrayList<>(readDelimited(entity, "acl/confluence-sync-records.binpb"));
        records.addAll(readDelimited(entity, "acl/confluence-private-sync-records.binpb"));
        Map<String, DynamicMessage> spaces = new LinkedHashMap<>();
        records.stream().filter(r -> r.hasField(entity.findFieldByName("space_access")))
                .forEach(r -> spaces.put(field(r, "entity_id").toString(), r));
        List<IdentityLink> mapped = new ArrayList<>(recordedLinks(records));
        List<IdentityLink> unclassed = List.copyOf(mapped);
        for (DynamicMessage record : records) {
            mapped.addAll(mapIdentityLinks(registry, classLinks, Map.of("entity", record), "confluence-classes"));
        }
        CallerIdentityExpansion expansion = new CallerIdentityExpansion(mapped);
        Map<String, RepositoryCaller> callers = new LinkedHashMap<>();
        callers.put("email:join-proof@example.invalid", expansion.expand(caller("email:join-proof@example.invalid")));
        records.stream().filter(r -> r.hasField(entity.findFieldByName("user")))
                .map(r -> "atlassian-account:" + field(r, "entity_id")).filter(id -> !id.endsWith(":account-1"))
                .forEach(id -> callers.put(id, expansion.expand(caller(id))));
        String member = callers.keySet().stream().filter(id -> !id.startsWith("email:")).reduce((a, b) -> b)
                .orElseThrow();
        callers.put(member + " without access classes", new CallerIdentityExpansion(unclassed).expand(caller(member)));
        callers.put("atlassian-account:outsider licensed", expansion.expand(caller("atlassian-account:outsider",
                "atlassian-access-class:ALL_LICENSED_USERS")));

        Map<String, DocumentSecurity> documents = new LinkedHashMap<>();
        Map<String, Boolean> complete = new LinkedHashMap<>();
        int restricted = 0;
        for (DynamicMessage item : records) {
            if (!item.hasField(entity.findFieldByName("access_chain"))) {
                continue;
            }
            Message chain = (Message) field(item, "access_chain");
            Map<String, DynamicMessage> values = new LinkedHashMap<>();
            values.put("space", spaces.get("space:" + field(chain, "space_id") + "/access"));
            values.put("item", item);
            DocumentSecurity security = mapSecurity(registry, edge, values, "confluence-acl");
            restricted += security.getRequiredRuleSetsCount() > 0 ? 1 : 0;
            documents.put(field(item, "entity_id").toString(), security);
            complete.put(field(item, "entity_id").toString(), values.get("space") != null
                    && isComplete(values.get("space"), "security_completeness")
                    && isComplete(item, "security_completeness"));
        }
        Map<String, Map<String, Boolean>> decisions = writeAndVerifySearchFixture(
                documents, complete, callers, out.resolve("confluence-items.ndjson"), out.resolve("confluence-callers.json"));
        assertThat(restricted).as("items under a view-restricted owner").isPositive();
        assertThat(decisions.get("email:join-proof@example.invalid")).doesNotContainValue(false);
        assertThat(callers.keySet().stream().filter(name -> name.startsWith("atlassian-account:account-"))
                .filter(name -> !name.contains(" ")).map(decisions::get))
                .as("a real user denied a page of the private space")
                .anySatisfy(d -> assertThat(d).containsValue(true).containsValue(false));
        assertThat(decisions.get("atlassian-account:outsider licensed")).containsValue(true).containsValue(false);
        assertThat(decisions.get(member + " without access classes")).doesNotContainValue(true);
    }

    @Test
    void protomoltDecidesWhatConfluenceAnsweredLive() throws Exception {
        List<DynamicMessage> records = new ArrayList<>(readDelimited(entity, "acl/confluence-sync-records.binpb"));
        records.addAll(readDelimited(entity, "acl/confluence-private-sync-records.binpb"));
        List<IdentityLink> withClasses = new ArrayList<>(recordedLinks(records));
        List<IdentityLink> withoutClasses = List.copyOf(withClasses);
        for (DynamicMessage record : records) {
            withClasses.addAll(mapIdentityLinks(registry, classLinks, Map.of("entity", record), "confluence-classes"));
        }
        assertThat(withClasses).extracting(l -> l.getTo().getIdentity()).contains("ALL_LICENSED_USERS");
        Map<String, DynamicMessage> spaces = new LinkedHashMap<>();
        records.stream().filter(r -> r.hasField(entity.findFieldByName("space_access")))
                .forEach(r -> spaces.put(field(r, "entity_id").toString(), r));
        assertThat(spaces).hasSize(2);
        Map<String, DynamicMessage> items = new LinkedHashMap<>();
        records.stream().filter(r -> r.hasField(entity.findFieldByName("access_chain")))
                .forEach(r -> items.put(field(r, "entity_id").toString(), r));

        Descriptor request = registry.findDescriptorByFullName(
                "ai.pipestream.confluence.v1.CheckContentPermissionRequest");
        Descriptor response = registry.findDescriptorByFullName(
                "ai.pipestream.confluence.v1.CheckContentPermissionResponse");
        int compared = 0;
        int classOnly = 0;
        int grantedButDenied = 0;
        try (InputStream in = ConfluenceRecordedSecurityTest.class.getClassLoader()
                .getResourceAsStream("acl/confluence-permission-checks.binpb")) {
            while (true) {
                DynamicMessage.Builder asked = DynamicMessage.newBuilder(request);
                if (!asked.mergeDelimitedFrom(in)) {
                    break;
                }
                DynamicMessage.Builder answered = DynamicMessage.newBuilder(response);
                assertThat(answered.mergeDelimitedFrom(in)).isTrue();
                DynamicMessage check = asked.build();
                boolean confluence = (Boolean) field(answered.build(), "has_permission");
                String content = field(check, "content_id").toString();
                String account = field((Message) field(check, "subject"), "principal_id").toString();
                DynamicMessage item = items.get("page:" + content + "/access");
                assertThat(item).as("the access chain of page %s", content).isNotNull();
                Message chain = (Message) field(item, "access_chain");
                Map<String, DynamicMessage> values = new LinkedHashMap<>();
                values.put("space", spaces.get("space:" + field(chain, "space_id") + "/access"));
                values.put("item", item);
                grantedButDenied += hasOwnerPermission(chain, account) && !confluence ? 1 : 0;
                DocumentSecurity security = mapSecurity(registry, edge, values, "confluence-acl");
                RepositoryCaller caller = new CallerIdentityExpansion(withClasses)
                        .expand(caller("atlassian-account:" + account));
                assertThat(allows(caller, security))
                        .as("%s on page %s, as Confluence answered", account, content).isEqualTo(confluence);
                boolean unclassed = allows(new CallerIdentityExpansion(withoutClasses)
                        .expand(caller("atlassian-account:" + account)), security);
                classOnly += confluence && !unclassed ? 1 : 0;
                compared++;
            }
        }
        assertThat(compared).isGreaterThanOrEqualTo(10);
        assertThat(classOnly).as("answers that need the licensed-users class").isPositive();
        assertThat(grantedButDenied).as("users on a page of a space they cannot view, denied by Confluence")
                .isPositive();
    }

    private static boolean hasOwnerPermission(Message chain, String account) {
        return ((List<?>) field(chain, "owners")).stream().map(Message.class::cast)
                .anyMatch(owner -> ((List<?>) field(owner, "permissions")).stream().map(Message.class::cast)
                        .anyMatch(permission -> field((Message) field(permission, "principal"), "principal_id")
                                .toString().equals(account)));
    }

    private static List<IdentityLink> recordedLinks(List<DynamicMessage> records) throws Exception {
        List<IdentityLink> mapped = new ArrayList<>();
        for (DynamicMessage record : records) {
            IdentityLink link = mapSingleIdentityLink(
                    registry, links, Map.of("entity", record), "confluence-identity");
            if (link.hasFrom() && link.hasTo()) {
                mapped.add(link);
            }
        }
        return mapped;
    }

    private static boolean isRestrictedChild(DynamicMessage record) {
        if (!record.hasField(entity.findFieldByName("access_chain"))) {
            return false;
        }
        Message chain = (Message) field(record, "access_chain");
        List<?> owners = (List<?>) field(chain, "owners");
        if (owners.size() < 2) {
            return false;
        }
        Message parent = (Message) owners.get(1);
        Message state = (Message) field(parent, "restriction_state");
        boolean viewRestricted = field(state, "state").toString()
                .equals("CONTENT_RESTRICTION_STATE_VIEW_RESTRICTED");
        boolean group = ((List<?>) field(parent, "permissions")).stream().map(Message.class::cast)
                .anyMatch(permission -> field((Message) field(permission, "principal"), "principal_type")
                        .toString().equals("CONTENT_PERMISSION_PRINCIPAL_TYPE_GROUP"));
        return viewRestricted && group;
    }
}
