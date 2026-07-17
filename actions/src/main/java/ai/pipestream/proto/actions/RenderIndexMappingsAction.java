package ai.pipestream.proto.actions;

import ai.pipestream.proto.index.lucene.LuceneFieldSpecs;
import ai.pipestream.proto.index.opensearch.OpenSearchMappingGenerator;
import ai.pipestream.proto.index.solr.SolrSchemaGenerator;
import ai.pipestream.proto.index.spi.CatalogIndexingHintSource;
import ai.pipestream.proto.index.spi.IndexingPlan;
import ai.pipestream.proto.index.spi.IndexingPlanFactory;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;

/** Renders the search-index artifact (OpenSearch/Solr/Lucene) for a protobuf message type. */
final class RenderIndexMappingsAction implements ProtoAction {

    @Override
    public String name() {
        return "render-index-mappings";
    }

    @Override
    public String description() {
        return "Renders the search-index artifact for a protobuf message type — OpenSearch index "
                + "mappings JSON, Solr managed-schema pieces, or Lucene field specs — from its "
                + "indexing hints (ai.pipestream.proto.index.hints.v1 options), inferring sensible "
                + "field kinds where no hint is declared.";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = ActionJson.baseInputSchema();
        ObjectNode properties = schema.putObject("properties");
        properties.set("schema", ActionJson.schemaSourceSchema());
        properties.set("type", ActionJson.typeProperty(
                "Fully qualified message type to plan indexing for; required unless the schema "
                        + "already identifies a single message."));
        ObjectNode engine = properties.putObject("engine");
        engine.put("type", "string");
        engine.put("description", "Target search engine for the rendered artifact.");
        ArrayNode engines = engine.putArray("enum");
        engines.add("opensearch");
        engines.add("solr");
        engines.add("lucene");
        ObjectNode sensitivityProp = properties.putObject("sensitivity");
        sensitivityProp.put("type", "object");
        sensitivityProp.put("description", "OpenSearch only: apply schema-declared "
                + "sensitivity classes — {\"encrypt\": [...]} renders those fields as "
                + "store-only ciphertext containers (index: false), {\"mask\": [...]} and "
                + "{\"exclude\": [...]} emit a security-plugin role fragment "
                + "(masked_fields / fls). {\"maskFormat\": {class: suffix}} appends a "
                + "per-class masked_fields format, e.g. '::SHA-512' or "
                + "'::/regex/::replacement'. {\"role\": {\"indexPatterns\": [...], "
                + "\"allowedActions\": [...]}} additionally renders security.role, a "
                + "complete role body ready to PUT at _plugins/_security/api/roles/{name}. "
                + "The response becomes {mappings, security}.");
        ActionJson.required(schema, "schema", "engine");
        schema.put("additionalProperties", false);
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        SchemaResolver.ResolvedSchema schema = SchemaResolver.resolve(input, "schema", context);
        Descriptor descriptor = schema.message(Inputs.optionalString(input, "type"), "/type");
        String engine = Inputs.requireString(input, "engine");
        IndexingPlan plan = IndexingPlanFactory.defaults(new CatalogIndexingHintSource())
                .create(descriptor);
        return switch (engine) {
            case "opensearch" -> {
                ObjectNode mappings = context.objectMapper()
                        .valueToTree(new OpenSearchMappingGenerator().generate(plan));
                ObjectNode sensitivity = Inputs.optionalObject(input, "sensitivity");
                yield sensitivity == null
                        ? mappings
                        : opensearchWithSensitivity(mappings, plan, descriptor,
                                sensitivity, context);
            }
            case "solr" -> solr(plan, context);
            case "lucene" -> lucene(plan, context);
            default -> throw Inputs.invalidInput(
                    "Unknown engine '" + engine + "'; expected one of opensearch, solr, lucene",
                    "/engine");
        };
    }

    /**
     * Applies schema-declared sensitivity to the OpenSearch artifacts. Classes listed
     * under {@code encrypt} become store-only ciphertext containers ({@code index: false}
     * — the engine cannot search what it cannot read, and refuses to try); {@code mask}
     * and {@code exclude} become a security-plugin role fragment ({@code masked_fields}
     * hash values at query time, {@code fls} exclusions hide fields outright).
     */
    private static ObjectNode opensearchWithSensitivity(ObjectNode mappings, IndexingPlan plan,
                                                        com.google.protobuf.Descriptors.Descriptor descriptor,
                                                        ObjectNode sensitivity,
                                                        ActionContext context)
            throws ActionException {
        java.util.List<String> mask = classes(sensitivity, "mask");
        java.util.List<String> exclude = classes(sensitivity, "exclude");
        java.util.List<String> encrypt = classes(sensitivity, "encrypt");
        ObjectNode maskFormat = Inputs.optionalObject(sensitivity, "maskFormat");
        ObjectNode properties = (ObjectNode) mappings.get("properties");
        ArrayNode maskedFields = context.objectMapper().createArrayNode();
        ArrayNode fls = context.objectMapper().createArrayNode();
        for (IndexingPlan.IndexedField field : plan.indexable()) {
            String cls = sensitivityOf(descriptor, field.path());
            if (cls.isEmpty()) {
                continue;
            }
            if (encrypt.contains(cls) && properties != null
                    && properties.has(field.fieldName())) {
                ObjectNode container = context.objectMapper().createObjectNode();
                container.put("type", "keyword");
                container.put("index", false);
                container.put("doc_values", false);
                properties.set(field.fieldName(), container);
            }
            if (mask.contains(cls)) {
                // The security plugin's per-field format rides on the entry itself:
                // "field::SHA-512" picks the hash, "field::/regex/::replacement" rewrites.
                String format = maskFormat != null && maskFormat.hasNonNull(cls)
                        ? maskFormat.get(cls).asText()
                        : "";
                maskedFields.add(field.fieldName() + format);
            }
            if (exclude.contains(cls)) {
                fls.add("~" + field.fieldName());
            }
        }
        ObjectNode output = context.objectMapper().createObjectNode();
        output.set("mappings", mappings);
        ObjectNode security = output.putObject("security");
        security.set("maskedFields", maskedFields);
        security.set("fls", fls);
        ObjectNode roleRequest = Inputs.optionalObject(sensitivity, "role");
        if (roleRequest != null) {
            security.set("role", role(roleRequest, maskedFields, fls, context));
        }
        return output;
    }

    /**
     * A complete security-plugin role body, ready to PUT at
     * {@code _plugins/_security/api/roles/{name}}: the caller supplies the index patterns the
     * role covers (and optionally the allowed actions, default {@code read}); the schema
     * supplies what is masked and what is hidden. Empty {@code masked_fields}/{@code fls} are
     * omitted, since an empty list and an absent one mean different things to the plugin.
     */
    private static ObjectNode role(ObjectNode request, ArrayNode maskedFields, ArrayNode fls,
                                   ActionContext context) throws ActionException {
        ArrayNode patterns = Inputs.optionalArray(request, "indexPatterns");
        if (patterns == null || patterns.isEmpty()) {
            throw Inputs.invalidInput(
                    "sensitivity.role needs indexPatterns: the index names the role covers",
                    "/sensitivity/role/indexPatterns");
        }
        Inputs.stringElements(patterns, "/sensitivity/role/indexPatterns");
        ObjectNode role = context.objectMapper().createObjectNode();
        ObjectNode permission = role.putArray("index_permissions").addObject();
        permission.set("index_patterns", patterns.deepCopy());
        ArrayNode actions = Inputs.optionalArray(request, "allowedActions");
        if (actions != null && !actions.isEmpty()) {
            Inputs.stringElements(actions, "/sensitivity/role/allowedActions");
            permission.set("allowed_actions", actions.deepCopy());
        } else {
            permission.putArray("allowed_actions").add("read");
        }
        if (!maskedFields.isEmpty()) {
            permission.set("masked_fields", maskedFields.deepCopy());
        }
        if (!fls.isEmpty()) {
            permission.set("fls", fls.deepCopy());
        }
        return role;
    }

    private static java.util.List<String> classes(ObjectNode sensitivity, String key)
            throws ActionException {
        ArrayNode node = Inputs.optionalArray(sensitivity, key);
        return node == null ? java.util.List.of()
                : Inputs.stringElements(node, "/sensitivity/" + key);
    }

    private static String sensitivityOf(com.google.protobuf.Descriptors.Descriptor descriptor,
                                        String path) {
        com.google.protobuf.Descriptors.Descriptor current = descriptor;
        String[] segments = path.split("\\.");
        for (int i = 0; i < segments.length; i++) {
            com.google.protobuf.Descriptors.FieldDescriptor field =
                    current.findFieldByName(segments[i]);
            if (field == null) {
                return "";
            }
            if (i == segments.length - 1) {
                return ai.pipestream.proto.meta.DescriptorMetadata.field(field)
                        .map(meta -> meta.getSensitivity())
                        .orElse("");
            }
            if (field.getJavaType()
                    != com.google.protobuf.Descriptors.FieldDescriptor.JavaType.MESSAGE) {
                return "";
            }
            current = field.getMessageType();
        }
        return "";
    }

    private static ObjectNode solr(IndexingPlan plan, ActionContext context) {
        SolrSchemaGenerator.SolrSchema solrSchema = new SolrSchemaGenerator().generate(plan);
        ObjectNode output = context.objectMapper().createObjectNode();
        output.set("fieldTypes", context.objectMapper().valueToTree(solrSchema.fieldTypes()));
        output.set("fields", context.objectMapper().valueToTree(solrSchema.fields()));
        output.set("copyFields", context.objectMapper().valueToTree(solrSchema.copyFields()));
        return output;
    }

    private static ObjectNode lucene(IndexingPlan plan, ActionContext context) {
        LuceneFieldSpecs specs = LuceneFieldSpecs.from(plan);
        ObjectNode output = context.objectMapper().createObjectNode();
        output.put("messageFullName", specs.messageFullName());
        ArrayNode fields = output.putArray("fields");
        for (LuceneFieldSpecs.FieldSpec spec : specs.fields()) {
            ObjectNode field = fields.addObject();
            field.put("name", spec.name());
            field.put("kind", spec.kind().name());
            field.put("stored", spec.stored());
            field.put("indexed", spec.indexed());
            field.put("sortable", spec.sortable());
            field.put("facetable", spec.facetable());
            field.put("analyzer", spec.analyzer());
            field.put("searchAnalyzer", spec.searchAnalyzer());
            field.put("vectorDims", spec.vectorDims());
            field.put("vectorSimilarity", spec.vectorSimilarity() == null
                    ? null : spec.vectorSimilarity().name());
            field.put("vectorElementType", spec.vectorElementType() == null
                    ? null : spec.vectorElementType().name());
            field.put("dateFormat", spec.dateFormat());
            ObjectNode engineParams = field.putObject("engineParams");
            spec.engineParams().forEach(engineParams::put);
        }
        return output;
    }
}
