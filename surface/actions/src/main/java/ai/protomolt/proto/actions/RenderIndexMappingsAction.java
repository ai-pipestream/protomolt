package ai.protomolt.proto.actions;

import ai.protomolt.proto.search.index.lucene.LuceneFieldSpecs;
import ai.protomolt.proto.search.index.opensearch.OpenSearchMappingGenerator;
import ai.protomolt.proto.search.index.qdrant.QdrantSchemaGenerator;
import ai.protomolt.proto.search.index.qdrant.QdrantVectorSpec;
import ai.protomolt.proto.search.index.solr.SolrSchemaGenerator;
import ai.protomolt.proto.search.index.spi.CatalogIndexingHintSource;
import ai.protomolt.proto.search.index.spi.IndexMapping;
import ai.protomolt.proto.search.index.spi.IndexMappingFactory;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;

/** Renders the search-index artifact (OpenSearch/Solr/Lucene/Qdrant) for a protobuf message type. */
final class RenderIndexMappingsAction implements ProtoAction {

    @Override
    public String name() {
        return "render-index-mappings";
    }

    @Override
    public String requiredScope() {
        return Scopes.SCHEMA_READ;
    }

    @Override
    public String description() {
        return "Renders the search-index artifact for a protobuf message type — OpenSearch index "
                + "mappings JSON, Solr managed-schema pieces, Lucene field specs, or a Qdrant "
                + "collection schema (named vectors with size+distance, payload field indexes) — "
                + "from its indexing hints (ai.pipestream.proto.index.hints.v1 options), "
                + "inferring sensible field kinds where no hint is declared.";
    }

    @Override
    public Descriptor requestType() {
        return CatalogContract.request("RenderIndexMappingsRequest");
    }

    @Override
    public Descriptor responseType() {
        return CatalogContract.response("RenderIndexMappingsResponse");
    }

    @Override
    public Message execute(Message input, ActionContext context) throws ActionException {
        SchemaResolver.ResolvedSchema schema = SchemaResolver.resolve(input, "schema", context);
        Descriptor descriptor = schema.message(
                SynthesizeShapeAction.named(input, "type"), "/type");
        String engine = Fields.enumName(input, "engine");
        IndexMapping mapping = IndexMappingFactory.defaults(new CatalogIndexingHintSource())
                .create(descriptor);
        // The artifact is nested under a named field, alongside the engine it was rendered
        // for, so the response has a declared protobuf contract. Each engine defines its own
        // artifact shape, so the artifact itself stays a structure.
        return Reply.of(responseType())
                .set("engine", engine)
                .set("mappings", renderFor(engine, mapping, descriptor, input, context))
                .build();
    }

    private static ObjectNode renderFor(String engine, IndexMapping mapping, Descriptor descriptor,
                                        Message input, ActionContext context)
            throws ActionException {
        // The contract names the engine with an enum and refuses the unset value, so
        // every case here is one this verb renders.
        return switch (engine) {
            case "INDEX_ENGINE_OPENSEARCH" -> {
                ObjectNode mappings = context.objectMapper()
                        .valueToTree(new OpenSearchMappingGenerator().generate(mapping));
                yield Fields.has(input, "sensitivity")
                        ? opensearchWithSensitivity(mappings, mapping, descriptor,
                                Fields.json(input, "sensitivity"), context)
                        : mappings;
            }
            case "INDEX_ENGINE_SOLR" -> solr(mapping, context);
            case "INDEX_ENGINE_LUCENE" -> lucene(mapping, context);
            case "INDEX_ENGINE_QDRANT" -> qdrant(mapping, context);
            default -> throw Inputs.invalidInput(
                    "Unknown engine '" + engine + "'; expected one of opensearch, solr, lucene, qdrant",
                    "/engine");
        };
    }

    private static ObjectNode qdrant(IndexMapping mapping, ActionContext context)
            throws ActionException {
        QdrantSchemaGenerator.QdrantSchema schema;
        try {
            schema = new QdrantSchemaGenerator().generate(mapping);
        } catch (IllegalArgumentException e) {
            // The mapping is derived entirely from the caller's schema, so a mapping the
            // generator rejects (e.g. a VECTOR hint with no vector_dims) is caller
            // input, not a server fault.
            throw Inputs.invalidInput(e.getMessage(), "/schema");
        }
        ObjectNode output = context.objectMapper().createObjectNode();
        ArrayNode vectors = output.putArray("vectors");
        for (QdrantVectorSpec spec : schema.vectors()) {
            ObjectNode vector = vectors.addObject();
            vector.put("name", spec.name());
            vector.put("size", spec.size());
            vector.put("distance", spec.distance().name());
        }
        ArrayNode payloadIndexes = output.putArray("payloadIndexes");
        for (QdrantSchemaGenerator.PayloadIndex index : schema.payloadIndexes()) {
            ObjectNode payloadIndex = payloadIndexes.addObject();
            payloadIndex.put("name", index.fieldName());
            payloadIndex.put("type", index.fieldType().name());
        }
        return output;
    }

    /**
     * Applies schema-declared sensitivity to the OpenSearch artifacts. Classes listed
     * under {@code encrypt} become store-only ciphertext containers ({@code index: false}
     * — the engine cannot search what it cannot read, and refuses to try); {@code mask}
     * and {@code exclude} become a security-plugin role fragment ({@code masked_fields}
     * hash values at query time, {@code fls} exclusions hide fields outright).
     */
    private static ObjectNode opensearchWithSensitivity(ObjectNode mappings, IndexMapping mapping,
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
        for (IndexMapping.IndexedField field : mapping.indexable()) {
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
                return ai.protomolt.proto.meta.DescriptorMetadata.field(field)
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

    private static ObjectNode solr(IndexMapping mapping, ActionContext context) {
        SolrSchemaGenerator.SolrSchema solrSchema = new SolrSchemaGenerator().generate(mapping);
        ObjectNode output = context.objectMapper().createObjectNode();
        output.set("fieldTypes", context.objectMapper().valueToTree(solrSchema.fieldTypes()));
        output.set("fields", context.objectMapper().valueToTree(solrSchema.fields()));
        output.set("copyFields", context.objectMapper().valueToTree(solrSchema.copyFields()));
        return output;
    }

    private static ObjectNode lucene(IndexMapping mapping, ActionContext context) {
        LuceneFieldSpecs specs = LuceneFieldSpecs.from(mapping);
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
