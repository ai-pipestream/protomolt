<template>
  <v-card data-testid="concept-scheme-browser">
    <v-card-title class="text-subtitle-2 d-flex align-center flex-wrap ga-2">
      <v-icon size="small">mdi-sitemap-outline</v-icon>
      <span>SKOS concept lookup &amp; scheme browser</span>
      <v-spacer />
      <v-btn
        v-if="canOpenInAccount"
        size="x-small"
        variant="tonal"
        prepend-icon="mdi-open-in-new"
        data-testid="concept-open-in-account"
        @click="openInAccount"
      >
        Open in account documents
      </v-btn>
    </v-card-title>

    <v-card-text>
      <div class="text-body-2 text-medium-emphasis mb-3">
        Query documents tagged with a SKOS <code>ConceptRef</code> through <code>ListDocuments</code>, inspect the
        mounted scheme versions and expansion evidence, or browse a <code>ConceptScheme</code> hierarchy.
      </div>

      <v-form @submit.prevent="runConceptLookup(true)">
        <v-row density="compact">
          <v-col cols="12" sm="4">
            <v-text-field
              v-model="draft.accountId"
              label="Account ID"
              density="compact"
              hide-details
              autocorrect="off"
              autocapitalize="off"
              spellcheck="false"
              data-testid="concept-lookup-account"
            />
          </v-col>
          <v-col cols="12" sm="4">
            <v-text-field
              v-model="draft.schemeId"
              label="Concept scheme ID"
              placeholder="topics"
              density="compact"
              hide-details
              autocorrect="off"
              autocapitalize="off"
              spellcheck="false"
              data-testid="concept-lookup-scheme"
            />
          </v-col>
          <v-col cols="12" sm="4">
            <v-text-field
              v-model="draft.conceptId"
              label="Concept ID"
              placeholder="wind"
              density="compact"
              hide-details
              autocorrect="off"
              autocapitalize="off"
              spellcheck="false"
              data-testid="concept-lookup-concept"
            />
          </v-col>
          <v-col cols="6" sm="3">
            <v-checkbox
              v-model="draft.narrower"
              label="Include narrower concepts"
              density="compact"
              hide-details
              data-testid="concept-lookup-narrower"
            />
          </v-col>
          <v-col cols="6" sm="3">
            <v-checkbox
              v-model="draft.closeMatch"
              label="Include close matches"
              density="compact"
              hide-details
              data-testid="concept-lookup-close-match"
            />
          </v-col>
          <v-col cols="6" sm="3">
            <v-text-field
              v-model="draft.graphId"
              label="Graph ID (optional)"
              density="compact"
              hide-details
              autocorrect="off"
              autocapitalize="off"
              spellcheck="false"
              data-testid="concept-lookup-graph"
            />
          </v-col>
          <v-col cols="6" sm="3">
            <v-text-field
              v-model="draft.graphAddressId"
              label="Graph address ID (optional)"
              density="compact"
              hide-details
              autocorrect="off"
              autocapitalize="off"
              spellcheck="false"
              data-testid="concept-lookup-node"
            />
          </v-col>
        </v-row>

        <div class="d-flex align-center flex-wrap ga-2 mt-2">
          <v-btn
            type="submit"
            color="primary"
            size="small"
            :loading="lookupLoading"
            data-testid="concept-lookup-submit"
          >
            Find documents by concept
          </v-btn>
          <v-switch
            v-model="draft.includeTombstoned"
            label="Include tombstoned"
            density="compact"
            hide-details
            color="primary"
            class="ml-2"
            data-testid="concept-lookup-tombstoned"
          />
        </div>
      </v-form>

      <v-alert
        v-if="lookupInputError"
        type="warning"
        variant="tonal"
        density="compact"
        class="mt-3"
        data-testid="concept-lookup-input-error"
      >
        {{ lookupInputError }}
      </v-alert>

      <RpcFailureAlert v-if="lookupFailure" :failure="lookupFailure" title="Listing documents by concept" class="mt-3" />

      <div v-if="mountedSchemes.length > 0" class="d-flex align-center flex-wrap ga-1 mt-3" data-testid="mounted-schemes-bar">
        <span class="text-caption text-medium-emphasis mr-1">Mounted schemes observed:</span>
        <v-chip
          v-for="m in mountedSchemes"
          :key="m.schemeId"
          size="x-small"
          variant="tonal"
          class="text-mono"
          :data-testid="`mounted-scheme-chip-${m.schemeId}`"
          @click="useSchemeId(m.schemeId)"
        >
          {{ m.schemeId }}@{{ m.version }}
        </v-chip>
      </div>

      <ConceptEvidence v-if="evidence" :evidence="evidence" class="mt-3" />

      <v-table v-if="rows.length > 0" density="compact" hover class="mt-2" data-testid="concept-lookup-results">
        <thead>
          <tr>
            <th>Document</th>
            <th>Title</th>
            <th>Type</th>
            <th>Revision</th>
            <th>Committed</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="row in rows"
            :key="row.key"
            class="cursor-pointer"
            :data-testid="`concept-doc-row-${row.docId}`"
            @click="openDocumentRow(row)"
          >
            <td>
              <div class="text-mono">{{ row.docId }}</div>
              <div class="text-caption text-medium-emphasis text-mono">{{ row.graphId }} / {{ row.graphAddressId }}</div>
            </td>
            <td>
              <v-chip v-if="row.tombstone" size="x-small" color="warning" variant="tonal" class="mr-1">tombstone</v-chip>
              <span v-if="row.title !== null">{{ row.title }}</span>
              <span v-else class="text-medium-emphasis">(no title)</span>
            </td>
            <td>{{ row.documentType ?? '' }}</td>
            <td class="text-mono">{{ row.revision }}</td>
            <td class="text-mono text-caption">{{ row.committedAt ?? '' }}</td>
          </tr>
        </tbody>
      </v-table>
      <div
        v-else-if="lookupExecuted && !lookupLoading && !lookupFailure"
        class="text-body-2 text-medium-emphasis mt-3"
        data-testid="concept-no-documents"
      >
        No documents match this concept filter.
      </div>

      <div v-if="cursor.hasPrevious || cursor.hasNext" class="d-flex align-center ga-2 mt-2">
        <v-btn
          size="x-small"
          variant="text"
          :disabled="!cursor.hasPrevious || lookupLoading"
          data-testid="concept-prev-page"
          @click="previousPage"
        >
          Previous
        </v-btn>
        <span class="text-caption text-medium-emphasis">Page {{ cursor.page }}</span>
        <v-btn
          size="x-small"
          variant="text"
          :disabled="!cursor.hasNext || lookupLoading"
          data-testid="concept-next-page"
          @click="nextPage"
        >
          Next
        </v-btn>
      </div>

      <v-divider class="my-4" />

      <div class="d-flex align-center flex-wrap ga-2 mb-2">
        <span class="text-subtitle-2">ConceptScheme hierarchy inspector</span>
        <v-spacer />
        <v-btn
          size="x-small"
          variant="text"
          :prepend-icon="showLoader ? 'mdi-chevron-up' : 'mdi-file-upload-outline'"
          data-testid="toggle-scheme-loader"
          @click="showLoader = !showLoader"
        >
          {{ showLoader ? 'Hide loader' : 'Load ConceptScheme protobuf' }}
        </v-btn>
      </div>

      <v-expand-transition>
        <div v-if="showLoader" class="mb-3 pa-3 rounded border" data-testid="scheme-loader-panel">
          <div class="text-caption text-medium-emphasis mb-2">
            Decode an operator <code>ai.protomolt.proto.types.v1.ConceptScheme</code> protobuf binary or base64 payload
            to inspect its hierarchy, labels, notations, and cross-scheme mappings locally.
          </div>
          <v-row density="compact">
            <v-col cols="12" sm="4">
              <v-text-field
                v-model="loaderSchemeId"
                label="Scheme ID"
                placeholder="topics"
                density="compact"
                hide-details
                autocorrect="off"
                autocapitalize="off"
                spellcheck="false"
                data-testid="loader-scheme-id"
              />
            </v-col>
            <v-col cols="12" sm="4">
              <v-text-field
                v-model="loaderVersion"
                label="Version (optional)"
                placeholder="v1"
                density="compact"
                hide-details
                autocorrect="off"
                autocapitalize="off"
                spellcheck="false"
                data-testid="loader-scheme-version"
              />
            </v-col>
            <v-col cols="12" sm="4" class="d-flex align-center">
              <label class="text-caption">
                <span class="d-block text-medium-emphasis">Binary file (.binpb)</span>
                <input
                  type="file"
                  accept=".binpb,.pb,application/octet-stream"
                  data-testid="loader-file-input"
                  @change="onSchemeFileChosen"
                />
              </label>
            </v-col>
            <v-col cols="12" class="mt-1">
              <v-textarea
                v-model="loaderBase64"
                label="ConceptScheme protobuf (base64)"
                rows="2"
                density="compact"
                hide-details
                autocorrect="off"
                autocapitalize="off"
                spellcheck="false"
                data-testid="loader-base64-input"
              />
            </v-col>
          </v-row>
          <div class="d-flex align-center ga-2 mt-2">
            <v-btn size="small" variant="tonal" color="primary" data-testid="loader-decode-base64" @click="loadFromBase64">
              Decode base64 scheme
            </v-btn>
          </div>
          <v-alert
            v-if="loaderError"
            type="warning"
            variant="tonal"
            density="compact"
            class="mt-2"
            data-testid="loader-error"
          >
            {{ loaderError }}
          </v-alert>
        </div>
      </v-expand-transition>

      <div v-if="schemes.length > 0">
        <div class="d-flex align-center flex-wrap ga-1 mb-3" data-testid="loaded-schemes-list">
          <v-chip
            v-for="s in schemes"
            :key="s.schemeId"
            size="small"
            :color="activeScheme?.schemeId === s.schemeId ? 'primary' : undefined"
            :variant="activeScheme?.schemeId === s.schemeId ? 'flat' : 'tonal'"
            :data-testid="`scheme-chip-${s.schemeId}`"
            @click="selectScheme(s.schemeId)"
          >
            <span>{{ s.primaryLabel }}</span>
            <span class="text-mono ml-1">({{ s.schemeId }}{{ s.version ? `@${s.version}` : '' }}, {{ s.concepts.length }})</span>
          </v-chip>
        </div>

        <v-row v-if="activeScheme" density="compact">
          <v-col cols="12" md="5">
            <v-text-field
              v-model="conceptSearch"
              label="Filter concepts (label, hidden label, notation, ID)"
              density="compact"
              hide-details
              clearable
              prepend-inner-icon="mdi-magnify"
              autocorrect="off"
              autocapitalize="off"
              spellcheck="false"
              class="mb-2"
              data-testid="scheme-concept-filter"
            />

            <div
              v-if="conceptSearch.trim() !== ''"
              class="rounded border pa-2"
              style="max-height: 360px; overflow-y: auto"
              data-testid="concept-filtered-list"
            >
              <div v-if="filteredConcepts.length === 0" class="text-caption text-medium-emphasis" data-testid="concept-filter-empty">
                No concepts in <code>{{ activeScheme.schemeId }}</code> match "{{ conceptSearch.trim() }}".
              </div>
              <div
                v-for="item in filteredConcepts"
                :key="item.id"
                class="d-flex align-center justify-space-between py-1 px-2 rounded cursor-pointer"
                :class="{ 'bg-surface-light': selectedConcept?.id === item.id }"
                :data-testid="`filtered-concept-${item.id}`"
                @click="selectConcept(item.id)"
              >
                <div>
                  <span class="text-body-2">{{ item.primaryLabel }}</span>
                  <span class="text-caption text-mono text-medium-emphasis ml-1">({{ item.id }})</span>
                  <v-chip v-if="item.deprecated" size="x-small" color="warning" variant="tonal" class="ml-1">deprecated</v-chip>
                </div>
              </div>
            </div>

            <div
              v-else
              role="tree"
              :aria-label="`Concept hierarchy for ${activeScheme.schemeId}`"
              class="rounded border pa-2"
              style="max-height: 360px; overflow-y: auto"
              data-testid="concept-hierarchy-tree"
            >
              <div
                v-for="row in visibleTreeRows"
                :key="row.key"
                role="treeitem"
                :aria-level="row.depth + 1"
                :aria-expanded="row.hasChildren ? row.expanded : undefined"
                :aria-selected="selectedConcept?.id === row.concept.id"
                class="d-flex align-center py-1 rounded"
                :class="{ 'bg-surface-light': selectedConcept?.id === row.concept.id }"
                :style="{ paddingLeft: `${row.depth * 16 + 4}px` }"
                :data-testid="`tree-row-${row.concept.id}`"
              >
                <button
                  v-if="row.hasChildren"
                  type="button"
                  class="btn-plain mr-1 text-caption text-mono"
                  :aria-label="`${row.expanded ? 'Collapse' : 'Expand'} ${row.concept.primaryLabel}`"
                  :data-testid="`tree-toggle-${row.concept.id}`"
                  @click.stop="toggleTreeNode(row.concept.id)"
                >
                  {{ row.expanded ? '▾' : '▸' }}
                </button>
                <span v-else class="mr-3" />
                <button
                  type="button"
                  class="btn-plain text-left flex-grow-1"
                  :data-testid="`tree-select-${row.concept.id}`"
                  @click="selectConcept(row.concept.id)"
                >
                  <span class="text-body-2">{{ row.concept.primaryLabel }}</span>
                  <span class="text-caption text-mono text-medium-emphasis ml-1">({{ row.concept.id }})</span>
                  <v-chip v-if="row.concept.deprecated" size="x-small" color="warning" variant="tonal" class="ml-1">
                    deprecated
                  </v-chip>
                  <v-chip v-if="row.cycleDetected" size="x-small" color="error" variant="tonal" class="ml-1">
                    cycle guard
                  </v-chip>
                </button>
              </div>
            </div>
          </v-col>

          <v-col cols="12" md="7">
            <div v-if="selectedConcept" class="rounded border pa-3" data-testid="concept-detail-panel">
              <div class="d-flex align-center flex-wrap ga-2 mb-2">
                <span class="text-subtitle-2">{{ selectedConcept.primaryLabel }}</span>
                <v-chip v-if="selectedConcept.primaryLang" size="x-small" variant="outlined">
                  {{ selectedConcept.primaryLang }}
                </v-chip>
                <code class="text-caption">{{ selectedConcept.refKey }}</code>
                <v-chip v-if="selectedConcept.deprecated" size="x-small" color="warning" variant="tonal" data-testid="concept-deprecated-chip">
                  deprecated
                </v-chip>
                <v-spacer />
                <v-btn
                  size="x-small"
                  color="primary"
                  variant="tonal"
                  :data-testid="`concept-query-btn-${selectedConcept.id}`"
                  @click="querySelectedConcept(selectedConcept)"
                >
                  Query documents
                </v-btn>
              </div>

              <div v-if="selectedConcept.iri !== null" class="text-caption mb-2" data-testid="concept-iri">
                <span class="text-medium-emphasis">IRI: </span>
                <a
                  v-if="selectedConcept.safeIriHref !== null"
                  :href="selectedConcept.safeIriHref"
                  target="_blank"
                  rel="noopener noreferrer"
                  class="text-mono"
                  data-testid="concept-iri-link"
                >{{ selectedConcept.iri }}</a>
                <code v-else data-testid="concept-iri-text">{{ selectedConcept.iri }}</code>
              </div>

              <div v-if="selectedConcept.replacedBy.length > 0" class="text-caption mb-2" data-testid="concept-replaced-by">
                <span class="text-medium-emphasis">Replaced by: </span>
                <span v-for="(rep, i) in selectedConcept.replacedBy" :key="rep.conceptId">
                  <button
                    v-if="rep.known"
                    type="button"
                    class="btn-link text-mono"
                    :data-testid="`relation-replaced-by-${rep.conceptId}`"
                    @click="selectConcept(rep.conceptId)"
                  >
                    {{ rep.primaryLabel }} ({{ rep.conceptId }})
                  </button>
                  <code v-else>{{ rep.conceptId }} (unresolved)</code>
                  <span v-if="i < selectedConcept.replacedBy.length - 1">, </span>
                </span>
              </div>

              <div v-if="selectedConcept.replaces.length > 0" class="text-caption mb-2" data-testid="concept-replaces">
                <span class="text-medium-emphasis">Replaces deprecated: </span>
                <span v-for="(pred, i) in selectedConcept.replaces" :key="pred.conceptId">
                  <button
                    type="button"
                    class="btn-link text-mono"
                    :data-testid="`relation-replaces-${pred.conceptId}`"
                    @click="selectConcept(pred.conceptId)"
                  >
                    {{ pred.primaryLabel }} ({{ pred.conceptId }})
                  </button>
                  <span v-if="i < selectedConcept.replaces.length - 1">, </span>
                </span>
              </div>

              <div v-if="selectedConcept.prefLabels.length > 1" class="text-caption mb-2" data-testid="concept-pref-labels">
                <span class="text-medium-emphasis">Preferred labels: </span>
                <span v-for="(lbl, i) in selectedConcept.prefLabels" :key="i">
                  {{ lbl.text }}<span v-if="lbl.lang"> [{{ lbl.lang }}]</span><span v-if="i < selectedConcept.prefLabels.length - 1">; </span>
                </span>
              </div>

              <div v-if="selectedConcept.altLabels.length > 0" class="text-caption mb-2" data-testid="concept-alt-labels">
                <span class="text-medium-emphasis">Alternate labels: </span>
                <span v-for="(lbl, i) in selectedConcept.altLabels" :key="i">
                  {{ lbl.text }}<span v-if="lbl.lang"> [{{ lbl.lang }}]</span><span v-if="i < selectedConcept.altLabels.length - 1">; </span>
                </span>
              </div>

              <div v-if="selectedConcept.notations.length > 0" class="text-caption mb-2" data-testid="concept-notations">
                <span class="text-medium-emphasis">Notations: </span>
                <span v-for="(n, i) in selectedConcept.notations" :key="i">
                  <code>{{ n.value }}</code>
                  <template v-if="n.datatypeIri !== null">
                    (<a
                      v-if="n.safeDatatypeHref !== null"
                      :href="n.safeDatatypeHref"
                      target="_blank"
                      rel="noopener noreferrer"
                      class="text-mono"
                    >{{ n.datatypeIri }}</a><code v-else>{{ n.datatypeIri }}</code>)
                  </template>
                  <span v-if="i < selectedConcept.notations.length - 1">, </span>
                </span>
              </div>

              <div v-if="selectedConcept.definitions.length > 0" class="text-caption mb-2" data-testid="concept-definitions">
                <div class="text-medium-emphasis">Definition:</div>
                <div v-for="(def, i) in selectedConcept.definitions" :key="i">
                  {{ def.text }}<span v-if="def.lang" class="text-medium-emphasis"> [{{ def.lang }}]</span>
                </div>
              </div>

              <div v-if="selectedConcept.scopeNotes.length > 0" class="text-caption mb-2" data-testid="concept-scope-notes">
                <div class="text-medium-emphasis">Scope note:</div>
                <div v-for="(note, i) in selectedConcept.scopeNotes" :key="i">
                  {{ note.text }}<span v-if="note.lang" class="text-medium-emphasis"> [{{ note.lang }}]</span>
                </div>
              </div>

              <div v-if="selectedConcept.broader.length > 0" class="text-caption mb-1" data-testid="concept-broader">
                <span class="text-medium-emphasis">Broader: </span>
                <span v-for="(b, i) in selectedConcept.broader" :key="b.conceptId">
                  <button
                    v-if="b.known"
                    type="button"
                    class="btn-link text-mono"
                    :data-testid="`relation-broader-${b.conceptId}`"
                    @click="selectConcept(b.conceptId)"
                  >
                    {{ b.primaryLabel }} ({{ b.conceptId }})
                  </button>
                  <code v-else>{{ b.conceptId }} (unresolved)</code>
                  <span v-if="i < selectedConcept.broader.length - 1">, </span>
                </span>
              </div>

              <div v-if="selectedConcept.narrower.length > 0" class="text-caption mb-1" data-testid="concept-narrower">
                <span class="text-medium-emphasis">Narrower: </span>
                <span v-for="(n, i) in selectedConcept.narrower" :key="n.conceptId">
                  <button
                    type="button"
                    class="btn-link text-mono"
                    :data-testid="`relation-narrower-${n.conceptId}`"
                    @click="selectConcept(n.conceptId)"
                  >
                    {{ n.primaryLabel }} ({{ n.conceptId }})
                  </button>
                  <span v-if="i < selectedConcept.narrower.length - 1">, </span>
                </span>
              </div>

              <div v-if="selectedConcept.related.length > 0" class="text-caption mb-2" data-testid="concept-related">
                <span class="text-medium-emphasis">Related: </span>
                <span v-for="(r, i) in selectedConcept.related" :key="r.conceptId">
                  <button
                    v-if="r.known"
                    type="button"
                    class="btn-link text-mono"
                    :data-testid="`relation-related-${r.conceptId}`"
                    @click="selectConcept(r.conceptId)"
                  >
                    {{ r.primaryLabel }} ({{ r.conceptId }})
                  </button>
                  <code v-else>{{ r.conceptId }} (unresolved)</code>
                  <span v-if="i < selectedConcept.related.length - 1">, </span>
                </span>
              </div>

              <div v-if="selectedConcept.mappings.length > 0" class="text-caption mb-2" data-testid="concept-mappings">
                <div class="text-medium-emphasis">Cross-scheme mappings:</div>
                <div v-for="group in selectedConcept.mappings" :key="group.kind">
                  <span>{{ group.label }}: </span>
                  <span v-for="(target, i) in group.targets" :key="target.key">
                    <button
                      type="button"
                      class="btn-link text-mono"
                      :data-testid="`mapping-target-${target.key}`"
                      @click="selectCrossSchemeTarget(target.schemeId, target.conceptId)"
                    >
                      {{ target.key }}
                    </button>
                    <span v-if="i < group.targets.length - 1">, </span>
                  </span>
                </div>
              </div>

              <div v-if="expansionPreview" class="text-caption mt-3 pt-2 border-t" data-testid="in-scheme-expansion-preview">
                <div class="text-medium-emphasis">
                  In-scheme expansion preview (narrower: {{ draft.narrower ? 'on' : 'off' }}, close_match:
                  {{ draft.closeMatch ? 'on' : 'off' }}):
                </div>
                <div v-if="expansionPreview.replacedRedirected" data-testid="expansion-redirected">
                  Redirects deprecated <code>{{ expansionPreview.requestedId }}</code> to
                  <code>{{ expansionPreview.canonicalIds.join(', ') }}</code>
                </div>
                <div>
                  In-scheme concepts ({{ expansionPreview.inSchemeConcepts.length }}):
                  <code v-for="(c, i) in expansionPreview.inSchemeConcepts" :key="c.id">
                    {{ c.id }}<span v-if="i < expansionPreview.inSchemeConcepts.length - 1">, </span>
                  </code>
                </div>
                <div v-if="expansionPreview.crossSchemeTargets.length > 0" data-testid="expansion-cross-scheme">
                  Cross-scheme expansion targets:
                  <code v-for="(t, i) in expansionPreview.crossSchemeTargets" :key="t.key">
                    {{ t.key }} ({{ t.kind }})<span v-if="i < expansionPreview.crossSchemeTargets.length - 1">, </span>
                  </code>
                </div>
                <div v-if="expansionPreview.unresolvedInScheme.length > 0" data-testid="expansion-unresolved">
                  Unresolved in-scheme references:
                  <code v-for="(u, i) in expansionPreview.unresolvedInScheme" :key="i">
                    {{ u.fromId }} {{ u.relation }} {{ u.targetId }}<span v-if="i < expansionPreview.unresolvedInScheme.length - 1">, </span>
                  </code>
                </div>
              </div>
            </div>
          </v-col>
        </v-row>
      </div>
      <div v-else class="text-caption text-medium-emphasis" data-testid="no-loaded-schemes">
        No <code>ConceptScheme</code> protobuf is loaded yet. Use <strong>Load ConceptScheme protobuf</strong> to
        inspect a scheme's hierarchy and labels, or run <strong>Find documents by concept</strong> directly against
        the repository's mounted schemes.
      </div>
    </v-card-text>
  </v-card>
</template>

<script setup lang="ts">
import { computed, reactive, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import ConceptEvidence from '@/components/repository/ConceptEvidence.vue'
import RpcFailureAlert from '@/components/repository/RpcFailureAlert.vue'
import { useRepositoryClients } from '@/repository/clients'
import {
  buildConceptLookupRequest,
  conceptAccountRouteQuery,
  decodeConceptScheme,
  decodeConceptSchemeBase64,
  expandConceptInScheme,
  filterConcepts,
  mergeMountedSchemes,
  newConceptLookupDraft,
  type ConceptNodeView,
  type ConceptSchemeView,
  type ConceptTreeNode,
} from '@/repository/conceptBrowserCore'
import { evidenceView, type EvidenceView } from '@/repository/linksCore'
import {
  ListingInputError,
  PageCursor,
  documentRow,
  type DocumentRow,
} from '@/repository/listingCore'
import { isOwnCancellation, rpcFailure, type RpcFailure } from '@/repository/rpcErrorCore'
import { useCallScope } from '@/repository/useCallScope'

const props = withDefaults(
  defineProps<{
    accountId?: string
    initialSchemes?: ConceptSchemeView[]
  }>(),
  {
    accountId: '',
    initialSchemes: () => [],
  },
)

const router = useRouter()
const clients = useRepositoryClients()
const scope = useCallScope()

const draft = reactive(newConceptLookupDraft(props.accountId))
watch(
  () => props.accountId,
  (next) => {
    if (next.trim() !== '' && draft.accountId !== next) {
      draft.accountId = next
    }
  },
)

const cursor = reactive(new PageCursor())
const rows = ref<DocumentRow[]>([])
const evidence = ref<EvidenceView | null>(null)
const mountedSchemes = ref<{ schemeId: string; version: string }[]>([])
const lookupLoading = ref(false)
const lookupExecuted = ref(false)
const lookupInputError = ref<string | null>(null)
const lookupFailure = ref<RpcFailure | null>(null)

const canOpenInAccount = computed(
  () => draft.accountId.trim() !== '' && draft.schemeId.trim() !== '' && draft.conceptId.trim() !== '',
)

async function runConceptLookup(resetPage: boolean) {
  lookupInputError.value = null
  lookupFailure.value = null
  if (resetPage) cursor.reset()

  let request
  try {
    request = buildConceptLookupRequest(draft, cursor.token)
  } catch (error) {
    if (error instanceof ListingInputError) {
      lookupInputError.value = error.message
      return
    }
    throw error
  }

  const signal = scope.next()
  lookupLoading.value = true
  if (resetPage) {
    rows.value = []
    evidence.value = null
  }
  try {
    const response = await clients.documents.listDocuments(request, { signal })
    if (signal.aborted) return
    rows.value = response.documents.map(documentRow)
    const ev = evidenceView(response.conceptEvidence)
    evidence.value = ev
    mountedSchemes.value = mergeMountedSchemes(mountedSchemes.value, ev)
    cursor.answered(response.nextPageToken)
    lookupExecuted.value = true
  } catch (error) {
    if (isOwnCancellation(error, signal)) return
    lookupFailure.value = rpcFailure(error)
  } finally {
    if (!signal.aborted) lookupLoading.value = false
  }
}

function nextPage() {
  cursor.forward()
  void runConceptLookup(false)
}

function previousPage() {
  cursor.back()
  void runConceptLookup(false)
}

function openInAccount() {
  lookupInputError.value = null
  try {
    const query = conceptAccountRouteQuery(draft)
    void router.push({
      name: 'repository-account',
      params: { accountId: draft.accountId.trim() },
      query,
    })
  } catch (error) {
    if (error instanceof ListingInputError) {
      lookupInputError.value = error.message
      return
    }
    throw error
  }
}

function openDocumentRow(row: DocumentRow) {
  void router.push({
    name: 'repository-document',
    query: {
      account: draft.accountId.trim(),
      graph: row.graphId,
      node: row.graphAddressId,
      doc: row.docId,
    },
  })
}

// Scheme hierarchy browser state
const schemes = ref<ConceptSchemeView[]>([...props.initialSchemes])
const activeSchemeId = ref<string>(props.initialSchemes[0]?.schemeId ?? '')
const selectedConceptId = ref<string>(props.initialSchemes[0]?.topConcepts[0]?.id ?? '')
const conceptSearch = ref('')
const expandedNodeIds = reactive(new Set<string>())

watch(
  () => props.initialSchemes,
  (next) => {
    for (const scheme of next) upsertScheme(scheme)
  },
)

const activeScheme = computed<ConceptSchemeView | null>(
  () => schemes.value.find((s) => s.schemeId === activeSchemeId.value) ?? schemes.value[0] ?? null,
)

const filteredConcepts = computed<ConceptNodeView[]>(() =>
  activeScheme.value === null ? [] : filterConcepts(activeScheme.value, conceptSearch.value),
)

const selectedConcept = computed<ConceptNodeView | null>(() => {
  const scheme = activeScheme.value
  if (scheme === null) return null
  return scheme.byId.get(selectedConceptId.value) ?? scheme.topConcepts[0] ?? scheme.concepts[0] ?? null
})

const expansionPreview = computed(() => {
  const scheme = activeScheme.value
  const concept = selectedConcept.value
  if (scheme === null || concept === null) return null
  return expandConceptInScheme(scheme, concept.id, {
    narrower: draft.narrower,
    closeMatch: draft.closeMatch,
  })
})

interface FlatTreeRow {
  key: string
  concept: ConceptNodeView
  depth: number
  hasChildren: boolean
  expanded: boolean
  cycleDetected: boolean
}

const visibleTreeRows = computed<FlatTreeRow[]>(() => {
  const scheme = activeScheme.value
  if (scheme === null) return []
  const out: FlatTreeRow[] = []
  const visit = (nodes: readonly ConceptTreeNode[], depth: number, pathKey: string) => {
    for (const node of nodes) {
      const key = `${pathKey}/${node.concept.id}`
      const hasChildren = node.children.length > 0
      const expanded = expandedNodeIds.has(node.concept.id)
      out.push({
        key,
        concept: node.concept,
        depth,
        hasChildren,
        expanded,
        cycleDetected: node.cycleDetected,
      })
      if (hasChildren && expanded) {
        visit(node.children, depth + 1, key)
      }
    }
  }
  visit(scheme.tree, 0, scheme.schemeId)
  return out
})

function expandDefaultRoots(scheme: ConceptSchemeView) {
  for (const root of scheme.topConcepts) {
    expandedNodeIds.add(root.id)
  }
}

for (const initial of props.initialSchemes) {
  expandDefaultRoots(initial)
}

function upsertScheme(scheme: ConceptSchemeView) {
  const idx = schemes.value.findIndex((s) => s.schemeId === scheme.schemeId)
  if (idx >= 0) schemes.value.splice(idx, 1, scheme)
  else schemes.value.push(scheme)
  activeSchemeId.value = scheme.schemeId
  expandDefaultRoots(scheme)
  const firstId = scheme.topConcepts[0]?.id ?? scheme.concepts[0]?.id ?? ''
  selectedConceptId.value = firstId
  draft.schemeId = scheme.schemeId
  if (firstId !== '' && draft.conceptId.trim() === '') {
    draft.conceptId = firstId
  }
}

function useSchemeId(schemeId: string) {
  draft.schemeId = schemeId
  if (schemes.value.some((s) => s.schemeId === schemeId)) {
    selectScheme(schemeId)
  }
}

function selectScheme(schemeId: string) {
  activeSchemeId.value = schemeId
  draft.schemeId = schemeId
  const scheme = schemes.value.find((s) => s.schemeId === schemeId)
  if (scheme !== undefined) {
    const nextConcept = scheme.topConcepts[0]?.id ?? scheme.concepts[0]?.id ?? ''
    selectedConceptId.value = nextConcept
    if (nextConcept !== '') draft.conceptId = nextConcept
  }
}

function toggleTreeNode(conceptId: string) {
  if (expandedNodeIds.has(conceptId)) expandedNodeIds.delete(conceptId)
  else expandedNodeIds.add(conceptId)
}

function selectConcept(conceptId: string) {
  selectedConceptId.value = conceptId
  if (activeScheme.value !== null) {
    draft.schemeId = activeScheme.value.schemeId
  }
  draft.conceptId = conceptId
}

function querySelectedConcept(concept: ConceptNodeView) {
  draft.schemeId = concept.schemeId
  draft.conceptId = concept.id
  void runConceptLookup(true)
}

function selectCrossSchemeTarget(schemeId: string, conceptId: string) {
  draft.schemeId = schemeId
  draft.conceptId = conceptId
  const targetScheme = schemes.value.find((s) => s.schemeId === schemeId)
  if (targetScheme !== undefined) {
    activeSchemeId.value = schemeId
    if (targetScheme.byId.has(conceptId)) {
      selectedConceptId.value = conceptId
    }
  }
}

// Loader state
const showLoader = ref(false)
const loaderSchemeId = ref('')
const loaderVersion = ref('')
const loaderBase64 = ref('')
const loaderError = ref<string | null>(null)

function loadFromBase64() {
  loaderError.value = null
  try {
    const view = decodeConceptSchemeBase64(loaderSchemeId.value, loaderBase64.value, loaderVersion.value || null)
    upsertScheme(view)
    loaderBase64.value = ''
    showLoader.value = false
  } catch (error) {
    loaderError.value = error instanceof Error ? error.message : String(error)
  }
}

async function onSchemeFileChosen(event: Event) {
  const input = event.target as HTMLInputElement | null
  const file = input?.files?.[0]
  if (!file) return
  loaderError.value = null
  try {
    const inferredId = loaderSchemeId.value.trim() || file.name.replace(/\.(binpb|pb)$/i, '')
    const bytes = new Uint8Array(await file.arrayBuffer())
    const view = decodeConceptScheme(inferredId, bytes, loaderVersion.value || null)
    upsertScheme(view)
    showLoader.value = false
  } catch (error) {
    loaderError.value = error instanceof Error ? error.message : String(error)
  } finally {
    if (input) input.value = ''
  }
}
</script>

<style scoped>
.btn-plain {
  background: none;
  border: none;
  padding: 0;
  color: inherit;
  font: inherit;
  cursor: pointer;
}
.btn-link {
  background: none;
  border: none;
  padding: 0;
  color: rgb(var(--v-theme-primary));
  text-decoration: underline;
  font: inherit;
  cursor: pointer;
}
</style>
