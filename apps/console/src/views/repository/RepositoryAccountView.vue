<template>
  <div>
    <v-card class="mb-4">
      <v-card-title class="text-subtitle-1 d-flex align-center flex-wrap ga-2">
        <v-btn icon="mdi-arrow-left" size="small" variant="text" :to="{ name: 'repository' }" aria-label="All accounts" />
        <v-icon size="small">mdi-domain</v-icon>
        <span class="text-mono">{{ accountId }}</span>
        <v-spacer />
        <v-btn size="small" variant="tonal" prepend-icon="mdi-upload" data-testid="account-upload"
               :to="{ name: 'repository-upload', query: { account: accountId } }">
          Upload
        </v-btn>
        <v-btn size="small" variant="tonal" prepend-icon="mdi-magnify" :to="{ name: 'repository-lookup', query: { account: accountId } }">
          Look up
        </v-btn>
      </v-card-title>
    </v-card>

    <v-card class="mb-4">
      <v-card-title class="text-subtitle-2 d-flex align-center flex-wrap ga-2">
        <span>Drives</span>
        <v-spacer />
        <v-btn
          size="x-small"
          variant="tonal"
          prepend-icon="mdi-plus"
          data-testid="toggle-create-drive"
          @click="showCreateDrive = !showCreateDrive"
        >
          {{ showCreateDrive ? 'Hide new drive' : 'New drive' }}
        </v-btn>
      </v-card-title>
      <v-card-text>
        <div v-if="showCreateDrive" class="mb-3 pa-3 rounded border" data-testid="create-drive-panel">
          <div class="text-caption font-weight-medium mb-2">Provision drive (DriveService.CreateDrive)</div>
          <v-row density="compact">
            <v-col cols="12" sm="4">
              <v-text-field
                v-model="driveForm.name"
                label="Drive name"
                density="compact"
                hide-details
                autocorrect="off"
                autocapitalize="off"
                spellcheck="false"
                data-testid="create-drive-name"
              />
            </v-col>
            <v-col cols="12" sm="4">
              <v-select
                v-model="driveForm.driveType"
                :items="driveTypeItems"
                label="Drive type"
                density="compact"
                hide-details
                data-testid="create-drive-type"
              />
            </v-col>
            <v-col cols="12" sm="4">
              <v-text-field
                v-model="driveForm.provider"
                label="Provider (default s3)"
                density="compact"
                hide-details
                autocorrect="off"
                autocapitalize="off"
                spellcheck="false"
                data-testid="create-drive-provider"
              />
            </v-col>
            <v-col cols="12" sm="6" class="mt-1">
              <v-text-field
                v-model="driveForm.bucket"
                label="Bucket (optional)"
                density="compact"
                hide-details
                autocorrect="off"
                autocapitalize="off"
                spellcheck="false"
                data-testid="create-drive-bucket"
              />
            </v-col>
            <v-col cols="12" sm="6" class="mt-1">
              <v-text-field
                v-model="driveForm.prefix"
                label="Prefix (optional)"
                density="compact"
                hide-details
                autocorrect="off"
                autocapitalize="off"
                spellcheck="false"
                data-testid="create-drive-prefix"
              />
            </v-col>
          </v-row>
          <v-alert v-if="createDriveInputError" type="warning" variant="tonal" density="compact" class="mt-2" data-testid="create-drive-input-error">
            {{ createDriveInputError }}
          </v-alert>
          <RpcFailureAlert v-if="createDriveFailure" :failure="createDriveFailure" title="Provisioning drive" class="mt-2" />
          <div class="d-flex align-center ga-2 mt-2">
            <v-btn size="small" color="primary" :loading="creatingDrive" data-testid="create-drive-submit" @click="submitCreateDrive">
              Create drive
            </v-btn>
          </div>
        </div>

        <RpcFailureAlert v-if="drivesFailure" :failure="drivesFailure" title="Listing drives" />
        <v-table v-if="drives.length > 0" density="compact" data-testid="drive-table">
          <thead>
            <tr>
              <th scope="col">Name</th>
              <th scope="col">Type</th>
              <th scope="col">Status</th>
              <th scope="col">Provider</th>
              <th scope="col">Location</th>
              <th scope="col"><span class="d-sr-only">Actions</span></th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="drive in drives" :key="drive.driveId">
              <td>
                <div>{{ drive.name }}</div>
                <div class="text-caption text-medium-emphasis text-mono">{{ drive.driveId }}</div>
              </td>
              <td>{{ driveType(drive.driveType) }}</td>
              <td>{{ driveStatus(drive.status) }}</td>
              <td>{{ drive.provider }}</td>
              <td class="text-mono text-caption">{{ drive.bucket }}{{ drive.prefix ? `/${drive.prefix}` : '' }}</td>
              <td class="text-right">
                <v-btn
                  size="x-small"
                  variant="tonal"
                  :loading="inspectingDriveName === drive.name && driveInspectorLoading"
                  :data-testid="`inspect-drive-${drive.name}`"
                  @click="inspectDrive(drive.name)"
                >
                  {{ inspectedDrive?.name === drive.name ? 'Hide inspector' : 'Inspect' }}
                </v-btn>
              </td>
            </tr>
          </tbody>
        </v-table>
        <div v-else-if="!drivesLoading && !drivesFailure" class="text-body-2 text-medium-emphasis">This account has no drives.</div>

        <RpcFailureAlert v-if="driveInspectorFailure" :failure="driveInspectorFailure" title="Inspecting drive" class="mt-2" />
        <div v-if="inspectedDrive" class="mt-3 pa-3 rounded border" data-testid="drive-inspector-panel">
          <div class="d-flex align-center flex-wrap ga-2 mb-2">
            <span class="text-subtitle-2 text-mono" data-testid="drive-inspector-name">{{ inspectedDrive.name }}</span>
            <v-chip size="x-small" variant="tonal" class="text-mono">{{ inspectedDrive.driveType }}</v-chip>
            <v-chip size="x-small" variant="tonal" color="success" class="text-mono">{{ inspectedDrive.status }}</v-chip>
            <span class="text-caption text-medium-emphasis text-mono">ID {{ inspectedDrive.driveId }}</span>
            <v-spacer />
            <v-btn size="x-small" variant="text" data-testid="drive-inspector-close" @click="inspectedDrive = null">
              Close
            </v-btn>
          </div>
          <div class="text-caption text-mono mb-1" data-testid="drive-inspector-coords">
            provider {{ inspectedDrive.provider }} · location {{ inspectedDrive.location }} · region {{ inspectedDrive.region }} · credentials {{ inspectedDrive.credentialsRef }}
            <span v-if="inspectedDrive.createdAt"> · created {{ inspectedDrive.createdAt }}</span>
          </div>
          <div class="text-caption text-medium-emphasis mb-2" data-testid="drive-inspector-provider-summary">
            {{ inspectedDrive.providerSummary }}
          </div>
          <div v-if="inspectedDrive.providerOptions.length > 0" class="d-flex flex-wrap ga-1 mb-2" data-testid="drive-inspector-options">
            <v-chip
              v-for="opt in inspectedDrive.providerOptions"
              :key="opt.key"
              size="x-small"
              variant="outlined"
              class="text-mono"
            >
              opt {{ opt.key }}={{ opt.value }}
            </v-chip>
          </div>
          <div v-if="inspectedDrive.metadata.length > 0" class="d-flex flex-wrap ga-1 mb-2" data-testid="drive-inspector-metadata">
            <v-chip
              v-for="m in inspectedDrive.metadata"
              :key="m.key"
              size="x-small"
              variant="tonal"
              color="info"
              class="text-mono"
              :data-testid="`drive-meta-${m.key}`"
            >
              {{ m.key }}={{ m.value }}
            </v-chip>
          </div>
          <div class="d-flex align-center flex-wrap ga-1" data-testid="drive-inspector-archives">
            <span class="text-caption text-medium-emphasis">Bound archives:</span>
            <template v-if="archivesForInspectedDrive.length > 0">
              <v-chip
                v-for="arch in archivesForInspectedDrive"
                :key="arch.name"
                size="x-small"
                variant="tonal"
                color="primary"
                class="text-mono"
                :to="{ name: 'repository-archive', query: { account: accountId, archive: arch.name } }"
                :data-testid="`drive-bound-archive-${arch.name}`"
              >
                {{ arch.name }}
              </v-chip>
            </template>
            <span v-else class="text-caption text-medium-emphasis">none</span>
          </div>
        </div>
        <v-btn v-if="drivesNext" size="small" variant="text" class="mt-2" :loading="drivesLoading" @click="loadDrives(false)">
          More drives
        </v-btn>
      </v-card-text>
    </v-card>

    <v-card class="mb-4">
      <v-card-title class="text-subtitle-2 d-flex align-center flex-wrap ga-2">
        <span>Archives</span>
        <v-spacer />
        <v-btn
          size="x-small"
          variant="tonal"
          prepend-icon="mdi-plus"
          data-testid="toggle-create-archive"
          @click="toggleCreateArchive"
        >
          {{ showCreateArchive ? 'Hide new archive' : 'New archive' }}
        </v-btn>
      </v-card-title>
      <v-card-text>
        <div v-if="showCreateArchive" class="mb-3 pa-3 rounded border" data-testid="create-archive-panel">
          <div class="text-caption font-weight-medium mb-2">Create archive (ArchiveService.CreateArchive)</div>
          <v-row density="compact">
            <v-col cols="12" sm="4">
              <v-text-field
                v-model="archiveForm.name"
                label="Archive name"
                density="compact"
                hide-details
                autocorrect="off"
                autocapitalize="off"
                spellcheck="false"
                data-testid="create-archive-name"
              />
            </v-col>
            <v-col cols="12" sm="4">
              <v-text-field
                v-model="archiveForm.driveName"
                label="Bound drive name"
                density="compact"
                hide-details
                autocorrect="off"
                autocapitalize="off"
                spellcheck="false"
                data-testid="create-archive-drive"
              />
            </v-col>
            <v-col cols="12" sm="4">
              <v-select
                v-model="archiveForm.versioning"
                :items="versioningItems"
                label="Versioning policy"
                density="compact"
                hide-details
                data-testid="create-archive-versioning"
              />
            </v-col>
            <v-col cols="12" class="mt-1">
              <v-text-field
                v-model="archiveForm.description"
                label="Description (optional)"
                density="compact"
                hide-details
                data-testid="create-archive-description"
              />
            </v-col>
          </v-row>
          <div v-if="drives.length > 0" class="d-flex align-center flex-wrap ga-1 mt-2">
            <span class="text-caption text-medium-emphasis mr-1">Pick drive:</span>
            <v-chip
              v-for="d in drives"
              :key="d.driveId || d.name"
              size="x-small"
              variant="tonal"
              class="text-mono"
              :data-testid="`pick-archive-drive-${d.name}`"
              @click="archiveForm.driveName = d.name"
            >
              {{ d.name }}
            </v-chip>
          </div>
          <v-alert v-if="createArchiveInputError" type="warning" variant="tonal" density="compact" class="mt-2" data-testid="create-archive-input-error">
            {{ createArchiveInputError }}
          </v-alert>
          <RpcFailureAlert v-if="createArchiveFailure" :failure="createArchiveFailure" title="Creating archive" class="mt-2" />
          <div class="d-flex align-center ga-2 mt-2">
            <v-btn size="small" color="primary" :loading="creatingArchive" data-testid="create-archive-submit" @click="submitCreateArchive">
              Create archive
            </v-btn>
          </div>
        </div>

        <RpcFailureAlert v-if="archivesFailure" :failure="archivesFailure" title="Listing archives" />
        <v-list v-if="archives.length > 0" density="compact" data-testid="archive-list">
          <v-list-item
            v-for="a in archives"
            :key="a.name"
            :to="{ name: 'repository-archive', query: { account: accountId, archive: a.name } }"
            :title="a.name"
            :subtitle="a.description || `drive ${a.driveName}`"
            prepend-icon="mdi-archive-outline"
          />
        </v-list>
        <div v-else-if="!archivesLoading && !archivesFailure" class="text-body-2 text-medium-emphasis">This account has no archives.</div>
      </v-card-text>
    </v-card>

    <OperationOutcomeInspector :key="accountId" :account-id="accountId" :initial-operation-id="initialOperationId" />

    <v-card class="mb-4">
      <v-card-title class="text-subtitle-2 d-flex align-center flex-wrap ga-2">
        <span>Documents</span>
        <v-spacer />
        <v-btn
          size="x-small"
          variant="tonal"
          prepend-icon="mdi-sitemap-outline"
          data-testid="toggle-inline-concept-browser"
          @click="showConceptBrowser = !showConceptBrowser"
        >
          {{ showConceptBrowser ? 'Hide SKOS concept picker' : 'Browse SKOS concepts' }}
        </v-btn>
      </v-card-title>
      <v-card-text>
        <div class="d-flex align-center flex-wrap ga-1 mb-2" data-testid="scope-quick-picks">
          <span class="text-caption text-medium-emphasis mr-1">Quick scopes:</span>
          <v-chip
            size="x-small"
            variant="tonal"
            class="text-mono"
            data-testid="quick-scope-intake"
            @click="pickScope(`intake:${accountId}`, query.graphAddressId)"
          >
            intake:{{ accountId }}
          </v-chip>
          <v-chip
            v-for="ws in sessionWriterScopes"
            :key="`ws:${ws.datasourceId}:${ws.connectorId ?? ''}`"
            size="x-small"
            color="primary"
            variant="tonal"
            class="text-mono"
            :data-testid="`quick-writer-scope-${ws.datasourceId}`"
            @click="pickScope(`intake:${accountId}`, ws.datasourceId)"
          >
            intake:{{ accountId }} / {{ ws.datasourceId }}
          </v-chip>
          <v-chip
            v-for="pair in observedScopePairs"
            :key="`${pair.graphId}/${pair.graphAddressId}`"
            size="x-small"
            variant="outlined"
            class="text-mono"
            :data-testid="`quick-scope-pair-${pair.graphId}-${pair.graphAddressId}`"
            @click="pickScope(pair.graphId, pair.graphAddressId)"
          >
            {{ pair.graphId }} / {{ pair.graphAddressId }}
          </v-chip>
        </div>

        <v-row density="compact">
          <v-col cols="12" sm="6" md="3">
            <v-text-field v-model="query.graphId" label="Graph ID (scope)" density="compact" hide-details
                          autocorrect="off" autocapitalize="off" spellcheck="false" data-testid="scope-graph-id" />
          </v-col>
          <v-col cols="12" sm="6" md="3">
            <v-text-field v-model="query.graphAddressId" label="Graph address ID (scope)" density="compact" hide-details
                          autocorrect="off" autocapitalize="off" spellcheck="false" data-testid="scope-node-id" />
          </v-col>
          <v-col cols="6" md="2">
            <v-select v-model="query.sortField" :items="sortItems" label="Sort by" density="compact" hide-details data-testid="sort-field" />
          </v-col>
          <v-col cols="6" md="2">
            <v-switch v-model="query.descending" label="Descending" density="compact" hide-details color="primary"
                      :disabled="query.sortField === DocumentSortField.UNSPECIFIED" />
          </v-col>
          <v-col cols="6" md="2">
            <v-switch v-model="query.includeTombstoned" label="Tombstoned" density="compact" hide-details color="primary"
                      data-testid="include-tombstoned" />
          </v-col>
        </v-row>

        <v-row density="compact" class="mt-1" data-testid="concept-filter">
          <v-col cols="12" sm="6" md="3">
            <v-text-field v-model="query.filter.concept.schemeId" label="Concept scheme ID" density="compact" hide-details
                          autocorrect="off" autocapitalize="off" spellcheck="false" data-testid="concept-scheme" />
          </v-col>
          <v-col cols="12" sm="6" md="3">
            <v-text-field v-model="query.filter.concept.conceptId" label="Concept ID" density="compact" hide-details
                          autocorrect="off" autocapitalize="off" spellcheck="false" data-testid="concept-id" />
          </v-col>
          <v-col cols="6" md="3">
            <v-checkbox v-model="query.filter.concept.narrower" label="Include narrower concepts" density="compact" hide-details
                        data-testid="concept-narrower" />
          </v-col>
          <v-col cols="6" md="3">
            <v-checkbox v-model="query.filter.concept.closeMatch" label="Include close matches" density="compact" hide-details
                        data-testid="concept-close-match" />
          </v-col>
        </v-row>

        <v-expand-transition>
          <div v-if="showConceptBrowser" class="my-3" data-testid="inline-concept-browser">
            <ConceptSchemeBrowser :account-id="accountId" embedded @select="applyPickedConcept" />
          </div>
        </v-expand-transition>

        <v-expansion-panels variant="accordion" class="my-3">
          <v-expansion-panel title="Filters" data-testid="filter-panel">
            <v-expansion-panel-text>
              <v-row density="compact">
                <v-col v-for="f in TEXT_FILTERS" :key="f.field" cols="12" sm="6" md="4">
                  <v-text-field v-model="query.filter.text[f.field]" :label="f.label" density="compact" hide-details
                                :data-testid="`filter-${f.field}`" />
                </v-col>
                <v-col cols="12" sm="6" md="4">
                  <v-text-field v-model="query.filter.committedAfter" type="datetime-local" label="Committed at or after"
                                density="compact" hide-details />
                </v-col>
                <v-col cols="12" sm="6" md="4">
                  <v-text-field v-model="query.filter.committedBefore" type="datetime-local" label="Committed before"
                                density="compact" hide-details />
                </v-col>
                <v-col cols="12" sm="6" md="2">
                  <v-select v-model="query.filter.owner.kind" :items="ownerKinds" label="Owner" density="compact" hide-details />
                </v-col>
                <v-col cols="12" sm="6" md="2">
                  <v-text-field v-model="query.filter.owner.id" :disabled="query.filter.owner.kind === 'any'"
                                :label="query.filter.owner.kind === 'module' ? 'Module ID' : 'Datasource ID'" density="compact" hide-details />
                </v-col>
              </v-row>
              <div class="text-caption text-medium-emphasis mt-3 mb-1">Metadata (each pair must match exactly)</div>
              <v-row v-for="(pair, i) in query.filter.metadata" :key="i" dense>
                <v-col cols="5"><v-text-field v-model="pair.key" :label="`Key ${i + 1}`" density="compact" hide-details /></v-col>
                <v-col cols="5"><v-text-field v-model="pair.value" :label="`Value ${i + 1}`" density="compact" hide-details /></v-col>
                <v-col cols="2">
                  <v-btn icon="mdi-close" size="small" variant="text" :aria-label="`Remove metadata pair ${i + 1}`"
                         @click="query.filter.metadata.splice(i, 1)" />
                </v-col>
              </v-row>
              <v-btn size="small" variant="text" prepend-icon="mdi-plus" @click="query.filter.metadata.push({ key: '', value: '' })">
                Add metadata pair
              </v-btn>
            </v-expansion-panel-text>
          </v-expansion-panel>
        </v-expansion-panels>

        <div class="d-flex align-center ga-2 mb-3">
          <v-btn color="primary" size="small" :loading="listing" data-testid="apply-query" @click="applyQuery">List documents</v-btn>
          <v-btn size="small" variant="text" data-testid="clear-filters" @click="clearFilters">Clear filters</v-btn>
          <v-spacer />
          <v-text-field v-model.number="query.pageSize" type="number" label="Page size" density="compact" hide-details style="max-width: 120px" />
        </div>

        <div v-if="activeFilterChips.length > 0" class="d-flex align-center flex-wrap ga-1 mb-3" data-testid="account-active-filters">
          <span class="text-caption text-medium-emphasis mr-1">Active URL filters:</span>
          <v-chip
            v-for="chip in activeFilterChips"
            :key="chip.key"
            size="x-small"
            color="primary"
            variant="tonal"
            closable
            class="text-mono"
            :data-testid="`active-filter-${chip.key}`"
            @click:close="chip.clear()"
          >
            {{ chip.label }}
          </v-chip>
        </div>

        <v-alert v-if="inputError" type="warning" variant="tonal" density="compact" class="mb-3" data-testid="input-error">
          {{ inputError }}
        </v-alert>
        <RpcFailureAlert v-if="listFailure" :failure="listFailure" title="Listing documents" />
        <ConceptEvidence v-if="evidence" :evidence="evidence" class="mb-2" />

        <v-table v-if="rows.length > 0" density="compact" hover data-testid="document-table">
          <thead>
            <tr>
              <th scope="col">Document</th>
              <th scope="col">Title</th>
              <th scope="col">Type</th>
              <th scope="col">MIME type</th>
              <th scope="col">Owner</th>
              <th scope="col">Revision</th>
              <th scope="col">Committed</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="row in rows" :key="row.key" class="cursor-pointer" :data-testid="`doc-row-${row.docId}`" @click="openRow(row)">
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
              <td class="text-mono text-caption">{{ row.sourceMimeType ?? '' }}</td>
              <td class="text-caption">{{ row.owner ?? '' }}</td>
              <td class="text-mono">{{ row.revision }}</td>
              <td class="text-mono text-caption">{{ row.committedAt ?? '' }}</td>
            </tr>
          </tbody>
        </v-table>
        <div v-else-if="listed && !listing && !listFailure" class="text-body-2 text-medium-emphasis" data-testid="no-documents">
          No documents match.
        </div>

        <div v-if="listed" class="d-flex align-center ga-2 mt-3">
          <v-btn size="small" variant="text" prepend-icon="mdi-chevron-left" :disabled="!cursor.hasPrevious || listing"
                 data-testid="page-previous" @click="previousPage">Previous</v-btn>
          <span class="text-caption">Page {{ cursor.page }}</span>
          <v-btn size="small" variant="text" append-icon="mdi-chevron-right" :disabled="!cursor.hasNext || listing"
                 data-testid="page-next" @click="nextPage">Next</v-btn>
        </div>
      </v-card-text>
    </v-card>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref, shallowRef, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { create } from '@bufbuild/protobuf'
import ConceptEvidence from '@/components/repository/ConceptEvidence.vue'
import ConceptSchemeBrowser from '@/components/repository/ConceptSchemeBrowser.vue'
import OperationOutcomeInspector from '@/components/repository/OperationOutcomeInspector.vue'
import RpcFailureAlert from '@/components/repository/RpcFailureAlert.vue'
import type { ConsoleSessionStatus } from '@/gen/ai/protomolt/proto/authz/v1/console_session_pb'
import { VersioningPolicy, type Archive } from '@/gen/ai/protomolt/proto/repo/archive/v1/archive_pb'
import { ListArchivesRequestSchema } from '@/gen/ai/protomolt/proto/repo/archive/v1/archive_service_pb'
import {
  CreateDriveRequestSchema,
  DriveStatusSchema,
  DriveType,
  DriveTypeSchema,
  GetDriveRequestSchema,
  ListDrivesRequestSchema,
  type Drive,
} from '@/gen/ai/protomolt/proto/repo/v1/drive_pb'
import { DocumentSortField } from '@/gen/ai/protomolt/proto/repo/v2/document_service_pb'
import { buildCreateArchiveRequest, describeDriveDetails, type DriveDetailsView } from '@/repository/archiveCore'
import { useRepositoryClients, useRepositorySession } from '@/repository/clients'
import { enumLabel } from '@/repository/formatCore'
import { evidenceView, type EvidenceView } from '@/repository/linksCore'
import { scopesIn } from '@/repository/sessionWritersCore'
import { UploadInputError } from '@/repository/uploadCore'
import {
  ListingInputError,
  PageCursor,
  TEXT_FILTERS,
  buildListRequest,
  documentRow,
  emptyFilterForm,
  listingQueryFromRoute,
  listingRouteQuery,
  newListingQuery,
  sameRouteQuery,
  type DocumentRow,
  type ListingQuery,
} from '@/repository/listingCore'
import { isOwnCancellation, rpcFailure, type RpcFailure } from '@/repository/rpcErrorCore'
import { useCallScope } from '@/repository/useCallScope'

const clients = useRepositoryClients()
const sessions = useRepositorySession()
const route = useRoute()
const router = useRouter()
const accountId = computed(() => String(route.params.accountId))
const initialOperationId = computed(() =>
  typeof route.query.operationId === 'string' ? route.query.operationId : '')
const showConceptBrowser = ref(false)
const session = shallowRef<ConsoleSessionStatus | null>(null)
const sessionWriterScopes = computed(() =>
  session.value?.authenticated ? scopesIn(session.value.intakeWriters, accountId.value) : [],
)

const sortItems = [
  { title: 'Call number', value: DocumentSortField.UNSPECIFIED },
  { title: 'Committed at', value: DocumentSortField.COMMITTED_AT },
  { title: 'Title', value: DocumentSortField.TITLE },
]
const ownerKinds = [
  { title: 'Any owner', value: 'any' },
  { title: 'Intake datasource', value: 'datasource' },
  { title: 'Step module', value: 'module' },
]

// Drives
const drivesScope = useCallScope()
const createDriveScope = useCallScope()
const inspectDriveScope = useCallScope()
const drives = ref<Drive[]>([])
const drivesNext = ref('')
const drivesLoading = ref(false)
const drivesFailure = ref<RpcFailure | null>(null)
const showCreateDrive = ref(false)
const creatingDrive = ref(false)
const createDriveInputError = ref<string | null>(null)
const createDriveFailure = ref<RpcFailure | null>(null)
const inspectedDrive = shallowRef<DriveDetailsView | null>(null)
const inspectingDriveName = ref('')
const driveInspectorLoading = ref(false)
const driveInspectorFailure = ref<RpcFailure | null>(null)
const archivesForInspectedDrive = computed(() =>
  inspectedDrive.value ? archives.value.filter((a) => a.driveName === inspectedDrive.value!.name) : [],
)

async function inspectDrive(name: string) {
  if (inspectedDrive.value?.name === name) {
    inspectedDrive.value = null
    return
  }
  const signal = inspectDriveScope.next()
  inspectingDriveName.value = name
  driveInspectorLoading.value = true
  driveInspectorFailure.value = null
  try {
    const answer = await clients.drives.getDrive(
      create(GetDriveRequestSchema, {
        accountId: accountId.value,
        coordinate: { case: 'name', value: name },
      }),
      { signal },
    )
    if (signal.aborted) return
    if (!answer.drive) throw new Error('GetDrive answered without a drive.')
    inspectedDrive.value = describeDriveDetails(answer.drive)
  } catch (error) {
    if (!isOwnCancellation(error, signal)) driveInspectorFailure.value = rpcFailure(error)
  } finally {
    if (!signal.aborted) driveInspectorLoading.value = false
  }
}
const driveTypeItems = [
  { title: 'INTAKE (staged source material)', value: DriveType.INTAKE },
  { title: 'PIPELINE (claim-check bodies)', value: DriveType.PIPELINE },
  { title: 'CUSTOM (account-defined)', value: DriveType.CUSTOM },
]
const driveForm = reactive({
  name: '',
  driveType: DriveType.INTAKE,
  provider: 's3',
  bucket: '',
  prefix: '',
})

function driveType(value: number) {
  return enumLabel(DriveTypeSchema, value)
}
function driveStatus(value: number) {
  return enumLabel(DriveStatusSchema, value)
}

async function submitCreateDrive() {
  createDriveInputError.value = null
  createDriveFailure.value = null
  const name = driveForm.name.trim()
  if (name === '') {
    createDriveInputError.value = 'Drive name: is blank'
    return
  }
  const signal = createDriveScope.next()
  creatingDrive.value = true
  try {
    await clients.drives.createDrive(
      create(CreateDriveRequestSchema, {
        accountId: accountId.value,
        name,
        driveType: driveForm.driveType,
        provider: driveForm.provider.trim(),
        bucket: driveForm.bucket.trim(),
        prefix: driveForm.prefix.trim(),
      }),
      { signal },
    )
    if (signal.aborted) return
    driveForm.name = ''
    driveForm.bucket = ''
    driveForm.prefix = ''
    showCreateDrive.value = false
    await loadDrives(true)
  } catch (error) {
    if (!isOwnCancellation(error, signal)) createDriveFailure.value = rpcFailure(error)
  } finally {
    if (!signal.aborted) creatingDrive.value = false
  }
}

async function loadDrives(first: boolean) {
  const signal = drivesScope.next()
  drivesLoading.value = true
  drivesFailure.value = null
  try {
    const page = await clients.drives.listDrives(
      create(ListDrivesRequestSchema, { accountId: accountId.value, limit: 100, continuationToken: first ? '' : drivesNext.value }),
      { signal },
    )
    drives.value = first ? page.drives : [...drives.value, ...page.drives]
    drivesNext.value = page.nextContinuationToken
  } catch (error) {
    if (!isOwnCancellation(error, signal)) drivesFailure.value = rpcFailure(error)
  } finally {
    drivesLoading.value = false
  }
}

// Archives
const archivesScope = useCallScope()
const createArchiveScope = useCallScope()
const archives = shallowRef<Archive[]>([])
const archivesLoading = ref(false)
const archivesFailure = ref<RpcFailure | null>(null)
const showCreateArchive = ref(false)
const creatingArchive = ref(false)
const createArchiveInputError = ref<string | null>(null)
const createArchiveFailure = ref<RpcFailure | null>(null)
const versioningItems = [
  { title: 'RETAINED (keep every version)', value: VersioningPolicy.RETAINED },
  { title: 'NONE (single retained state)', value: VersioningPolicy.NONE },
]
const archiveForm = reactive({
  name: '',
  driveName: '',
  versioning: VersioningPolicy.RETAINED,
  description: '',
})

function toggleCreateArchive() {
  showCreateArchive.value = !showCreateArchive.value
  if (showCreateArchive.value && archiveForm.driveName.trim() === '' && drives.value.length > 0) {
    archiveForm.driveName = drives.value[0].name
  }
}

async function submitCreateArchive() {
  createArchiveInputError.value = null
  createArchiveFailure.value = null
  let req
  try {
    req = buildCreateArchiveRequest({
      accountId: accountId.value,
      name: archiveForm.name,
      driveName: archiveForm.driveName,
      versioning: archiveForm.versioning,
      description: archiveForm.description,
    })
  } catch (error) {
    if (error instanceof UploadInputError) {
      createArchiveInputError.value = error.message
      return
    }
    throw error
  }
  const signal = createArchiveScope.next()
  creatingArchive.value = true
  try {
    await clients.archives.createArchive(req, { signal })
    if (signal.aborted) return
    archiveForm.name = ''
    archiveForm.description = ''
    showCreateArchive.value = false
    await loadArchives()
  } catch (error) {
    if (!isOwnCancellation(error, signal)) createArchiveFailure.value = rpcFailure(error)
  } finally {
    if (!signal.aborted) creatingArchive.value = false
  }
}

async function loadArchives() {
  const signal = archivesScope.next()
  archivesLoading.value = true
  archivesFailure.value = null
  archives.value = []
  try {
    const all: Archive[] = []
    let token = ''
    do {
      const page = await clients.archives.listArchives(
        create(ListArchivesRequestSchema, { accountId: accountId.value, limit: 100, continuationToken: token }),
        { signal },
      )
      all.push(...page.archives)
      token = page.nextContinuationToken
    } while (token !== '')
    if (!signal.aborted) archives.value = all
  } catch (error) {
    if (!signal.aborted) archivesFailure.value = rpcFailure(error)
  } finally {
    if (!signal.aborted) archivesLoading.value = false
  }
}

// Documents
const listScope = useCallScope()
const query = reactive(newListingQuery(accountId.value))
const cursor = reactive(new PageCursor()) as PageCursor
const rows = shallowRef<DocumentRow[]>([])
const listing = ref(false)
const listed = ref(false)
const listFailure = ref<RpcFailure | null>(null)
const inputError = ref<string | null>(null)
const evidence = shallowRef<EvidenceView | null>(null)

const observedScopePairs = computed(() => {
  const seen = new Set<string>()
  const out: { graphId: string; graphAddressId: string }[] = []
  for (const ws of sessionWriterScopes.value) {
    seen.add(`intake:${accountId.value}\u0000${ws.datasourceId}`)
  }
  for (const row of rows.value) {
    if (!row.graphId || !row.graphAddressId) continue
    const key = `${row.graphId}\u0000${row.graphAddressId}`
    if (seen.has(key)) continue
    seen.add(key)
    out.push({ graphId: row.graphId, graphAddressId: row.graphAddressId })
  }
  return out
})

const activeFilterChips = computed(() => {
  const chips: Array<{ key: string; label: string; clear: () => void }> = []
  if (query.graphId.trim() !== '') {
    chips.push({
      key: 'graph',
      label: `graph: ${query.graphId}`,
      clear: () => {
        query.graphId = ''
        applyQuery()
      },
    })
  }
  if (query.graphAddressId.trim() !== '') {
    chips.push({
      key: 'node',
      label: `node: ${query.graphAddressId}`,
      clear: () => {
        query.graphAddressId = ''
        applyQuery()
      },
    })
  }
  if (query.filter.concept.schemeId.trim() !== '' && query.filter.concept.conceptId.trim() !== '') {
    chips.push({
      key: 'concept',
      label: `concept: ${query.filter.concept.schemeId}/${query.filter.concept.conceptId}`,
      clear: () => {
        query.filter.concept = { schemeId: '', conceptId: '', narrower: false, closeMatch: false }
        applyQuery()
      },
    })
  }
  for (const f of TEXT_FILTERS) {
    if (query.filter.text[f.field].trim() !== '') {
      chips.push({
        key: f.field,
        label: `${f.field}: ${query.filter.text[f.field]}`,
        clear: () => {
          query.filter.text[f.field] = ''
          applyQuery()
        },
      })
    }
  }
  if (query.sortField !== DocumentSortField.UNSPECIFIED) {
    const sortTitle = sortItems.find((s) => s.value === query.sortField)?.title ?? String(query.sortField)
    chips.push({
      key: 'sort',
      label: `sort: ${sortTitle}${query.descending ? ' (desc)' : ''}`,
      clear: () => {
        query.sortField = DocumentSortField.UNSPECIFIED
        query.descending = false
        applyQuery()
      },
    })
  }
  return chips
})

function pickScope(graphId: string, graphAddressId: string) {
  query.graphId = graphId
  query.graphAddressId = graphAddressId
}

function applyPickedConcept(selection: { schemeId: string; conceptId: string; narrower: boolean; closeMatch: boolean }) {
  query.filter.concept.schemeId = selection.schemeId
  query.filter.concept.conceptId = selection.conceptId
  query.filter.concept.narrower = selection.narrower
  query.filter.concept.closeMatch = selection.closeMatch
  applyQuery()
}

// The query the page tokens were issued for. Paging uses it, not the form,
// so editing the form changes nothing until List documents is clicked.
let listedQuery: ListingQuery = structuredQuery(query)

function structuredQuery(source: ListingQuery): ListingQuery {
  return {
    ...source,
    filter: {
      text: { ...source.filter.text },
      committedAfter: source.filter.committedAfter,
      committedBefore: source.filter.committedBefore,
      metadata: source.filter.metadata.map((m) => ({ ...m })),
      owner: { ...source.filter.owner },
      concept: { ...source.filter.concept },
    },
  }
}

async function loadPage() {
  inputError.value = null
  listFailure.value = null
  let request
  try {
    request = buildListRequest(listedQuery, cursor.token)
  } catch (error) {
    if (error instanceof ListingInputError) {
      inputError.value = error.message
      return
    }
    throw error
  }
  const signal = listScope.next()
  listing.value = true
  try {
    const page = await clients.documents.listDocuments(request, { signal })
    rows.value = page.documents.map(documentRow)
    evidence.value = evidenceView(page.conceptEvidence)
    cursor.answered(page.nextPageToken)
    listed.value = true
  } catch (error) {
    if (!isOwnCancellation(error, signal)) {
      rows.value = []
      evidence.value = null
      listFailure.value = rpcFailure(error)
    }
  } finally {
    listing.value = false
  }
}

// A listed query lives in the route, so a reload, the back button and a shared
// link list it again. List documents writes the form there; the route watcher
// below then lists from page 1. Listing the query already shown lists it again.
function applyQuery() {
  // A form the server would refuse stays out of the route; it is named here instead.
  try {
    buildListRequest(query, '')
  } catch (error) {
    if (!(error instanceof ListingInputError)) throw error
    inputError.value = error.message
    return
  }
  const next = listingRouteQuery(query)
  if (initialOperationId.value !== '') next.operationId = initialOperationId.value
  if (sameRouteQuery(next, route.query)) {
    listedQuery = structuredQuery(query)
    cursor.reset()
    void loadPage()
    return
  }
  void router.push({ name: 'repository-account', params: { accountId: accountId.value }, query: next })
}

/** Lists the query the route names from page 1, or names the route value it cannot read. */
function listFromRoute() {
  cursor.reset()
  rows.value = []
  evidence.value = null
  try {
    Object.assign(query, listingQueryFromRoute(accountId.value, route.query))
  } catch (error) {
    if (!(error instanceof ListingInputError)) throw error
    Object.assign(query, newListingQuery(accountId.value))
    listedQuery = structuredQuery(query)
    listFailure.value = null
    listed.value = false
    inputError.value = `The link's listing query cannot be read: ${error.message}`
    return
  }
  listedQuery = structuredQuery(query)
  void loadPage()
}
function clearFilters() {
  query.filter = emptyFilterForm()
}
function nextPage() {
  cursor.forward()
  void loadPage()
}
function previousPage() {
  cursor.back()
  void loadPage()
}

function openRow(row: DocumentRow) {
  const query: Record<string, string> = { account: accountId.value, graph: row.graphId, node: row.graphAddressId, doc: row.docId }
  // A current read of a tombstoned document is NOT_FOUND; its tombstone
  // revision itself opens, with the history behind it.
  if (row.tombstone) query.revision = row.revision
  void router.push({ name: 'repository-document', query })
}

function loadAll() {
  void loadDrives(true)
  void loadArchives()
  listFromRoute()
  void sessions
    .status()
    .then((s) => {
      session.value = s
    })
    .catch(() => {})
}

onMounted(loadAll)

// The same view serves every account; a different account starts over, and a
// new listing query in the route lists again from page 1. Other routes (a
// document this screen opened) leave the screen as it is.
let shownAccount = accountId.value
watch(
  () => route.fullPath,
  () => {
    if (route.name !== 'repository-account') return
    if (accountId.value !== shownAccount) {
      shownAccount = accountId.value
      listed.value = false
      drives.value = []
      drivesNext.value = ''
      inspectedDrive.value = null
      driveInspectorFailure.value = null
      loadAll()
      return
    }
    listFromRoute()
  },
)

// Call number order is always ascending; leaving a sort field clears Descending
// so it cannot linger behind a disabled switch.
watch(
  () => query.sortField,
  (field) => {
    if (field === DocumentSortField.UNSPECIFIED) query.descending = false
  },
)
</script>
