<template>
  <div>
    <v-card class="mb-4">
      <v-card-title class="text-subtitle-1 d-flex align-center flex-wrap ga-2">
        <v-btn icon="mdi-arrow-left" size="small" variant="text" aria-label="Archive entries"
               :to="{ name: 'repository-archive', query: { account: queryText('account'), archive: queryText('archive') } }" />
        <v-icon size="small">mdi-file-cabinet</v-icon>
        <span class="text-mono" data-testid="entry-id">{{ queryText('entry') }}</span>
        <v-chip v-if="pinned !== null" size="small" variant="tonal" class="text-mono" data-testid="version-chip">
          version {{ pinned }}{{ requested === null ? ' (current)' : '' }}
        </v-chip>
        <v-spacer />
        <v-btn
          v-if="manifest && address"
          size="small"
          variant="tonal"
          prepend-icon="mdi-file-document-outline"
          data-testid="open-entry-v2-document"
          @click="openAsV2Document"
        >
          Open as v2 document
        </v-btn>
        <v-btn
          v-if="manifest"
          size="small"
          variant="tonal"
          prepend-icon="mdi-pencil-box-multiple-outline"
          data-testid="toggle-put-entry"
          @click="togglePutEntry"
        >
          {{ showPutEntry ? 'Hide PutEntry' : 'Put entry' }}
        </v-btn>
        <v-btn
          v-if="manifest"
          size="small"
          variant="tonal"
          prepend-icon="mdi-shape-outline"
          data-testid="toggle-classify-bridge"
          @click="toggleClassifyBridge"
        >
          {{ showClassifyBridge ? 'Hide classify & bridge' : 'Classify & bridge' }}
        </v-btn>
        <v-btn
          v-if="manifest"
          size="small"
          variant="tonal"
          prepend-icon="mdi-upload"
          data-testid="toggle-rendition-upload"
          @click="showUpload = !showUpload"
        >
          {{ showUpload ? 'Hide rendition upload' : 'Upload rendition' }}
        </v-btn>
        <v-btn
          v-if="manifest && requested === null && pinned !== null"
          size="small"
          color="error"
          variant="tonal"
          prepend-icon="mdi-delete-outline"
          data-testid="delete-archive-entry"
          @click="deletingEntry = true"
        >
          Delete entry
        </v-btn>
        <v-btn v-if="requested !== null" size="small" variant="text" data-testid="open-current-version" @click="openVersion(null)">
          Open current version
        </v-btn>
      </v-card-title>
      <v-card-text v-if="info" class="text-body-2 pt-0" data-testid="entry-info">
        <div>{{ info.title || info.filename }}</div>
        <div class="text-caption text-medium-emphasis">
          {{ info.contentType }} · {{ classification }} · updated {{ instant(info.updatedAt) ?? 'not recorded' }}
          <span v-if="info.entryUuid"> · UUID <span class="text-mono">{{ info.entryUuid }}</span></span>
          <span v-if="info.sourceUri"> · {{ info.sourceUri }}</span>
        </div>
        <div v-if="classificationEvidence.length > 0" class="d-flex flex-wrap ga-1 mt-1" data-testid="entry-classification-evidence">
          <v-chip
            v-for="(ev, idx) in classificationEvidence"
            :key="idx"
            size="x-small"
            variant="tonal"
            color="info"
            class="text-mono"
            :data-testid="`classification-evidence-${ev.signal}`"
          >
            {{ ev.signal }}: {{ ev.observation }}
          </v-chip>
        </div>
        <div v-if="entryMetadata.length > 0" class="d-flex flex-wrap ga-1 mt-2" data-testid="entry-metadata">
          <v-chip
            v-for="m in entryMetadata"
            :key="m.key"
            size="x-small"
            variant="outlined"
            class="text-mono"
            :data-testid="`entry-meta-${m.key}`"
          >
            {{ m.key }}={{ m.value }}
          </v-chip>
        </div>
      </v-card-text>
    </v-card>

    <v-card v-if="showPutEntry && address" class="mb-4" data-testid="put-entry-panel">
      <v-card-title class="text-subtitle-2">Atomic metadata &amp; inline renditions commit (ArchiveService.PutEntry)</v-card-title>
      <v-card-text>
        <v-row density="compact">
          <v-col cols="12" sm="4">
            <v-text-field
              v-model="putForm.title"
              label="Entry title"
              density="compact"
              hide-details
              data-testid="put-entry-title"
            />
          </v-col>
          <v-col cols="12" sm="4">
            <v-text-field
              v-model="putForm.filename"
              label="Original filename (optional)"
              density="compact"
              hide-details
              autocorrect="off"
              autocapitalize="off"
              spellcheck="false"
              data-testid="put-entry-filename"
            />
          </v-col>
          <v-col cols="12" sm="4">
            <v-text-field
              v-model="putForm.contentType"
              label="Primary content type (optional)"
              density="compact"
              hide-details
              autocorrect="off"
              autocapitalize="off"
              spellcheck="false"
              data-testid="put-entry-content-type"
            />
          </v-col>
          <v-col cols="12" sm="5" class="mt-2">
            <v-text-field
              v-model="putForm.sourceUri"
              label="Source URI (optional)"
              density="compact"
              hide-details
              autocorrect="off"
              autocapitalize="off"
              spellcheck="false"
              data-testid="put-entry-source-uri"
            />
          </v-col>
          <v-col cols="12" sm="4" class="mt-2">
            <v-text-field
              v-model="putForm.producingModule"
              label="Writer module (optional)"
              density="compact"
              hide-details
              autocorrect="off"
              autocapitalize="off"
              spellcheck="false"
              data-testid="put-entry-module"
            />
          </v-col>
          <v-col cols="12" sm="3" class="mt-2">
            <v-text-field
              v-model="putExpectedVersionText"
              label="Expected version (0 = any)"
              density="compact"
              hide-details
              autocorrect="off"
              autocapitalize="off"
              spellcheck="false"
              data-testid="put-entry-expected-version"
            />
          </v-col>
        </v-row>

        <div class="d-flex align-center justify-space-between mt-3 mb-1">
          <span class="text-caption font-weight-medium">Metadata labels (replaces map when non-empty)</span>
          <v-btn size="x-small" variant="tonal" prepend-icon="mdi-plus" data-testid="put-entry-add-meta" @click="addPutMetaRow">
            Add label
          </v-btn>
        </div>
        <v-row v-for="(m, idx) in putForm.metadata" :key="idx" density="compact" class="mb-1 align-center">
          <v-col cols="5">
            <v-text-field
              v-model="m.key"
              label="Key"
              density="compact"
              hide-details
              autocorrect="off"
              autocapitalize="off"
              spellcheck="false"
              :data-testid="`put-entry-meta-key-${idx}`"
            />
          </v-col>
          <v-col cols="6">
            <v-text-field
              v-model="m.value"
              label="Value"
              density="compact"
              hide-details
              :data-testid="`put-entry-meta-val-${idx}`"
            />
          </v-col>
          <v-col cols="1" class="text-right">
            <v-btn
              icon="mdi-close"
              size="x-small"
              variant="text"
              aria-label="Remove metadata label"
              :data-testid="`put-entry-meta-remove-${idx}`"
              @click="putForm.metadata.splice(idx, 1)"
            />
          </v-col>
        </v-row>

        <div class="d-flex align-center justify-space-between mt-3 mb-1">
          <span class="text-caption font-weight-medium">Inline renditions (siblings not listed are carried forward)</span>
          <v-btn size="x-small" variant="tonal" prepend-icon="mdi-plus" data-testid="put-entry-add-rendition" @click="addPutRenditionRow">
            Add inline rendition
          </v-btn>
        </div>
        <v-card v-for="(r, idx) in putForm.renditions" :key="idx" variant="outlined" class="mb-2 pa-2" :data-testid="`put-rendition-row-${idx}`">
          <v-row density="compact" class="align-center">
            <v-col cols="12" sm="3">
              <v-text-field
                v-model="r.name"
                label="Rendition name (e.g. markdown)"
                density="compact"
                hide-details
                autocorrect="off"
                autocapitalize="off"
                spellcheck="false"
                :data-testid="`put-rendition-name-${idx}`"
              />
            </v-col>
            <v-col cols="12" sm="3">
              <v-text-field
                v-model="r.subKey"
                label="Sub-key (optional)"
                density="compact"
                hide-details
                autocorrect="off"
                autocapitalize="off"
                spellcheck="false"
                :data-testid="`put-rendition-subkey-${idx}`"
              />
            </v-col>
            <v-col cols="12" sm="3">
              <v-text-field
                v-model="r.mediaType"
                label="Media type"
                density="compact"
                hide-details
                autocorrect="off"
                autocapitalize="off"
                spellcheck="false"
                :data-testid="`put-rendition-mime-${idx}`"
              />
            </v-col>
            <v-col cols="10" sm="2">
              <v-text-field
                v-model="r.schemaSubject"
                label="Schema subject"
                density="compact"
                hide-details
                autocorrect="off"
                autocapitalize="off"
                spellcheck="false"
                :data-testid="`put-rendition-schema-${idx}`"
              />
            </v-col>
            <v-col cols="2" sm="1" class="text-right">
              <v-btn
                icon="mdi-delete-outline"
                size="x-small"
                variant="text"
                aria-label="Remove inline rendition"
                :data-testid="`put-rendition-remove-${idx}`"
                @click="putForm.renditions.splice(idx, 1)"
              />
            </v-col>
            <v-col cols="12" class="mt-1">
              <v-textarea
                v-model="r.text"
                label="UTF-8 payload text / JSON / Markdown"
                rows="2"
                density="compact"
                hide-details
                :data-testid="`put-rendition-text-${idx}`"
              />
            </v-col>
          </v-row>
        </v-card>

        <v-alert v-if="putInputError" type="warning" variant="tonal" density="compact" class="mt-2" data-testid="put-entry-input-error">
          {{ putInputError }}
        </v-alert>
        <RpcFailureAlert v-if="putFailure" :failure="putFailure" title="Saving entry via PutEntry" class="mt-2" />
        <div class="d-flex align-center ga-2 mt-2">
          <v-btn size="small" color="primary" :loading="putBusy" data-testid="put-entry-submit" @click="submitPutEntry">
            Commit PutEntry
          </v-btn>
        </div>
        <v-alert
          v-if="putOutcome"
          :type="putOutcome.deduplicated || putOutcome.replayed ? 'info' : 'success'"
          variant="tonal"
          density="compact"
          class="mt-2"
          data-testid="put-entry-outcome"
        >
          <div class="d-flex align-center flex-wrap ga-2">
            <v-chip size="x-small" variant="flat" class="text-mono" data-testid="put-entry-status-chip">
              {{ putOutcome.statusLabel }}
            </v-chip>
            <span class="font-weight-medium">{{ putOutcome.headline }}</span>
          </div>
          <div class="text-caption text-mono mt-1">
            root checksum {{ putOutcome.rootChecksum || '—' }} · {{ byteSize(putOutcome.totalBytes) }}
            <template v-if="putOutcome.operationId">
              · op:
              <router-link
                :to="{ name: 'repository-lookup', query: { account: queryText('account'), op: putOutcome.operationId } }"
                class="text-mono"
                data-testid="put-entry-op-link"
              >
                {{ putOutcome.operationId }}
              </router-link>
            </template>
          </div>
        </v-alert>
      </v-card-text>
    </v-card>

    <v-card v-if="showClassifyBridge && address" class="mb-4" data-testid="classify-bridge-panel">
      <v-card-title class="text-subtitle-2">Characterization &amp; format bridges</v-card-title>
      <v-card-text>
        <v-row density="compact">
          <v-col cols="12" md="6">
            <div class="text-subtitle-2 mb-2">Classify &amp; characterize (ArchiveService.ClassifyEntry)</div>
            <v-select
              v-model="classifyForm.formatCase"
              :items="DECLARED_FORMAT_ITEMS"
              label="Declared format"
              density="compact"
              hide-details
              class="mb-2"
              data-testid="classify-format"
            />
            <v-text-field
              v-if="classifyForm.formatCase !== ''"
              v-model="classifyForm.filename"
              label="Declared filename"
              density="compact"
              hide-details
              class="mb-2"
              autocorrect="off"
              autocapitalize="off"
              spellcheck="false"
              data-testid="classify-filename"
            />
            <v-row v-if="classifyForm.formatCase === 'delimited'" density="compact" class="mb-2">
              <v-col cols="6">
                <v-text-field
                  v-model="classifyForm.delimiter"
                  label="Delimiter (e.g. , or \t)"
                  density="compact"
                  hide-details
                  autocorrect="off"
                  autocapitalize="off"
                  spellcheck="false"
                  data-testid="classify-delimiter"
                />
              </v-col>
              <v-col cols="6">
                <v-select
                  v-model="classifyForm.header"
                  :items="headerPresenceItems"
                  label="Header row"
                  density="compact"
                  hide-details
                  data-testid="classify-header"
                />
              </v-col>
            </v-row>
            <v-text-field
              v-if="classifyForm.formatCase === 'image'"
              v-model="classifyForm.mediaType"
              label="Image media type (e.g. image/png)"
              density="compact"
              hide-details
              class="mb-2"
              autocorrect="off"
              autocapitalize="off"
              spellcheck="false"
              data-testid="classify-image-media-type"
            />
            <v-text-field
              v-model="classifyForm.module"
              label="Producing module (optional)"
              density="compact"
              hide-details
              class="mb-2"
              autocorrect="off"
              autocapitalize="off"
              spellcheck="false"
              data-testid="classify-module"
            />
            <v-alert v-if="classifyInputError" type="warning" variant="tonal" density="compact" class="mb-2" data-testid="classify-input-error">
              {{ classifyInputError }}
            </v-alert>
            <RpcFailureAlert v-if="classifyFailure" :failure="classifyFailure" title="Classifying the entry" class="mb-2" />
            <v-btn size="small" color="primary" :loading="classifyBusy" data-testid="classify-submit" @click="submitClassifyEntry">
              Classify entry
            </v-btn>
            <v-alert v-if="classifyResolved" type="success" variant="tonal" density="compact" class="mt-2" data-testid="classify-result">
              <div class="font-weight-medium">Resolved: {{ classificationLabel(classifyResolved) }}</div>
              <div v-for="(ev, idx) in classificationEvidenceRows(classifyResolved)" :key="idx" class="text-caption text-mono">
                {{ ev.signal }}: {{ ev.observation }}
              </div>
            </v-alert>
          </v-col>

          <v-col cols="12" md="6">
            <div class="text-subtitle-2 mb-2">Format bridges (ArchiveService.BridgeEntry)</div>
            <v-select
              v-model="selectedBridges"
              :items="BRIDGE_KIND_ITEMS"
              label="Bridges to run (empty runs all applicable)"
              multiple
              chips
              closable-chips
              density="compact"
              hide-details
              class="mb-2"
              data-testid="bridge-kinds"
            />
            <v-text-field
              v-model="bridgeModule"
              label="Producing module (optional)"
              density="compact"
              hide-details
              class="mb-2"
              autocorrect="off"
              autocapitalize="off"
              spellcheck="false"
              data-testid="bridge-module"
            />
            <RpcFailureAlert v-if="bridgeFailure" :failure="bridgeFailure" title="Bridging the entry" class="mb-2" />
            <v-btn size="small" color="primary" :loading="bridgeBusy" data-testid="bridge-submit" @click="submitBridgeEntry">
              Run bridges
            </v-btn>
            <div v-if="bridgeResponse" class="mt-2" data-testid="bridge-result">
              <div class="text-caption text-mono mb-1" data-testid="bridge-result-version">
                Entry version after run: {{ bridgeResponse.version }}
              </div>
              <v-table v-if="bridgeOutcomes.length > 0" density="compact" data-testid="bridge-outcomes-table">
                <thead>
                  <tr>
                    <th scope="col">Bridge</th>
                    <th scope="col">Rendition</th>
                    <th scope="col">Status</th>
                    <th scope="col">Detail</th>
                    <th scope="col"><span class="d-sr-only">Actions</span></th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="(row, i) in bridgeOutcomes" :key="i" :data-testid="`bridge-outcome-${row.bridge}`">
                    <td class="text-mono text-caption">{{ row.bridge }}</td>
                    <td class="text-mono text-caption">{{ row.rendition || '—' }}</td>
                    <td>
                      <v-chip size="x-small" variant="tonal" :color="bridgeStatusColor(row.status)" class="text-mono">
                        {{ row.status }}
                      </v-chip>
                    </td>
                    <td class="text-caption">{{ row.detail || '—' }}</td>
                    <td>
                      <v-btn
                        v-if="row.inspectable"
                        size="x-small"
                        variant="tonal"
                        :data-testid="`bridge-inspect-${row.rendition}`"
                        @click="inspectBridgedRendition(row.rendition, bridgeResponse.version)"
                      >
                        Inspect bytes
                      </v-btn>
                    </td>
                  </tr>
                </tbody>
              </v-table>
            </div>
          </v-col>
        </v-row>
      </v-card-text>
    </v-card>

    <v-card v-if="showUpload && address" class="mb-4" data-testid="rendition-upload-panel">
        <v-card-title class="text-subtitle-2">Upload rendition (/v1/archive:upload)</v-card-title>
        <v-card-text>
          <v-row density="compact">
            <v-col cols="12" sm="4">
              <v-text-field
                v-model="uploadForm.rendition"
                label="Rendition name"
                density="compact"
                hide-details
                autocorrect="off"
                autocapitalize="off"
                spellcheck="false"
                data-testid="rendition-up-name"
              />
            </v-col>
            <v-col cols="12" sm="4">
              <v-text-field
                v-model="uploadForm.subKey"
                label="Sub-key (optional)"
                density="compact"
                hide-details
                autocorrect="off"
                autocapitalize="off"
                spellcheck="false"
                data-testid="rendition-up-subkey"
              />
            </v-col>
            <v-col cols="12" sm="4">
              <v-text-field
                v-model="uploadForm.expectedVersion"
                label="Expected version (blank or 0 for any)"
                density="compact"
                hide-details
                autocorrect="off"
                autocapitalize="off"
                spellcheck="false"
                data-testid="rendition-up-expected"
              />
            </v-col>
            <v-col cols="12" sm="6" class="mt-1">
              <v-text-field
                v-model="uploadForm.contentType"
                label="Media type (optional, defaults to file type)"
                density="compact"
                hide-details
                autocorrect="off"
                autocapitalize="off"
                spellcheck="false"
                data-testid="rendition-up-type"
              />
            </v-col>
            <v-col cols="12" sm="6" class="mt-1">
              <v-text-field
                v-model="uploadForm.module"
                label="Producing module (optional)"
                density="compact"
                hide-details
                autocorrect="off"
                autocapitalize="off"
                spellcheck="false"
                data-testid="rendition-up-module"
              />
            </v-col>
          </v-row>
          <div class="mt-2">
            <input type="file" data-testid="rendition-up-file" @change="pickUploadFile" />
            <span v-if="uploadFile" class="text-caption text-mono ml-2">
              {{ uploadFile.name }} · {{ byteSize(uploadFile.size) }}
            </span>
          </div>
          <v-alert v-if="uploadInputError" type="warning" variant="tonal" density="compact" class="mt-2" data-testid="rendition-up-input-error">
            {{ uploadInputError }}
          </v-alert>
          <RpcFailureAlert v-if="uploadFailure" :failure="uploadFailure" title="Uploading rendition" class="mt-2" />
          <div class="d-flex align-center ga-2 mt-3">
            <v-btn size="small" color="primary" :loading="uploadBusy" data-testid="rendition-up-submit" @click="submitRenditionUpload">
              Upload rendition
            </v-btn>
          </div>
          <v-alert
            v-if="uploadSaved"
            type="success"
            variant="tonal"
            density="compact"
            class="mt-3"
            data-testid="rendition-upload-committed"
          >
            <div class="font-weight-medium">
              Committed as version {{ uploadSaved.version }}
              <span v-if="uploadSaved.replayed"> (replayed)</span>
            </div>
            <div class="text-caption text-mono">
              SHA-256 {{ uploadSaved.sha256 }} · operation
              <router-link
                v-if="uploadSaved.operationId"
                :to="{ name: 'repository-lookup', query: { account: queryText('account'), op: uploadSaved.operationId } }"
                class="text-mono"
                data-testid="rendition-upload-op-link"
              >
                {{ uploadSaved.operationId }}
              </router-link>
            </div>
            <v-btn
              size="x-small"
              variant="tonal"
              class="mt-2"
              data-testid="rendition-upload-open-version"
              @click="openVersion(uploadSaved.version)"
            >
              Open version {{ uploadSaved.version }}
            </v-btn>
        </v-alert>
      </v-card-text>
    </v-card>

    <v-alert v-if="routeError" type="warning" variant="tonal" density="compact" class="mb-3" data-testid="route-error">{{ routeError }}</v-alert>
    <RpcFailureAlert v-if="manifestFailure" :failure="manifestFailure" title="Reading the entry manifest" />

    <v-card v-if="manifest && compareVersion !== null" class="mb-4" data-testid="version-compare">
      <v-card-title class="text-subtitle-2 d-flex align-center flex-wrap ga-2">
        <span>Version comparison</span>
        <v-switch
          v-model="compareChangedOnly"
          label="Changed only"
          density="compact"
          hide-details
          color="primary"
          class="ml-2"
          data-testid="version-compare-changed-only"
        />
        <v-spacer />
        <v-btn size="x-small" variant="text" prepend-icon="mdi-swap-horizontal" data-testid="version-compare-swap" @click="swapVersionCompare">
          Swap
        </v-btn>
        <v-btn size="x-small" variant="text" data-testid="version-compare-close" @click="closeVersionCompare">
          Close comparison
        </v-btn>
      </v-card-title>
      <v-card-text>
        <RpcFailureAlert v-if="compareFailure" :failure="compareFailure" title="Reading the compared version manifest" />
        <template v-else-if="compareManifest">
          <div class="text-caption text-mono mb-2" data-testid="version-compare-heading">
            version {{ compareManifest.version }} ({{ byteSize(compareManifest.totalBytes) }}) → version {{ manifest.version }} ({{ byteSize(manifest.totalBytes) }})
          </div>
          <p v-if="comparedRenditions.length === 0" class="text-body-2 text-medium-emphasis" data-testid="version-compare-empty">
            No renditions differ between version {{ compareManifest.version }} and version {{ manifest.version }}.
          </p>
          <v-table v-else density="compact" data-testid="version-compare-table">
            <thead>
              <tr>
                <th scope="col">Rendition</th>
                <th scope="col">Change</th>
                <th scope="col">Version {{ compareManifest.version }}</th>
                <th scope="col">Version {{ manifest.version }}</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="row in comparedRenditions" :key="row.key" :data-testid="`version-diff-${row.label}`">
                <td class="text-mono">{{ row.label }}</td>
                <td>
                  <v-chip size="x-small" variant="tonal" :color="diffChipColor(row.change)">{{ row.change }}</v-chip>
                  <div v-if="row.differs.length > 0" class="text-caption text-medium-emphasis">{{ row.differs.join(', ') }}</div>
                </td>
                <td class="text-caption">
                  <template v-if="row.base">
                    <div :class="{ 'font-weight-bold': row.differs.includes('state') }">{{ row.base.state }}</div>
                    <div :class="{ 'font-weight-bold': row.differs.includes('mediaType') }" class="text-mono">{{ row.base.mediaType }}</div>
                    <div :class="{ 'font-weight-bold': row.differs.includes('sizeBytes') }">{{ byteSize(row.base.sizeBytes) }}</div>
                    <div :class="{ 'font-weight-bold': row.differs.includes('sha256') }" class="text-mono">
                      {{ row.base.sha256 ? `sha256 ${row.base.sha256.slice(0, 16)}` : 'no digest' }}
                    </div>
                    <div v-if="row.base.contentProfile" :class="{ 'font-weight-bold': row.differs.includes('contentProfile') }">
                      {{ row.base.contentProfile }}
                    </div>
                  </template>
                  <span v-else class="text-medium-emphasis">absent</span>
                </td>
                <td class="text-caption">
                  <template v-if="row.target">
                    <div :class="{ 'font-weight-bold': row.differs.includes('state') }">{{ row.target.state }}</div>
                    <div :class="{ 'font-weight-bold': row.differs.includes('mediaType') }" class="text-mono">{{ row.target.mediaType }}</div>
                    <div :class="{ 'font-weight-bold': row.differs.includes('sizeBytes') }">{{ byteSize(row.target.sizeBytes) }}</div>
                    <div :class="{ 'font-weight-bold': row.differs.includes('sha256') }" class="text-mono">
                      {{ row.target.sha256 ? `sha256 ${row.target.sha256.slice(0, 16)}` : 'no digest' }}
                    </div>
                    <div v-if="row.target.contentProfile" :class="{ 'font-weight-bold': row.differs.includes('contentProfile') }">
                      {{ row.target.contentProfile }}
                    </div>
                  </template>
                  <span v-else class="text-medium-emphasis">absent</span>
                </td>
              </tr>
            </tbody>
          </v-table>
        </template>
      </v-card-text>
    </v-card>

    <v-card v-if="manifest" class="mb-4">
      <v-card-title class="text-subtitle-2">Renditions</v-card-title>
      <v-card-text>
        <div class="text-caption text-medium-emphasis mb-2">
          {{ byteSize(manifest.totalBytes) }} in this version<span v-if="manifest.rootChecksum"> · root checksum
          <span class="text-mono">{{ manifest.rootChecksum }}</span></span>
        </div>
        <v-table v-if="renditions.length > 0" density="compact" class="mb-3" data-testid="rendition-table">
          <thead>
            <tr>
              <th scope="col">Rendition</th>
              <th scope="col">Media type</th>
              <th scope="col">State</th>
              <th scope="col">Size</th>
              <th scope="col">SHA-256</th>
              <th scope="col">Content profile</th>
              <th scope="col">Written</th>
              <th scope="col"><span class="d-sr-only">Actions</span></th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="r in renditions" :key="r.key" :data-testid="`rendition-${r.label}`">
              <td class="text-mono">{{ r.label }}</td>
              <td class="text-mono text-caption">{{ r.mediaType }}</td>
              <td>
                {{ r.state }}
                <div v-if="r.deletedReason" class="text-caption text-medium-emphasis">{{ r.deletedReason }}</div>
              </td>
              <td class="text-mono">{{ byteSize(r.sizeBytes) }}</td>
              <td class="text-mono text-caption">{{ r.sha256 }}</td>
              <td class="text-caption" :data-testid="`rendition-profile-${r.label}`">{{ r.contentProfile ?? '—' }}</td>
              <td class="text-caption">
                {{ r.writtenAt ?? '' }}
                <div v-if="r.writtenBy">{{ r.writtenBy }}</div>
              </td>
              <td>
                <v-btn v-if="r.readable" size="x-small" variant="tonal" :data-testid="`select-rendition-${r.label}`" @click="selected = r">Select</v-btn>
              </td>
            </tr>
          </tbody>
        </v-table>
        <div v-else class="text-body-2 text-medium-emphasis">This version records no renditions.</div>
        <ByteReader
          v-if="selected"
          :key="selected.key + String(pinned)"
          :title="selected.label"
          :filename="filenameFor(selected)"
          :media-type="selected.mediaType || null"
          :read="readSelected"
        />
      </v-card-text>
    </v-card>

    <v-card v-if="manifest" class="mb-4" data-testid="versions">
      <v-card-title class="text-subtitle-2">Versions</v-card-title>
      <v-card-text>
        <RpcFailureAlert v-if="versionsFailure" :failure="versionsFailure" title="Listing versions" />
        <v-table v-if="versions.length > 0" density="compact">
          <thead>
            <tr>
              <th scope="col">Version</th>
              <th scope="col">Created</th>
              <th scope="col">Renditions</th>
              <th scope="col">Bytes</th>
              <th scope="col"><span class="d-sr-only">Actions</span></th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="v in versions" :key="v.version.toString()" :data-testid="`version-${v.version}`">
              <td class="text-mono">{{ v.version }}</td>
              <td class="text-mono text-caption">{{ v.createdAt ?? '' }}</td>
              <td>{{ v.renditions }}</td>
              <td>{{ byteSize(v.totalBytes) }}</td>
              <td class="d-flex align-center ga-1">
                <template v-if="v.version !== pinned">
                  <v-btn size="x-small" variant="tonal" :data-testid="`open-version-${v.version}`"
                         @click="openVersion(v.version)">Open</v-btn>
                  <v-btn size="x-small" variant="text" :data-testid="`compare-version-${v.version}`"
                         @click="openVersionCompare(v.version)">Compare</v-btn>
                </template>
                <span v-else class="text-caption text-medium-emphasis">shown</span>
              </td>
            </tr>
          </tbody>
        </v-table>
        <v-btn v-if="versionsNext" size="small" variant="text" class="mt-2" data-testid="versions-more" @click="loadVersions(false)">
          Older versions
        </v-btn>
      </v-card-text>
    </v-card>

    <DeleteDocumentDialog
      v-if="deletingEntry && address && pinned !== null"
      :address="entryNodeAddress(address)"
      :revision="pinned"
      @deleted="onEntryDeleted"
      @reload="onEntryReloadAfterConflict"
      @close="deletingEntry = false"
    />
  </div>
</template>

<script setup lang="ts">
import { computed, inject, reactive, ref, shallowRef, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { create } from '@bufbuild/protobuf'
import ByteReader from '@/components/repository/ByteReader.vue'
import DeleteDocumentDialog from '@/components/repository/DeleteDocumentDialog.vue'
import RpcFailureAlert from '@/components/repository/RpcFailureAlert.vue'
import type { BridgeKind } from '@/gen/ai/protomolt/proto/asset/v1/bridge_pb'
import type { Classification } from '@/gen/ai/protomolt/proto/asset/v1/classification_pb'
import { HeaderPresence } from '@/gen/ai/protomolt/proto/asset/v1/format_pb'
import {
  GetEntryManifestRequestSchema,
  ListVersionsRequestSchema,
  type BridgeEntryResponse,
  type UploadRenditionResponse,
} from '@/gen/ai/protomolt/proto/repo/archive/v1/archive_service_pb'
import type { EntryAddress, EntryInfo, VersionManifest } from '@/gen/ai/protomolt/proto/repo/archive/v1/entry_pb'
import type { Accession } from '@/gen/ai/protomolt/proto/repo/v2/manifest_pb'
import {
  BRIDGE_KIND_ITEMS,
  DECLARED_FORMAT_ITEMS,
  bridgeOutcomeRows,
  buildBridgeEntryRequest,
  buildClassifyEntryRequest,
  buildPutEntryRequest,
  classificationEvidenceRows,
  classificationLabel,
  describePutEntryOutcome,
  entryAddress,
  entryCallNumber,
  entryNodeAddress,
  parseVersion,
  planRenditionUpload,
  readRendition,
  renditionDiff,
  renditionRows,
  sendRenditionUpload,
  versionRow,
  type ClassifyEntryForm,
  type PutEntryForm,
  type PutEntryOutcomeSummary,
  type RenditionChange,
  type RenditionRow,
  type VersionRow,
} from '@/repository/archiveCore'
import type { ByteRead } from '@/repository/byteReadCore'
import { REPOSITORY_UPLOAD_REQUEST, useRepositoryClients } from '@/repository/clients'
import { byteSize, instant } from '@/repository/formatCore'
import { documentRoute } from '@/repository/linksCore'
import { rpcFailure, type RpcFailure } from '@/repository/rpcErrorCore'
import { UploadInputError, fileSha256, newOperationId } from '@/repository/uploadCore'
import { useCallScope } from '@/repository/useCallScope'

const VERSIONS_PAGE_SIZE = 25

const clients = useRepositoryClients()
const newRequest = inject(REPOSITORY_UPLOAD_REQUEST, () => new XMLHttpRequest())
const route = useRoute()
const router = useRouter()
const readScope = useCallScope()
const versionsScope = useCallScope()
const compareScope = useCallScope()
const uploadScope = useCallScope()
const classifyScope = useCallScope()
const bridgeScope = useCallScope()
const putScope = useCallScope()

function queryText(name: string): string {
  const value = route.query[name]
  return typeof value === 'string' ? value : ''
}

const routeError = ref<string | null>(null)
const address = shallowRef<EntryAddress | null>(null)
const requested = ref<bigint | null>(null)
const pinned = ref<bigint | null>(null)
const info = shallowRef<EntryInfo | null>(null)
const manifest = shallowRef<VersionManifest | null>(null)
const manifestFailure = ref<RpcFailure | null>(null)
const selected = ref<RenditionRow | null>(null)
const versions = shallowRef<VersionRow[]>([])
const versionsNext = ref('')
const versionsFailure = ref<RpcFailure | null>(null)

// Version comparison state
const compareVersion = ref<bigint | null>(null)
const compareManifest = shallowRef<VersionManifest | null>(null)
const compareFailure = ref<RpcFailure | null>(null)
const compareChangedOnly = ref(false)

// Classify & Bridge state
const showClassifyBridge = ref(false)
const deletingEntry = ref(false)

// PutEntry state (#694)
const showPutEntry = ref(false)
const putExpectedVersionText = ref('0')
const putForm = reactive<PutEntryForm>({
  title: '',
  filename: '',
  contentType: '',
  sourceUri: '',
  expectedVersion: 0n,
  producingModule: 'console',
  metadata: [],
  renditions: [],
})
const putBusy = ref(false)
const putInputError = ref<string | null>(null)
const putFailure = ref<RpcFailure | null>(null)
const putOutcome = shallowRef<PutEntryOutcomeSummary | null>(null)

function togglePutEntry() {
  showPutEntry.value = !showPutEntry.value
  if (showPutEntry.value && info.value) {
    putForm.title = info.value.title
    putForm.filename = info.value.filename
    putForm.contentType = info.value.contentType
    putForm.sourceUri = info.value.sourceUri
    putExpectedVersionText.value = (pinned.value ?? 0n).toString()
    putForm.metadata = Object.entries(info.value.metadata ?? {})
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([key, value]) => ({ key, value }))
    if (putForm.renditions.length === 0) {
      putForm.renditions = [
        {
          name: 'markdown',
          subKey: '',
          mediaType: 'text/markdown; charset=utf-8',
          schemaSubject: '',
          text: '',
        },
      ]
    }
  }
}

function addPutMetaRow() {
  putForm.metadata.push({ key: '', value: '' })
}

function addPutRenditionRow() {
  putForm.renditions.push({
    name: '',
    subKey: '',
    mediaType: 'text/plain; charset=utf-8',
    schemaSubject: '',
    text: '',
  })
}

async function submitPutEntry() {
  putInputError.value = null
  putFailure.value = null
  putOutcome.value = null
  if (!address.value) return
  const rawExpected = putExpectedVersionText.value.trim()
  let expectedVersion = 0n
  if (rawExpected !== '') {
    try {
      expectedVersion = BigInt(rawExpected)
    } catch {
      putInputError.value = `Expected version: "${rawExpected}" is not a whole number`
      return
    }
  }
  putForm.expectedVersion = expectedVersion
  let req
  try {
    req = buildPutEntryRequest(address.value, putForm, newOperationId())
  } catch (error) {
    if (error instanceof UploadInputError) {
      putInputError.value = error.message
      return
    }
    throw error
  }
  const signal = putScope.next()
  putBusy.value = true
  try {
    const answer = await clients.archives.putEntry(req, { signal })
    if (signal.aborted) return
    putOutcome.value = describePutEntryOutcome(answer)
    putExpectedVersionText.value = answer.version.toString()
    await load()
  } catch (error) {
    if (!signal.aborted) putFailure.value = rpcFailure(error)
  } finally {
    if (!signal.aborted) putBusy.value = false
  }
}

function openAsV2Document() {
  if (!address.value) return
  void router.push(documentRoute(entryCallNumber(address.value), pinned.value))
}

function onEntryDeleted(accession: Accession) {
  deletingEntry.value = false
  if (!address.value) return
  void router.push(documentRoute(entryCallNumber(address.value), accession.revision))
}

function onEntryReloadAfterConflict(tombstone: bigint | null) {
  deletingEntry.value = false
  if (tombstone !== null && address.value) {
    void router.push(documentRoute(entryCallNumber(address.value), tombstone))
    return
  }
  void load()
}
const headerPresenceItems = [
  { title: 'PRESENT (first row names columns)', value: HeaderPresence.PRESENT },
  { title: 'ABSENT (data starts at row 1)', value: HeaderPresence.ABSENT },
]
const classifyForm = reactive<ClassifyEntryForm>({
  formatCase: '',
  filename: '',
  delimiter: ',',
  header: HeaderPresence.PRESENT,
  mediaType: 'image/png',
  module: '',
})
const classifyBusy = ref(false)
const classifyInputError = ref<string | null>(null)
const classifyFailure = ref<RpcFailure | null>(null)
const classifyResolved = shallowRef<Classification | null>(null)

const selectedBridges = ref<BridgeKind[]>([])
const bridgeModule = ref('')
const bridgeBusy = ref(false)
const bridgeFailure = ref<RpcFailure | null>(null)
const bridgeResponse = shallowRef<BridgeEntryResponse | null>(null)
const bridgeOutcomes = computed(() => (bridgeResponse.value ? bridgeOutcomeRows(bridgeResponse.value) : []))

// Rendition upload state
const showUpload = ref(false)
const uploadForm = reactive({
  rendition: 'original',
  subKey: '',
  expectedVersion: '',
  contentType: '',
  module: '',
})
const uploadFile = shallowRef<File | null>(null)
const uploadBusy = ref(false)
const uploadInputError = ref<string | null>(null)
const uploadFailure = ref<RpcFailure | null>(null)
const uploadSaved = shallowRef<UploadRenditionResponse | null>(null)

const renditions = computed(() => (manifest.value ? renditionRows(manifest.value) : []))
const classification = computed(() => classificationLabel(classifyResolved.value ?? info.value?.classification))
const classificationEvidence = computed(() =>
  classificationEvidenceRows(classifyResolved.value ?? info.value?.classification),
)
const entryMetadata = computed(() => {
  const map = info.value?.metadata
  if (!map) return []
  return Object.entries(map)
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
    .map(([key, value]) => ({ key, value }))
})

function toggleClassifyBridge() {
  showClassifyBridge.value = !showClassifyBridge.value
  if (showClassifyBridge.value && classifyForm.filename.trim() === '' && info.value?.filename) {
    classifyForm.filename = info.value.filename
  }
}

function bridgeStatusColor(status: string): string {
  return status === 'PRODUCED'
    ? 'success'
    : status === 'UNCHANGED'
      ? 'info'
      : status === 'DEFERRED'
        ? 'warning'
        : status === 'FAILED'
          ? 'error'
          : 'default'
}

async function submitClassifyEntry() {
  classifyInputError.value = null
  classifyFailure.value = null
  classifyResolved.value = null
  if (!address.value) return
  let req
  try {
    req = buildClassifyEntryRequest(address.value, classifyForm)
  } catch (error) {
    if (error instanceof UploadInputError) {
      classifyInputError.value = error.message
      return
    }
    throw error
  }
  const signal = classifyScope.next()
  classifyBusy.value = true
  try {
    const answer = await clients.archives.classifyEntry(req, { signal })
    if (signal.aborted) return
    classifyResolved.value = answer.classification ?? null
    await load()
  } catch (error) {
    if (!signal.aborted) classifyFailure.value = rpcFailure(error)
  } finally {
    if (!signal.aborted) classifyBusy.value = false
  }
}

async function submitBridgeEntry() {
  bridgeFailure.value = null
  bridgeResponse.value = null
  if (!address.value) return
  const req = buildBridgeEntryRequest(address.value, selectedBridges.value, bridgeModule.value)
  const signal = bridgeScope.next()
  bridgeBusy.value = true
  try {
    const answer = await clients.archives.bridgeEntry(req, { signal })
    if (signal.aborted) return
    bridgeResponse.value = answer
    await load()
  } catch (error) {
    if (!signal.aborted) bridgeFailure.value = rpcFailure(error)
  } finally {
    if (!signal.aborted) bridgeBusy.value = false
  }
}

async function inspectBridgedRendition(renditionName: string, version: bigint) {
  if (requested.value !== null && requested.value !== version) {
    openVersion(version)
    return
  }
  const match = renditions.value.find((r) => r.name === renditionName && r.readable)
  if (match) {
    selected.value = match
  }
}

const comparedRenditions = computed(() => {
  if (!compareManifest.value || !manifest.value) return []
  const rows = renditionDiff(compareManifest.value, manifest.value)
  return compareChangedOnly.value ? rows.filter((r) => r.change !== 'unchanged') : rows
})

function diffChipColor(change: RenditionChange): string {
  return change === 'added' ? 'success' : change === 'removed' ? 'error' : change === 'changed' ? 'warning' : 'default'
}

function pickUploadFile(event: Event) {
  const files = (event.target as HTMLInputElement).files
  uploadFile.value = files && files.length > 0 ? files[0] : null
}

async function submitRenditionUpload() {
  uploadInputError.value = null
  uploadFailure.value = null
  uploadSaved.value = null
  if (!address.value) return
  if (!uploadFile.value) {
    uploadInputError.value = 'File: choose a rendition file to upload'
    return
  }
  const chosen = uploadFile.value
  const operationId = newOperationId()
  try {
    planRenditionUpload(
      {
        accountId: address.value.accountId,
        archive: address.value.archive,
        entryId: address.value.entryId,
        rendition: uploadForm.rendition,
        subKey: uploadForm.subKey,
        contentType: uploadForm.contentType,
        module: uploadForm.module,
        expectedVersion: uploadForm.expectedVersion,
      },
      chosen,
      operationId,
      '0'.repeat(64),
    )
  } catch (error) {
    if (error instanceof UploadInputError) {
      uploadInputError.value = error.message
      return
    }
    throw error
  }
  const signal = uploadScope.next()
  uploadBusy.value = true
  try {
    const sha256 = await fileSha256(chosen, signal)
    if (signal.aborted) return
    const plan = planRenditionUpload(
      {
        accountId: address.value.accountId,
        archive: address.value.archive,
        entryId: address.value.entryId,
        rendition: uploadForm.rendition,
        subKey: uploadForm.subKey,
        contentType: uploadForm.contentType,
        module: uploadForm.module,
        expectedVersion: uploadForm.expectedVersion,
      },
      chosen,
      operationId,
      sha256,
    )
    const result = await sendRenditionUpload(plan, newRequest, () => {}, signal)
    if (signal.aborted) return
    if (result.kind === 'saved') {
      uploadSaved.value = result.response
    } else {
      uploadFailure.value = result.failure
    }
  } catch (error) {
    if (!signal.aborted) uploadFailure.value = rpcFailure(error)
  } finally {
    if (!signal.aborted) uploadBusy.value = false
  }
}

// The suggested save name: the rendition's name and sub-key ahead of the
// entry's filename; the save dialog can change it.
function filenameFor(r: RenditionRow): string {
  const base = info.value?.filename || queryText('entry')
  return `${r.name}${r.subKey ? `-${r.subKey}` : ''}-${base}`
}

const readSelected: ByteRead = async (range, sink, signal, onProgress, stopAfter) => {
  const r = selected.value
  if (r === null || address.value === null || pinned.value === null) throw new Error('No rendition is selected.')
  const result = await readRendition(
    clients.archives,
    { address: address.value, version: pinned.value, name: r.name, subKey: r.subKey, range },
    sink,
    signal,
    onProgress,
    stopAfter,
  )
  return { receivedBytes: result.receivedBytes, verification: result.verification, mediaType: result.header.rendition?.mediaType ?? '' }
}

async function loadVersions(first: boolean) {
  const target = address.value
  if (target === null) return
  const signal = versionsScope.next()
  versionsFailure.value = null
  try {
    const page = await clients.archives.listVersions(
      create(ListVersionsRequestSchema, { address: target, limit: VERSIONS_PAGE_SIZE, continuationToken: first ? '' : versionsNext.value }),
      { signal },
    )
    if (signal.aborted) return
    const rows = page.versions.map(versionRow)
    versions.value = first ? rows : [...versions.value, ...rows]
    versionsNext.value = page.nextContinuationToken
  } catch (error) {
    if (!signal.aborted) versionsFailure.value = rpcFailure(error)
  }
}

async function loadCompareVersion(target: EntryAddress, baseVersion: bigint) {
  const signal = compareScope.next()
  compareManifest.value = null
  compareFailure.value = null
  try {
    const answer = await clients.archives.getEntryManifest(
      create(GetEntryManifestRequestSchema, { address: target, version: baseVersion }),
      { signal },
    )
    if (signal.aborted) return
    if (answer.manifest === undefined) throw new Error('GetEntryManifest answered without a manifest.')
    compareManifest.value = answer.manifest
  } catch (error) {
    if (!signal.aborted) compareFailure.value = rpcFailure(error)
  }
}

async function load() {
  const signal = readScope.next()
  versionsScope.next()
  compareScope.next()
  deletingEntry.value = false
  routeError.value = null
  manifestFailure.value = null
  info.value = null
  manifest.value = null
  pinned.value = null
  selected.value = null
  versions.value = []
  versionsNext.value = ''
  compareManifest.value = null
  compareFailure.value = null
  address.value = null
  let target: EntryAddress
  try {
    target = entryAddress(queryText('account'), queryText('archive'), queryText('entry'))
    requested.value = parseVersion(queryText('version'))
    compareVersion.value = parseVersion(queryText('compare'))
  } catch (error) {
    compareVersion.value = null
    routeError.value = (error as Error).message
    return
  }
  address.value = target
  try {
    const answer = await clients.archives.getEntryManifest(
      create(GetEntryManifestRequestSchema, { address: target, version: requested.value ?? 0n }),
      { signal },
    )
    if (signal.aborted) return
    if (answer.manifest === undefined) throw new Error('GetEntryManifest answered without a manifest.')
    if (requested.value !== null && answer.manifest.version !== requested.value) {
      throw new Error(`GetEntryManifest answered for version ${answer.manifest.version}; version ${requested.value} was requested.`)
    }
    info.value = answer.info ?? null
    manifest.value = answer.manifest
    // Rendition reads pin the version this manifest describes.
    pinned.value = answer.manifest.version
  } catch (error) {
    if (!signal.aborted) manifestFailure.value = rpcFailure(error)
    return
  }
  if (compareVersion.value !== null) {
    void loadCompareVersion(target, compareVersion.value)
  }
  void loadVersions(true)
}

function openVersion(version: bigint | null) {
  const query = { ...route.query }
  if (version === null) delete query.version
  else query.version = version.toString()
  delete query.compare
  void router.push({ name: 'repository-archive-entry', query })
}

function openVersionCompare(baseVersion: bigint) {
  const query: Record<string, string> = {
    account: queryText('account'),
    archive: queryText('archive'),
    entry: queryText('entry'),
    version: (pinned.value ?? requested.value ?? 1n).toString(),
    compare: baseVersion.toString(),
  }
  void router.push({ name: 'repository-archive-entry', query })
}

function swapVersionCompare() {
  if (compareVersion.value === null || pinned.value === null) return
  const query: Record<string, string> = {
    account: queryText('account'),
    archive: queryText('archive'),
    entry: queryText('entry'),
    version: compareVersion.value.toString(),
    compare: pinned.value.toString(),
  }
  void router.push({ name: 'repository-archive-entry', query })
}

function closeVersionCompare() {
  const query = { ...route.query }
  delete query.compare
  void router.push({ name: 'repository-archive-entry', query })
}

watch(() => route.fullPath, () => void load(), { immediate: true })
</script>
